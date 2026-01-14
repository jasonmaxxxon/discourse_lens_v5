from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs
import hashlib
import json
import os
import re
import time

from playwright.sync_api import sync_playwright, ElementHandle

AUTH_FILE = "auth_threads.json"
JSON_URL_HINTS = ("graphql", "api/graphql")
XRAY_ENV = "DLENS_XRAY"
DOM_DEBUG_ENV = "DLENS_DOM_DEBUG"

UI_NOISE = {
    "reply",
    "replies",
    "like",
    "likes",
    "repost",
    "reposts",
    "share",
    "translate",
    "more",
    "view",
    "view replies",
    "view reply",
    "show replies",
    "see replies",
    "view more",
    "show more",
}

POSITIVE_EXPANDER = {
    "reply",
    "replies",
    "view replies",
    "view reply",
    "show replies",
    "see replies",
    "more replies",
    "replying",
    "回覆",
    "回复",
    "查看回复",
    "查看回覆",
    "顯示回覆",
    "顯示回复",
    "更多回覆",
    "更多回复",
}

NEGATIVE_EXPANDER = {
    "view more",
    "show more",
    "show more comments",
    "view more comments",
    "insights",
    "activity",
    "follow",
    "following",
    "share",
}

L1_LOAD_MORE_HINTS = {
    "view more comments",
    "show more comments",
    "more comments",
    "查看更多留言",
    "顯示更多留言",
    "显示更多留言",
    "查看更多評論",
    "顯示更多評論",
    "显示更多评论",
}


@dataclass
class CommentNode:
    source_layer: str
    source_comment_id: Optional[str]
    parent_source_comment_id: Optional[str]
    root_source_comment_id: Optional[str]
    synthetic_id: Optional[str]
    equivalence_key: Optional[str]
    author: Optional[str]
    text: str
    media_refs: List[Dict[str, str]]
    evidence_confidence: float
    raw: Optional[Dict[str, Any]]


def normalize_url(url: str) -> str:
    if "threads.com" in url:
        return url.replace("threads.com", "threads.net")
    return url


def _normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _tokenize(text: str) -> List[str]:
    return [t for t in re.split(r"\W+", (text or "").lower()) if t]


def _text_overlap_score(a: str, b: str) -> float:
    a_tokens = set(_tokenize(a))
    b_tokens = set(_tokenize(b))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / float(len(a_tokens))


def _clip_text(text: str, min_len: int = 80, max_len: int = 120) -> str:
    text = _normalize_text(text)
    if len(text) <= max_len:
        return text
    return text[: max(min_len, max_len)].strip()


def _extract_l1_text(node: Dict[str, Any]) -> str:
    caption = node.get("caption")
    if isinstance(caption, dict) and caption.get("text"):
        return str(caption.get("text"))
    if isinstance(caption, str):
        return caption
    info = node.get("text_post_app_info") or {}
    if isinstance(info, dict) and info.get("text"):
        return str(info.get("text"))
    return ""


def _extract_l1_timestamp(node: Dict[str, Any]) -> Optional[str]:
    for key in ("created_at", "taken_at", "timestamp", "created_time"):
        if node.get(key) is not None:
            return str(node.get(key))
    return None


def _extract_l1_reply_count(node: Dict[str, Any]) -> Optional[int]:
    info = node.get("text_post_app_info") or {}
    for key in ("direct_reply_count", "reply_count", "comment_count"):
        val = info.get(key) if isinstance(info, dict) else None
        if val is None:
            val = node.get(key)
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
    return None


def _extract_l1_like_count(node: Dict[str, Any]) -> int:
    info = node.get("text_post_app_info") or {}
    for key in ("like_count", "likes"):
        val = info.get(key) if isinstance(info, dict) else None
        if val is None:
            val = node.get(key)
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
    return 0


def _parse_compact_number(text: str) -> Optional[int]:
    if not text:
        return None
    clean = text.replace(",", "").upper()
    m = re.search(r"([\d\.]+)\s*([KM]?)", clean)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2)
    if suffix == "K":
        num *= 1000
    elif suffix == "M":
        num *= 1_000_000
    return int(num)


def _get_ui_comment_target(page) -> Optional[int]:
    try:
        text = page.locator("article").first.inner_text().lower()
    except Exception:
        return None
    m = re.search(r"([\d\.,]+)\s*(replies|comments)", text)
    if not m:
        return None
    return _parse_compact_number(m.group(1))


def _is_valid_comment(node: Dict[str, Any]) -> bool:
    if not isinstance(node, dict):
        return False
    if not node.get("pk") and not node.get("id"):
        return False
    if not node.get("user") and not node.get("owner"):
        return False
    has_caption = bool(node.get("caption"))
    has_text_info = bool(node.get("text_post_app_info"))
    has_media = bool(node.get("image_versions2") or node.get("carousel_media"))
    return has_caption or has_text_info or has_media


def _extract_all_nodes_recursive(data, collected_nodes, seen_ids):
    if isinstance(data, dict):
        if _is_valid_comment(data):
            pk = str(data.get("pk") or data.get("id"))
            if pk not in seen_ids:
                seen_ids.add(pk)
                collected_nodes.append(data)
        for key, value in data.items():
            if key in ["viewer", "extensions", "me", "user", "owner"]:
                continue
            _extract_all_nodes_recursive(value, collected_nodes, seen_ids)
    elif isinstance(data, list):
        for item in data:
            _extract_all_nodes_recursive(item, collected_nodes, seen_ids)


def _extract_pk(node: Dict[str, Any]) -> Optional[str]:
    if not node:
        return None
    return str(node.get("pk") or node.get("id"))


def _extract_author(node: Dict[str, Any]) -> Optional[str]:
    u = node.get("user") or node.get("owner") or {}
    return u.get("username") if isinstance(u, dict) else None


def _extract_images(node: Dict[str, Any]) -> List[Dict[str, str]]:
    if not node:
        return []
    cands = node.get("image_versions2", {}).get("candidates", [])
    return [{"src": c["url"]} for c in cands if c.get("url")]


def extract_metrics(page) -> Dict[str, int]:
    try:
        t = page.locator("article").first.inner_text().lower()
        replies = int(t.split("replies")[0].split()[-1].replace(",", "")) if "replies" in t else 0
        likes = int(t.split("likes")[0].split()[-1].replace(",", "")) if "likes" in t else 0
        return {"replies": replies, "likes": likes}
    except Exception:
        return {"replies": 0, "likes": 0}


def _decode_request(request) -> Dict[str, Any]:
    info = {"name": None, "doc_id": None, "vars": []}
    try:
        raw = request.post_data
        body = {}
        if request.post_data_json:
            body = request.post_data_json
        elif raw and "variables=" in raw:
            parsed = parse_qs(raw)
            if "variables" in parsed:
                body = {"variables": json.loads(parsed["variables"][0])}
            if "doc_id" in parsed:
                body["doc_id"] = parsed["doc_id"][0]
            if "fb_api_req_friendly_name" in parsed:
                body["friendly_name"] = parsed["fb_api_req_friendly_name"][0]
        info["name"] = body.get("friendly_name") or body.get("fb_api_req_friendly_name")
        info["doc_id"] = body.get("doc_id")
        info["vars"] = list(body.get("variables", {}).keys())
    except Exception:
        pass
    return info


def _collect_l1_from_packets(
    packets: List[Dict[str, Any]], target_sc: Optional[str]
) -> Tuple[Optional[Dict[str, Any]], Optional[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    all_nodes: List[Dict[str, Any]] = []
    seen_ids = set()
    for pkt in packets:
        _extract_all_nodes_recursive(pkt, all_nodes, seen_ids)
    root = None
    if target_sc:
        for n in all_nodes:
            if n.get("code") == target_sc or n.get("shortcode") == target_sc:
                root = n
                break
    root_pk = _extract_pk(root)
    l1_comments = [n for n in all_nodes if _extract_pk(n) != root_pk]
    return root, root_pk, l1_comments, all_nodes


def _should_drill_dom_json_fallback(
    root_node: Optional[Dict[str, Any]],
    l1_comments: List[Dict[str, Any]],
    metrics: Dict[str, int],
) -> Tuple[bool, str]:
    reply_signal = _extract_l1_reply_count(root_node or {}) or metrics.get("replies", 0)
    if not reply_signal:
        return False, "no_reply_signal"
    if len(l1_comments) >= reply_signal and reply_signal > 0:
        return False, "l1_coverage_ok"
    return True, "reply_signal_gap"


def _make_synthetic_id(parent_pk: str, author: str, text: str, media_urls: List[str]) -> str:
    base = "|".join(
        [
            parent_pk or "",
            (author or "").lower(),
            _normalize_text(text).lower(),
            ",".join(sorted([u for u in media_urls if u])),
        ]
    )
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"{parent_pk}::dom::{digest}"


def _make_equivalence_key(author: str, text: str, media_urls: List[str]) -> str:
    base = "|".join(
        [
            (author or "").lower(),
            _normalize_text(text).lower(),
            ",".join(sorted([u for u in media_urls if u])),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _build_comment_node_from_json(node: Dict[str, Any], root_pk: Optional[str]) -> CommentNode:
    media_refs = _extract_images(node)
    media_urls = [m.get("src") for m in media_refs if m.get("src")]
    return CommentNode(
        source_layer="json_l1",
        source_comment_id=_extract_pk(node),
        parent_source_comment_id=node.get("parent_comment_id") or node.get("parent_id"),
        root_source_comment_id=root_pk,
        synthetic_id=None,
        equivalence_key=_make_equivalence_key(_extract_author(node) or "", _extract_l1_text(node), media_urls),
        author=_extract_author(node),
        text=_normalize_text(_extract_l1_text(node)),
        media_refs=media_refs,
        evidence_confidence=0.95,
        raw=node,
    )


def _build_comment_node_from_dom(
    reply: Dict[str, Any], parent_pk: Optional[str], root_pk: Optional[str]
) -> CommentNode:
    media_urls = [m.get("src") for m in reply.get("media_refs", []) if m.get("src")]
    return CommentNode(
        source_layer="dom_l2",
        source_comment_id=None,
        parent_source_comment_id=parent_pk,
        root_source_comment_id=root_pk,
        synthetic_id=reply.get("synthetic_id"),
        equivalence_key=_make_equivalence_key(reply.get("author") or "", reply.get("text", ""), media_urls),
        author=reply.get("author"),
        text=_normalize_text(reply.get("text", "")),
        media_refs=reply.get("media_refs", []),
        evidence_confidence=reply.get("evidence_confidence", 0.6),
        raw=None,
    )


def _container_candidates_from_anchor(anchor: ElementHandle) -> List[ElementHandle]:
    candidates = []
    for selector in ("div[data-pressable-container='true']", "article", "div[role='article']"):
        try:
            handle = anchor.evaluate_handle(
                "(node, sel) => node.closest(sel)", selector
            )
            if handle:
                candidates.append(handle)
        except Exception:
            continue
    try:
        fallback = anchor.evaluate_handle("(node) => node.closest('div')")
        if fallback:
            candidates.append(fallback)
    except Exception:
        pass
    return candidates


def _filter_body_text_chunks(chunks: List[str]) -> str:
    cleaned = []
    for chunk in chunks:
        low = chunk.strip().lower()
        if not low:
            continue
        if low in UI_NOISE:
            continue
        if re.match(r"^\d+\s*[smhdw]$", low):
            continue
        cleaned.append(chunk.strip())
    return _normalize_text(" ".join(cleaned))


def extract_dom_comment_body_text(container: ElementHandle) -> str:
    try:
        chunks = container.evaluate(
            """
            (node) => {
              const texts = [];
              const walker = document.createTreeWalker(node, NodeFilter.SHOW_TEXT);
              const isBlocked = (el) => {
                if (!el || el.nodeType !== 1) return false;
                if (el.closest('button,[role="button"],a[role="button"],time')) return true;
                if (el.closest('[aria-label]')) return true;
                if (el.closest('[aria-hidden="true"]')) return true;
                return false;
              };
              while (walker.nextNode()) {
                const textNode = walker.currentNode;
                const parent = textNode.parentElement;
                if (!parent || isBlocked(parent)) continue;
                const txt = textNode.textContent || '';
                if (txt.trim()) texts.push(txt.trim());
              }
              return texts;
            }
            """
        )
    except Exception:
        return ""
    return _filter_body_text_chunks([str(c) for c in chunks])


def extract_dom_comment_body_text_from_html(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    blocked = soup.select("button, [role='button'], a[role='button'], time, [aria-label], [aria-hidden='true']")
    for el in blocked:
        el.extract()
    texts = [t.strip() for t in soup.stripped_strings]
    return _filter_body_text_chunks(texts)


def _verify_container(container: ElementHandle, username: str, fingerprint_text: str) -> bool:
    try:
        author_link = container.query_selector(f"a[href*='/@{username}']")
        if not author_link:
            return False
        body_text = extract_dom_comment_body_text(container)
        if not fingerprint_text:
            return True
        overlap = _text_overlap_score(fingerprint_text, body_text)
        return overlap >= 0.3 or fingerprint_text[:40].lower() in body_text.lower()
    except Exception:
        return False


def locate_l1_container(page, l1_node: Dict[str, Any]) -> Tuple[Optional[ElementHandle], str]:
    username = _extract_author(l1_node) or ""
    if not username:
        return None, "missing_author"
    fingerprint_text = _clip_text(_extract_l1_text(l1_node))
    anchors = page.locator(f"a[href*='/@{username}']").all()
    for anchor in anchors:
        try:
            anchor_handle = anchor.element_handle()
        except Exception:
            continue
        if not anchor_handle:
            continue
        for candidate in _container_candidates_from_anchor(anchor_handle):
            if _verify_container(candidate, username, fingerprint_text):
                if os.getenv(DOM_DEBUG_ENV, "0") == "1":
                    _debug_container_snapshot(candidate)
                return candidate, "ok"
    return None, "no_container_match"


def _looks_like_reply_text(line: str) -> bool:
    low = line.strip().lower()
    if not low:
        return False
    if low in UI_NOISE:
        return False
    if re.match(r"^\d+\s*[smhdw]$", low):
        return False
    return True


def _extract_reply_text_from_block(block: ElementHandle) -> str:
    return extract_dom_comment_body_text(block)


def _extract_media_from_block(block: ElementHandle) -> List[Dict[str, str]]:
    media = []
    try:
        for img in block.query_selector_all("img"):
            src = img.get_attribute("src")
            if src:
                media.append({"type": "image", "src": src})
        for vid in block.query_selector_all("video"):
            src = vid.get_attribute("src")
            if not src:
                source = vid.query_selector("source")
                src = source.get_attribute("src") if source else None
            if src:
                media.append({"type": "video", "src": src})
    except Exception:
        pass
    return media


def _is_reply_block(block: ElementHandle) -> bool:
    try:
        author_link = block.query_selector("a[href^='/@']")
        if not author_link:
            return False
        text = _extract_reply_text_from_block(block)
        return bool(text)
    except Exception:
        return False


def _find_body_region(block: ElementHandle) -> Optional[ElementHandle]:
    candidates = []
    try:
        candidates = block.query_selector_all("*[dir='auto'], *[dir='ltr'], *[dir='rtl'], p, div[role='presentation']")
    except Exception:
        return None
    best = None
    best_len = 0
    for cand in candidates:
        try:
            if cand.query_selector("button,[role='button'],a[role='button'],time"):
                continue
        except Exception:
            continue
        text = extract_dom_comment_body_text(cand)
        if len(text) > best_len:
            best = cand
            best_len = len(text)
    return best


def _get_dom_path(handle: ElementHandle) -> str:
    try:
        return handle.evaluate(
            """
            (node) => {
              const parts = [];
              let el = node;
              while (el && el.nodeType === 1 && parts.length < 6) {
                let part = el.tagName.toLowerCase();
                if (el.id) {
                  part += `#${el.id}`;
                } else if (el.classList && el.classList.length) {
                  part += '.' + Array.from(el.classList).slice(0, 2).join('.');
                }
                const parent = el.parentElement;
                if (parent) {
                  const siblings = Array.from(parent.children).filter(c => c.tagName === el.tagName);
                  if (siblings.length > 1) {
                    const index = siblings.indexOf(el) + 1;
                    part += `:nth-of-type(${index})`;
                  }
                }
                parts.unshift(part);
                el = el.parentElement;
              }
              return parts.join(' > ');
            }
            """
        )
    except Exception:
        return ""


def _debug_container_snapshot(container: ElementHandle) -> None:
    try:
        outer_html = container.evaluate("(node) => node.outerHTML") or ""
    except Exception:
        outer_html = ""
    try:
        attrs = container.evaluate(
            """
            (node) => {
              const out = { data: {}, role: null };
              if (node.getAttribute) {
                out.role = node.getAttribute('role');
                for (const attr of node.attributes || []) {
                  if (attr.name.startsWith('data-')) {
                    out.data[attr.name] = attr.value;
                  }
                }
              }
              return out;
            }
            """
        )
    except Exception:
        attrs = {"data": {}, "role": None}
    preview = outer_html[:800].replace("\n", " ")
    print(f"[DOM DEBUG] container role={attrs.get('role')} data={attrs.get('data')} outerHTML[:800]={preview}")


def _is_l1_load_more_text(text: str) -> bool:
    low = text.lower()
    if "repl" in low:
        return False
    return any(hint in low for hint in L1_LOAD_MORE_HINTS)


def find_l1_load_more_button(page) -> Optional[ElementHandle]:
    selectors = [
        "button",
        "div[role='button']",
        "span",
        "a[role='button']",
    ]
    for sel in selectors:
        try:
            candidates = page.locator(sel).all()
        except Exception:
            continue
        for cand in candidates[:120]:
            try:
                text = (cand.text_content() or "").strip()
            except Exception:
                continue
            if not text or not _is_l1_load_more_text(text):
                continue
            try:
                return cand.element_handle()
            except Exception:
                continue
    return None


def _debug_l1_load_more_click(
    button: ElementHandle, before_count: int, after_count: int
) -> None:
    try:
        text = (button.text_content() or "").strip()
    except Exception:
        text = ""
    try:
        container = button.evaluate_handle(
            "(node) => node.closest(\"div[data-pressable-container='true'], article, div[role='article'], div\")"
        )
    except Exception:
        container = None
    outer_html = ""
    if container:
        try:
            outer_html = container.evaluate("(node) => node.outerHTML") or ""
        except Exception:
            outer_html = ""
    preview = outer_html[:300].replace("\n", " ")
    print(
        f"[L1 LOAD MORE] text={text!r} l1_delta={after_count - before_count} container_outerHTML[:300]={preview}"
    )


def extract_dom_replies(container: ElementHandle, parent_pk: Optional[str]) -> List[Dict[str, Any]]:
    replies = []
    candidates = []
    for selector in ("div[data-pressable-container='true']", "article", "div[role='article']"):
        try:
            candidates.extend(container.query_selector_all(selector))
        except Exception:
            continue
    for block in candidates:
        body_region = _find_body_region(block)
        if not body_region:
            continue
        if not _is_reply_block(block):
            continue
        author = None
        try:
            author_link = block.query_selector("a[href^='/@']")
            if author_link:
                href = author_link.get_attribute("href") or ""
                author = href.split("/@")[-1].split("/")[0] or author_link.text_content()
        except Exception:
            author = None
        text = extract_dom_comment_body_text(body_region)
        media = _extract_media_from_block(block)
        media_urls = [m.get("src") for m in media if m.get("src")]
        synthetic_id = _make_synthetic_id(parent_pk or "unknown", author or "", text, media_urls)
        dom_path = _get_dom_path(block)
        replies.append(
            {
                "author": author,
                "text": text,
                "media_refs": media,
                "synthetic_id": synthetic_id,
                "evidence_confidence": 0.7,
                "dom_path": dom_path,
            }
        )
    return replies


def _count_reply_blocks(container: ElementHandle) -> int:
    try:
        return len([b for b in container.query_selector_all("div, article") if _is_reply_block(b)])
    except Exception:
        return 0


def _find_footer_zone(container: ElementHandle) -> Optional[ElementHandle]:
    tokens = ("like", "reply", "repost", "share", "讚", "喜欢", "喜歡", "回覆", "回复", "轉發", "转发", "分享")
    try:
        candidates = container.query_selector_all("div, span, footer, section")
    except Exception:
        return None
    best = None
    best_score = 0
    for cand in candidates[:120]:
        try:
            text = (cand.inner_text() or "").lower()
        except Exception:
            continue
        hits = sum(1 for t in tokens if t in text)
        if hits >= 2:
            score = hits * 10 - len(text)
            if score > best_score:
                best = cand
                best_score = score
    return best


def _debug_expander(container: ElementHandle, expander: ElementHandle) -> None:
    if os.getenv(DOM_DEBUG_ENV, "0") != "1":
        return
    try:
        text = (expander.text_content() or "").strip()
    except Exception:
        text = ""
    try:
        tag = expander.evaluate("(node) => node.tagName.toLowerCase()")
    except Exception:
        tag = "unknown"
    try:
        role = expander.get_attribute("role")
    except Exception:
        role = None
    try:
        expander_box = expander.bounding_box() or {}
        container_box = container.bounding_box() or {}
        rel_x = (expander_box.get("x", 0) - container_box.get("x", 0)) if expander_box else None
        rel_y = (expander_box.get("y", 0) - container_box.get("y", 0)) if expander_box else None
    except Exception:
        expander_box = {}
        rel_x = rel_y = None
    print(
        "[DOM DEBUG] expander text=%r tag=%s role=%s box=%s rel=(%s,%s)"
        % (text, tag, role, expander_box, rel_x, rel_y)
    )


def find_replies_expander(container: ElementHandle) -> Optional[ElementHandle]:
    zone = _find_footer_zone(container)
    search_root = zone or container
    try:
        candidates = search_root.query_selector_all(
            "button, div[role='button'], span, a[role='button']"
        )
    except Exception:
        return None
    for cand in candidates[:40]:
        try:
            text = (cand.text_content() or "").strip()
        except Exception:
            continue
        if not text:
            continue
        low = text.lower()
        if any(neg in low for neg in NEGATIVE_EXPANDER):
            continue
        if any(pos in low for pos in POSITIVE_EXPANDER):
            _debug_expander(container, cand)
            return cand
    return None


def wait_dom_settled(container: ElementHandle, timeout: int = 2500, idle_ms: int = 500) -> bool:
    try:
        return container.evaluate(
            """
            (node, args) => {
              const timeout = args.timeout || 2500;
              const idleMs = args.idleMs || 500;
              const startTs = Date.now();
              return new Promise((resolve) => {
                let lastChange = Date.now();
                const obs = new MutationObserver(() => {
                  lastChange = Date.now();
                });
                obs.observe(node, { childList: true, subtree: true, characterData: true });
                const tick = () => {
                  const now = Date.now();
                  if (now - lastChange >= idleMs) {
                    obs.disconnect();
                    resolve(true);
                    return;
                  }
                  if (now - startTs >= timeout) {
                    obs.disconnect();
                    resolve(false);
                    return;
                  }
                  setTimeout(tick, Math.min(100, idleMs));
                };
                setTimeout(tick, Math.min(100, idleMs));
              });
            }
            """,
            {"timeout": timeout, "idleMs": idle_ms},
        )
    except Exception:
        return False


def _drill_replies_for_parent(
    page,
    parent_node: Dict[str, Any],
    xray_hook,
    max_replies: int,
    packet_index: int,
) -> Tuple[List[Dict[str, Any]], str]:
    container, reason = locate_l1_container(page, parent_node)
    if not container:
        return [], f"locate_failed:{reason}"
    expander = find_replies_expander(container)
    if not expander:
        return [], "no_expander"
    before = _count_reply_blocks(container)
    try:
        xray_hook.start_window(packet_index=packet_index)
        expander.click(force=True)
        wait_dom_settled(container, timeout=2500, idle_ms=500)
    except Exception:
        return [], "click_failed"
    after = _count_reply_blocks(container)
    if after <= before:
        xray_hook.flush()
        return [], "click_no_growth"
    xray_hook.flush()
    replies = extract_dom_replies(container, _extract_pk(parent_node))
    return replies[:max_replies], "ok"


class XRayHook:
    def __init__(self) -> None:
        self.enabled = os.getenv(XRAY_ENV, "0") == "1"
        self.window_until = 0.0
        self.buffer: List[Dict[str, Any]] = []
        self.start_index = 0

    def start_window(self, seconds: float = 3.0, packet_index: int = 0) -> None:
        if not self.enabled:
            return
        self.window_until = time.time() + seconds
        self.start_index = packet_index

    def capture(self, response, packet_index: int) -> None:
        if not self.enabled or time.time() > self.window_until:
            return
        if packet_index < self.start_index:
            return
        try:
            req_info = _decode_request(response.request)
            data = response.json()
            top_keys = list(data.keys()) if isinstance(data, dict) else []
            self.buffer.append(
                {
                    "name": req_info.get("name"),
                    "doc_id": req_info.get("doc_id"),
                    "vars": req_info.get("vars", []),
                    "top_keys": top_keys,
                    "url": response.url,
                }
            )
        except Exception:
            return

    def flush(self) -> None:
        if not self.enabled:
            return
        if not self.buffer:
            print("[XRAY] no packets captured in drill window")
            return
        scored = []
        for item in self.buffer:
            score = 0
            for token in ("repl", "reply", "thread", "comment", "parent"):
                if (item.get("name") or "").lower().find(token) >= 0:
                    score += 2
                if any(token in k.lower() for k in item.get("top_keys", [])):
                    score += 1
                if any(token in v.lower() for v in item.get("vars", [])):
                    score += 1
            scored.append((score, item))
        scored.sort(key=lambda x: x[0], reverse=True)
        print("[XRAY] drill window packets:")
        for score, item in scored[:6]:
            print(
                f"  score={score} name={item.get('name')} doc_id={item.get('doc_id')} "
                f"vars={item.get('vars')} top_keys={item.get('top_keys')}"
            )
        self.buffer.clear()


def _scan_visual_expander_containers(page) -> List[ElementHandle]:
    containers = []
    try:
        candidates = page.locator(
            "button, div[role='button'], span, a[role='button']"
        ).all()
    except Exception:
        return containers
    for cand in candidates[:120]:
        try:
            text = (cand.text_content() or "").strip()
        except Exception:
            continue
        if not text:
            continue
        low = text.lower()
        if any(neg in low for neg in NEGATIVE_EXPANDER):
            continue
        if any(pos in low for pos in POSITIVE_EXPANDER):
            try:
                handle = cand.element_handle()
                if not handle:
                    continue
                container = handle.evaluate_handle(
                    "(node) => node.closest(\"div[data-pressable-container='true'], article, div[role='article']\")"
                )
                if container:
                    containers.append(container)
            except Exception:
                continue
    return containers


def _map_container_to_l1(
    container: ElementHandle, l1_nodes: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    try:
        author_link = container.query_selector("a[href^='/@']")
        if not author_link:
            return None
        href = author_link.get_attribute("href") or ""
        username = href.split("/@")[-1].split("/")[0]
    except Exception:
        return None
    body_text = extract_dom_comment_body_text(container)
    if not username or not body_text:
        return None
    best = None
    best_score = 0.0
    for node in l1_nodes:
        if (_extract_author(node) or "").lower() != username.lower():
            continue
        fingerprint = _clip_text(_extract_l1_text(node))
        score = _text_overlap_score(fingerprint, body_text)
        if score > best_score:
            best = node
            best_score = score
    if best_score >= 0.3:
        return best
    return None


def fetch_thread(url: str, *, max_comments: Optional[int] = None, high_engagement: bool = True):
    if not os.path.exists(AUTH_FILE):
        return {"mode": "error", "debug": "no_auth"}

    packets: List[Dict[str, Any]] = []
    xray = XRayHook()

    def on_response(response):
        try:
            if response.request.resource_type not in ("xhr", "fetch"):
                return
            if any(hint in response.url for hint in JSON_URL_HINTS):
                if "application/json" in response.headers.get("content-type", ""):
                    packets.append(response.json())
                    xray.capture(response, len(packets) - 1)
        except Exception:
            pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=AUTH_FILE)
        page = context.new_page()
        page.on("response", on_response)

        url = normalize_url(url)
        print(f"[Hybrid] loading {url}")
        nav_start = time.time()
        page.goto(url, timeout=60000, wait_until="load")
        nav_ms = int((time.time() - nav_start) * 1000)
        print(f"[Timing] navigation_ms={nav_ms}")

        time.sleep(3.0)
        target_sc = re.search(r"/post/([A-Za-z0-9_-]+)", url)
        target_sc = target_sc.group(1) if target_sc else None

        ui_target = _get_ui_comment_target(page)
        if ui_target:
            print(f"[L1] ui_target_comments={ui_target}")

        l1_loop_start = time.time()
        l1_rounds = 0
        no_growth = 0
        max_rounds = 12
        root = None
        root_pk = None
        l1_comments: List[Dict[str, Any]] = []
        all_nodes: List[Dict[str, Any]] = []
        while l1_rounds < max_rounds:
            round_start = time.time()
            root, root_pk, l1_comments, all_nodes = _collect_l1_from_packets(packets, target_sc)
            l1_count = len(l1_comments)
            if ui_target and l1_count >= ui_target:
                print(f"[L1] target_reached l1_count={l1_count}")
                break
            if no_growth >= 4:
                print("[L1] stalled: no_growth>=4")
                break
            button = find_l1_load_more_button(page)
            if not button:
                print("[L1] no_load_more_button")
                break
            page.mouse.wheel(0, 2000)
            before_count = l1_count
            try:
                button.click(force=True)
            except Exception:
                print("[L1] load_more_click_failed")
                break
            try:
                body = page.query_selector("body")
                if body:
                    wait_dom_settled(body, timeout=2500, idle_ms=500)
            except Exception:
                pass
            root, root_pk, l1_comments, all_nodes = _collect_l1_from_packets(packets, target_sc)
            after_count = len(l1_comments)
            _debug_l1_load_more_click(button, before_count, after_count)
            if after_count <= before_count:
                no_growth += 1
            else:
                no_growth = 0
            l1_rounds += 1
            round_ms = int((time.time() - round_start) * 1000)
            print(f"[Timing] l1_round={l1_rounds} ms={round_ms} l1_count={after_count}")

        l1_total_ms = int((time.time() - l1_loop_start) * 1000)
        print(f"[Timing] l1_loop_ms={l1_total_ms}")

        metrics = extract_metrics(page)
        visual_containers = _scan_visual_expander_containers(page)
        candidate_nodes = []
        for container in visual_containers:
            mapped = _map_container_to_l1(container, [root] + l1_comments if root else l1_comments)
            if mapped:
                candidate_nodes.append(mapped)
        unique_candidates = []
        seen = set()
        for node in candidate_nodes:
            pk = _extract_pk(node) or ""
            if pk in seen:
                continue
            seen.add(pk)
            unique_candidates.append(node)
        unique_candidates.sort(key=_extract_l1_like_count, reverse=True)
        should_drill = bool(unique_candidates)
        drill_reason = "visual_candidates" if should_drill else "no_visual_candidates"

        dom_replies: List[Dict[str, Any]] = []
        drill_logs: List[str] = []
        if not should_drill:
            should_drill, drill_reason = _should_drill_dom_json_fallback(root, l1_comments, metrics)
            if should_drill and root:
                unique_candidates = [root]

        if should_drill and unique_candidates:
            max_parents = 3
            max_l2_per_parent = 12
            start_time = time.time()
            for parent in unique_candidates[:max_parents]:
                if time.time() - start_time > 25:
                    drill_logs.append("drill_time_budget_exceeded")
                    break
                replies, status = _drill_replies_for_parent(
                    page, parent, xray, max_l2_per_parent, len(packets)
                )
                drill_logs.append(f"{_extract_pk(parent) or 'unknown'}:{status}")
                dom_replies.extend(replies)

        comment_nodes = [_build_comment_node_from_json(n, root_pk) for n in l1_comments]
        comment_nodes.extend(
            [_build_comment_node_from_dom(r, _extract_pk(root), root_pk) for r in dom_replies]
        )

        browser.close()
        total_ms = int((time.time() - nav_start) * 1000)
        print(f"[Timing] total_ms={total_ms}")

        return {
            "mode": "hybrid_commander",
            "url": url,
            "post_payload": root,
            "comments_payload": [asdict(n) for n in comment_nodes],
            "metrics": metrics,
            "images": _extract_images(root),
            "debug": {
                "packets": len(packets),
                "nodes_found": len(all_nodes),
                "comments_l1": len(l1_comments),
                "comments_dom_l2": len(dom_replies),
                "drill_enabled": should_drill,
                "drill_reason": drill_reason,
                "drill_logs": drill_logs,
            },
        }


if __name__ == "__main__":
    import sys

    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.threads.net/@nitraaa_/post/DTZZIw2geFT"
    print(fetch_thread(url))
