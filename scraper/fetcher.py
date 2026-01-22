from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

from scraper.parser import extract_data_from_html
from scraper.scroll_utils import scroll_until_stable

AUTH_FILE = "auth_threads.json"
TIME_TOKEN_RE = re.compile(r"^\d+\s?[smhdw]$", re.IGNORECASE)
UI_LINE_TOKENS = {
    "follow",
    "following",
    "more",
    "translate",
    "like",
    "reply",
    "replies",
    "repost",
    "share",
    "view more",
    "view replies",
    "view more replies",
    "top",
    "view activity",
    "author",
    "作者",
    "顯示",
    "更多",
    "查看更多",
    "查看更多回覆",
    "查看回覆",
}


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _write_text(path: str, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload or "")


def normalize_url(url: str) -> str:
    if "threads.com" in url:
        new_url = url.replace("threads.com", "threads.net")
        print(f"🔁 偵測到 threads.com，已自動改成：{new_url}")
        return new_url
    return url


def capture_archive_snapshot(page, url: str) -> Dict[str, Any]:
    html = page.content()
    dom_json = page.evaluate(
        """
    () => {
      const pick = (el, depth=0, maxDepth=6, maxChildren=40) => {
        if (!el || depth > maxDepth) return null;
        const children = [];
        const nodes = el.children ? Array.from(el.children).slice(0, maxChildren) : [];
        for (const c of nodes) {
          const child = pick(c, depth+1, maxDepth, maxChildren);
          if (child) children.push(child);
        }
        const cls = el.classList ? Array.from(el.classList).slice(0, 12) : [];
        const txt = (el.innerText || "").trim();
        return {
          tag: el.tagName ? el.tagName.toLowerCase() : null,
          id: el.id || null,
          class: cls,
          text_len: txt.length,
          text_sample: txt.slice(0, 160),
          children
        };
      };

      const hasArticle = !!document.querySelector("article");
      const commentCandidates = document.querySelectorAll("article, div, section");
      const commentCountSeen = commentCandidates ? commentCandidates.length : 0;

      return {
        url: location.href,
        title: document.title,
        ready_state: document.readyState,
        ua: navigator.userAgent,
        viewport: { w: window.innerWidth, h: window.innerHeight },
        selectors_probe: {
          has_article: hasArticle,
          comment_count_seen: commentCountSeen
        },
        root: pick(document.body)
      };
    }
    """
    )

    return {"archive_html": html, "archive_dom_json": dom_json}


def deep_scroll_comments(page, max_loops: int = 15, target_comment_blocks: int = 80):
    expand_texts = ["View more replies", "View more", "Show replies"]

    def _on_loop(_loop_idx: int) -> bool:
        for text in expand_texts:
            try:
                for btn in page.get_by_text(text, exact=False).all():
                    btn.click(timeout=2000)
                    page.wait_for_timeout(500)
            except Exception:
                pass

        blocks = page.query_selector_all('div[data-pressable-container="true"]')
        return len(blocks) - 1 >= target_comment_blocks

    scroll_until_stable(page, max_loops=max_loops, wait_ms=1500, wheel_px=3000, stability_threshold=3, on_loop=_on_loop)


def extract_metrics(page) -> dict:
    def parse_human_number(s: str) -> int:
        if not s:
            return 0
        s = s.strip()
        try:
            lower = s.lower().replace(",", "")
            if lower.endswith("k"):
                return int(float(lower[:-1]) * 1000)
            if lower.endswith("m"):
                return int(float(lower[:-1]) * 1_000_000)
            return int(float(lower))
        except Exception:
            return 0

    def extract_from_label(label: str) -> int:
        if not label:
            return 0
        parts = label.split()
        for part in parts:
            n = parse_human_number(part)
            if n:
                return n
        return 0

    metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0}
    article = page.query_selector("article")
    if not article:
        print("⚠️ extract_metrics: no article found")
        return metrics

    aria_map = {
        "likes": [" like ", " likes "],
        "replies": [" reply ", " replies "],
        "reposts": [" repost ", " reposts "],
        "views": [" view ", " views "],
    }
    for key, phrases in aria_map.items():
        try:
            locs = article.query_selector_all("[aria-label]")
        except Exception:
            locs = []
        for loc in locs:
            try:
                label_raw = loc.get_attribute("aria-label") or ""
                label = label_raw.lower()
            except Exception:
                continue
            for phrase in phrases:
                hay = f" {label} "
                if phrase in hay:
                    val = extract_from_label(label)
                    if val:
                        metrics[key] = val
                        break
            if metrics[key]:
                break

    def extract_from_buttons():
        try:
            btns = article.query_selector_all("button, span")
        except Exception:
            btns = []
        for btn in btns:
            try:
                text_before = (btn.inner_text() or "").strip()
            except Exception:
                text_before = ""
            try:
                after_node = btn.query_selector("span")
                text_after = (after_node.inner_text() or "").strip() if after_node else ""
            except Exception:
                text_after = ""

            combined_lower = (text_before or text_after or "").lower()
            if not any(str.isdigit(c) for c in combined_lower):
                continue

            aria = ""
            try:
                aria = (btn.get_attribute("aria-label") or "").lower()
            except Exception:
                aria = ""

            def try_set_metric(key: str):
                if metrics[key]:
                    return
                candidate = text_before or text_after
                val = extract_from_label(candidate.lower())
                if val:
                    metrics[key] = val

            for key, phrases in aria_map.items():
                matched = False
                for phrase in phrases:
                    hay = f" {aria} "
                    if phrase in hay or phrase in f" {combined_lower} ":
                        try_set_metric(key)
                        matched = True
                        break
                if matched:
                    break

    extract_from_buttons()

    try:
        text_full = (article.inner_text() or "").lower()
    except Exception:
        text_full = ""

    for key, token in aria_map.items():
        if metrics[key]:
            continue
        if not any(str.isdigit(c) for c in text_full):
            continue
        for phrase in token if isinstance(token, list) else [token]:
            hay = f" {text_full} "
            if phrase in hay:
                snippet = text_full.split(phrase, 1)[0].split()[-1:]
                val = extract_from_label(" ".join(snippet))
                if val:
                    metrics[key] = val
                    break

    if not any(metrics.values()):
        try:
            interaction_text = article.inner_text()
            print(f"⚠️ extract_metrics: unable to find metrics. Interaction text sample: {interaction_text[:200]}")
        except Exception:
            print("⚠️ extract_metrics: unable to find metrics and cannot read interaction text.")

    return metrics


def _extract_post_id(url: str) -> str:
    if not url:
        return ""
    match = re.search(r"/post/([^/?]+)", url)
    if match:
        return match.group(1)
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _normalize_permalink(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return f"https://www.threads.net{href}"


def _normalize_handle(handle: str) -> str:
    if not handle:
        return ""
    handle = handle.strip().lower()
    if handle.startswith("@"):
        handle = handle[1:]
    return re.sub(r"[^a-z0-9._-]+", "", handle)


def _strip_ui_headers(text: str) -> str:
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned: List[str] = []
    for line in lines:
        lower = line.lower()
        if lower in UI_LINE_TOKENS:
            continue
        if line in ("·", "•"):
            continue
        if re.match(r"^\d+(?:\.\d+)?[km]?$", lower):
            continue
        if TIME_TOKEN_RE.match(lower):
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _is_profile_image(url: str) -> bool:
    if not url:
        return True
    lower = url.lower()
    if (
        "s150x150" in lower
        or "profile" in lower
        or "avatar" in lower
        or "profile_pic" in lower
        or "thumbnail" in lower
    ):
        return True
    return False


def _dedupe_urls(urls: List[str]) -> List[str]:
    seen = set()
    out = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _clean_image_urls(urls: List[str]) -> List[str]:
    if not urls:
        return []
    filtered = [u for u in urls if not _is_profile_image(u)]
    return _dedupe_urls(filtered)


def _norm_soft(text: str) -> str:
    if not text:
        return ""
    cleaned = _strip_ui_headers(text)
    if not cleaned:
        return ""
    return " ".join(cleaned.split()).strip().lower()


def _norm_hard(text: str) -> str:
    soft = _norm_soft(text)
    if not soft:
        return ""
    soft = re.sub(r"[!！]+", "!", soft)
    soft = re.sub(r"[?？]+", "?", soft)
    soft = soft.replace("。。。", "。").replace("．．．", "。")
    soft = re.sub(r"[\U0001F300-\U0001FAFF]", "", soft)
    return " ".join(soft.split()).strip()


def _text_head(text: str, *, max_len: int = 120) -> str:
    cleaned = _norm_hard(text)
    if not cleaned:
        return ""
    return cleaned[:max_len]


def _fingerprint_comment(user: str, text: str) -> str:
    handle = _normalize_handle(user)
    head = _text_head(text)
    if not handle and not head:
        return ""
    base = f"{handle}|{head}"
    short_hash = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
    return f"{base}|{short_hash}"


def _unique_id(base: str, seen: set[str]) -> str:
    if not base:
        base = hashlib.md5(str(time.time()).encode("utf-8")).hexdigest()[:12]
    if base not in seen:
        seen.add(base)
        return base
    ordinal = 1
    while True:
        candidate = f"{base}_{ordinal}"
        if candidate not in seen:
            seen.add(candidate)
            return candidate
        ordinal += 1


def _build_parsed_tree(comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_id: Dict[str, Dict[str, Any]] = {}
    parent_map: Dict[str, List[str]] = {}
    for c in comments:
        cid = c.get("comment_id")
        if not cid:
            continue
        by_id[cid] = c
        parent = c.get("parent_id")
        if parent and parent != cid:
            parent_map.setdefault(parent, []).append(cid)

    roots: List[str] = []
    for cid, c in by_id.items():
        parent = c.get("parent_id")
        if not parent or parent not in by_id or parent == cid:
            roots.append(cid)

    def build_node(cid: str, stack: set[str]) -> Optional[Dict[str, Any]]:
        if cid in stack:
            return None
        c = by_id[cid]
        node = {
            "comment_id": cid,
            "parent_id": c.get("parent_id"),
            "author": c.get("author") or "",
            "text": c.get("text") or "",
            "metrics": c.get("metrics") or {},
            "replies": [],
        }
        stack.add(cid)
        for child_id in parent_map.get(cid, []):
            child = build_node(child_id, stack)
            if child is not None:
                node["replies"].append(child)
        stack.remove(cid)
        return node

    parsed = [build_node(cid, set()) for cid in roots]
    parsed = [p for p in parsed if p is not None]
    return {"total": len(by_id), "comments": parsed}


def _project_time_fields(parsed_tree: Dict[str, Any], meta_by_id: Dict[str, Dict[str, Any]]) -> None:
    def _walk(node: Dict[str, Any]) -> None:
        meta = meta_by_id.get(node.get("comment_id") or "", {})
        node["time_token"] = meta.get("time_token")
        node["approx_created_at_utc"] = meta.get("approx_created_at_utc")
        node["time_precision"] = meta.get("time_precision")
        for child in node.get("replies", []):
            _walk(child)

    for root in parsed_tree.get("comments") or []:
        _walk(root)


def _extract_body_text(raw_text: str) -> str:
    if not raw_text:
        return ""
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    body_lines: List[str] = []
    for idx, line in enumerate(lines):
        lower = line.lower()
        if lower in UI_LINE_TOKENS:
            continue
        if line in ("·", "•"):
            continue
        if re.match(r"^\d+(?:\.\d+)?[km]?$", lower):
            continue
        if TIME_TOKEN_RE.match(lower):
            continue
        if idx < 2 and " " not in line and not TIME_TOKEN_RE.match(lower):
            continue
        body_lines.append(line)
    return "\n".join(body_lines).strip()


def _ui_noise_score(text: str) -> int:
    if not text:
        return 0
    score = 0
    for line in text.splitlines():
        lower = line.strip().lower()
        if lower in UI_LINE_TOKENS or line.strip() in ("·", "•"):
            score += 1
    return score


def _parse_time_token(time_token: Optional[str]) -> Optional[timedelta]:
    if not time_token:
        return None
    match = re.match(r"^(\d+)\s*([smhdw])$", time_token.strip().lower())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "s":
        return timedelta(seconds=value)
    if unit == "m":
        return timedelta(minutes=value)
    if unit == "h":
        return timedelta(hours=value)
    if unit == "d":
        return timedelta(days=value)
    if unit == "w":
        return timedelta(weeks=value)
    return None


def _approx_created_at(crawled_at: datetime, time_token: Optional[str]) -> Optional[str]:
    delta = _parse_time_token(time_token)
    if not delta:
        return None
    return (crawled_at - delta).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_time_token(time_token: Optional[str]) -> Optional[str]:
    if not time_token:
        return None
    match = re.match(r"^(\d+)\s*([smhdw])$", time_token.strip().lower())
    if not match:
        return None
    return f"{match.group(1)}{match.group(2)}"


def _harvest_cards_structured(page) -> List[Dict[str, Any]]:
    return page.evaluate(
        r"""
        () => {
          const parseCount = (label) => {
            if (label === null || label === undefined) return null;
            const cleaned = label.replace(/,/g, "");
            const match = cleaned.match(/(\d+(?:\.\d+)?)/);
            if (!match) return null;
            let value = parseFloat(match[1]);
            const suffix = cleaned.slice(match.index + match[1].length);
            if (/[萬万]/.test(suffix)) value *= 10000;
            else if (/千/.test(suffix)) value *= 1000;
            else if (/[kK]/.test(suffix)) value *= 1000;
            else if (/[mM]/.test(suffix)) value *= 1000000;
            return Math.round(value);
          };

          const resolveLabel = (el) => {
            if (!el) return { aria: "", labelledby: "", text: "" };
            const aria = (el.getAttribute("aria-label") || "").trim();
            let labelledbyText = "";
            const labelled = el.getAttribute("aria-labelledby") || "";
            if (labelled) {
              const ids = labelled.split(/\s+/);
              const parts = ids.map((id) => {
                const node = document.getElementById(id);
                return node ? (node.textContent || "").trim() : "";
              });
              labelledbyText = parts.join(" ").trim();
            }
            const text = (el.innerText || "").trim();
            return { aria, labelledby: labelledbyText, text };
          };

          const classifyMetric = (label) => {
            const lower = (label || "").toLowerCase();
            if (/(like|likes)/.test(lower) || /讚|赞|喜歡|喜欢/.test(label)) return "likes";
            if (/(reply|replies|comment|comments)/.test(lower) || /回覆|回复|留言/.test(label)) return "replies";
            if (/(repost|reposts|reshare|re-share)/.test(lower) || /轉發|转发|轉貼|转贴/.test(label)) return "reposts";
            if (/(share|shares)/.test(lower) || /分享|傳送|传送/.test(label)) return "shares";
            return "";
          };

          const resolveMetrics = (card) => {
            const metrics = {
              likes: { present: false, value: null, source: "" },
              replies: { present: false, value: null, source: "" },
              reposts: { present: false, value: null, source: "" },
              shares: { present: false, value: null, source: "" },
            };
            const pickNearbyNumber = (el) => {
              if (!el) return null;
              const container = el.closest("[role='button'], button, a") || el.parentElement;
              if (!container) return null;
              const t = (container.innerText || "").trim();
              if (t && t.length <= 12 && /[0-9]/.test(t)) return t;
              const spans = Array.from(container.querySelectorAll("span, div"));
              for (const sp of spans) {
                const st = (sp.innerText || "").trim();
                if (st && st.length <= 12 && /[0-9]/.test(st)) return st;
              }
              return null;
            };
            const anchors = Array.from(
              card.querySelectorAll("svg[aria-label], [role='button'][aria-label]")
            );
            for (const el of anchors) {
              const aria = (el.getAttribute("aria-label") || "").trim();
              if (!aria) continue;
              const key = classifyMetric(aria);
              if (!key) continue;
              metrics[key].present = true;
              metrics[key].source = "icon_aria";
              const token = pickNearbyNumber(el);
              if (token) {
                const parsed = parseCount(token);
                if (parsed !== null) {
                  metrics[key].value = parsed;
                }
              }
            }
            const pressables = Array.from(
              card.querySelectorAll("[aria-label], [role='button'], svg[aria-label]")
            ).slice(0, 20);
            const buttonDump = pressables.map((el) => {
              const labels = resolveLabel(el);
              return {
                tag: (el.tagName || "").toLowerCase(),
                aria_label: labels.aria,
                inner_text: labels.text,
                labelledby_resolved_text: labels.labelledby,
              };
            });
            return { metrics, button_dump: buttonDump };
          };

          const parseSrcsetLargest = (srcset) => {
            if (!srcset) return "";
            const parts = srcset.split(",").map((p) => p.trim()).filter((p) => p);
            let best = { url: "", score: 0 };
            for (const part of parts) {
              const segs = part.split(/\s+/);
              const url = segs[0] || "";
              const size = segs[1] || "";
              let score = 0;
              if (size.endsWith("w")) score = parseFloat(size.replace("w", "")) || 0;
              else if (size.endsWith("x")) score = (parseFloat(size.replace("x", "")) || 0) * 1000;
              if (score >= best.score) best = { url, score };
            }
            return best.url;
          };

          const cleanBodyText = (rawText) => {
            if (!rawText) return "";
            const uiTokens = new Set([
              "follow",
              "more",
              "translate",
              "like",
              "reply",
              "replies",
              "repost",
              "share",
              "view more",
              "查看更多",
              "顯示",
              "更多",
            ]);
            const lines = rawText.split(/\n/).map((l) => l.trim()).filter((l) => l);
            const out = [];
            for (let i = 0; i < lines.length; i++) {
              const line = lines[i];
              const lower = line.toLowerCase();
              if (uiTokens.has(lower)) continue;
              if (/^\d+(?:\.\d+)?[km]?$/.test(lower)) continue;
              if (/^\d+\s?[smhdw]$/i.test(lower)) continue;
              if (i < 2 && !line.includes(" ") && !/^\d+\s?[smhdw]$/i.test(lower)) continue;
              out.push(line);
            }
            return out.join("\n").trim();
          };

          const extractPermalink = (card) => {
            const rawText = (card.innerText || "").trim();
            const links = Array.from(card.querySelectorAll("a[href]"))
              .map((a) => a.getAttribute("href") || "")
              .filter((href) => href);
            let permalink = "";
            for (const link of Array.from(card.querySelectorAll("a[href]"))) {
              const text = (link.innerText || "").trim();
              if (/\b\d+\s*[smhdw]\b/i.test(text)) {
                permalink = link.getAttribute("href") || "";
                break;
              }
            }
            if (!permalink && links.length) {
              const candidates = links.filter((l) => l.includes("/post/") || l.includes("/@"));
              const pick = candidates[candidates.length - 1] || links[links.length - 1];
              permalink = pick || "";
            }
            return { permalink, rawText };
          };

          const findCardRoot = (node) => {
            if (!node) return null;
            const article = node.closest("article") || node.closest("div[role='article']");
            if (article) return article;
            let cur = node;
            let depth = 0;
            while (cur && depth < 8) {
              if (cur.querySelector && cur.querySelector('a[href*="/post/"]')) return cur;
              cur = cur.parentElement;
              depth += 1;
            }
            return node;
          };

          const pressables = Array.from(document.querySelectorAll('div[data-pressable-container="true"]'));
          const seenRoots = new WeakSet();
          const cardRoots = [];
          for (const pressable of pressables) {
            const root = findCardRoot(pressable);
            if (!root) continue;
            if (seenRoots.has(root)) continue;
            seenRoots.add(root);
            cardRoots.push(root);
          }

          const out = [];
          for (const card of cardRoots) {
            const { permalink, rawText } = extractPermalink(card);
            const bodyText = cleanBodyText(rawText);
            const links = Array.from(card.querySelectorAll("a[href]"))
              .map((a) => a.getAttribute("href") || "")
              .filter((href) => href);
            let timeToken = "";
            const timeMatch = rawText.match(/\b\d+\s*[smhdw]\b/i);
            if (timeMatch) timeToken = timeMatch[0];
            const userAnchor = card.querySelector("a[href^='/@']");
            const user = userAnchor ? (userAnchor.textContent || "").trim() : "";
            const metricsResult = resolveMetrics(card);
            const images = [];
            const imgNodes = Array.from(card.querySelectorAll("img"));
            for (const img of imgNodes) {
              const src = img.getAttribute("src") || "";
              const dataSrc = img.getAttribute("data-src") || "";
              const srcset = img.getAttribute("srcset") || img.getAttribute("data-srcset") || "";
              const url = parseSrcsetLargest(srcset) || dataSrc || src;
              if (url) images.push(url);
            }
            const sourceNodes = Array.from(card.querySelectorAll("source[srcset], source[data-srcset]"));
            for (const source of sourceNodes) {
              const srcset = source.getAttribute("srcset") || source.getAttribute("data-srcset") || "";
              const url = parseSrcsetLargest(srcset);
              if (url) images.push(url);
            }
            out.push({
              user,
              body_text: bodyText,
              raw_text: rawText,
              permalink,
              time_token: timeToken,
              links,
              metrics: metricsResult.metrics,
              button_dump: metricsResult.button_dump,
              images,
            });
          }
          return out;
        }
        """
    )


def _extract_page_images(page) -> List[str]:
    urls = page.evaluate(
        r"""
        () => {
          const parseSrcsetLargest = (srcset) => {
            if (!srcset) return "";
            const parts = srcset.split(",").map((p) => p.trim()).filter((p) => p);
            let best = { url: "", score: 0 };
            for (const part of parts) {
              const segs = part.split(/\s+/);
              const url = segs[0] || "";
              const size = segs[1] || "";
              let score = 0;
              if (size.endsWith("w")) score = parseFloat(size.replace("w", "")) || 0;
              else if (size.endsWith("x")) score = (parseFloat(size.replace("x", "")) || 0) * 1000;
              if (score >= best.score) best = { url, score };
            }
            return best.url;
          };
          const out = [];
          const imgs = Array.from(document.querySelectorAll("img"));
          for (const img of imgs) {
            const src = img.getAttribute("src") || "";
            const dataSrc = img.getAttribute("data-src") || "";
            const srcset = img.getAttribute("srcset") || img.getAttribute("data-srcset") || "";
            const url = parseSrcsetLargest(srcset) || dataSrc || src;
            if (url) out.push(url);
          }
          const sources = Array.from(document.querySelectorAll("source[srcset], source[data-srcset]"));
          for (const source of sources) {
            const srcset = source.getAttribute("srcset") || source.getAttribute("data-srcset") || "";
            const url = parseSrcsetLargest(srcset);
            if (url) out.push(url);
          }
          return out;
        }
        """
    )
    if not isinstance(urls, list):
        return []
    filtered = [u for u in urls if not _is_profile_image(u)]
    return _dedupe_urls(filtered)


def _metric_value(card: Dict[str, Any], key: str) -> int:
    raw = (card.get("metrics") or {}).get(key) or {}
    value = raw.get("value")
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def _metric_present(card: Dict[str, Any], key: str) -> bool:
    raw = (card.get("metrics") or {}).get(key) or {}
    return bool(raw.get("present"))


def _metrics_confidence(present: Dict[str, bool]) -> str:
    if not present:
        return "missing"
    values = [bool(present.get(k)) for k in ("likes", "replies", "reposts", "shares")]
    if all(values):
        return "exact"
    if any(values):
        return "partial"
    return "missing"


def _has_reply_button(card: Dict[str, Any]) -> bool:
    keywords = (
        "view replies",
        "view more replies",
        "see more",
        "查看更多回覆",
        "查看回覆",
        "顯示更多回覆",
        "查看更多",
    )
    for item in card.get("button_dump") or []:
        for field in ("aria_label", "inner_text", "labelledby_resolved_text"):
            value = (item.get(field) or "").strip().lower()
            if not value:
                continue
            if any(k in value for k in keywords):
                return True
    raw = (card.get("raw_text") or "").lower()
    return any(k in raw for k in keywords)


def _build_drill_candidates(main_cards: List[Dict[str, Any]], max_drill_tabs: int) -> List[Dict[str, Any]]:
    candidates = []
    for card in main_cards:
        reply_count = _metric_value(card, "replies")
        has_button = _has_reply_button(card)
        if reply_count > 0 or has_button:
            candidates.append(
                {
                    "card": card,
                    "reply_count": reply_count,
                    "like_count": _metric_value(card, "likes"),
                    "has_button": has_button,
                }
            )
    candidates.sort(key=lambda c: (c["reply_count"], c["like_count"]), reverse=True)
    return candidates[: max_drill_tabs or 0]


def _click_expand_buttons_on_page(page, max_clicks: int = 6) -> int:
    return int(
        page.evaluate(
            r"""
            (maxClicks) => {
              const keywordHit = (value) => {
                const v = (value || "").toLowerCase();
                return (
                  v.includes("view replies") ||
                  v.includes("view more replies") ||
                  v.includes("see more") ||
                  v.includes("查看更多回覆") ||
                  v.includes("查看回覆") ||
                  v.includes("顯示更多回覆") ||
                  v.includes("查看更多")
                );
              };
              const buttons = Array.from(document.querySelectorAll("button, [role='button'], a"));
              let clicked = 0;
              for (const btn of buttons) {
                const label = (btn.getAttribute("aria-label") || "").trim();
                const text = (btn.innerText || "").trim();
                if (!keywordHit(label) && !keywordHit(text)) continue;
                try {
                  btn.click();
                  clicked += 1;
                  if (clicked >= maxClicks) break;
                } catch (e) {}
              }
              return clicked;
            }
            """,
            max_clicks,
        )
        or 0
    )


def _incremental_scroll_harvest(
    page,
    *,
    max_rounds: int = 4,
    stable_rounds: int = 2,
    max_extra_rounds: int = 1,
) -> List[Dict[str, Any]]:
    seen_fps: set[str] = set()
    stable = 0
    last_count = -1
    extra = 0
    rounds = 0
    while rounds < max_rounds + extra:
        cards = _harvest_cards_structured(page)
        card_count = len(cards)
        new_count = 0
        for card in cards:
            body_text = (card.get("body_text") or "").strip()
            if not body_text:
                body_text = _extract_body_text(card.get("raw_text") or "")
            fp = _fingerprint_comment(card.get("user") or "", body_text)
            if fp and fp not in seen_fps:
                seen_fps.add(fp)
                new_count += 1
        if new_count == 0:
            stable += 1
        else:
            stable = 0
        if card_count > last_count and extra < max_extra_rounds:
            extra += 1
        last_count = card_count
        if rounds >= 1 and stable >= stable_rounds:
            break
        page.mouse.wheel(0, 2200)
        page.wait_for_timeout(350)
        rounds += 1
    return _harvest_cards_structured(page)


def fetch_page_html(url: str, target_comment_blocks: int = 80) -> dict:
    if not os.path.exists(AUTH_FILE):
        raise FileNotFoundError("⚠️ 找不到 auth_threads.json，請先執行 login.py。")

    url = normalize_url(url)
    initial_html = ""
    scrolled_html = ""
    metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0}
    archive_html = ""
    archive_dom_json = {}
    page_images: List[str] = []
    crawled_at = datetime.now(timezone.utc)
    crawled_at_utc = crawled_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    headless_flag = os.environ.get("DLENS_HEADLESS", "1") != "0"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless_flag)
        context = browser.new_context(storage_state=AUTH_FILE)
        page = context.new_page()

        try:
            print(f"🕸️ 正在載入 {url} ...")
            response = page.goto(url, timeout=60000, wait_until="domcontentloaded")

            if response is None:
                print("⚠️ 沒有拿到任何 HTTP 回應 (response is None)")
                browser.close()
                return {"initial_html": "", "scrolled_html": ""}

            status = response.status
            print(f"📡 HTTP 狀態碼：{status}")

            if status < 200 or status >= 300:
                print("❌ 非 2xx 回應（可能是 404/403/500 等），無法抓取此頁。")
                browser.close()
                return {"initial_html": "", "scrolled_html": ""}

            page.wait_for_timeout(450)
            try:
                metrics = extract_metrics(page)
            except Exception as e:
                print(f"⚠️ extract_metrics error: {e}")

            replies_count = 0
            try:
                replies_count = int(metrics.get("replies") or 0)
            except Exception:
                replies_count = 0
            target_comment_blocks = min(max(replies_count + 10, 60), 200)

            try:
                snap = capture_archive_snapshot(page, url)
                archive_html = snap.get("archive_html") or ""
                archive_dom_json = snap.get("archive_dom_json") or {}
                print(f"📦 Archive captured: html_len={len(archive_html)}")
            except Exception as e:
                print(f"⚠️ Archive capture failed (best-effort): {e}")

            initial_html = page.content()
            print(f"✅ 初始 HTML 抓取完成，長度：{len(initial_html)} 字元")

            print("🔁 深度捲動留言區...")
            deep_scroll_comments(page, target_comment_blocks=target_comment_blocks)

            page.evaluate("window.scrollTo(0, 0);")
            page.wait_for_timeout(800)

            scrolled_html = page.content()
            page_images = _extract_page_images(page)
            print(f"✅ 深度捲動後 HTML 抓取完成，長度：{len(scrolled_html)} 字元")

        except Exception as e:
            print(f"❌ Fetch Error: {e}")
        finally:
            browser.close()

    return {
        "initial_html": initial_html,
        "scrolled_html": scrolled_html,
        "metrics": metrics,
        "archive_html": archive_html if "archive_html" in locals() else "",
        "archive_dom_json": archive_dom_json if "archive_dom_json" in locals() else {},
        "page_images": page_images,
        "crawled_at_utc": crawled_at_utc,
    }


def fetch_thread(url: str, *, max_comments=None, high_engagement: bool = True):
    url = normalize_url(url)

    html_payload = fetch_page_html(url, target_comment_blocks=60)

    parsed = extract_data_from_html(
        html_payload,
        url,
        fetcher_metrics=(html_payload.get("metrics") or {}),
    )

    comments = parsed.get("comments") or []
    if max_comments:
        comments = comments[: int(max_comments)]

    post_payload = parsed.get("post") or {}
    page_images = html_payload.get("page_images") or []
    if page_images:
        post_payload["post_images"] = page_images
    else:
        post_payload.setdefault("post_images", [])

    return {
        "mode": "dom_v6",
        "url": url,
        "post_payload": post_payload,
        "comments_payload": comments,
        "metrics": parsed.get("metrics") or {},
        "images": parsed.get("images") or [],
        "debug": {
            "comments_total": len(comments),
            "archive_snapshot_taken": bool(html_payload.get("archive_snapshot_taken")),
        },
    }


def run_fetcher_test(
    url: str,
    *,
    max_drill_tabs: int = 5,
    output_dir: str = "artifacts/fetcher_test_turbo_v15_linux",
    headless: bool = True,
) -> Dict[str, Any]:
    url = normalize_url(url)
    run_id = f"run_{int(time.time())}"
    run_dir = os.path.join(output_dir, run_id)
    _ensure_dir(run_dir)
    crawled_at = datetime.now(timezone.utc)
    crawled_at_utc = crawled_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if not os.path.exists(AUTH_FILE):
        raise FileNotFoundError("auth_threads.json not found; run login first.")

    start = time.time()
    main_comments: List[Dict[str, Any]] = []
    drill_comments: List[Dict[str, Any]] = []
    post_payload: Dict[str, Any] = {}
    post_images: List[str] = []
    main_cards: List[Dict[str, Any]] = []
    drill_cards_all: List[Dict[str, Any]] = []
    meta_by_id: Dict[str, Dict[str, Any]] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
        )
        context = browser.new_context(storage_state=AUTH_FILE, viewport={"width": 1280, "height": 900})
        page = context.new_page()

        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(450)
        metrics = extract_metrics(page)
        initial_html = page.content()

        replies_count = int(metrics.get("replies") or 0)
        target_comment_blocks = min(max(replies_count + 10, 60), 160)
        deep_scroll_comments(page, target_comment_blocks=target_comment_blocks)
        page.evaluate("window.scrollTo(0, 0);")
        page.wait_for_timeout(800)
        scrolled_html = page.content()
        post_images = _extract_page_images(page)

        parsed = extract_data_from_html(
            {"initial_html": initial_html, "scrolled_html": scrolled_html, "metrics": metrics},
            url,
            fetcher_metrics=metrics,
        )
        post_payload = parsed.get("post") or {}
        if not post_payload:
            post_payload = {
                "post_id": _extract_post_id(url),
                "url": url,
                "author": parsed.get("author") or "",
                "post_text": parsed.get("post_text") or "",
                "post_text_raw": parsed.get("post_text_raw") or "",
                "metrics": parsed.get("metrics") or {},
                "images": parsed.get("images") or [],
            }
        comments_payload = parsed.get("comments") or []

        seen_ids: set[str] = set()
        fp_to_id: Dict[str, str] = {}
        fp_to_node: Dict[str, Dict[str, Any]] = {}
        for c in comments_payload:
            author = c.get("user") or ""
            raw_text = c.get("text") or ""
            text = _strip_ui_headers(raw_text)
            fp = _fingerprint_comment(author, text)
            base_id = c.get("source_comment_id") or _extract_post_id(c.get("permalink") or "")
            if not base_id:
                base_id = hashlib.md5(fp.encode("utf-8")).hexdigest()[:16] if fp else ""
            comment_id = _unique_id(base_id, seen_ids)
            fp_to_id[fp] = comment_id if fp else fp_to_id.get(fp, "")
            node = {
                "comment_id": comment_id,
                "parent_id": None,
                "author": author,
                "text": text,
                "metrics": {
                    "likes": c.get("likes") or 0,
                    "replies": c.get("replies") or 0,
                    "reposts": c.get("reposts") or 0,
                    "shares": c.get("shares") or 0,
                },
                "fingerprint": fp,
                "comment_images": [],
                "time_token": None,
                "approx_created_at_utc": None,
                "time_precision": None,
            }
            main_comments.append(node)
            if fp and fp not in fp_to_node:
                fp_to_node[fp] = node
            if comment_id:
                meta_by_id[comment_id] = {
                    "source": "main",
                    "drilled": False,
                    "metrics_present": {},
                }

        main_cards = _harvest_cards_structured(page)
        for card in main_cards:
            user = (card.get("user") or "").strip()
            body_text = (card.get("body_text") or "").strip()
            if not body_text:
                body_text = _extract_body_text(card.get("raw_text") or "")
            body_text = _strip_ui_headers(body_text)
            fp = _fingerprint_comment(user, body_text)
            if not fp:
                continue
            metrics_payload = {
                "likes": _metric_value(card, "likes"),
                "replies": _metric_value(card, "replies"),
                "reposts": _metric_value(card, "reposts"),
                "shares": _metric_value(card, "shares"),
            }
            node = fp_to_node.get(fp)
            if node:
                node["metrics"] = metrics_payload
                images = _clean_image_urls(card.get("images") or [])
                if images:
                    node["comment_images"] = _dedupe_urls((node.get("comment_images") or []) + images)
                time_token = _normalize_time_token(card.get("time_token") or "")
                if time_token and not node.get("time_token"):
                    node["time_token"] = time_token
                cid = node.get("comment_id") or ""
                if cid:
                    meta = meta_by_id.setdefault(
                        cid,
                        {
                            "source": "main",
                            "drilled": False,
                            "metrics_present": {},
                        },
                    )
                    meta["metrics_present"].update(
                        {
                            "likes": _metric_present(card, "likes"),
                            "replies": _metric_present(card, "replies"),
                            "reposts": _metric_present(card, "reposts"),
                            "shares": _metric_present(card, "shares"),
                        }
                    )

        candidates = _build_drill_candidates(main_cards, max_drill_tabs)
        drill_page = context.new_page()

        for candidate in candidates:
            card = candidate["card"]
            parent_user = (card.get("user") or "").strip()
            parent_text = (card.get("body_text") or "").strip()
            if not parent_text:
                parent_text = _extract_body_text(card.get("raw_text") or "")
            parent_text = _strip_ui_headers(parent_text)
            parent_fp = _fingerprint_comment(parent_user, parent_text)
            parent_id = fp_to_id.get(parent_fp, "") or _extract_post_id(card.get("permalink") or "")
            drill_url = _normalize_permalink(card.get("permalink") or "")
            if not drill_url:
                continue

            try:
                drill_page.goto(drill_url, timeout=12000, wait_until="domcontentloaded")
            except Exception:
                continue
            drill_page.wait_for_timeout(450)
            for _ in range(2):
                if _click_expand_buttons_on_page(drill_page, max_clicks=6) <= 0:
                    break
                drill_page.wait_for_timeout(300)

            cards = _incremental_scroll_harvest(drill_page)
            drill_cards_all.extend(cards)
            per_page_seen: set[str] = set()
            for idx, dcard in enumerate(cards):
                raw_text = dcard.get("raw_text") or ""
                body_text = (dcard.get("body_text") or "").strip()
                if not body_text:
                    body_text = _extract_body_text(raw_text)
                body_text = _strip_ui_headers(body_text)
                user = (dcard.get("user") or "").strip()
                fp = _fingerprint_comment(user, body_text)
                if not fp or fp in per_page_seen:
                    continue
                per_page_seen.add(fp)

                if idx == 0:
                    continue
                if fp in fp_to_id:
                    node = fp_to_node.get(fp)
                    if node:
                        if parent_id and node.get("comment_id") != parent_id and not node.get("parent_id"):
                            node["parent_id"] = parent_id
                        if _ui_noise_score(body_text) < _ui_noise_score(node.get("text") or ""):
                            node["text"] = body_text
                        images = _clean_image_urls(dcard.get("images") or [])
                        if images:
                            node["comment_images"] = _dedupe_urls((node.get("comment_images") or []) + images)
                        time_token = _normalize_time_token(dcard.get("time_token") or "")
                        if time_token and not node.get("time_token"):
                            node["time_token"] = time_token
                        cid = node.get("comment_id") or ""
                        if cid:
                            meta = meta_by_id.setdefault(
                                cid,
                                {
                                    "source": "main",
                                    "drilled": True,
                                    "metrics_present": {},
                                },
                            )
                            meta["drilled"] = True
                            meta["metrics_present"].update(
                                {
                                    "likes": _metric_present(dcard, "likes"),
                                    "replies": _metric_present(dcard, "replies"),
                                    "reposts": _metric_present(dcard, "reposts"),
                                    "shares": _metric_present(dcard, "shares"),
                                }
                            )
                    continue

                base_id = _extract_post_id(dcard.get("permalink") or "") or hashlib.md5(fp.encode("utf-8")).hexdigest()[:16]
                comment_id = _unique_id(base_id, seen_ids)
                fp_to_id[fp] = comment_id
                drill_node = {
                    "comment_id": comment_id,
                    "parent_id": parent_id or None,
                    "author": user,
                    "text": body_text,
                    "metrics": {
                        "likes": _metric_value(dcard, "likes"),
                        "replies": _metric_value(dcard, "replies"),
                        "reposts": _metric_value(dcard, "reposts"),
                        "shares": _metric_value(dcard, "shares"),
                    },
                    "fingerprint": fp,
                    "comment_images": _clean_image_urls(dcard.get("images") or []),
                    "time_token": _normalize_time_token(dcard.get("time_token") or ""),
                    "approx_created_at_utc": None,
                    "time_precision": None,
                }
                drill_comments.append(drill_node)
                fp_to_node[fp] = drill_node
                meta_by_id[comment_id] = {
                    "source": "drill",
                    "drilled": False,
                    "metrics_present": {
                        "likes": _metric_present(dcard, "likes"),
                        "replies": _metric_present(dcard, "replies"),
                        "reposts": _metric_present(dcard, "reposts"),
                        "shares": _metric_present(dcard, "shares"),
                    },
                }

        drill_page.close()
        browser.close()

    merged: List[Dict[str, Any]] = []
    for node in main_comments + drill_comments:
        if node.get("parent_id") == node.get("comment_id"):
            node["parent_id"] = None
        if not isinstance(node.get("comment_images"), list):
            node["comment_images"] = []
        time_token = _normalize_time_token(node.get("time_token") or "")
        if time_token:
            node["time_token"] = time_token
            node["approx_created_at_utc"] = _approx_created_at(crawled_at, time_token)
            node["time_precision"] = "approx"
        else:
            node["time_token"] = None
            node["approx_created_at_utc"] = None
            node["time_precision"] = None
        merged.append(node)
    parsed_tree = _build_parsed_tree(merged)
    meta_by_id = {
        node.get("comment_id"): {
            "time_token": node.get("time_token"),
            "approx_created_at_utc": node.get("approx_created_at_utc"),
            "time_precision": node.get("time_precision"),
        }
        for node in merged
        if node.get("comment_id")
    }
    _project_time_fields(parsed_tree, meta_by_id)
    edges = [
        {"parent_id": node.get("parent_id"), "comment_id": node.get("comment_id")}
        for node in merged
        if node.get("parent_id")
    ]
    post_id = (post_payload.get("post_id") or "").strip() or _extract_post_id(url)
    manifest = {
        "run_id": run_id,
        "url": url,
        "output_dir": run_dir,
        "merged_total": parsed_tree.get("total"),
        "created_at": int(time.time()),
        "crawled_at_utc": crawled_at_utc,
    }
    if post_images:
        post_payload["post_images"] = post_images
    else:
        post_payload.setdefault("post_images", [])
    _write_json(os.path.join(run_dir, "manifest.json"), manifest)
    _write_json(os.path.join(run_dir, "post_payload.json"), post_payload)
    _write_json(os.path.join(run_dir, "comments_flat.json"), merged)
    _write_json(os.path.join(run_dir, "edges.json"), edges)
    _write_json(os.path.join(run_dir, "merged_comments_parsed.json"), parsed_tree)

    raw_initial_path = os.path.join(run_dir, "raw_html_initial.html")
    raw_final_path = os.path.join(run_dir, "raw_html_final.html")
    _write_text(raw_initial_path, initial_html)
    _write_text(raw_final_path, scrolled_html)
    raw_cards_path = os.path.join(run_dir, "raw_cards.json")
    _write_json(
        raw_cards_path,
        {
            "main_cards": main_cards,
            "drill_cards": drill_cards_all,
        },
    )
    threads_posts_raw = {
        "run_id": run_id,
        "crawled_at_utc": crawled_at_utc,
        "post_url": url,
        "post_id": post_id,
        "fetcher_version": "v15",
        "run_dir": run_dir,
        "raw_html_initial_path": raw_initial_path,
        "raw_html_final_path": raw_final_path,
        "raw_cards_path": raw_cards_path,
    }
    _write_json(os.path.join(run_dir, "threads_posts_raw.json"), threads_posts_raw)

    threads_comments = []
    for node in merged:
        cid = node.get("comment_id") or ""
        meta = meta_by_id.get(cid, {})
        metrics_present = meta.get("metrics_present") or {}
        threads_comments.append(
            {
                "run_id": run_id,
                "crawled_at_utc": crawled_at_utc,
                "post_id": post_id,
                "post_url": url,
                "comment_id": cid,
                "parent_comment_id": node.get("parent_id"),
                "author_handle": node.get("author") or "",
                "text": node.get("text") or "",
                "time_token": node.get("time_token"),
                "approx_created_at_utc": node.get("approx_created_at_utc"),
                "like_count": int((node.get("metrics") or {}).get("likes") or 0),
                "reply_count_ui": int((node.get("metrics") or {}).get("replies") or 0),
                "repost_count_ui": int((node.get("metrics") or {}).get("reposts") or 0),
                "share_count_ui": int((node.get("metrics") or {}).get("shares") or 0),
                "metrics_confidence": _metrics_confidence(metrics_present),
                "source": meta.get("source") or "main",
                "comment_images": node.get("comment_images") or [],
            }
        )
    _write_json(os.path.join(run_dir, "threads_comments.json"), threads_comments)

    threads_edges = []
    for node in merged:
        parent_id = node.get("parent_id")
        child_id = node.get("comment_id")
        if not parent_id or not child_id or parent_id == child_id:
            continue
        threads_edges.append(
            {
                "run_id": run_id,
                "post_id": post_id,
                "parent_comment_id": parent_id,
                "child_comment_id": child_id,
                "edge_type": "reply",
            }
        )
    _write_json(os.path.join(run_dir, "threads_comment_edges.json"), threads_edges)

    total_runtime = round(time.time() - start, 2)
    print(
        f"V15 Linux Turbo Summary: merged_total={parsed_tree.get('total')} "
        f"runtime={total_runtime}s output_dir={run_dir}"
    )
    return {"summary": {"merged_total": parsed_tree.get("total"), "runtime_seconds": total_runtime, "output_dir": run_dir}}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CDX fetcher test runner (v15)")
    parser.add_argument("url", help="Threads post URL")
    parser.add_argument("--max-drill-tabs", type=int, default=5)
    parser.add_argument("--output-dir", default="artifacts/fetcher_test_turbo_v15_linux")
    parser.add_argument("--headless", action="store_true")

    args = parser.parse_args()
    run_fetcher_test(
        args.url,
        max_drill_tabs=args.max_drill_tabs,
        output_dir=args.output_dir,
        headless=args.headless,
    )
