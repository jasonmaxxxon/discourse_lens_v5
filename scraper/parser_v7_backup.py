from bs4 import BeautifulSoup
import re
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# UI 垃圾字，不能當作者 / user / 內容
UI_TOKENS = {
    "follow",
    "following",
    "more",
    "top",
    "translate",
    "verified",
    "edited",
    "author",
    "liked by original author",
}
FIRST_THREAD_TOKENS = {"first thread", "first threads"}

# 留言 / 主文 footer 區的 token
FOOTER_TOKENS = {"translate", "like", "reply", "repost", "share"}

# 時間格式：2d, 17h, 5m, 3w
TIME_PATTERN = re.compile(r"^\d+\s*[smhdw]$")
COMMENT_ID_PATTERNS = [
    re.compile(r'"comment_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"feedback_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"id"\s*:\s*"([^"]+)"'),
    re.compile(r'"pk"\s*:\s*"([^"]+)"'),
    re.compile(r'"media_id"\s*:\s*"([^"]+)"'),
]
PARENT_ID_PATTERNS = [
    re.compile(r'"parent_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"parent_comment_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"thread_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"parent_source_comment_id"\s*:\s*"([^"]+)"'),
]
AUTHOR_ID_PATTERNS = [
    re.compile(r'"user_id"\s*:\s*"([^"]+)"'),
    re.compile(r'"author_id"\s*:\s*"([^"]+)"'),
]
CREATED_AT_PATTERNS = [
    re.compile(r'"created_at"\s*:\s*"([^"]+)"'),
    re.compile(r'"timestamp"\s*:\s*"([^"]+)"'),
]

JSON_TEXT_KEYS = ("text", "body", "comment_text", "message", "caption")
JSON_ID_KEYS = ("id", "pk", "comment_id", "feedback_id", "thread_id")
JSON_PARENT_KEYS = ("parent_comment_id", "parent_id", "reply_to_comment_id", "reply_to_id")
JSON_REPLY_TO_KEYS = ("reply_to_author", "reply_to_username", "reply_to_user")
JSON_TS_KEYS = ("taken_at", "created_at", "timestamp", "created_time")


def _parse_timestamp(val: Any) -> Optional[str]:
    """
    Best-effort timestamp normalization. Returns ISO string or None.
    Accepts epoch seconds (str/int) or ISO-like strings.
    """
    if val is None:
        return None
    try:
        # epoch seconds (int or numeric string)
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.strip().isdigit()):
            import datetime

            ts = float(val)
            return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat()
        if isinstance(val, str):
            import datetime

            try:
                return datetime.datetime.fromisoformat(val.replace("Z", "+00:00")).isoformat()
            except Exception:
                pass
    except Exception:
        return None
    return None


def _normalize_text(val: str) -> str:
    return " ".join((val or "").split()).strip()


def parse_number(text: str) -> int:
    """
    安全解析 like / view / reply / repost / share 數：
    - 支援: '1', '12', '1.2K', '3.4M'
    - 忽略: 沒數字的字串
    """
    if not text:
        return 0

    clean = text.replace(",", "").upper()
    m = re.search(r"([\d\.]+)\s*([KM]?)", clean)
    if not m:
        return 0

    num = float(m.group(1))
    suffix = m.group(2)

    if suffix == "K":
        num *= 1000
    elif suffix == "M":
        num *= 1_000_000

    return int(num)


def extract_block_user(lines) -> str:
    """
    從一個 block（主文 / 留言）裡抽出 user：
    - 跳過 Follow / More / Translate 等 UI
    - 跳過時間 2d / 17h
    - 跳過 Verified / Edited / Author / Liked by original author
    """
    for line in lines:
        candidate = line.strip()
        if not candidate:
            continue
        lower = candidate.lower()
        if lower in UI_TOKENS:
            continue
        if lower in FIRST_THREAD_TOKENS:
            continue
        if TIME_PATTERN.match(lower):
            continue
        # 柔性處理 header 分隔符（Threads 可能用 · 或 •）
        for sep in ("·", "•"):
            if sep in candidate:
                parts = candidate.split(sep, 1)
                return parts[0].strip()
        # 時間格式視為 meta，不當 user
        if TIME_PATTERN.match(candidate.lower()):
            continue
        return candidate
    logger.warning("Header parse fallback: unable to find user; lines=%s", lines[:3])
    return "Unknown"


def extract_block_likes(lines) -> int:
    """
    從 block 的行裡找 Like 數：
    - 找到第一個 'Like' 行 → 下一行當作數字
    """
    for i, line in enumerate(lines):
        if line.strip().lower() == "like" and i + 1 < len(lines):
            return parse_number(lines[i + 1])
    return 0


def extract_block_body(lines) -> str:
    """
    從 block（主文 / 留言）中抽出「純內容」：
    - tolerant: 如果沒偵測到 footer，就保留 header 以外的全部內容
    """
    start_idx = 0
    found_more = False
    for i, line in enumerate(lines):
        if line.strip().lower() == "more":
            start_idx = i + 1
            found_more = True
            break

    if not found_more:
        for i, line in enumerate(lines):
            candidate = line.strip()
            if not candidate:
                continue
            lower = candidate.lower()
            if lower in UI_TOKENS:
                continue
            if TIME_PATTERN.match(lower):
                continue
            start_idx = i
            break

    def soft_match(text: str) -> bool:
        low = text.strip().lower()
        return low in FOOTER_TOKENS or low in UI_TOKENS

    body_lines = []
    for line in lines[start_idx:]:
        if soft_match(line):
            break
        body_lines.append(line)

    return "\n".join(body_lines).strip()


def extract_metrics_from_lines(lines) -> dict:
    """
    Fallback-only metrics extractor for主文 lines:
    - Only parses likes / reply_count / repost_count / share_count when present
      as "<token>\\n<number>" pairs.
    """
    likes = reply_count = repost_count = share_count = 0

    for i, line in enumerate(lines):
        lower = line.strip().lower()
        if lower == "like" and i + 1 < len(lines):
            likes = parse_number(lines[i + 1])
        if lower in ("reply", "replies") and i + 1 < len(lines):
            reply_count = parse_number(lines[i + 1])
        if lower == "repost" and i + 1 < len(lines):
            repost_count = parse_number(lines[i + 1])
        if lower == "share" and i + 1 < len(lines):
            share_count = parse_number(lines[i + 1])

    return {"likes": likes, "reply_count": reply_count, "repost_count": repost_count, "share_count": share_count}


def _extract_comment_meta(block) -> Dict[str, Any]:
    """
    Best-effort extraction of native comment identifiers from a comment block.
    Looks at attributes and embedded JSON strings.
    """
    meta: Dict[str, Any] = {}
    if not block:
        return meta

    # Direct attributes (id, data-*). Threads sometimes nests ids on the element.
    attr_candidates = []
    try:
        for k, v in (block.attrs or {}).items():
            if isinstance(v, list):
                attr_candidates.extend([str(x) for x in v])
            else:
                attr_candidates.append(str(v))
    except Exception:
        pass

    text_blob = ""
    try:
        text_blob = block.decode()  # includes inner HTML
    except Exception:
        try:
            text_blob = str(block)
        except Exception:
            text_blob = ""

    def _search(patterns):
        for p in patterns:
            for hay in attr_candidates + [text_blob]:
                m = p.search(hay)
                if m:
                    return m.group(1)
        return None

    meta["source_comment_id"] = _search(COMMENT_ID_PATTERNS)
    meta["parent_comment_id"] = _search(PARENT_ID_PATTERNS)
    meta["author_id"] = _search(AUTHOR_ID_PATTERNS)
    meta["created_at"] = _search(CREATED_AT_PATTERNS)
    return meta


def _extract_reply_to_author(block_lines) -> Optional[str]:
    """
    Detect reply target from visible strings such as "Replying to @foo".
    """
    for line in block_lines:
        low = line.lower()
        if "replying to" in low:
            m = re.search(r"replying to\s+@?([\w\.\-]+)", low, re.IGNORECASE)
            if m:
                return m.group(1)
    return None


def _json_get_first(node: dict, keys: tuple) -> Optional[Any]:
    for k in keys:
        if k in node and node[k] is not None:
            return node[k]
    return None


def _json_extract_text(node: dict) -> Optional[str]:
    val = _json_get_first(node, JSON_TEXT_KEYS)
    if isinstance(val, dict):
        val = val.get("text")
    if isinstance(val, str):
        return val
    return None


def _json_extract_text_fragments(node: dict) -> Optional[list]:
    val = node.get("text_fragments") or node.get("fragments")
    if isinstance(val, list):
        return val
    if isinstance(val, dict) and isinstance(val.get("fragments"), list):
        return val.get("fragments")
    return None


def _json_extract_author(node: dict) -> tuple[str, Optional[str]]:
    for key in ("user", "owner", "author"):
        val = node.get(key)
        if isinstance(val, dict):
            handle = val.get("username") or val.get("user_name") or val.get("handle")
            author_id = val.get("id") or val.get("pk")
            if handle:
                return str(handle), str(author_id) if author_id is not None else None
    handle = node.get("username") or node.get("user_name") or node.get("handle")
    if handle:
        return str(handle), None
    return "", None


def _json_extract_id(node: dict) -> Optional[str]:
    val = _json_get_first(node, JSON_ID_KEYS)
    if val is None:
        return None
    return str(val)


def _json_extract_parent_id(node: dict) -> Optional[str]:
    val = _json_get_first(node, JSON_PARENT_KEYS)
    if val is None:
        return None
    return str(val)


def _json_extract_reply_to_author(node: dict) -> Optional[str]:
    val = _json_get_first(node, JSON_REPLY_TO_KEYS)
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        handle = val.get("username") or val.get("user_name") or val.get("handle")
        if handle:
            return str(handle)
    return None


def _json_extract_taken_at(node: dict) -> Optional[str]:
    val = _json_get_first(node, JSON_TS_KEYS)
    if val is None:
        return None
    return _parse_timestamp(val)


def _json_extract_counts(node: dict, keys: tuple) -> Optional[int]:
    val = _json_get_first(node, keys)
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        return None


def _reconstruct_text(node: dict) -> tuple[str, Optional[list]]:
    if not isinstance(node, dict):
        return "", None
    text = node.get("text")
    if isinstance(text, str) and text.strip():
        return text, None
    cap = node.get("caption")
    if isinstance(cap, dict):
        t = cap.get("text")
        if isinstance(t, str) and t.strip():
            return t, None
    fragments = node.get("text_fragments") or node.get("fragments")
    if isinstance(fragments, list):
        parts = []
        for frag in fragments:
            if isinstance(frag, dict):
                ft = frag.get("text")
                if isinstance(ft, str) and ft.strip():
                    parts.append(ft)
            elif isinstance(frag, str) and frag.strip():
                parts.append(frag)
        return "".join(parts).strip(), fragments
    return "", None


def _is_real_comment(node: dict, text: str, images: Optional[list]) -> bool:
    if not isinstance(node, dict):
        return False
    typename = node.get("__typename")
    if isinstance(typename, str) and ("Connection" in typename or "Edge" in typename):
        return False
    cid = node.get("source_comment_id") or node.get("comment_id") or node.get("id") or node.get("pk")
    if not cid:
        return False
    author = node.get("user") or node.get("author") or node.get("owner") or {}
    author_handle = None
    if isinstance(author, dict):
        author_handle = author.get("username") or author.get("handle") or author.get("name") or author.get("user_name")
    if not (isinstance(author_handle, str) and author_handle.strip()):
        return False
    has_media = isinstance(images, list) and len(images) > 0
    has_text = isinstance(text, str) and text.strip() != ""
    if not has_text and not has_media:
        return False
    return True


def _parse_single_html(html: str, url: str) -> dict:
    """
    單次 HTML → 結構化資料。
    用來支援：
      1) 初始畫面 (Top comments snapshot)
      2) 深度捲動後畫面
    """
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "url": url,
        "author": "",
        "post_text": "",
        "post_text_raw": "",
        "is_first_thread": False,
        "metrics": {
            "likes": 0,
            "views": 0,
            "reply_count": 0,
            "repost_count": 0,
            "share_count": 0,
        },
        "images": [],
        "comments": [],
    }

    posts = soup.find_all("div", {"data-pressable-container": "true"})
    if not posts:
        return data

    # 主文
    main_post = posts[0]
    full_text = main_post.get_text("\n", strip=True)
    data["post_text_raw"] = full_text
    lines = full_text.split("\n")
    lower_lines = [ln.lower().strip() for ln in lines if ln.strip()]
    if any(ln in FIRST_THREAD_TOKENS for ln in lower_lines):
        data["is_first_thread"] = True

    # 圖片
    for img in main_post.find_all("img"):
        alt = img.get("alt", "") or ""
        if "profile picture" in alt.lower():
            continue
        src = img.get("src", "").strip()
        if not src:
            srcset = img.get("srcset", "")
            if srcset:
                src = srcset.split(" ", 1)[0].strip()
        if not src:
            continue
        if "s150x150" in src:
            continue
        data["images"].append({"src": src, "alt": alt})

    # 作者 + 主文內容 + 互動數
    data["author"] = extract_block_user(lines)
    data["post_text"] = extract_block_body(lines)

    m = extract_metrics_from_lines(lines)
    data["metrics"]["likes"] = m["likes"]
    data["metrics"]["reply_count"] = m["reply_count"]
    data["metrics"]["repost_count"] = m["repost_count"]
    data["metrics"]["share_count"] = m["share_count"]

    # Views（fallback: 只用於 live DOM metrics 缺失時）
    views = 0
    for text_node in soup.stripped_strings:
        low = text_node.lower()
        # 避免吃到 "View 3 more replies"
        if "views" in low and "reply" not in low and "view more" not in low:
            views = parse_number(text_node)
            break
    data["metrics"]["views"] = views

    # 留言區：posts[1:] 每一個都是一個留言 block
    for block in posts[1:]:
        raw_block = block.get_text("\n", strip=True)
        if not raw_block:
            continue

        block_lines = raw_block.split("\n")
        c_user = extract_block_user(block_lines)
        c_likes = extract_block_likes(block_lines)
        c_body = extract_block_body(block_lines)
        meta = _extract_comment_meta(block)
        reply_to_author = _extract_reply_to_author(block_lines)

        if not c_user and not c_body:
            continue

        text_fragments = None  # placeholder: HTML-only parse does not expose fragments
        text_final = c_body
        if (not text_final or not text_final.strip()) and text_fragments:
            try:
                text_final = "".join(
                    [
                        f.get("text", "") if isinstance(f, dict) else str(f)
                        for f in text_fragments
                    ]
                ).strip()
            except Exception:
                text_final = text_final or ""

        data["comments"].append(
            {
                # canonical keys for downstream DB mapping
                "author_handle": c_user,
                "text": text_final,
                "text_fragments": text_fragments,
                "reply_to_author": reply_to_author,
                "taken_at": _parse_timestamp(meta.get("created_at")),
                "like_count": c_likes,
                "reply_count": None,
                "raw_json": {
                    "raw": raw_block,
                    "meta": meta,
                },
                # legacy/compat keys (keep existing behavior)
                "user": c_user,
                "text": text_final,
                "likes": c_likes,
                "raw": raw_block,
                "source_comment_id": meta.get("source_comment_id"),
                "parent_comment_id": meta.get("parent_comment_id"),
                "parent_source_comment_id": meta.get("parent_source_comment_id"),
                "author_id": meta.get("author_id"),
                "created_at": meta.get("created_at"),
                "root_source_comment_id": meta.get("root_source_comment_id"),
            }
        )

    return data


def _parse_json_interception(bundle: dict, url: str) -> dict:
    post_payload = bundle.get("post_payload") or {}
    comments_payload = bundle.get("comments_payload") or []
    metrics_raw = bundle.get("metrics") or {}
    images = bundle.get("images") or []

    author_handle, _ = _json_extract_author(post_payload)
    post_text_raw = _json_extract_text(post_payload) or ""
    post_text = _normalize_text(post_text_raw)
    root_id = _json_extract_id(post_payload)

    metrics = {
        "likes": int(metrics_raw.get("likes") or 0),
        "views": int(metrics_raw.get("views") or 0),
        "reply_count": int(metrics_raw.get("reply_count") or metrics_raw.get("replies") or 0),
        "repost_count": int(metrics_raw.get("repost_count") or metrics_raw.get("reposts") or 0),
        "share_count": int(metrics_raw.get("share_count") or metrics_raw.get("shares") or 0),
    }

    comments = []
    before_count = len(comments_payload)
    dropped_empty = 0
    with_source = 0

    for node in comments_payload:
        if not isinstance(node, dict):
            continue
        if root_id and _json_extract_id(node) == root_id:
            continue
        text, fragments = _reconstruct_text(node)
        images_node = node.get("images") or node.get("media") or []
        if not _is_real_comment(node, text, images_node):
            dropped_empty += 1
            continue
        author, author_id = _json_extract_author(node)
        source_comment_id = _json_extract_id(node)
        if source_comment_id:
            with_source += 1
        parent_source_comment_id = _json_extract_parent_id(node)
        reply_to_author = _json_extract_reply_to_author(node)
        taken_at = _json_extract_taken_at(node)
        like_count = _json_extract_counts(node, ("like_count", "likes", "like_count_total")) or 0
        reply_count = _json_extract_counts(node, ("reply_count", "replies", "child_comment_count"))

        comments.append(
            {
                "user": author,
                "author_handle": author,
                "author_id": author_id,
                "text": text,
                "text_fragments": fragments or _json_extract_text_fragments(node),
                "like_count": like_count,
                "likes": like_count,
                "reply_count": reply_count,
                "taken_at": taken_at,
                "created_at": node.get("created_at") or node.get("timestamp"),
                "source_comment_id": source_comment_id,
                "parent_source_comment_id": parent_source_comment_id,
                "root_source_comment_id": node.get("root_source_comment_id"),
                "reply_to_author": reply_to_author,
                "raw_json": node,
            }
        )

    after_count = len(comments)
    pct = (with_source / after_count * 100.0) if after_count else 0.0
    print(
        f"[Parser] root_author={author_handle} comments_before={before_count} "
        f"comments_after={after_count} empty_dropped={dropped_empty} "
        f"source_id_coverage={pct:.1f}%"
    )

    data = {
        "url": url,
        "author": author_handle,
        "post_text": post_text,
        "post_text_raw": post_text_raw,
        "is_first_thread": False,
        "metrics": metrics,
        "images": images,
        "comments": comments,
    }

    comments_sorted = sorted(
        comments,
        key=lambda c: c.get("like_count", 0) or 0,
        reverse=True,
    )
    data["comments_by_likes"] = comments_sorted
    data["metrics"]["reply_count"] = max(int(metrics.get("reply_count") or 0), len(comments))
    return data


def extract_data_from_html(html_or_bundle, url: str) -> dict:
    """
    將 Threads 單帖的 HTML 解析成結構化 dict。
    支援兩種輸入：
      1) 舊版：單一 HTML 字串
      2) 新版：{"initial_html": ..., "scrolled_html": ...}

    新版策略：
      - initial_html：畫面剛載入時的 DOM → 一定包含 UI 顯示的 Top comments
      - scrolled_html：深度捲動後的 DOM → 載入更多留言
      - 兩者的 comments 會合併去重，並標記 from_top_snapshot=True/False
      - comments_by_likes：根據 likes 排序好的視圖，用於「高讚好留言 Top 5」
    """
    if isinstance(html_or_bundle, dict) and html_or_bundle.get("mode") == "json_interception":
        return _parse_json_interception(html_or_bundle, url)

    def _normalize_metrics(raw: dict | None) -> dict:
        defaults = {
            "likes": 0,
            "reply_count": 0,
            "repost_count": 0,
            "share_count": 0,
            "views": 0,
        }
        if not raw:
            return defaults
        normalized = defaults.copy()
        normalized["likes"] = int(raw.get("likes") or 0)
        normalized["reply_count"] = int(raw.get("reply_count") or raw.get("replies") or 0)
        normalized["repost_count"] = int(raw.get("repost_count") or raw.get("reposts") or 0)
        normalized["share_count"] = int(raw.get("share_count") or raw.get("shares") or 0)
        normalized["views"] = int(raw.get("views") or 0)
        return normalized

    if isinstance(html_or_bundle, dict):
        if html_or_bundle.get("mode") == "html_snapshot":
            initial_html = ""
            scrolled_html = html_or_bundle.get("html") or ""
            fetcher_metrics = _normalize_metrics(html_or_bundle.get("metrics"))
        else:
            initial_html = html_or_bundle.get("initial_html") or ""
            scrolled_html = html_or_bundle.get("scrolled_html") or ""
            fetcher_metrics = _normalize_metrics(html_or_bundle.get("metrics"))
    else:
        # 向下兼容：只給一份 HTML 的舊用法
        initial_html = ""
        scrolled_html = html_or_bundle or ""
        fetcher_metrics = _normalize_metrics(None)

    # 先用「優先 scrolled_html，沒有就用 initial_html」當主資料
    main_html = scrolled_html or initial_html
    base = _parse_single_html(main_html, url)

    # 從深度捲動後 HTML 抓到的留言
    comments_scrolled = list(base.get("comments", []))

    # 再從 initial_html 再解析一次，專門抓「剛開頁時的 Top comments」
    comments_initial = []
    if initial_html:
        top_struct = _parse_single_html(initial_html, url)
        comments_initial = top_struct.get("comments", [])

        # 若 main_html 其實就是 initial_html（沒有 scrolled_html），則不需要再合併一次
        if not scrolled_html:
            comments_scrolled = []

    # 合併兩邊留言，去重，並標示來源
    merged_comments = []
    seen_keys = set()

    for src_list, is_top in ((comments_initial, True), (comments_scrolled, False)):
        for c in src_list:
            key = c.get("source_comment_id") or (c.get("user", ""), c.get("text", ""))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            c["from_top_snapshot"] = is_top
            merged_comments.append(c)

    base["comments"] = merged_comments

    # 重新計算「抓到的實際留言數」與按讚排序視圖
    comments_sorted = sorted(
        merged_comments,
        key=lambda c: c.get("likes", 0),
        reverse=True,
    )
    base["comments_by_likes"] = comments_sorted

    # reply_count 至少不會小於實際抓到的留言樣本數
    fallback_metrics = base.get("metrics") or {}
    fallback_metrics["reply_count"] = max(
        fallback_metrics.get("reply_count", 0),
        len(merged_comments),
    )

    # Metrics merge: prefer fetcher metrics, fill zeros from parser fallback
    final_metrics = (fetcher_metrics or {}).copy()
    for key in ("likes", "reply_count", "repost_count", "share_count", "views"):
        if not final_metrics.get(key):
            final_metrics[key] = int(fallback_metrics.get(key, 0) or 0)
    final_metrics["reply_count"] = max(int(final_metrics.get("reply_count") or 0), len(merged_comments))
    base["metrics"] = final_metrics

    # Debug coverage report for native ids
    total_comments = len(merged_comments)
    if total_comments:
        with_source = sum(1 for c in merged_comments if c.get("source_comment_id"))
        pct = round((with_source / total_comments) * 100, 1)
        print(f"[Parser] source_comment_id coverage: {with_source}/{total_comments} ({pct}%)")
        if with_source == 0:
            logger.warning("[Parser] ⚠️ Comment Identity Degraded: source_comment_id missing for ALL comments. Reply tree will be broken.")

    return base
