from playwright.sync_api import sync_playwright
import time
import os
import json
from typing import Any, Dict

from scraper.scroll_utils import scroll_until_stable
from scraper.parser import extract_data_from_html

AUTH_FILE = "auth_threads.json"


def capture_archive_snapshot(page, url: str) -> Dict[str, Any]:
    """
    Return { archive_html: str, archive_dom_json: dict }.
    Must NOT throw unless page is invalid; caller handles exceptions.
    """
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
    """
    深度捲動頁面並嘗試展開更多留言 / 回覆。
    - 透過滑鼠滾動向下載入更多內容
    - 嘗試點擊 "View more replies" / "View more" / "Show replies"
    - 若 scrollHeight 多次未變化則提前停止
    - 若留言 block 數量已達 target_comment_blocks 也會提前停止
    """
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


def normalize_url(url: str) -> str:
    # 如果是 threads.com，就自動改成 threads.net
    if "threads.com" in url:
        new_url = url.replace("threads.com", "threads.net")
        print(f"🔁 偵測到 threads.com，已自動改成：{new_url}")
        return new_url
    return url


def extract_metrics(page) -> dict:
    """
    Extract accurate like / reply / repost / view counts from the main article.
    Priority:
    1) aria-label (e.g., "288 likes")
    2) icon + sibling text
    3) last-resort: text fallback
    Always returns ints, missing values default to 0.
    """

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

    # Step 1: aria-labels
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

    # Step 2: icon + sibling text within article buttons/spans
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

            # map by aria-label presence on button
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

    # Step 3: last-resort text search within article (only if digits present)
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


def fetch_page_html(url: str, target_comment_blocks: int = 80) -> dict:
    """
    打開 Threads 貼文並返回「兩份」HTML：
    - initial_html  : 只等首次載入完成，尚未深度捲動 → 一定包含畫面上第一批 Top comments
    - scrolled_html : 經過 deep_scroll_comments 後的完整 DOM → 用來抓更多留言樣本

    回傳格式：
    {
        "initial_html": "<html ...>...</html>",
        "scrolled_html": "<html ...>...</html>",
    }
    """

    if not os.path.exists(AUTH_FILE):
        raise FileNotFoundError("⚠️ 找不到 auth_threads.json，請先執行 login.py。")

    url = normalize_url(url)
    initial_html = ""
    scrolled_html = ""
    metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0}
    archive_html = ""
    archive_dom_json = {}

    headless_flag = os.environ.get("DLENS_HEADLESS", "1") != "0"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless_flag)
        context = browser.new_context(storage_state=AUTH_FILE)
        page = context.new_page()

        try:
            print(f"🕸️ 正在載入 {url} ...")
            response = page.goto(url, timeout=60000, wait_until="load")

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

            # 等待網路穩定，先抓「初始畫面」HTML → 這一刻的 Top comments 一定在 DOM 裡
            page.wait_for_load_state("networkidle")
            time.sleep(3)
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

            # 深度捲動載入更多留言 & 展開「View more replies」
            print("🔁 深度捲動留言區...")
            deep_scroll_comments(page, target_comment_blocks=target_comment_blocks)

            # 深度捲動後，為了保證 Top comments 仍然在 DOM 中，再把畫面捲回最上方
            page.evaluate("window.scrollTo(0, 0);")
            page.wait_for_timeout(1500)

            scrolled_html = page.content()
            print(f"✅ 深度捲動後 HTML 抓取完成，長度：{len(scrolled_html)} 字元")

        except Exception as e:
            print(f"❌ Fetch Error: {e}")
        finally:
            browser.close()

    return {
        "initial_html": initial_html,
        "scrolled_html": scrolled_html,
        "metrics": metrics,
        "archive_html": archive_html if 'archive_html' in locals() else "",
        "archive_dom_json": archive_dom_json if 'archive_dom_json' in locals() else {},
    }


def fetch_thread(url: str, *, max_comments=None, high_engagement: bool = True):
    """
    V6-compatible entrypoint expected by pipelines/core.py
    - uses DOM scroll/click to capture HTML (fast, robust)
    - parses comments via scraper.parser.extract_data_from_html
    - returns the standard payload: post_payload/comments_payload/metrics/images
    """
    url = normalize_url(url)

    # 1) open + deep scroll (DOM)
    html_payload = fetch_page_html(url, target_comment_blocks=60)

    # 2) parse HTML -> structured post/comments
    parsed = extract_data_from_html(
        html_payload,
        url,
        fetcher_metrics=(html_payload.get("metrics") or {}),
    )

    comments = parsed.get("comments") or []
    if max_comments:
        comments = comments[: int(max_comments)]

    return {
        "mode": "dom_v6",
        "url": url,
        "post_payload": parsed.get("post"),
        "comments_payload": comments,
        "metrics": parsed.get("metrics") or {},
        "images": parsed.get("images") or [],
        "debug": {
            "comments_total": len(comments),
            "archive_snapshot_taken": bool(html_payload.get("archive_snapshot_taken")),
        },
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python scraper/fetcher.py <threads_url>")
        sys.exit(1)
    url = sys.argv[1]
    result = fetch_page_html(url, target_comment_blocks=60)
    print("METRICS:", result.get("metrics"))
