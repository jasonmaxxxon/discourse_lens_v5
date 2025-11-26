from playwright.sync_api import sync_playwright
import time
import os

AUTH_FILE = "auth_threads.json"


def deep_scroll_comments(page, max_loops: int = 15):
    """
    深度捲動頁面並嘗試展開更多留言 / 回覆。
    - 透過滑鼠滾動向下載入更多內容
    - 嘗試點擊 "View more replies" / "View more" / "Show replies"
    - 若 scrollHeight 多次未變化則提前停止
    """
    stable_count = 0
    last_height = 0
    expand_texts = ["View more replies", "View more", "Show replies"]

    for _ in range(max_loops):
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(1500)

        for text in expand_texts:
            try:
                for btn in page.get_by_text(text, exact=False).all():
                    btn.click(timeout=2000)
                    page.wait_for_timeout(500)
            except Exception:
                # 忽略找不到或點擊失敗，繼續下一個
                pass

        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            stable_count += 1
        else:
            stable_count = 0
        last_height = height

        if stable_count >= 3:
            break


def normalize_url(url: str) -> str:
    # 如果是 threads.com，就自動改成 threads.net
    if "threads.com" in url:
        new_url = url.replace("threads.com", "threads.net")
        print(f"🔁 偵測到 threads.com，已自動改成：{new_url}")
        return new_url
    return url

def fetch_page_html(url: str) -> str:
    """
    Step 1: 打開 Threads 網頁
    Step 2: 用 storage_state (auth_threads.json) 登入
    Step 3: 回傳完整 HTML 字串
    """
    
    if not os.path.exists(AUTH_FILE):
        raise FileNotFoundError("⚠️ 找不到 auth_threads.json，請先執行 login.py。")

    url = normalize_url(url)
    html_content = ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(storage_state=AUTH_FILE)
        page = context.new_page()

        try:
            print(f"🕸️ 正在載入 {url} ...")
            response = page.goto(url, timeout=60000, wait_until="load")
            
            if response is None:
                print("⚠️ 沒有拿到任何 HTTP 回應 (response is None)")
                browser.close()
                return ""

            status = response.status
            print(f"📡 HTTP 狀態碼：{status}")

            if status < 200 or status >= 300:
                print(f"❌ 非 2xx 回應（可能是 404/403/500 等），無法抓取此頁。")
                browser.close()
                return ""

            page.wait_for_load_state("networkidle")
            time.sleep(3)  # 讓畫面多載入一些
            print("🔁 深度捲動留言區...")
            deep_scroll_comments(page)

            html_content = page.content()
            print(f"✅ 抓到 HTML，長度：{len(html_content)} 字元")

        except Exception as e:
            print(f"❌ Fetch Error: {e}")
        finally:
            browser.close()

    return html_content
