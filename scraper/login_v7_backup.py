from playwright.sync_api import sync_playwright
import time

AUTH_FILE = "auth_threads.json"

def login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()

        page = context.new_page()
        print("🔐 打開 Threads 登入頁...")

        page.goto("https://www.threads.net/login", timeout=60000)

        print("📝 請在開啟的視窗登入 Threads（帳號、密碼、2FA）")
        print("👉 完成登入後回到 Terminal 按 Enter")
        input()

        context.storage_state(path=AUTH_FILE)
        print(f"✅ 已儲存登入 session 到：{AUTH_FILE}")

        browser.close()

if __name__ == "__main__":
    login()
