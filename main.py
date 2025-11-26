from database.store import save_thread
from scraper.fetcher import fetch_page_html
from scraper.parser import extract_data_from_html

def run_pipeline(url: str):
    print("\n🚀 Pipeline started.")

    # Step 1: fetch HTML
    html = fetch_page_html(url)
    if not html:
        print("❌ 無法抓取 HTML")
        return

    print("🧩 HTML OK，開始解析...")

    # Step 2: parse
    data = extract_data_from_html(html, url)

    # Step 3: result preview
    print("\n===== 結果預覽 =====")
    print("作者:", data["author"])
    print("主文（乾淨）:", data["post_text"][:200], "...")
    print("Like:", data["metrics"]["likes"])
    print("Views:", data["metrics"]["views"])
    print("Reply 總數 (UI):", data["metrics"]["reply_count"])
    print("Repost 總數 (UI):", data["metrics"]["repost_count"])
    print("Share 總數 (UI):", data["metrics"]["share_count"])
    print("實際抓到留言樣本:", len(data["comments"]))
    print("====================")

    # Step 4: save to DB
    save_thread(data)

    # 印留言列表
    print("\n===== 留言 Sample =====")
    for idx, c in enumerate(data["comments"], start=1):
        print(f"\n--- Comment #{idx} ---")
        print("User:", c["user"])
        print("Likes:", c["likes"])
        print("Text:", c["text"])
    print("======================\n")


if __name__ == "__main__":
    mode = input("輸入模式: (1) 單一URL / (2) 多條URL列表 [1/2]：").strip()

    if mode == "2":
        print("請輸入多條 URL，每行一條，輸入空行結束：")
        urls = []
        while True:
            line = input().strip()
            if not line:
                break
            # 自動 threads.com → threads.net
            if "threads.com" in line:
                line = line.replace("threads.com", "threads.net")
                print(f"🔁 偵測到 threads.com，已自動改成：{line}")
            urls.append(line)

        for url in urls:
            print("\n==============================")
            print(f"正在處理: {url}")
            run_pipeline(url)
        print("\n🎉 批次處理完成。")
    else:
        url = input("請輸入 Threads URL：").strip()

        # 自動 threads.com → threads.net
        if "threads.com" in url:
            url = url.replace("threads.com", "threads.net")
            print(f"🔁 偵測到 threads.com，已自動改成：{url}")

        run_pipeline(url)
