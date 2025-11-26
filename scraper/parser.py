from bs4 import BeautifulSoup
import re

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

# 留言 / 主文 footer 區的 token
FOOTER_TOKENS = {"translate", "like", "reply", "repost", "share"}

# 時間格式：2d, 17h, 5m, 3w
TIME_PATTERN = re.compile(r"^\d+\s*[smhdw]$")


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
        if TIME_PATTERN.match(lower):
            continue
        # 有些會是 'ukiii.zzzzz · Author'，簡單切掉後半
        if "·" in candidate:
            candidate = candidate.split("·", 1)[0].strip()
        return candidate
    return ""


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
    - 如果有 'More'：從 'More' 的下一行開始
    - 沒有 'More'：從第一個非 UI / 非時間行開始
    - 在遇到 Translate / Like / Reply / Repost / Share 時結束
    """
    start_idx = 0

    # 先找 'More'
    found_more = False
    for i, line in enumerate(lines):
        if line.strip().lower() == "more":
            start_idx = i + 1
            found_more = True
            break

    # 沒有 'More' 的情況：從第一個非 UI / 非時間行開始
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

    body_lines = []
    for line in lines[start_idx:]:
        lower = line.strip().lower()
        if lower in FOOTER_TOKENS:
            break
        body_lines.append(line)

    return "\n".join(body_lines).strip()


def extract_metrics_from_lines(lines) -> dict:
    """
    從主文 lines 裡抽出:
    - likes
    - reply_count
    - repost_count
    - share_count
    """
    likes = reply_count = repost_count = share_count = 0

    for i, line in enumerate(lines):
        lower = line.strip().lower()
        # Like
        if lower == "like" and i + 1 < len(lines):
            likes = parse_number(lines[i + 1])
        # Reply / Replies
        if lower in ("reply", "replies") and i + 1 < len(lines):
            reply_count = parse_number(lines[i + 1])
        # Repost
        if lower == "repost" and i + 1 < len(lines):
            repost_count = parse_number(lines[i + 1])
        # Share
        if lower == "share" and i + 1 < len(lines):
            share_count = parse_number(lines[i + 1])

    return {
        "likes": likes,
        "reply_count": reply_count,
        "repost_count": repost_count,
        "share_count": share_count,
    }


def extract_data_from_html(html: str, url: str) -> dict:
    """
    將 Threads 單帖的 HTML 解析成結構化 dict：
    - author
    - post_text（已移除 Follow / More / Translate / Like... 等）
    - metrics: likes, views, reply_count, repost_count, share_count
    - comments: 粗略抓一批留言 (user, text, likes, raw_block)
    """
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "url": url,
        "author": "",
        "post_text": "",
        "post_text_raw": "",
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

    # 1) 找主貼區塊：Threads 主文通常在第一個 data-pressable-container
    posts = soup.find_all("div", {"data-pressable-container": "true"})
    if not posts:
        print("⚠️ 找不到主文區塊")
        return data

    main_post = posts[0]
    full_text = main_post.get_text("\n", strip=True)
    data["post_text_raw"] = full_text  # 原始版，保留 debug 用

    lines = full_text.split("\n")

    # 2) 取出主文中的圖片 metadata
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

    # 3) 作者：跳過 UI 行，拿第一個真正的 username
    data["author"] = extract_block_user(lines)

    # 4) 主文純內容（移除 Translate / More / Like / Reply / Repost / Share）
    data["post_text"] = extract_block_body(lines)

    # 5) 主文互動數：Like / Reply / Repost / Share
    m = extract_metrics_from_lines(lines)
    data["metrics"]["likes"] = m["likes"]
    data["metrics"]["reply_count"] = m["reply_count"]
    data["metrics"]["repost_count"] = m["repost_count"]
    data["metrics"]["share_count"] = m["share_count"]

    # 6) Views：從整頁文字裡找包含 "views" 的字串（例如 "96K views"）
    views = 0
    for text_node in soup.stripped_strings:
        low = text_node.lower()
        # 避免吃到 "View 3 more replies" 類型
        if "views" in low and "reply" not in low and "view more" not in low:
            views = parse_number(text_node)
            break
    data["metrics"]["views"] = views

    # 7) 留言：用 posts[1:] 當留言區塊（sample，不是全量）
    for block in posts[1:]:
        raw_block = block.get_text("\n", strip=True)
        if not raw_block:
            continue

        block_lines = raw_block.split("\n")

        c_user = extract_block_user(block_lines)
        c_likes = extract_block_likes(block_lines)
        c_body = extract_block_body(block_lines)

        data["comments"].append(
            {
                "user": c_user,    # 乾淨 user，例如 ryoohkilo
                "text": c_body,    # 只剩留言內容（無 Translate / More / Like / Reply / Repost / Share）
                "likes": c_likes,  # 98, 75, ...
                "raw": raw_block,  # 原始 block，方便之後 debug / 清洗
            }
        )

    return data
