"""
feed_hotlist_builder.py — Spec / Checklist
------------------------------------------
Goal: scroll the Threads feed, collect unique post URLs, and write a ranked
hotlist to feed_hotlist.json and feed_hotlist.txt. All existing behaviors
remain the same unless explicitly noted below.

New Requirement (6) Language Filtering (Chinese Traditional only)
-----------------------------------------------------------------
- After extracting each /post/... URL from the feed, perform language
  filtering before adding the URL to the hotlist set.
- Use only the feed preview snippet (DOM text near the <a href="/post/...">
  element). Do NOT open the full post.
- Keep posts whose preview text appears to be Traditional Chinese; filter out
  English-dominant, Simplified-dominant, emoji-only, or irrelevant snippets.

Implementation notes:
1) Snippet extraction when collecting URLs
   - When a new <a> pointing to "/post/..." is found, grab nearby text:
       parent = anchor.locator("xpath=../..")  # or similar DOM parent
       snippet = parent.text_content().strip()
   - This snippet feeds the language check.

2) Helper: looks_traditional_chinese(text: str) -> bool
   - Return True only when ALL hold:
       * Contains at least one Traditional Chinese character (any CJK char
         not in a Simplified-only list).
       * Simplified-only characters are < 30% of total chars.
       * ASCII letters A-Z a-z are < 40% of total chars.
   - Heuristic outline:
       simplified_only = {"体", "们", "这", "进", "发", ...}  # quick lookup
       for ch in text:
           if ch.isascii() and ch.isalpha(): ascii_count += 1
           elif "\u4e00" <= ch <= "\u9fff":
               if ch in simplified_only: simp_count += 1
               else: trad_count += 1
       total = len(text)
       trad_ratio = trad_count / total
       simp_ratio = simp_count / total
       ascii_ratio = ascii_count / total
       return trad_ratio > 0.15 and ascii_ratio < 0.40 and simp_ratio < 0.30
   - Any other case → return False.

3) Filtering during scrolling
   - For each new URL:
       if looks_traditional_chinese(snippet):
           add URL to the dedupe set
           print(f"🀄 保留繁中貼文: {url}")
       else:
           print(f"⛔ 濾掉非繁中: {url}")
   - Only URLs passing the check are kept.

4) Logging summary
   - Each loop logs keep/drop as above.
   - Final summary prints total kept vs. filtered counts.

5) Output behavior
   - feed_hotlist.json and feed_hotlist.txt MUST contain only filtered
     Traditional Chinese posts. Sorting/deduplication rules stay unchanged.

6) Hard cap at 50 posts per run
   - main() prompt changes to default 50:
       try:
           max_posts = int(input("最多想抓幾條貼文？（預設 50）\n> ") or "50")
       except ValueError:
           max_posts = 50
       if max_posts > 50:
           max_posts = 50
   - In collect_feed_post_urls, after turning the set into a list:
       urls_list = sorted(list(urls))
       if len(urls_list) > max_posts:
           urls_list = urls_list[:max_posts]
   - This guarantees a hard ceiling of 50 URLs per run, even if the user
     enters a larger number.

Other existing steps (unchanged)
--------------------------------
- Use Playwright with auth_threads.json for a logged-in feed view.
- Scroll the feed, collect <a href="/post/..."> links, deduplicate, and
  preserve existing ranking logic.
- Write both JSON and TXT outputs in the same structure/format as before.
"""
