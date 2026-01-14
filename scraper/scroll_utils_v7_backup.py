"""
Shared Playwright scrolling utilities to reduce duplicate wheel/height loops.
"""

def scroll_until_stable(
    page,
    max_loops: int = 15,
    wait_ms: int = 1200,
    wheel_px: int = 2800,
    stability_threshold: int = 3,
    on_loop=None,
):
    """
    Scrolls the page downward until scrollHeight stops growing or loop limit is reached.
    - on_loop: optional callable loop_idx -> bool; return True to break early.
    """
    stable_rounds = 0
    last_height = 0

    for loop_idx in range(max_loops):
        page.mouse.wheel(0, wheel_px)
        page.wait_for_timeout(wait_ms)

        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            stable_rounds += 1
        else:
            stable_rounds = 0
        last_height = height

        if on_loop and on_loop(loop_idx):
            break

        if stable_rounds >= stability_threshold:
            break
