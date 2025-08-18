import re

SUS_WORDS = re.compile(r"(sped up|slowed|nightcore|8d|8\-?d|lyrics|karaoke)", re.I)


def iso8601_duration_to_seconds(iso: str) -> int:
    import re
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m: return 0
    h = int(m.group(1) or 0)
    mn = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mn * 60 + s


def looks_like_music(title: str, category_id: int, allowed_categories) -> bool:
    if category_id not in allowed_categories: return False
    if SUS_WORDS.search(title or ""): return False
    return True
