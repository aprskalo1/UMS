import re
from typing import Optional

from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0  # deterministic


def iso8601_duration_to_seconds(iso: str) -> int:
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", iso or "")
    if not m: return 0
    h = int(m.group(1) or 0)
    mn = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mn * 60 + s


def safe_lang(text: str) -> str:
    try:
        return detect((text or "")[:200])
    except Exception:
        return ""


def has_any_token(text: str, tokens) -> bool:
    t = (text or "").lower()
    return any(tok.lower() in t for tok in tokens or [])


def strong_negative(text: str, negatives_cfg: dict) -> bool:
    t = (text or "").lower()
    for group in ("global", "kids_gaming", "drama_series"):
        for tok in (negatives_cfg.get(group) or []):
            if tok.lower() in t:
                return True
    return False


def positive_signal_count(title: str, positives_cfg: dict, topic_categories: list[str]) -> int:
    cnt = 0
    if has_any_token(title, (positives_cfg.get("title_tokens") or [])):
        cnt += 1
    if topic_categories:
        cnt += 1
    return cnt


def should_keep_cat10(title: str, dur: int, is_live: int,
                      min_sec: int, max_sec: int,
                      negatives_cfg: dict) -> bool:
    if is_live: return False
    if dur < min_sec or dur > max_sec: return False
    if strong_negative(title, negatives_cfg): return False
    return True


def should_keep_noncat10(title: str,
                         dur: int,
                         is_live: int,
                         scene_langs: Optional[list[str]],
                         positives_cfg: dict,
                         negatives_cfg: dict,
                         laneB_cfg: dict,
                         topic_categories: list[str]) -> bool:
    if is_live: return False
    if dur < int(laneB_cfg.get("min_sec", 90)) or dur > int(laneB_cfg.get("max_sec", 420)):
        return False
    if strong_negative(title, negatives_cfg):
        return False
    if scene_langs:
        lang = safe_lang(title)
        if lang and lang not in set(scene_langs):
            return False
    need = int(laneB_cfg.get("require_positive_signals", 2))
    pos = positive_signal_count(title, positives_cfg, topic_categories if laneB_cfg.get("allow_if_topic_music", True) else [])
    return pos >= need
