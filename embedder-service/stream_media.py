import subprocess, tempfile, os
from typing import Dict, Tuple, Optional
import yt_dlp
from logger import logger
from config import TARGET_SAMPLING_RATE


def resolve_youtube_media(page_url: str) -> Tuple[str, Dict[str, str]]:
    ydl_opts = {
        "quiet": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,
        "geo_bypass": True,
        "retries": 2,
        "socket_timeout": 15,
        "extractor_args": {"youtube": {"player_client": ["android"]}},
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(page_url, download=False)

    def pick_best(formats: list) -> Optional[Dict[str, any]]:
        if not formats:
            return None
        audio_only = [
            f for f in formats
            if f.get("url")
               and (f.get("vcodec") in (None, "none"))
               and (f.get("acodec") not in (None, "none"))
        ]
        candidates = audio_only or [f for f in formats if f.get("url")]

        def score(f):
            proto = (f.get("protocol") or "").lower()
            is_progressive = 0 if ("m3u8" in proto or proto == "http_dash_segments" or f.get("ext") == "m3u8") else 1
            return is_progressive, f.get("abr") or f.get("tbr") or 0, f.get("asr") or 0

        return max(candidates, key=score) if candidates else None

    req = info.get("requested_downloads") or []
    best = pick_best(req) or pick_best(info.get("formats") or [])

    if best:
        headers = best.get("http_headers") or info.get("http_headers") or {}
        return best["url"], headers

    if info.get("url"):
        return info["url"], info.get("http_headers", {})

    raise RuntimeError("No playable formats with direct URL")


def _headers_to_ffmpeg_arg(headers: Dict[str, str]) -> Optional[str]:
    if not headers:
        return None

    return "".join([f"{k}: {v}\r\n" for k, v in headers.items()])


def stream_clip_to_temp_wav(media_url: str, headers: Dict[str, str], start_s: int, dur_s: int) -> str:
    hdr_arg = _headers_to_ffmpeg_arg(headers)
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp_path = tmp.name
    tmp.close()

    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]

    if start_s and start_s > 0:
        cmd += ["-ss", str(start_s)]

    cmd += [
        "-headers", hdr_arg if hdr_arg else "",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-i", media_url, "-vn", "-ac", "1", "-ar", str(TARGET_SAMPLING_RATE)
    ]

    if dur_s and dur_s > 0:
        cmd += ["-t", str(dur_s)]

    cmd += ["-y", tmp_path]
    cmd = [c for c in cmd if c != ""]

    logger.info(f"ffmpeg (YouTube) -> {tmp_path} [ss={start_s}, t={'FULL' if not (dur_s and dur_s > 0) else dur_s}]")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        raise RuntimeError(f"ffmpeg failed: {e}") from e

    return tmp_path
