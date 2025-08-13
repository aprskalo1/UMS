import subprocess, tempfile, os
from typing import Dict, Tuple, Optional
import yt_dlp
from logger import logger
from config import TARGET_SAMPLING_RATE


def resolve_youtube_media(page_url: str) -> Tuple[str, Dict[str, str]]:
    ydl_opts = {"quiet": True, "noplaylist": True, "skip_download": True, "extract_flat": False}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(page_url, download=False)

    fmts = info.get("formats") or []
    audio_only = [f for f in fmts if (f.get("vcodec") in (None, "none")) and (f.get("acodec") not in (None, "none"))]

    def score(f):
        is_progressive = 0 if ("hls" in (f.get("protocol") or "") or f.get("ext") == "m3u8") else 1
        return is_progressive, f.get("abr") or 0, f.get("asr") or 0

    if audio_only:
        best = sorted(audio_only, key=score, reverse=True)[0]
        return best["url"], best.get("http_headers", info.get("http_headers", {}))

    return info["url"], info.get("http_headers", {})


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
