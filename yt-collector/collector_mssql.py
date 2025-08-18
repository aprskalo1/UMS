from collections import Counter
from typing import List, Dict, Any, Set
from dateutil import parser as dtparser
from config import Settings
from dao_mssql import MSSQLDAO
from filters import iso8601_duration_to_seconds, looks_like_music
from yt_client import YouTubeClient

YOUTUBE_URL = "https://www.youtube.com/watch?v={}"


class Collector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.yt = YouTubeClient()
        self.dao = MSSQLDAO(settings)

    def discover_playlists(self, country: str, scene_cfg) -> List[str]:
        seen: Set[str] = set()
        payloads = []
        for kw in scene_cfg.keywords:
            items = self.yt.search_playlists(q=kw, region_code=country, max_results=self.settings.global_.search_max_playlists_per_scene)
            for it in items:
                pid = it["id"]["playlistId"]
                if pid in seen: continue
                seen.add(pid)
                sn = it["snippet"]
                payloads.append({
                    "playlist_id": pid,
                    "title": sn.get("title"),
                    "description": sn.get("description"),
                    "country": country,
                    "scene": scene_cfg.name,
                    "etag": it.get("etag")
                })
        self.dao.upsert_playlists(payloads)
        return list(seen)

    def ingest_playlist(self, playlist_id: str, country: str, scene_cfg):
        items = self.yt.playlist_items(playlist_id)
        video_ids = [it["contentDetails"]["videoId"] for it in items if "contentDetails" in it]

        metas = self.yt.videos_metadata(video_ids)
        track_rows: List[Dict[str, Any]] = []
        allowed = set(self.settings.global_.allow_categories)

        seed_set = set(video_ids[: self.settings.global_.per_playlist_seed_count])

        for m in metas:
            vid = m["id"]
            sn = m.get("snippet", {})
            st = m.get("statistics", {})
            cd = m.get("contentDetails", {})
            title = sn.get("title") or ""
            cat = int(sn.get("categoryId") or 0)

            dur = iso8601_duration_to_seconds(cd.get("duration"))
            up = sn.get("publishedAt")
            try:
                upload_dt = dtparser.parse(up) if up else None
            except Exception:
                upload_dt = None

            track_rows.append({
                "platform": "youtube",
                "source_id": vid,
                "source_url": YOUTUBE_URL.format(vid),
                "title": title,
                "description": sn.get("description"),
                "channel_id": sn.get("channelId"),
                "channel_title": sn.get("channelTitle"),
                "category_id": cat,
                "upload_date": upload_dt,
                "duration_sec": dur,
                "view_count": int(st.get("viewCount", 0) or 0),
                "like_count": int(st.get("likeCount", 0) or 0),
                "comment_count": int(st.get("commentCount", 0) or 0),
                "default_audio_language": sn.get("defaultAudioLanguage"),
                "default_language": sn.get("defaultLanguage"),
                "is_live": 1 if (cd.get("caption") == "live" or (sn.get("liveBroadcastContent") == "live")) else 0,
                "is_music": 1 if looks_like_music(title, cat, allowed) else 0,
                "country_bucket": country,
                "scene": scene_cfg.name,
                "seed": 1 if vid in seed_set else 0,
                "etag": m.get("etag"),
                "source_playlist_ids": f'["{playlist_id}"]',
            })

        self.dao.upsert_tracks(track_rows)

        id_map = self.dao.map_source_ids_to_track_ids(video_ids)

        links = []
        for pos, it in enumerate(items):
            vid = it["contentDetails"]["videoId"]
            tid = id_map.get(vid)
            if not tid: continue
            links.append({"playlist_id": playlist_id, "track_id": tid, "position": pos})
        self.dao.upsert_track_playlists(links)

        ordered_tids = [id_map.get(it["contentDetails"]["videoId"]) for it in items if id_map.get(it["contentDetails"]["videoId"])]
        pair_counts = Counter()
        n = len(ordered_tids)
        for i in range(n):
            jmax = min(n, i + 50)
            for j in range(i + 1, jmax):
                a = ordered_tids[i]
                b = ordered_tids[j]
                if a == b: continue
                lo, hi = (a, b) if a < b else (b, a)
                pair_counts[(lo, hi)] += 1

        if pair_counts:
            pair_rows = [{"a": a, "b": b, "cnt": cnt} for (a, b), cnt in pair_counts.items()]
            self.dao.increment_cooccurrence(pair_rows)

    def crawl_scene(self, country: str, scene_cfg):
        playlist_ids = self.discover_playlists(country, scene_cfg)
        for pid in playlist_ids:
            self.ingest_playlist(pid, country, scene_cfg)
