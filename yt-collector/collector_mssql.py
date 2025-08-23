from typing import List, Dict, Set

from config import Settings
from dao_mssql import MSSQLDAO
from filters import iso8601_duration_to_seconds, should_keep_noncat10, should_keep_cat10
from yt_client import YouTubeClient

YOUTUBE_URL = "https://www.youtube.com/watch?v={}"


class Collector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.yt = YouTubeClient()
        self.dao = MSSQLDAO(settings)

    def discover_playlists(self, country: str, scene_cfg) -> List[Dict[str, str]]:
        seen_source: Set[str] = set()
        payloads = []
        for kw in scene_cfg.keywords:
            items = self.yt.search_playlists(q=kw, region_code=country,
                                             max_results=self.settings.global_.search_max_playlists_per_scene)
            for it in items:
                src_id = it["id"]["playlistId"]
                if src_id in seen_source:
                    continue
                seen_source.add(src_id)
                sn = it["snippet"]
                payloads.append({
                    "platform": "youtube",
                    "source_playlist_id": src_id,
                    "title": sn.get("title"),
                    "description": sn.get("description"),
                    "country": country,
                    "scene": scene_cfg.name,
                    "etag": it.get("etag")
                })
        self.dao.upsert_playlists(payloads)
        src_to_guid = self.dao.map_playlist_sources_to_guids("youtube", list(seen_source))
        return [{"source_playlist_id": src, "playlist_id": src_to_guid[src]} for src in src_to_guid.keys()]

    def ingest_playlist(self, playlist_source_id: str, playlist_guid: str, country: str, scene_cfg) -> int:
        items = self.yt.playlist_items(playlist_source_id)

        pq = self.settings.global_.playlist_quality or {}
        max_scan = int(pq.get("max_items_scan", 0) or 0)

        video_ids = [it["contentDetails"]["videoId"] for it in items if "contentDetails" in it]
        metas = self.yt.videos_metadata(video_ids)

        g = self.settings.global_
        laneA = (g.filtering.get("laneA_music") or {})
        laneB = (g.filtering.get("laneB_nonmusic") or {})
        positives_cfg = (g.filtering.get("positives") or {})
        negatives_cfg = (g.filtering.get("negatives") or {})
        allowed_cat10 = set(g.allow_categories or [10])
        allowed_nonmusic = set(laneB.get("allow_non_music_categories") or [])
        scene_langs = getattr(scene_cfg, "lang_codes", None)

        observed = len(items) if max_scan == 0 else min(max_scan, len(items))
        passed = 0

        rows = []
        seed_set = set(video_ids[: g.per_playlist_seed_count])

        meta_by_id = {m["id"]: m for m in metas}

        for idx, it in enumerate(items):
            vid = it["contentDetails"]["videoId"]
            m = meta_by_id.get(vid)
            if not m:
                continue
            sn = m.get("snippet", {})
            st = m.get("statistics", {})
            cd = m.get("contentDetails", {})
            status = m.get("status", {})

            title = sn.get("title") or ""
            cat = int(sn.get("categoryId") or 0)
            dur = iso8601_duration_to_seconds(cd.get("duration"))
            up = sn.get("publishedAt")
            from dateutil import parser as dtuparser
            try:
                upload_dt = dtuparser.parse(up) if up else None
            except Exception:
                upload_dt = None
            is_live = 1 if (sn.get("liveBroadcastContent") == "live") else 0
            made_for_kids = bool(status.get("madeForKids"))

            keep = (
                    (not made_for_kids)
                    and (0 < dur <= 1800)
                    and (
                            (cat in allowed_cat10 and should_keep_cat10(
                                title=title, dur=dur, is_live=is_live,
                                min_sec=int(laneA.get("min_sec", g.duration_min_sec)),
                                max_sec=int(laneA.get("max_sec", g.duration_max_sec)),
                                negatives_cfg=negatives_cfg
                            ))
                            or
                            (cat in allowed_nonmusic and should_keep_noncat10(
                                title=title, dur=dur, is_live=is_live,
                                scene_langs=scene_langs,
                                positives_cfg=positives_cfg,
                                negatives_cfg=negatives_cfg,
                                laneB_cfg=laneB,
                                topic_categories=[]
                            ))
                    )
            )

            if keep:
                if max_scan == 0 or idx < max_scan:
                    passed += 1

                rows.append({
                    "platform": "youtube",
                    "source_id": vid,
                    "source_url": f"https://www.youtube.com/watch?v={vid}",
                    "title": title,
                    "channel_id": sn.get("channelId"),
                    "category_id": cat,
                    "upload_date": upload_dt,
                    "duration_sec": dur,
                    "view_count": int(st.get("viewCount", 0) or 0),
                    "is_live": is_live,
                    "etag": m.get("etag"),
                    "country_bucket": country,
                    "scene": scene_cfg.name,
                    "seed": 1 if vid in seed_set else 0,
                })

        self.dao.upsert_tracks(rows)

        kept_ids = [r["source_id"] for r in rows]
        id_map = self.dao.map_source_ids_to_track_ids(kept_ids)

        links = []
        for pos, it in enumerate(items):
            vid = it["contentDetails"]["videoId"]
            tid = id_map.get(vid)
            if tid:
                links.append({"playlist_id": playlist_guid, "track_id": tid, "position": pos})
        self.dao.upsert_track_playlists(links)

        music_ratio = (passed / observed) if observed else 0.0
        size_penalty_after = int(pq.get("size_penalty_after", 200))
        size_penalty = max(0.0, (len(items) - size_penalty_after) / max(1, len(items))) if len(items) > size_penalty_after else 0.0
        trust_score = max(0.0, music_ratio - size_penalty)

        self.dao.update_playlist_quality(playlist_guid, music_ratio=music_ratio, size=len(items), trust_score=trust_score)

        min_ratio = float(pq.get("ignore_cooccurrence_below_ratio", 0.30))
        if music_ratio >= min_ratio:
            from collections import Counter
            ordered_tids = [id_map.get(it["contentDetails"]["videoId"]) for it in items if id_map.get(it["contentDetails"]["videoId"])]
            pair_counts = Counter()
            n = len(ordered_tids)
            for i in range(n):
                jmax = min(n, i + 50)
                for j in range(i + 1, jmax):
                    a = ordered_tids[i]
                    b = ordered_tids[j]
                    if not a or not b or a == b:
                        continue
                    lo, hi = (a, b) if a < b else (b, a)
                    pair_counts[(lo, hi)] += 1
            if pair_counts:
                pair_rows = [{"a": a, "b": b, "cnt": cnt} for (a, b), cnt in pair_counts.items()]
                self.dao.increment_cooccurrence(pair_rows)

        self.dao.touch_playlist_scanned(playlist_guid)
        return len(kept_ids)

    def crawl_scene(self, country: str, scene_cfg):
        pairs = self.discover_playlists(country, scene_cfg)
        total = 0
        for p in pairs:
            total += self.ingest_playlist(p["source_playlist_id"], p["playlist_id"], country, scene_cfg)
        return total
