from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine
from config import get_sqlalchemy_url, Settings


class MSSQLDAO:
    def __init__(self, settings: Optional[Settings] = None):
        sa_url = get_sqlalchemy_url(settings)
        self.engine: Engine = create_engine(
            sa_url,
            connect_args={"autocommit": False},
            fast_executemany=True,
            future=True
        )

    def upsert_playlists(self, playlists: List[Dict[str, Any]]):
        if not playlists: return
        sql = text("""
            MERGE dbo.Playlists AS tgt
            USING (SELECT :playlist_id AS playlist_id, :title AS title, :description AS description,
                          :country AS country, :scene AS scene, :etag AS etag) AS src
            ON (tgt.playlist_id = src.playlist_id)
            WHEN MATCHED THEN UPDATE SET
              tgt.title = src.title,
              tgt.description = src.description,
              tgt.country = COALESCE(tgt.country, src.country),
              tgt.scene = COALESCE(tgt.scene, src.scene),
              tgt.etag = src.etag,
              tgt.last_scanned_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT
              (playlist_id, title, description, country, scene, etag, created_at, last_scanned_at)
            VALUES
              (src.playlist_id, src.title, src.description, src.country, src.scene, src.etag, SYSUTCDATETIME(), SYSUTCDATETIME());
        """)
        with self.engine.begin() as cx:
            cx.execute(sql, playlists)

    def upsert_tracks(self, rows: List[Dict[str, Any]]):
        if not rows: return
        sql = text("""
            MERGE dbo.Tracks AS tgt
            USING (
                SELECT
                  :platform AS platform, :source_id AS source_id, :source_url AS source_url,
                  :title AS title, :description AS description, :channel_id AS channel_id, :channel_title AS channel_title,
                  :category_id AS category_id, :upload_date AS upload_date, :duration_sec AS duration_sec,
                  :view_count AS view_count, :like_count AS like_count, :comment_count AS comment_count,
                  :default_audio_language AS default_audio_language, :default_language AS default_language,
                  :is_live AS is_live, :is_music AS is_music, :country_bucket AS country_bucket, :scene AS scene,
                  :seed AS seed, :etag AS etag, :source_playlist_ids AS source_playlist_ids
            ) AS src
            ON (tgt.platform = src.platform AND tgt.source_id = src.source_id)
            WHEN MATCHED THEN UPDATE SET
              tgt.title = src.title,
              tgt.description = src.description,
              tgt.channel_id = src.channel_id,
              tgt.channel_title = src.channel_title,
              tgt.category_id = src.category_id,
              tgt.upload_date = src.upload_date,
              tgt.duration_sec = src.duration_sec,
              tgt.view_count = src.view_count,
              tgt.like_count = src.like_count,
              tgt.comment_count = src.comment_count,
              tgt.default_audio_language = src.default_audio_language,
              tgt.default_language = src.default_language,
              tgt.is_live = src.is_live,
              tgt.is_music = src.is_music,
              tgt.country_bucket = COALESCE(tgt.country_bucket, src.country_bucket),
              tgt.scene = COALESCE(tgt.scene, src.scene),
              tgt.seed = IIF(tgt.seed=1, 1, src.seed),
              tgt.etag = src.etag,
              tgt.source_playlist_ids = src.source_playlist_ids,
              tgt.last_checked_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT
              (platform, source_id, source_url, status, title, description, channel_id, channel_title,
               category_id, upload_date, duration_sec, view_count, like_count, comment_count,
               default_audio_language, default_language, is_live, is_music, country_bucket, scene, seed, etag,
               source_playlist_ids, collected_at)
            VALUES
              (src.platform, src.source_id, src.source_url, 'collected', src.title, src.description, src.channel_id, src.channel_title,
               src.category_id, src.upload_date, src.duration_sec, src.view_count, src.like_count, src.comment_count,
               src.default_audio_language, src.default_language, src.is_live, src.is_music, src.country_bucket, src.scene, src.seed, src.etag,
               src.source_playlist_ids, SYSUTCDATETIME());
        """)
        with self.engine.begin() as cx:
            cx.execute(sql, rows)

    def map_source_ids_to_track_ids(self, source_ids: list[str]) -> dict[str, str]:
        if not source_ids:
            return {}

        ids = list(dict.fromkeys(source_ids))
        mapping: dict[str, str] = {}

        sql = text("""
                   SELECT source_id, id
                   FROM dbo.Tracks
                   WHERE platform = 'youtube'
                     AND source_id IN :ids
                   """).bindparams(bindparam("ids", expanding=True))

        CHUNK = 900
        with self.engine.begin() as cx:
            for i in range(0, len(ids), CHUNK):
                chunk = ids[i:i + CHUNK]
                rows = cx.execute(sql, {"ids": chunk}).all()
                for r in rows:
                    mapping[r.source_id] = str(r.id)

        return mapping

    def upsert_track_playlists(self, links: List[Dict[str, Any]]):
        if not links: return
        sql = text("""
            MERGE dbo.TrackPlaylists AS tgt
            USING (SELECT :playlist_id AS playlist_id, :track_id AS track_id, :position AS position) AS src
            ON (tgt.playlist_id = src.playlist_id AND tgt.track_id = src.track_id)
            WHEN MATCHED THEN UPDATE SET
              tgt.last_seen_at = SYSUTCDATETIME(),
              tgt.position = COALESCE(src.position, tgt.position)
            WHEN NOT MATCHED THEN INSERT (playlist_id, track_id, position, first_seen_at, last_seen_at)
            VALUES (src.playlist_id, src.track_id, src.position, SYSUTCDATETIME(), SYSUTCDATETIME());
        """)
        with self.engine.begin() as cx:
            cx.execute(sql, links)

    def increment_cooccurrence(self, pairs: List[Dict[str, Any]]):
        if not pairs: return
        sql = text("""
            MERGE dbo.CoOccurrence AS tgt
            USING (SELECT :a AS track_id_a, :b AS track_id_b, :cnt AS count) AS src
            ON (tgt.track_id_a = src.track_id_a AND tgt.track_id_b = src.track_id_b)
            WHEN MATCHED THEN UPDATE SET tgt.count = tgt.count + src.count
            WHEN NOT MATCHED THEN INSERT (track_id_a, track_id_b, count)
            VALUES (src.track_id_a, src.track_id_b, src.count);
        """)
        with self.engine.begin() as cx:
            cx.execute(sql, pairs)
