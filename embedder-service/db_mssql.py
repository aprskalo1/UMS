import pyodbc
from typing import List, Dict, Any
from datetime import datetime, timezone
from config import build_default_conn_str

CONN_STR = build_default_conn_str()


def get_conn():
    return pyodbc.connect(CONN_STR, autocommit=False)


def fetch_batch_to_process(limit: int = 16) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sql = """
    ;WITH cte AS (
        SELECT TOP (?) id, platform, source_url, start_s, dur_s, status
        FROM dbo.Tracks WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE status='collected' AND platform='youtube'
        ORDER BY collected_at
    )
    UPDATE cte
       SET status = 'processing'
    OUTPUT
       INSERTED.id,
       INSERTED.platform,
       INSERTED.source_url,
       COALESCE(INSERTED.start_s, 0) AS start_s,
       COALESCE(INSERTED.dur_s,   0) AS dur_s;
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (limit,))
        for r in cur.fetchall():
            rows.append({
                "id": str(r[0]),
                "platform": r[1],
                "source_url": r[2],
                "start_s": int(r[3]),
                "dur_s": int(r[4]),
            })
        conn.commit()
    return rows


def mark_processed(id_: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dbo.Tracks SET status='processed', processed_at=? WHERE id=?",
            datetime.now(timezone.utc), id_
        )
        conn.commit()


def mark_failed(id_: str, msg: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dbo.Tracks SET status='failed', error=? WHERE id=?",
            msg[:4000], id_
        )
        conn.commit()
