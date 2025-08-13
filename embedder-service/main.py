import os, time
from logger import logger
from config import (
    MAPPING_PATH,
    YT_START_SECONDS, YT_CLIP_SECONDS, BATCH_LIMIT
)
from mapping_store import CSVMappingStore, SQLMappingStore, CompositeMappingStore
from faiss_index import load_index, add_to_index, save_index
from audio_preparation import load_and_prep, windowed_embedding
from stream_media import resolve_youtube_media, stream_clip_to_temp_wav
from db_mssql import fetch_batch_to_process, mark_processed, mark_failed

csv_map = CSVMappingStore(MAPPING_PATH)
sql_map = SQLMappingStore()
mapper = CompositeMappingStore([csv_map, sql_map])
mapper.initialize()


def embed_from_db_once() -> int:
    start_wall = time.time()
    index = load_index()
    processed = 0

    jobs = fetch_batch_to_process(limit=BATCH_LIMIT)
    if not jobs:
        logger.info("No pending YouTube rows.")
    else:
        for job in jobs:
            jid = job["id"]
            url = job["source_url"]
            start_s = job.get("start_s", YT_START_SECONDS)
            dur_s = job.get("dur_s", YT_CLIP_SECONDS)
            tmp_wav = None

            logger.info(f"[{jid}] Resolving YouTube: {url}")
            try:
                media_url, headers = resolve_youtube_media(url)
                tmp_wav = stream_clip_to_temp_wav(media_url, headers, start_s=start_s, dur_s=dur_s)

                waveform = load_and_prep(tmp_wav, do_denoise=False)
                vec = windowed_embedding(waveform)
                add_to_index(index, vec)

                new_id = index.ntotal - 1
                mapper.add(new_id, f"{jid}")
                mark_processed(jid)
                processed += 1
                logger.info(f"[{jid}] Completed. FAISS id={new_id}")

            except Exception as e:
                logger.exception(f"[{jid}] Failed: {e}")
                try:
                    mark_failed(jid, str(e))
                except Exception as e2:
                    logger.exception(f"[{jid}] Mark-failed error: {e2}")
            finally:
                if tmp_wav and os.path.exists(tmp_wav):
                    try:
                        os.remove(tmp_wav)
                    except:
                        pass

    save_index(index)
    logger.info(f"Cycle complete: processed={processed} in {time.time() - start_wall:.3f}s")
    return processed


if __name__ == '__main__':
    POLL_SECONDS = 3
    while True:
        n = embed_from_db_once()
        if n == 0:
            time.sleep(POLL_SECONDS)
