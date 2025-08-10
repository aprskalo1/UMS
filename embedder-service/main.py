import os
import time
from config import AUDIO_PATH, MAPPING_PATH
from mapping_store import CSVMappingStore
from faiss_index import load_index, add_to_index, save_index
from audio_preparation import load_and_prep, windowed_embedding
from logger import logger

mapper = CSVMappingStore(MAPPING_PATH)
mapper.initialize()


def embed_all_audio():
    overall_start = time.time()
    index = load_index()
    processed = 0

    for fname in os.listdir(AUDIO_PATH):
        if not fname.lower().endswith('.wav'):
            continue
        path = os.path.join(AUDIO_PATH, fname)
        logger.info(f"Starting pipeline for {fname}")
        try:
            waveform = load_and_prep(path, do_denoise=False)
            vec = windowed_embedding(waveform)
            add_to_index(index, vec)
            new_id = index.ntotal - 1
            mapper.add(new_id, fname)
            processed += 1
            logger.info(f"Completed pipeline for {fname}")
        except Exception as e:
            logger.exception(f"Pipeline failed for {fname}: {e}")
            continue

    save_index(index)
    total_duration = time.time() - overall_start
    logger.info(f"Embedded {processed} files in {total_duration:.3f}s total.")


if __name__ == '__main__':
    embed_all_audio()
