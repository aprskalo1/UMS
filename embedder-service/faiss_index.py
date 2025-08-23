import os
from pathlib import Path
import faiss
import numpy as np
from config import FAISS_INDEX_PATH, EMBEDDING_DIM
from logger import logger


def _resolved_index_path() -> str:
    p = Path(FAISS_INDEX_PATH).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def load_index():
    path = _resolved_index_path()
    if os.path.exists(path):
        idx = faiss.read_index(path)
        logger.info(f"Loaded FAISS index: {path} (ntotal={idx.ntotal})")
        return idx
    idx = faiss.IndexFlatIP(EMBEDDING_DIM)
    logger.info(f"Created new FAISS index in memory (dim={EMBEDDING_DIM}); will save to {path}")
    return idx


def add_to_index(index, embedding: np.ndarray):
    # normalize
    norm = float(np.linalg.norm(embedding)) or 1.0
    vec = (embedding / norm).astype("float32")[None, :]
    index.add(vec)
    logger.info(f"Index add OK (ntotal={index.ntotal})")


def save_index(index):
    path = _resolved_index_path()
    faiss.write_index(index, path)
    logger.info(f"Saved FAISS index -> {path} (ntotal={index.ntotal})")
