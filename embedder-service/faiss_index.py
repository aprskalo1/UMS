import faiss
import numpy as np
import os
from config import FAISS_INDEX_PATH, EMBEDDING_DIM
from logger import logger
import time

def load_index():
    if os.path.exists(FAISS_INDEX_PATH):
        return faiss.read_index(FAISS_INDEX_PATH)
    return faiss.IndexFlatL2(EMBEDDING_DIM)

def add_to_index(index, embedding):
    start = time.time()
    try:
        vec = np.expand_dims(embedding, axis=0)
        index.add(vec)
        duration = time.time() - start
        logger.info(f"add_to_index took {duration:.3f}s")
    except Exception as e:
        logger.exception(f"Error in add_to_index: {e}")
        raise

def save_index(index):
    start = time.time()
    try:
        faiss.write_index(index, FAISS_INDEX_PATH)
        duration = time.time() - start
        logger.info(f"save_index took {duration:.3f}s")
    except Exception as e:
        logger.exception(f"Error in save_index: {e}")
        raise