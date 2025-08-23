# search_music.py
import os
import csv
import sys
from typing import Optional

import faiss
import numpy as np

from config import MUSIC_IDX, MAP_CSV, TOP_K  # TEST_IDX not used anymore


# ---------- helpers ----------

def prompt_path(prompt: str, default: str) -> str:
    while True:
        s = input(f"{prompt} [{default}]: ").strip()
        path = s or default
        if os.path.exists(path):
            return path
        print(f"Path does not exist: {path}")


def prompt_int(prompt: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> Optional[int]:
    while True:
        s = input(f"{prompt} [{default}]: ").strip()
        try:
            val = int(s) if s else default
        except ValueError:
            print("Please enter an integer.")
            continue
        if lo is not None and val < lo:
            print(f"Must be >= {lo}.")
            continue
        if hi is not None and val > hi:
            print(f"Must be <= {hi}.")
            continue
        return val


def prompt_yesno(prompt: str, default_no: bool = True) -> bool:
    s = input(f"{prompt} [y/N]: " if default_no else f"{prompt} [Y/n]: ").strip().lower()
    if not s:
        return not default_no
    return s.startswith("y")


def load_mapping(path: str) -> dict[int, str]:
    if not os.path.exists(path):
        sys.exit(f"ERR: mapping not found: {path}")
    m: dict[int, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        key_name = "db_id" if "db_id" in reader.fieldnames else ("filename" if "filename" in reader.fieldnames else None)
        if not key_name:
            return m
        for row in reader:
            try:
                m[int(row["faiss_id"])] = row[key_name]
            except Exception:
                continue
    return m


def load_index(idx_path: str):
    return faiss.read_index(idx_path)


def reconstruct_vec(index, fid: int) -> np.ndarray:
    ntotal = index.ntotal
    if fid < 0 or fid >= ntotal:
        sys.exit(f"ERR: query id {fid} out of range [0, {ntotal - 1}]")
    v = index.reconstruct(fid)  # works for IndexFlat*
    return np.asarray(v, dtype="float32")


def search(index, q_vec: np.ndarray, k: int, exclude_id: Optional[int] = None):
    want = k + (1 if exclude_id is not None else 0)
    D, I = index.search(q_vec.reshape(1, -1), want)
    sims, ids = D[0], I[0]
    if exclude_id is not None:
        mask = ids != exclude_id
        sims, ids = sims[mask], ids[mask]
    return sims[:k], ids[:k]


def cos_to_pct(cos_arr: np.ndarray) -> np.ndarray:
    return (cos_arr + 1.0) * 50.0


# ---------- main interactive loop ----------

def main():
    print("\n=== FAISS music search (query by FAISS id from music.index) ===\n")

    # choose paths (with defaults from config)
    idx_path = prompt_path("Path to music.index", MUSIC_IDX)
    map_path = prompt_path("Path to mapping.csv", MAP_CSV)

    # load once; you can restart program to switch paths
    index = load_index(idx_path)
    id2db = load_mapping(map_path)

    print(f"\nLoaded index: {os.path.abspath(idx_path)} (ntotal={index.ntotal})")
    print(f"Loaded mapping: {os.path.abspath(map_path)} (entries={len(id2db)})\n")

    default_qid = 0
    default_k = TOP_K

    while True:
        if index.ntotal == 0:
            print("Index is empty. Exiting.")
            return

        qid = prompt_int("Query FAISS id", default_qid, lo=0, hi=index.ntotal - 1)
        k = prompt_int("Top-K", default_k, lo=1, hi=index.ntotal)
        include_self = prompt_yesno("Include the query item itself in results?", default_no=True)

        q_vec = reconstruct_vec(index, qid)
        D, I = search(index, q_vec, k, exclude_id=None if include_self else qid)
        pct = cos_to_pct(np.asarray(D))

        q_db = id2db.get(qid, "<unknown>")

        print(f"\nQuery: FAISS id={qid}  DB={q_db}")
        print(f"Index: {idx_path}  (ntotal={index.ntotal})\n")
        print(f"{'Rk':<3} {'Score':>6}  {'FAISS_ID':>8}  DB_ID")
        print("-" * 60)
        for r, (sim, fid) in enumerate(zip(pct, I), start=1):
            print(f"{r:<3} {sim:6.1f}  {int(fid):8d}  {id2db.get(int(fid), '<unknown>')}")
        print()

        default_qid = qid
        default_k = k
        if not prompt_yesno("Search again?", default_no=False):
            break


if __name__ == "__main__":
    main()
