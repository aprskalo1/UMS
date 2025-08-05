import faiss
import csv, os, sys
from config import MUSIC_IDX, TEST_IDX, MAP_CSV, TOP_K

def load_mapping(path):
    if not os.path.exists(path):
        sys.exit(f"ERR: mapping not found: {path}")
    m = {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            try: m[int(row['faiss_id'])] = row['filename']
            except: pass
    return m

def load_query_vec(idx_path):
    idx = faiss.read_index(idx_path)
    v = idx.reconstruct(0)
    return v.astype('float32')

def search(idx_path, q_vec, k):
    idx = faiss.read_index(idx_path)
    d, i = idx.search(q_vec.reshape(1, -1), k)
    return d[0], i[0]

def cos_to_pct(cos_arr):
    return (cos_arr + 1.0) / 2.0 * 100.0

def main():
    id2fn = load_mapping(MAP_CSV)
    qv = load_query_vec(TEST_IDX)
    D, I = search(MUSIC_IDX, qv, TOP_K)
    pct = cos_to_pct(D)

    print(f"\nTop {TOP_K} (cosine→[0,100]):\n")
    print(f"{'Rk':<3} {'Score':>6}  ID   Filename")
    print("-"*40)
    for r, (sim, fid) in enumerate(zip(pct, I), start=1):
        fn = id2fn.get(int(fid), "<unknown>")
        print(f"{r:<3} {sim:6.1f}  {fid:3d}  {fn}")
    print()

if __name__ == "__main__":
    main()