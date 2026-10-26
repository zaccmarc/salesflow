"""Versão paralela (uma máquina): threads vs. processos, sem memória compartilhada."""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import config
from common import load_baseline, merge, new_agg, process_block, split_blocks


def run_parallel(path, executor="process", workers=2, block_size=None):
    """Devolve (agregado, tempo_s, n_blocos)."""
    block_size = block_size or int(config.BLOCK_SIZE_MB * 1024 * 1024)
    Pool = ThreadPoolExecutor if executor == "thread" else ProcessPoolExecutor
    t0 = time.perf_counter()
    blocks = split_blocks(path, block_size)
    total = new_agg()
    with Pool(max_workers=workers) as pool:
        futures = [pool.submit(process_block, path, s, e) for _, s, e in blocks]
        for fut in futures:
            total = merge(total, fut.result())
    return total, time.perf_counter() - t0, len(blocks)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--executor", choices=["thread", "process"], default="process")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--data", default=config.DATA_PATH)
    p.add_argument("--block-size-mb", type=float, default=config.BLOCK_SIZE_MB)
    p.add_argument("--baseline", default=os.path.join(config.RESULTS_DIR, "baseline.json"))
    a = p.parse_args()
    agg, elapsed, nblocks = run_parallel(a.data, a.executor, a.workers, int(a.block_size_mb * 1024 * 1024))
    base = load_baseline(a.baseline)
    ok = agg == base["agregado"]
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    out = os.path.join(config.RESULTS_DIR, f"parallel_{a.executor}_{a.workers}.json")
    with open(out, "w") as f:
        json.dump({"executor": a.executor, "workers": a.workers, "block_size_mb": a.block_size_mb,
                   "blocos": nblocks, "tempo_s": elapsed, "correto": ok}, f)
    print(f"{a.executor} x{a.workers}: {elapsed:.2f}s blocos={nblocks} correto={'sim' if ok else 'NAO'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
