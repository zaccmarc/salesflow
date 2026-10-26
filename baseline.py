"""Baseline sequencial: oráculo independente (não usa split_blocks/process_block)."""
import argparse
import csv
import json
import os
import platform
import sys
import time

import config
from common import new_agg, process_row


def run_baseline(path):
    agg = new_agg()
    t0 = time.perf_counter()
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        next(reader, None)  # cabeçalho
        for row in reader:
            process_row(row, agg)
    return agg, time.perf_counter() - t0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default=config.DATA_PATH)
    p.add_argument("--out", default=os.path.join(config.RESULTS_DIR, "baseline.json"))
    a = p.parse_args()
    agg, elapsed = run_baseline(a.data)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    out = {"agregado": agg, "tempo_s": elapsed, "arquivo": a.data,
           "bytes": os.path.getsize(a.data), "python": platform.python_version()}
    with open(a.out, "w") as f:
        json.dump(out, f)
    print(f"baseline: {elapsed:.2f}s valid={agg['valid']} invalid={agg['invalid']} ignored={agg['ignored']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
