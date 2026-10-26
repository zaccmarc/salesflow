"""Benchmark: roda a matriz de configurações, confere cada rodada contra o baseline."""
import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import threading

import config
from baseline import run_baseline
from coordinator import Coordinator, DistributedError
from parallel import run_parallel
from scripts.run_local_workers import start_workers, stop_workers

MB = 1024 * 1024
FIELDS = ["configuracao", "repeticao", "tempo_s", "speedup", "correto", "reatribuicoes"]


def machine_info():
    mem_kb = 0
    try:
        with open("/proc/meminfo") as f:
            mem_kb = int(f.readline().split()[1])
    except OSError:
        pass
    cpu = platform.processor()
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except (OSError, StopIteration):
        pass
    return {"so": platform.platform(), "cpu": cpu, "nucleos": os.cpu_count(),
            "ram_gb": round(mem_kb / 1024 / 1024, 1), "python": platform.python_version()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=config.DATA_PATH)
    ap.add_argument("--repeats", type=int, default=config.REPEATS)
    ap.add_argument("--block-size-mb", type=float, default=config.BLOCK_SIZE_MB)
    ap.add_argument("--block-sizes-mb", default="16,64,256")
    ap.add_argument("--kill-after", type=float, default=0.6, help="segundos até derrubar o worker")
    a = ap.parse_args()
    bs = int(a.block_size_mb * MB)
    rows = []

    run_baseline(a.data)  # aquecimento do cache de disco (não registrado)
    base_agg, _ = run_baseline(a.data)
    base_times = []
    for r in range(1, a.repeats + 1):
        agg, t = run_baseline(a.data)
        base_times.append(t)
        rows.append(["baseline", r, t, None, "sim" if agg == base_agg else "nao", 0])
    t_base = statistics.median(base_times)
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    with open(os.path.join(config.RESULTS_DIR, "baseline.json"), "w") as f:
        json.dump({"agregado": base_agg, "tempo_s": t_base, "arquivo": a.data,
                   "bytes": os.path.getsize(a.data), "python": platform.python_version()}, f)

    def record(name, r, t, agg, reat=0):
        ok = agg == base_agg
        rows.append([name, r, t, t_base / t, "sim" if ok else "nao", reat])
        print(f"{name} #{r}: {t:.2f}s correto={'sim' if ok else 'NAO'} reat={reat}", flush=True)

    for ex in ("thread", "process"):
        for w in (2, 4, 8):
            for r in range(1, a.repeats + 1):
                agg, t, _ = run_parallel(a.data, ex, w, bs)
                record(f"paralelo-{ex}-{w}", r, t, agg)

    for n in (1, 2, 3):
        procs, urls = start_workers(n, a.data)
        try:
            for r in range(1, a.repeats + 1):
                c = Coordinator(urls)
                import time
                t0 = time.perf_counter()
                agg, _ = c.run(a.data, bs)
                record(f"distribuido-{n}", r, time.perf_counter() - t0, agg, c.reassignments)
        finally:
            stop_workers(procs)

    # Falha: derruba 1 de 3 workers no meio da execução (blocos de 16 MB para haver muitos blocos)
    import time
    for r in range(1, a.repeats + 1):
        procs, urls = start_workers(3, a.data)
        try:
            c = Coordinator(urls, timeout=config.REQUEST_TIMEOUT)
            threading.Timer(a.kill_after, procs[1].kill).start()
            t0 = time.perf_counter()
            try:
                agg, _ = c.run(a.data, 16 * MB)
                record("falha-distribuido-3-1morto", r, time.perf_counter() - t0, agg, c.reassignments)
            except DistributedError as e:
                rows.append(["falha-distribuido-3-1morto", r, time.perf_counter() - t0, None, "nao", c.reassignments])
                print("falha:", e)
        finally:
            stop_workers(procs)

    # Tamanho do bloco (workers fixos: processos x4 e distribuído x3)
    sizes = [float(x) for x in a.block_sizes_mb.split(",")]
    for s in sizes:
        for r in range(1, a.repeats + 1):
            agg, t, _ = run_parallel(a.data, "process", 4, int(s * MB))
            record(f"bloco{int(s)}MB-processo-4", r, t, agg)
    procs, urls = start_workers(3, a.data)
    try:
        for s in sizes:
            for r in range(1, a.repeats + 1):
                c = Coordinator(urls)
                t0 = time.perf_counter()
                agg, _ = c.run(a.data, int(s * MB))
                record(f"bloco{int(s)}MB-distribuido-3", r, time.perf_counter() - t0, agg, c.reassignments)
    finally:
        stop_workers(procs)

    with open(os.path.join(config.RESULTS_DIR, "tempos.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        w.writerows(rows)
    with open(os.path.join(config.RESULTS_DIR, "maquina.json"), "w") as f:
        json.dump(machine_info(), f, indent=2)
    subprocess.run([sys.executable, os.path.join("scripts", "make_charts.py")], check=True)
    bad = [r for r in rows if r[4] != "sim"]
    print(f"{len(rows)} rodadas, {len(bad)} incorretas")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
