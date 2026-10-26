"""Gráficos a partir de resultados/tempos.csv (somente rodadas corretas, mediana)."""
import csv
import os
import statistics
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = os.environ.get("RESULTS_DIR", "resultados")


def medians(path=None):
    """{configuracao: {'tempo': mediana, 'speedup': mediana|None, 'reat': mediana}} só com correto=sim."""
    d = defaultdict(list)
    with open(path or os.path.join(RES, "tempos.csv")) as f:
        for r in csv.DictReader(f):
            if r["correto"] == "sim":
                d[r["configuracao"]].append(r)
    out = {}
    for k, rs in d.items():
        sp = [float(r["speedup"]) for r in rs if r["speedup"]]
        out[k] = {"tempo": statistics.median(float(r["tempo_s"]) for r in rs),
                  "speedup": statistics.median(sp) if sp else None,
                  "reat": statistics.median(int(r["reatribuicoes"]) for r in rs), "n": len(rs)}
    return out


def main():
    m = medians()
    ws = [2, 4, 8]
    plt.figure(figsize=(6, 4))
    for ex, label in (("thread", "threads"), ("process", "processos")):
        plt.plot(ws, [m[f"paralelo-{ex}-{w}"]["speedup"] for w in ws], marker="o", label=label)
    plt.axhline(1, color="gray", ls="--", lw=1)
    plt.xlabel("workers"); plt.ylabel("speedup vs. baseline"); plt.xticks(ws)
    plt.title("Paralelo: speedup (mediana)"); plt.legend(); plt.tight_layout()
    plt.savefig(os.path.join(RES, "speedup_paralelo.png"), dpi=120); plt.close()

    ns = [1, 2, 3]
    plt.figure(figsize=(6, 4))
    plt.plot(ns, [m[f"distribuido-{n}"]["tempo"] for n in ns], marker="o", label="distribuído")
    plt.axhline(m["baseline"]["tempo"], color="gray", ls="--", label="baseline")
    plt.xlabel("workers"); plt.ylabel("tempo (s)"); plt.xticks(ns)
    plt.title("Distribuído: tempo (mediana)"); plt.legend(); plt.tight_layout()
    plt.savefig(os.path.join(RES, "tempo_distribuido.png"), dpi=120); plt.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
