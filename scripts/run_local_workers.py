"""Sobe N workers como processos locais (portas 8001..). Fallback sem Docker."""
import argparse
import os
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def start_workers(n, data_path, base_port=8001):
    """Devolve (lista de Popen, lista de URLs) quando todos respondem a /health."""
    env = dict(os.environ, DATA_PATH=os.path.abspath(data_path))
    procs, urls = [], []
    for i in range(n):
        port = base_port + i
        env_i = dict(env, PORT=str(port))
        procs.append(subprocess.Popen([sys.executable, "worker.py"], cwd=ROOT, env=env_i,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        urls.append(f"http://127.0.0.1:{port}")
    deadline = time.time() + 30
    for u in urls:
        while True:
            try:
                if httpx.get(u + "/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.time() > deadline:
                stop_workers(procs)
                raise RuntimeError(f"worker {u} não subiu")
            time.sleep(0.2)
    return procs, urls


def stop_workers(procs):
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=3)
    ap.add_argument("--data", default="data/sample.csv")
    a = ap.parse_args()
    procs, urls = start_workers(a.n, a.data)
    print("workers:", ",".join(urls), flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_workers(procs)
