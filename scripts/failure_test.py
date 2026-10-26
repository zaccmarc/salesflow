"""Teste de falha com Docker: derruba worker2 durante a execução. Roda só se o Docker estiver disponível.

Uso: python scripts/failure_test.py   (precisa de data/sample.csv e resultados/baseline.json)
"""
import json
import subprocess
import sys
import threading
import time


def sh(*cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def docker_ok():
    try:
        return sh("docker", "info").returncode == 0 and sh("docker", "compose", "version").returncode == 0
    except FileNotFoundError:
        return False


def main():
    if not docker_ok():
        print("Docker indisponível: teste de falha com Docker NÃO executado (use pytest tests/test_failure.py).")
        return 0
    try:
        sh("docker", "compose", "up", "-d", "--build", "worker1", "worker2", "worker3", check=True)
        time.sleep(5)
        threading.Timer(1.0, lambda: sh("docker", "compose", "stop", "worker2")).start()
        r = sh("docker", "compose", "--profile", "run", "run", "--rm", "coordinator",
               "python", "coordinator.py", "--data", "/data/sample.csv", "--block-size-mb", "16")
        print(r.stdout, r.stderr)
        res = json.load(open("resultados/distributed_3.json"))
        ok = r.returncode == 0 and res["correto"]
        print(f"correto={ok} reatribuicoes={res['reatribuicoes']}")
        return 0 if ok else 1
    finally:
        sh("docker", "compose", "--profile", "run", "down")


if __name__ == "__main__":
    sys.exit(main())
