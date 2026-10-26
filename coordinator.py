"""Coordenador: fila de blocos, um fio por worker, retentativas e reatribuição."""
import argparse
import json
import os
import queue
import sys
import threading
import time

import httpx

import config
from common import load_baseline, merge, new_agg, split_blocks


class DistributedError(RuntimeError):
    pass


class Coordinator:
    def __init__(self, urls, timeout=None, max_attempts=None, backoff=None, health_interval=0.5, stall_timeout=10.0, on_event=None):
        self.on_event = on_event  # callback(evento, **dados) opcional, usado pela demo visual
        self.stall_timeout = stall_timeout
        self.urls = list(urls)
        self.timeout = timeout if timeout is not None else config.REQUEST_TIMEOUT
        self.max_attempts = max_attempts or config.MAX_ATTEMPTS
        self.backoff = backoff if backoff is not None else config.BACKOFF_BASE
        self.health_interval = health_interval
        self.reassignments = 0

    def _emit(self, event, **data):
        if self.on_event:
            self.on_event(event, **data)

    @staticmethod
    def store(results, body):
        """Guarda o resultado por block_id (sobrescreve, nunca soma). True se era o primeiro."""
        first = body["block_id"] not in results
        results[body["block_id"]] = body["agregado"]
        return first

    def run(self, path, block_size):
        blocks = split_blocks(path, block_size)
        q = queue.Queue()
        for b in blocks:
            q.put(b)
        results = {}              # block_id -> agregado (sobrescreve duplicatas)
        attempts = {b[0]: 0 for b in blocks}
        lock = threading.Lock()
        done = threading.Event()
        fatal = []
        pending = {"n": len(blocks)}
        suspect = set()
        all_down_since = {"t": None}
        if not blocks:
            return new_agg(), 0

        def healthy(client, url):
            try:
                return client.get(url + "/health", timeout=min(self.timeout, 5)).status_code == 200
            except httpx.HTTPError:
                return False

        def worker_loop(url):
            with httpx.Client() as client:
                while not done.is_set():
                    try:
                        bid, s, e = q.get(timeout=0.1)
                    except queue.Empty:
                        continue
                    self._emit("dispatch", block=bid, worker=url)
                    try:
                        r = client.post(url + "/process", json={"block_id": bid, "start": s, "end": e},
                                        timeout=self.timeout)
                        r.raise_for_status()
                        body = r.json()
                        with lock:
                            first = self.store(results, body)
                        self._emit("done", block=bid, worker=url)
                        with lock:
                            if first:
                                pending["n"] -= 1
                                if pending["n"] == 0:
                                    done.set()
                    except (httpx.HTTPError, ValueError, KeyError):
                        with lock:
                            attempts[bid] += 1
                            if attempts[bid] >= self.max_attempts:
                                fatal.append(f"bloco {bid} esgotou {self.max_attempts} tentativas")
                                done.set()
                                continue
                            self.reassignments += 1
                            n = attempts[bid]
                        self._emit("retry", block=bid, worker=url, attempt=n)
                        time.sleep(self.backoff * n)  # espera crescente
                        q.put((bid, s, e))
                        # worker suspeito: só volta a pegar blocos quando /health responder
                        with lock:
                            suspect.add(url)
                        self._emit("suspect", worker=url)
                        while not done.is_set() and not healthy(client, url):
                            time.sleep(self.health_interval)
                        with lock:
                            suspect.discard(url)
                        if not done.is_set():
                            self._emit("recovered", worker=url)

        threads = [threading.Thread(target=worker_loop, args=(u,), daemon=True) for u in self.urls]
        for t in threads:
            t.start()
        
        while not done.wait(0.2):
            with lock:
                if len(suspect) == len(self.urls):
                    if all_down_since["t"] is None:
                        all_down_since["t"] = time.time()
                    elif time.time() - all_down_since["t"] > self.stall_timeout:
                        fatal.append("todos os workers indisponíveis")
                        done.set()
                else:
                    all_down_since["t"] = None
        for t in threads:
            t.join(timeout=2)
        if fatal:
            raise DistributedError(fatal[0])
        if len(results) != len(blocks):
            raise DistributedError("resultado incompleto")
        total = new_agg()
        for bid in sorted(results):
            total = merge(total, results[bid])
        return total, len(blocks)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workers", default=os.environ.get("WORKER_URLS", ""),
                   help="URLs separadas por vírgula")
    p.add_argument("--data", default=config.DATA_PATH)
    p.add_argument("--block-size-mb", type=float, default=config.BLOCK_SIZE_MB)
    p.add_argument("--baseline", default=os.path.join(config.RESULTS_DIR, "baseline.json"))
    a = p.parse_args()
    urls = [u.strip() for u in a.workers.split(",") if u.strip()]
    if not urls:
        print("informe --workers ou WORKER_URLS", file=sys.stderr)
        return 2
    c = Coordinator(urls)
    t0 = time.perf_counter()
    try:
        agg, nblocks = c.run(a.data, int(a.block_size_mb * 1024 * 1024))
    except DistributedError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 3
    elapsed = time.perf_counter() - t0
    ok = agg == load_baseline(a.baseline)["agregado"]
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    with open(os.path.join(config.RESULTS_DIR, f"distributed_{len(urls)}.json"), "w") as f:
        json.dump({"workers": len(urls), "block_size_mb": a.block_size_mb, "blocos": nblocks,
                   "tempo_s": elapsed, "reatribuicoes": c.reassignments, "correto": ok}, f)
    print(f"distribuido x{len(urls)}: {elapsed:.2f}s blocos={nblocks} reatribuicoes={c.reassignments} "
          f"correto={'sim' if ok else 'NAO'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
