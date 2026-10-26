import threading
import time

import pytest

from coordinator import Coordinator, DistributedError
from scripts.run_local_workers import start_workers, stop_workers


def test_duplicate_block_result_is_idempotent():
    results = {}
    body = {"block_id": 3, "agregado": {"valid": 5}}
    assert Coordinator.store(results, body) is True
    assert Coordinator.store(results, body) is False  # duplicata sobrescreve, não soma
    assert results == {3: {"valid": 5}}


def test_timeout_reassigns_block(sample):
    path, base = sample
    # worker lento (excede o timeout) + worker normal
    slow, [slow_url] = start_workers(1, path, extra_env={"DELAY_MS": "3000"})
    fast, [fast_url] = start_workers(1, path)
    try:
        c = Coordinator([slow_url, fast_url], timeout=0.5, backoff=0.05, max_attempts=5, health_interval=0.1)
        agg, _ = c.run(path, 1_000_000)
        assert agg == base
        assert c.reassignments > 0
    finally:
        stop_workers(slow + fast)


def test_kill_worker_mid_run(sample):
    path, base = sample
    procs, urls = start_workers(3, path, extra_env={"DELAY_MS": "150"})
    try:
        threading.Timer(0.4, procs[1].kill).start()
        c = Coordinator(urls, timeout=5, backoff=0.05, max_attempts=3, health_interval=0.1)
        agg, _ = c.run(path, 250_000)
        assert agg == base
        assert c.reassignments > 0
    finally:
        stop_workers(procs)


def test_all_workers_down_fails_explicitly(sample):
    path, _ = sample
    c = Coordinator(["http://127.0.0.1:9", "http://127.0.0.1:10"], timeout=0.5, backoff=0.01,
                    health_interval=0.1, stall_timeout=1.0)
    with pytest.raises(DistributedError):
        c.run(path, 1_000_000)
