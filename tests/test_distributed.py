import pytest

from coordinator import Coordinator
from scripts.run_local_workers import start_workers, stop_workers


@pytest.fixture
def workers(sample):
    procs, urls = start_workers(3, sample[0])
    yield procs, urls
    stop_workers(procs)


@pytest.mark.parametrize("n", [1, 2, 3])
def test_distributed_equals_baseline(sample, workers, n):
    path, base = sample
    c = Coordinator(workers[1][:n])
    agg, nblocks = c.run(path, 500_000)
    assert nblocks > 1
    assert agg == base
    assert c.reassignments == 0
