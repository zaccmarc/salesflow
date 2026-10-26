import pytest

from parallel import run_parallel


@pytest.mark.parametrize("executor", ["thread", "process"])
@pytest.mark.parametrize("workers", [1, 2, 4])
def test_parallel_equals_baseline(sample, executor, workers):
    path, base = sample
    agg, _, nblocks = run_parallel(path, executor, workers, block_size=500_000)
    assert nblocks > 1
    assert agg == base
