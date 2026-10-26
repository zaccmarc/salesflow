import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def sample(tmp_path_factory):
    """CSV sintético pequeno (~7 MB) + baseline calculado."""
    from baseline import run_baseline
    path = str(tmp_path_factory.mktemp("data") / "sample.csv")
    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "generate_synthetic.py"),
                    "--rows", "60000", "--out", path], check=True, cwd=ROOT, capture_output=True)
    agg, _ = run_baseline(path)
    assert agg["valid"] > 0 and agg["invalid"] > 0
    return path, agg
