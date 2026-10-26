"""Baixa um arquivo mensal da base do Kaggle (requer credenciais)."""
import argparse
import os
import sys
from pathlib import Path

DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"


def has_credentials():
    return (Path.home() / ".kaggle" / "kaggle.json").exists() or (
        os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", default="2019-Oct.csv")
    p.add_argument("--out", default="data/raw")
    a = p.parse_args()
    if not has_credentials():
        print("Sem credenciais do Kaggle; use scripts/generate_synthetic.py", file=sys.stderr)
        return 2
    from kaggle.api.kaggle_api_extended import KaggleApi  # pip install kaggle
    api = KaggleApi()
    api.authenticate()
    Path(a.out).mkdir(parents=True, exist_ok=True)
    api.dataset_download_file(DATASET, a.file, path=a.out, force=False)
    print("baixado em", a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
