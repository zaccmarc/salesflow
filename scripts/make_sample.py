"""Recorta o cabeçalho + primeiras MAX_ROWS linhas de um CSV em data/sample.csv."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def write_source(out, origem, rows):
    with open(os.path.join(os.path.dirname(out) or ".", "SOURCE.txt"), "w") as f:
        f.write(f"origem={origem}\nlinhas={rows}\nbytes={os.path.getsize(out)}\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--out", default="data/sample.csv")
    p.add_argument("--max-rows", type=int, default=config.MAX_ROWS)
    a = p.parse_args()
    n = 0
    with open(a.input, "rb") as fi, open(a.out, "wb") as fo:
        fo.write(fi.readline())
        for line in fi:
            if n >= a.max_rows:
                break
            fo.write(line)
            n += 1
    write_source(a.out, "kaggle", n)
    print(f"{n} linhas em {a.out}")


if __name__ == "__main__":
    main()
