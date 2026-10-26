import csv

import pytest

from common import merge, new_agg, process_block, process_row, split_blocks

HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
LINES = [
    "t,purchase,1,2,cat.a,brand1,10.50,3,s\n",
    "t,view,1,2,cat.a,brand1,10.50,3,s\n",
    "t,purchase,1,2,,,7.25,3,s\n",
    "t,purchase,1,2,cat.b,brand2,,3,s\n",
    "t,purchase,1,2,cat.b,brand2,-1,3,s\n",
    "t,purchase,1,2,cat.b,brand2,99.99,3,s\n",
    "t,cart,1,2,cat.c,brand3,5,3,s\n",
    "t,purchase,1,2,cat.c,,0.01,3,s\n",
]


def sequential(path):
    agg = new_agg()
    with open(path, newline="") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            process_row(row, agg)
    return agg


def by_blocks(path, size):
    total = new_agg()
    for _, s, e in split_blocks(path, size):
        total = merge(total, process_block(path, s, e))
    return total


@pytest.mark.parametrize("trailing_newline", [True, False])
def test_every_block_size(tmp_path, trailing_newline):
    text = HEADER + "".join(LINES)
    if not trailing_newline:
        text = text.rstrip("\n")
    p = tmp_path / "f.csv"
    p.write_bytes(text.encode())
    expected = sequential(p)
    assert expected["valid"] == 4
    # de 1 byte até maior que o arquivo: cobre fronteira exatamente em '\n', no meio da linha, no cabeçalho
    for size in range(1, len(text) + 3):
        assert by_blocks(p, size) == expected, f"block_size={size}"


def test_boundary_exactly_at_newline(tmp_path):
    text = HEADER + "".join(LINES)
    p = tmp_path / "f.csv"
    p.write_bytes(text.encode())
    expected = sequential(p)
    offsets = [i + 1 for i, ch in enumerate(text) if ch == "\n"]
    for off in offsets:  # dois blocos cuja fronteira é o início exato de uma linha
        total = merge(process_block(p, 0, off), process_block(p, off, len(text)))
        assert total == expected, off


def test_header_only_and_single_row(tmp_path):
    p = tmp_path / "h.csv"
    p.write_bytes(HEADER.encode())
    assert by_blocks(p, 10) == new_agg()
    p.write_bytes((HEADER + LINES[0].rstrip("\n")).encode())
    assert by_blocks(p, 7)["valid"] == 1


def test_split_blocks_cover_file(tmp_path):
    p = tmp_path / "f.csv"
    p.write_bytes(b"x" * 1000)
    blocks = split_blocks(p, 300)
    assert blocks[0][1] == 0 and blocks[-1][2] == 1000
    assert all(blocks[i][2] == blocks[i + 1][1] for i in range(len(blocks) - 1))
    with pytest.raises(ValueError):
        split_blocks(p, 0)


def test_larger_file_many_sizes(sample):
    path, base = sample
    for size in (1000, 4096, 100_003, 1_000_000):
        assert by_blocks(path, size) == base
