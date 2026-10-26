"""Núcleo comum: regra de negócio, agregação e divisão do arquivo em blocos.

Todas as versões (baseline, paralela, distribuída) usam `process_row`.
Valores monetários são centavos inteiros, então a soma é exata.
"""
import csv
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from config import UNKNOWN

# Posições das colunas: event_time,event_type,product_id,category_id,
# category_code,brand,price,user_id,user_session
IDX_TYPE, IDX_CATEGORY, IDX_BRAND, IDX_PRICE = 1, 4, 5, 6
_CENT = Decimal("0.01")


def new_agg():
    return {"por_categoria": {}, "por_marca": {}, "valid": 0, "invalid": 0, "ignored": 0}


def price_to_cents(text):
    """'12.34' -> 1234. Devolve None se vazio, não numérico, não finito ou <= 0."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        d = Decimal(text)
    except InvalidOperation:
        return None
    if not d.is_finite() or d <= 0:
        return None
    cents = int((d * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    return cents if cents > 0 else None


def process_row(row, agg):
    """Aplica limpeza a uma linha (lista de campos) e acumula em `agg`."""
    if len(row) <= IDX_TYPE or row[IDX_TYPE].strip() != "purchase":
        agg["ignored"] += 1
        return
    cents = price_to_cents(row[IDX_PRICE]) if len(row) > IDX_PRICE else None
    if cents is None:
        agg["invalid"] += 1
        return
    cat = row[IDX_CATEGORY].strip() or UNKNOWN
    brand = row[IDX_BRAND].strip() or UNKNOWN
    agg["por_categoria"][cat] = agg["por_categoria"].get(cat, 0) + cents
    agg["por_marca"][brand] = agg["por_marca"].get(brand, 0) + cents
    agg["valid"] += 1


def merge(a, b):
    """Soma dois agregados (novo dicionário; não altera os argumentos)."""
    out = new_agg()
    for key in ("por_categoria", "por_marca"):
        d = dict(a[key])
        for k, v in b[key].items():
            d[k] = d.get(k, 0) + v
        out[key] = d
    for key in ("valid", "invalid", "ignored"):
        out[key] = a[key] + b[key]
    return out


def split_blocks(path, block_size):
    """Lista (block_id, start, end) por intervalos de bytes; block_size em bytes."""
    block_size = int(block_size)
    if block_size <= 0:
        raise ValueError("block_size deve ser > 0")
    size = os.path.getsize(path)
    return [(i, s, min(s + block_size, size))
            for i, s in enumerate(range(0, size, block_size))]


def process_block(path, start, end):
    """Processa as linhas que COMEÇAM em [start, end).

    Se start > 0, posiciona em start-1 e descarta até o próximo '\\n': se uma
    linha começa exatamente em start, ela é mantida. A última linha pode
    atravessar `end` e é lida inteira. O bloco 0 pula o cabeçalho.
    """
    agg = new_agg()
    with open(path, "rb") as f:
        if start > 0:
            f.seek(start - 1)
            f.readline()
        else:
            f.readline()  # cabeçalho
        pos = f.tell()

        def lines():
            nonlocal pos
            while pos < end:
                raw = f.readline()
                if not raw:
                    break
                pos += len(raw)
                yield raw.decode("utf-8", errors="replace")

        for row in csv.reader(lines()):
            process_row(row, agg)
    return agg
