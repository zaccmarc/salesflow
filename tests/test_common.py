from common import merge, new_agg, price_to_cents, process_row


def run(rows):
    agg = new_agg()
    for r in rows:
        process_row(r, agg)
    return agg


def row(et="purchase", cat="electronics.smartphone", brand="apple", price="10.00"):
    return ["2019-10-01 00:00:00 UTC", et, "1", "2", cat, brand, price, "3", "s"]


def test_valid_purchase():
    a = run([row(price="12.34")])
    assert a["por_categoria"] == {"electronics.smartphone": 1234}
    assert a["por_marca"] == {"apple": 1234}
    assert (a["valid"], a["invalid"], a["ignored"]) == (1, 0, 0)


def test_invalid_prices():
    for p in ["", "0", "-5", "abc", "nan", "inf", "0.001"]:
        a = run([row(price=p)])
        assert (a["valid"], a["invalid"]) == (0, 1), p
        assert a["por_categoria"] == {}


def test_unknown_category_and_brand():
    a = run([row(cat="", brand="  ")])
    assert a["por_categoria"] == {"desconhecido": 1000}
    assert a["por_marca"] == {"desconhecido": 1000}


def test_non_purchase_ignored():
    a = run([row(et="view"), row(et="cart", price=""), []])
    assert (a["valid"], a["invalid"], a["ignored"]) == (0, 0, 3)


def test_cents():
    assert price_to_cents("12.34") == 1234
    assert price_to_cents("0.1") == 10
    assert price_to_cents("19.999") == 2000
    assert price_to_cents("5") == 500


def test_merge_assoc_comm():
    a = run([row(price="1.10"), row(cat="x", price="2.20")])
    b = run([row(brand="b", price="3.30"), row(et="view")])
    c = run([row(price="0")])
    assert merge(a, b) == merge(b, a)
    assert merge(merge(a, b), c) == merge(a, merge(b, c))
    assert merge(a, new_agg()) == a
