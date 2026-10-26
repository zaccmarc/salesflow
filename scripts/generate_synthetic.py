"""Gera data/sample.csv sintético com o mesmo esquema da base do Kaggle."""
import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from scripts.make_sample import write_source

HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"
CATS = ["electronics.smartphone", "electronics.audio.headphone", "appliances.kitchen.washer",
        "computers.notebook", "apparel.shoes", "furniture.living_room.sofa", "kids.toys",
        "auto.accessories.player", "sport.bicycle", "construction.tools.drill"]
BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lg", "sony", "acer", "nike", "bosch", "lenovo",
          "asus", "philips", "dell", "hp", "canon"]
TYPES = ["view"] * 90 + ["cart"] * 6 + ["purchase"] * 4  # ~4% purchase, como na base real


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=config.MAX_ROWS)
    p.add_argument("--out", default="data/sample.csv")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    rnd = random.Random(a.seed)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", newline="") as f:
        f.write(HEADER + "\n")
        for i in range(a.rows):
            et = rnd.choice(TYPES)
            cat = rnd.choice(CATS)
            brand = rnd.choice(BRANDS)
            price = f"{rnd.uniform(1, 2500):.2f}"
            r = rnd.random()
            if r < 0.02:
                price = ""
            elif r < 0.03:
                price = "0"
            elif r < 0.04:
                price = f"-{rnd.uniform(1, 100):.2f}"
            elif r < 0.045:
                price = "abc"
            if rnd.random() < 0.10:
                cat = ""
            if rnd.random() < 0.12:
                brand = ""
            pid = rnd.randint(1_000_000, 1_100_000)
            ts = f"2019-10-{1 + i * 30 // a.rows:02d} {rnd.randint(0,23):02d}:{rnd.randint(0,59):02d}:{rnd.randint(0,59):02d} UTC"
            f.write(f"{ts},{et},{pid},{2053013555631882655},{cat},{brand},{price},"
                    f"{rnd.randint(10**8, 6*10**8)},{rnd.getrandbits(64):016x}\n")
    write_source(a.out, "sintetico", a.rows)
    print(f"{a.rows} linhas sintéticas em {a.out}")


if __name__ == "__main__":
    main()
