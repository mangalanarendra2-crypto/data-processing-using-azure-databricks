# generate_sample_data.py
"""
Creates a synthetic raw sales CSV under data/raw/, standing in for files
that would normally land in ADLS / a Unity Catalog volume from an upstream
source system. Includes deliberately messy rows (missing region, duplicate
transaction_id, a negative amount) so the silver-layer cleaning logic in
notebooks/02_silver_transformation.py has something real to do.

Run:
    python generate_sample_data.py
"""

import os
import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUT_PATH = os.path.join("data", "raw", "sales_2026_09.csv")
N_ROWS = 5000

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = ["Electronics", "Apparel", "Home & Garden", "Sports", "Grocery", "Toys"]


def generate_rows(n=N_ROWS):
    rows = []
    start = datetime(2026, 9, 1)

    for i in range(n):
        txn_id = f"TXN{100000 + i}"
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5, 300), 2)
        sale_amount = round(quantity * unit_price, 2)
        ts = start + timedelta(
            days=random.randint(0, 3),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        rows.append({
            "transaction_id": txn_id,
            "customer_id": f"CUST{random.randint(1000, 1499)}",
            "store_region": random.choice(REGIONS),
            "product_category": random.choice(CATEGORIES),
            "quantity": quantity,
            "unit_price": unit_price,
            "sale_amount": sale_amount,
            "sale_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })

    # ---- inject messy data on purpose, to exercise the silver cleaning rules ----
    # 1. missing region
    rows[10]["store_region"] = ""
    # 2. duplicate transaction_id (simulates an upstream retry/duplicate event)
    dup = dict(rows[20])
    rows.append(dup)
    # 3. negative sale_amount (bad data / refund miscoded as a sale)
    rows[30]["sale_amount"] = -15.0
    # 4. missing product_category
    rows[40]["product_category"] = ""
    # 5. null transaction_id (should be dropped entirely in silver)
    rows[50]["transaction_id"] = ""

    return rows


if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    rows = generate_rows()

    fieldnames = [
        "transaction_id", "customer_id", "store_region", "product_category",
        "quantity", "unit_price", "sale_amount", "sale_timestamp",
    ]
    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} rows -> {OUT_PATH}")
