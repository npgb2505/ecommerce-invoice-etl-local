from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "raw" / "ecommerce_invoices.csv"


PRODUCTS = [
    ("85123A", "White Hanging Heart T-Light Holder", 2.55),
    ("71053", "White Metal Lantern", 3.39),
    ("84406B", "Cream Cupid Hearts Coat Hanger", 2.75),
    ("84029G", "Knitted Union Flag Hot Water Bottle", 3.39),
    ("84029E", "Red Woolly Hottie White Heart", 3.39),
    ("22752", "Set 7 Babushka Nesting Boxes", 7.65),
    ("21730", "Glass Star Frosted T-Light Holder", 4.25),
    ("22633", "Hand Warmer Union Jack", 1.85),
]


def generate_invoices(rows: int = 1_200, seed: int = 2026) -> Path:
    rng = random.Random(seed)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    countries = ["United Kingdom", "France", "Germany", "Netherlands", "Spain"]
    start = datetime(2024, 1, 1, 8, 0)
    fieldnames = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    with OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index in range(1, rows + 1):
            stock, description, price = rng.choice(PRODUCTS)
            invoice_no = str(536000 + index // 4)
            quantity = rng.randint(1, 12)
            if index % 97 == 0:
                invoice_no = f"C{invoice_no}"
                quantity *= -1
            customer_id = str(12000 + rng.randrange(0, 180))
            if index % 83 == 0:
                customer_id = ""
            if index % 211 == 0:
                description = ""
            if index % 263 == 0:
                quantity = 0
            invoice_date = start + timedelta(hours=rng.randrange(0, 24 * 700))
            writer.writerow(
                {
                    "InvoiceNo": invoice_no,
                    "StockCode": stock,
                    "Description": description,
                    "Quantity": quantity,
                    "InvoiceDate": invoice_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "UnitPrice": price,
                    "CustomerID": customer_id,
                    "Country": rng.choices(countries, weights=[72, 9, 8, 6, 5], k=1)[0],
                }
            )

    print(f"Generated {rows} deterministic invoice lines at {OUTPUT}")
    return OUTPUT


if __name__ == "__main__":
    generate_invoices()

