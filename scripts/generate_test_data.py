#!/usr/bin/env python3
"""Generate synthetic customers and orders CSVs for testing.

Usage:
    python scripts/generate_test_data.py --customers 10000 --orders 10000

The script writes to include/data/raw/customers.csv and include/data/raw/orders.csv
and will create directories if missing.

It will try to use the faker library for more realistic data if available; otherwise
it will use simple randomized generation.
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    from faker import Faker

    HAS_FAKER = True
except Exception:
    HAS_FAKER = False

BASE_DIR = Path(".").resolve()
RAW_DIR = BASE_DIR / "include" / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS = [
    "Widget A",
    "Widget B",
    "Widget C",
    "Gadget X",
    "Gadget Y",
    "Gadget Z",
    "Doohickey",
    "Thingamajig",
]


def random_date_between(start_date: date, end_date: date) -> date:
    if start_date >= end_date:
        return start_date
    delta = end_date - start_date
    offset = random.randint(0, delta.days)
    return start_date + timedelta(days=offset)


def generate_customers(n: int, customers_path: Path):
    faker = Faker() if HAS_FAKER else None
    today = date.today()
    start_range = today - timedelta(days=365 * 10)  # last 10 years
    with customers_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "email", "signup_date"])
        for cid in range(1, n + 1):
            if HAS_FAKER:
                name = faker.name()
                email = faker.safe_email()
                signup = faker.date_between(start_date=start_range, end_date=today)
            else:
                first = random.choice(
                    [
                        "James",
                        "Mary",
                        "John",
                        "Patricia",
                        "Robert",
                        "Jennifer",
                        "Michael",
                        "Linda",
                        "William",
                        "Elizabeth",
                        "David",
                        "Barbara",
                        "Richard",
                        "Susan",
                        "Joseph",
                        "Jessica",
                    ]
                )
                last = random.choice(
                    [
                        "Smith",
                        "Johnson",
                        "Williams",
                        "Brown",
                        "Jones",
                        "Garcia",
                        "Miller",
                        "Davis",
                        "Rodriguez",
                        "Martinez",
                        "Hernandez",
                    ]
                )
                name = f"{first} {last}"
                email = f"{first.lower()}.{last.lower()}{cid % 1000}@example.com"
                signup = random_date_between(start_range, today)
            writer.writerow([cid, name, email, signup.isoformat()])


def generate_orders(m: int, customers_n: int, orders_path: Path, customers_lookup=None):
    faker = Faker() if HAS_FAKER else None
    today = date.today()
    with orders_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["order_id", "customer_id", "order_date", "item", "quantity", "unit_price"]
        )
        for oid in range(1, m + 1):
            cust_id = random.randint(1, customers_n)
            if customers_lookup and cust_id in customers_lookup:
                signup = customers_lookup[cust_id]
            else:
                signup = today - timedelta(days=random.randint(0, 365 * 10))
            order_date = random_date_between(signup, today)
            item = random.choice(PRODUCTS)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.0, 200.0), 2)
            writer.writerow(
                [
                    oid,
                    cust_id,
                    order_date.isoformat(),
                    item,
                    quantity,
                    f"{unit_price:.2f}",
                ]
            )


def load_customers_signup_dates(path: Path):
    lookup = {}
    if not path.exists():
        return lookup
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = int(row["customer_id"]) if row.get("customer_id") else None
            sd = row.get("signup_date")
            if cid and sd:
                try:
                    lookup[cid] = datetime.fromisoformat(sd).date()
                except Exception:
                    try:
                        lookup[cid] = datetime.strptime(sd, "%Y-%m-%d").date()
                    except Exception:
                        lookup[cid] = date.today() - timedelta(
                            days=random.randint(0, 365 * 10)
                        )
    return lookup


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic customers and orders CSVs"
    )
    parser.add_argument(
        "--customers", type=int, default=10000, help="Number of customers to generate"
    )
    parser.add_argument(
        "--orders", type=int, default=10000, help="Number of orders to generate"
    )
    parser.add_argument(
        "--out", type=str, default=str(RAW_DIR), help="Output raw data directory"
    )
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    cust_path = out_dir / "customers.csv"
    orders_path = out_dir / "orders.csv"

    print(f"Generating {args.customers} customers to {cust_path}")
    generate_customers(args.customers, cust_path)

    print("Loading customer signup dates for realistic order dates...")
    lookup = load_customers_signup_dates(cust_path)

    print(f"Generating {args.orders} orders to {orders_path}")
    generate_orders(args.orders, args.customers, orders_path, customers_lookup=lookup)

    print("Done.")


if __name__ == "__main__":
    main()
