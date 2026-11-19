"""Create a small, reproducible retail dataset in an existing PostgreSQL database.

Run: py setup_project.py
The script never stores your database password in a file.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from getpass import getpass
from pathlib import Path
import random

import psycopg

HOST = "localhost"
PORT = 5432
DATABASE = "retail_analytics"  # This database already exists.
USER = "postgres"


def sample_data():
    rng = random.Random(42)  # Same data each time; safe to rerun.
    base = datetime(2026, 1, 1, 9, 0, 0)
    first_names = ["Avery", "Jordan", "Morgan", "Taylor", "Riley", "Casey", "Parker", "Quinn"]
    last_names = ["Smith", "Patel", "Brown", "Lee", "Garcia", "Wilson", "Chen", "Johnson"]
    cities = ["Chicago", "Naperville", "Aurora", "Plainfield", "Joliet", "Evanston"]
    product_categories = ["Electronics", "Home", "Apparel", "Fitness", "Accessories"]

    customers = [
        (i, f"{first_names[(i - 1) % 8]} {last_names[((i - 1) // 8) % 8]}",
         f"customer{i:03d}@example.test", rng.choice(cities), base + timedelta(days=i % 90))
        for i in range(1, 101)
    ]
    products = [
        (i, f"{product_categories[(i - 1) % 5]} Item {i:02d}",
         product_categories[(i - 1) % 5], Decimal(rng.randrange(500, 15000)) / 100)
        for i in range(1, 31)
    ]
    inventory = [(i, rng.randint(30, 500), base + timedelta(days=290)) for i in range(1, 31)]

    orders = []
    items = []
    price_lookup = {row[0]: row[3] for row in products}
    for order_id in range(1, 501):
        ordered_at = base + timedelta(days=rng.randint(90, 290), hours=rng.randint(0, 12))
        status = rng.choices(["pending", "shipped", "delivered", "cancelled"], [10, 20, 60, 10])[0]
        updated_at = ordered_at + timedelta(days=rng.randint(0, 7))
        orders.append((order_id, rng.randint(1, 100), ordered_at, status, updated_at))
        for product_id in rng.sample(range(1, 31), rng.randint(1, 4)):
            items.append((order_id, product_id, rng.randint(1, 3), price_lookup[product_id]))

    return customers, products, inventory, orders, items


def main():
    print(f"Connecting to {DATABASE} on {HOST}:{PORT} as {USER}...")
    password = getpass("PostgreSQL password (input hidden): ")
    try:
        with psycopg.connect(host=HOST, port=PORT, dbname=DATABASE, user=USER,
                             password=password, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                schema = (Path(__file__).parent / "sql" / "schema.sql").read_text(encoding="utf-8")
                cur.execute(schema)
                customers, products, inventory, orders, items = sample_data()

                for sql, rows in [
                    ("INSERT INTO customers VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", customers),
                    ("INSERT INTO products VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", products),
                    ("INSERT INTO inventory VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", inventory),
                    ("INSERT INTO orders VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", orders),
                    ("INSERT INTO order_items VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", items),
                ]:
                    cur.executemany(sql, rows)

                print("\nSuccess! Current table counts:")
                for table in ("customers", "products", "inventory", "orders", "order_items"):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")  # Names are fixed, not user input.
                    print(f"  {table}: {cur.fetchone()[0]}")
                print("\nYou can safely rerun this script; existing sample records aren't duplicated.")
    except psycopg.OperationalError as exc:
        print("\nCouldn't connect. Verify the password, server, and that retail_analytics exists.")
        print(f"Connection error: {exc}")
        raise SystemExit(1) from None
    except psycopg.Error as exc:
        print(f"\nDatabase operation failed: {exc}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
