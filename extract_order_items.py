
import csv
import json
import hashlib
import getpass
import io
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg

# Configuration
OUTPUT_DIR = Path(__file__).resolve().parent / "data/incremental"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COLUMNS = ["order_id", "product_id", "quantity", "unit_price"]

# Connect to PostgreSQL
password = getpass.getpass("PostgreSQL password: ")

with psycopg.connect(
    host="localhost",
    port=5432,
    dbname="retail_analytics",
    user="postgres",
    password=password
) as conn:

    conn.execute(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    )

    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id, product_id, quantity, unit_price
            FROM public.order_items
            ORDER BY order_id, product_id
        """)
        rows = cur.fetchall()

# Generate deterministic CSV content
buffer = io.StringIO(newline="")
writer = csv.writer(buffer, lineterminator="\n")
writer.writerow(COLUMNS)
writer.writerows(rows)

csv_bytes = buffer.getvalue().encode("utf-8")

# Calculate SHA-256 hash of the dataset
current_hash = hashlib.sha256(csv_bytes).hexdigest()

# Find the newest previous versioned snapshot
previous_snapshots = sorted(
    OUTPUT_DIR.glob("order_items_*.csv")
)

previous_snapshots = [
    path for path in previous_snapshots
    if path.name != "order_items_snapshot.csv"
]

# Compare against previous snapshot
if previous_snapshots:
    latest_previous = previous_snapshots[-1]
    previous_hash = hashlib.sha256(
        latest_previous.read_bytes()
    ).hexdigest()

    if current_hash == previous_hash:
        print("No order-item changes detected.")
        print("Skipping snapshot creation.")
        raise SystemExit(0)

# Generate new snapshot
snapshot_id = datetime.now(timezone.utc).strftime(
    "%Y%m%d_%H%M%S_%f"
)

csv_path = OUTPUT_DIR / f"order_items_{snapshot_id}.csv"
json_path = OUTPUT_DIR / f"order_items_{snapshot_id}.json"

# Calculate reconciliation metrics
item_count = len(rows)
unit_count = sum(row[2] for row in rows)
gross_value = sum(
    (row[2] * row[3] for row in rows),
    Decimal("0.00")
)

# Write CSV and metadata
csv_path.write_bytes(csv_bytes)

metadata = {
    "snapshot_id": snapshot_id,
    "file_name": csv_path.name,
    "item_count": item_count,
    "unit_count": unit_count,
    "gross_value": str(gross_value.quantize(Decimal("0.01"))),
    "sha256": current_hash
}

json_path.write_text(
    json.dumps(metadata, indent=4),
    encoding="utf-8"
)

print(f"New snapshot: {csv_path.name}")
print(f"Order items: {item_count}")
print(f"Units: {unit_count}")
print(f"Gross order value: ${gross_value:.2f}")
print(f"SHA-256: {current_hash[:16]}...")
print("Snapshot and metadata saved successfully!")
