
import csv
import json
import getpass
from pathlib import Path

import psycopg

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "retail_analytics",
    "user": "postgres"
}

# Create output folder
DATA_DIR = Path("data/incremental")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"

# 1. Read the previous checkpoint
if CHECKPOINT_FILE.exists():
    checkpoint = json.loads(
        CHECKPOINT_FILE.read_text(encoding="utf-8")
    )
else:
    checkpoint = {
        "last_updated_at": "1900-01-01 00:00:00",
        "last_order_id": 0
    }

print("Previous checkpoint:", checkpoint)

# 2. Connect to PostgreSQL
password = getpass.getpass("PostgreSQL password: ")

with psycopg.connect(
    **DB_CONFIG,
    password=password
) as conn:

    with conn.cursor() as cur:

        # 3. Extract only new or updated records
        cur.execute("""
            SELECT
                order_id,
                customer_id,
                ordered_at,
                status,
                updated_at
            FROM public.orders
            WHERE (updated_at, order_id) > (%s, %s)
            ORDER BY updated_at, order_id
        """, (
            checkpoint["last_updated_at"],
            checkpoint["last_order_id"]
        ))

        rows = cur.fetchall()
        columns = [col.name for col in cur.description]

# 4. Handle no new records
if not rows:
    print("No new or updated orders found.")

else:
    # 5. Save the batch to a CSV file
    last_updated = rows[-1][4]
    last_id = rows[-1][0]

    batch_name = (
        last_updated.strftime("%Y%m%d_%H%M%S_%f")
        + f"_{last_id}"
    )

    output_file = DATA_DIR / f"orders_{batch_name}.csv"
    temp_file = DATA_DIR / f"orders_{batch_name}.tmp"

    with open(
        temp_file, "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    # Publish the completed CSV
    temp_file.replace(output_file)

    # 6. Update checkpoint after CSV is saved
    new_checkpoint = {
        "last_updated_at": str(last_updated),
        "last_order_id": last_id
    }

    temp_checkpoint = DATA_DIR / "checkpoint.tmp"
    temp_checkpoint.write_text(
        json.dumps(new_checkpoint, indent=4),
        encoding="utf-8"
    )
    temp_checkpoint.replace(CHECKPOINT_FILE)

    print(f"Extracted {len(rows)} orders.")
    print(f"Saved to: {output_file}")
    print("New checkpoint:", new_checkpoint)
