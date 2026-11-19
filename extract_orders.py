
import csv
import getpass
from pathlib import Path

import psycopg

# 1. Connect to our PostgreSQL database
password = getpass.getpass("PostgreSQL password: ")

with psycopg.connect(
    host="localhost",
    port=5432,
    dbname="retail_analytics",
    user="postgres",
    password=password
) as conn:

    # 2. Extract orders from PostgreSQL
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id, customer_id, ordered_at,
                   status, updated_at
            FROM public.orders
            ORDER BY order_id;
        """)

        orders = cur.fetchall()
        column_names = [col.name for col in cur.description]

# 3. Create a folder for our extracted data
output_folder = Path("data")
output_folder.mkdir(exist_ok=True)

# 4. Save the extracted records as CSV
output_file = output_folder / "orders.csv"

with open(output_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(column_names)
    writer.writerows(orders)

print(f"Successfully extracted {len(orders)} orders!")
print(f"Saved to: {output_file.resolve()}")
