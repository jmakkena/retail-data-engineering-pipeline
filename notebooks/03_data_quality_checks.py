# Databricks notebook source

from pyspark.sql import functions as F

orders = spark.table("workspace.silver.orders_clean")
items = spark.table("workspace.silver.order_items_clean")
bronze = spark.table("workspace.bronze.orders_raw")

checks = {
    "Silver orders have unique IDs":
        orders.count() == orders.select("order_id").distinct().count(),

    "No null order IDs":
        orders.filter(F.col("order_id").isNull()).count() == 0,

    "No invalid quantities":
        items.filter(F.col("quantity") <= 0).count() == 0,

    "No negative prices":
        items.filter(F.col("unit_price") < 0).count() == 0,

    "Every order item references an existing order":
        items.join(
            orders,
            "order_id",
            "left_anti"
        ).count() == 0,

    "Silver order count does not exceed Bronze":
        orders.count() <= bronze.count()
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

if not all(checks.values()):
    raise ValueError("Data quality checks failed!")

print("\nAll data quality checks passed!")


# COMMAND ----------


import json
from decimal import Decimal
from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/bronze/retail_raw"

# Find snapshots that have both CSV and JSON files
files = {f.name for f in dbutils.fs.ls(VOLUME_PATH)}

snapshots = sorted([
    name for name in files
    if name.startswith("order_items_")
    and name.endswith(".csv")
    and name != "order_items_snapshot.csv"
    and name.replace(".csv", ".json") in files
])

if not snapshots:
    raise ValueError("No complete snapshots found!")

# Select latest snapshot metadata
latest_file = snapshots[-1]
metadata_path = (
    f"{VOLUME_PATH}/{latest_file.replace('.csv', '.json')}"
)

metadata = json.loads(dbutils.fs.head(metadata_path, 10000))

# Expected PostgreSQL totals
expected_items = int(metadata["item_count"])
expected_units = int(metadata["unit_count"])
expected_value = Decimal(metadata["gross_value"])

# Actual Databricks Silver totals
items = spark.table("workspace.silver.order_items_clean")

actual = items.agg(
    F.count("*").alias("item_count"),
    F.sum("quantity").alias("unit_count"),
    F.round(
        F.sum(F.col("quantity") * F.col("unit_price")), 2
    ).alias("gross_value")
).first()

# Actual Gold revenue total
gold_total = spark.table(
    "workspace.gold.daily_revenue_metrics"
).agg(
    F.round(F.sum("gross_order_value"), 2).alias("value")
).first().value

# Reconciliation checks
checks = {
    "Order item count": actual.item_count == expected_items,
    "Total units": actual.unit_count == expected_units,
    "Silver gross value": actual.gross_value == expected_value,
    "Gold gross value": gold_total == expected_value
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

if not all(checks.values()):
    raise ValueError("Data reconciliation failed!")

print("\nSnapshot:", latest_file)
print("Order items:", actual.item_count)
print("Total units:", actual.unit_count)
print("Gross order value:", actual.gross_value)
print("\nAll reconciliation checks passed!")
