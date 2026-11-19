# Databricks notebook source

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DecimalType
)

VOLUME_PATH = "/Volumes/workspace/bronze/retail_raw"

# Find versioned CSV snapshots
files = {
    f.name for f in dbutils.fs.ls(VOLUME_PATH)
}

complete_snapshots = sorted([
    name for name in files
    if name.startswith("order_items_")
    and name.endswith(".csv")
    and name != "order_items_snapshot.csv"
    and name.replace(".csv", ".json") in files
])

if not complete_snapshots:
    raise ValueError("No complete snapshots found")

latest_file = complete_snapshots[-1]
latest_csv_path = f"{VOLUME_PATH}/{latest_file}"
latest_json_path = latest_csv_path.replace(".csv", ".json")

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(12, 2), False)
])

order_items_df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(latest_csv_path)
)

print("Latest snapshot:", latest_file)
print("Order items:", order_items_df.count())


# COMMAND ----------


from pyspark.sql import functions as F

# Validate the latest snapshot before publishing it
duplicates = (
    order_items_df
    .groupBy("order_id", "product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid = order_items_df.filter(
    F.col("order_id").isNull()
    | F.col("product_id").isNull()
    | F.col("quantity").isNull()
    | F.col("unit_price").isNull()
    | (F.col("quantity") <= 0)
    | (F.col("unit_price") < 0)
).count()

if duplicates > 0 or invalid > 0:
    raise ValueError(
        f"Snapshot validation failed: "
        f"{duplicates} duplicate keys, {invalid} invalid rows"
    )

# Make the validated snapshot available to SQL
order_items_df.createOrReplaceTempView("order_items_staging")

# Refresh Bronze using the latest complete snapshot
spark.sql("""
CREATE OR REPLACE TABLE workspace.bronze.order_items_raw
USING DELTA
AS SELECT * FROM order_items_staging
""")

# Refresh Silver from the validated Bronze snapshot
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.order_items_clean
USING DELTA
AS
SELECT *
FROM workspace.bronze.order_items_raw
""")

print("Bronze records:",
      spark.table("workspace.bronze.order_items_raw").count())

print("Silver records:",
      spark.table("workspace.silver.order_items_clean").count())

display(
    spark.table("workspace.silver.order_items_clean")
    .filter("order_id = 1 AND product_id = 1")
)


# COMMAND ----------


# Create Gold revenue analytics by joining orders and order items
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.daily_revenue_metrics
USING DELTA
AS
SELECT
    CAST(o.ordered_at AS DATE) AS order_date,

    COUNT(DISTINCT o.order_id) AS total_orders,

    ROUND(
        SUM(oi.quantity * oi.unit_price), 2
    ) AS gross_order_value,

    ROUND(
        SUM(
            CASE
                WHEN o.status = 'delivered'
                THEN oi.quantity * oi.unit_price
                ELSE 0
            END
        ), 2
    ) AS delivered_order_value,

    COUNT(DISTINCT o.customer_id) AS unique_customers

FROM workspace.silver.orders_clean o

JOIN workspace.silver.order_items_clean oi
    ON o.order_id = oi.order_id

GROUP BY CAST(o.ordered_at AS DATE)
""")

print("Gold revenue table created successfully!")

display(
    spark.table("workspace.gold.daily_revenue_metrics")
    .orderBy("order_date", ascending=False)
    .limit(10)
)


# COMMAND ----------


from pyspark.sql import functions as F

items = spark.table("workspace.silver.order_items_clean")
gold = spark.table("workspace.gold.daily_revenue_metrics")

# Verify source snapshot totals in Silver
totals = items.agg(
    F.sum("quantity").alias("total_units"),
    F.round(
        F.sum(F.col("quantity") * F.col("unit_price")), 2
    ).alias("gross_value")
).first()

# Verify Gold gross order value
gold_total = gold.agg(
    F.round(F.sum("gross_order_value"), 2).alias("total")
).first().total

print("Silver total units:", totals.total_units)
print("Silver gross order value:", totals.gross_value)
print("Gold gross order value:", gold_total)

assert totals.total_units == 2534
assert totals.gross_value == gold_total

print("Revenue reconciliation passed!")


# COMMAND ----------


import json

VOLUME_PATH = "/Volumes/workspace/bronze/retail_raw"

# Discover all versioned order-item CSV snapshots
snapshot_files = sorted([
    f.name
    for f in dbutils.fs.ls(VOLUME_PATH)
    if f.name.startswith("order_items_")
    and f.name.endswith(".csv")
    and f.name != "order_items_snapshot.csv"
])

# Only consider snapshots with matching metadata
complete_snapshots = [
    filename
    for filename in snapshot_files
    if filename.replace(".csv", ".json")
    in {
        f.name for f in dbutils.fs.ls(VOLUME_PATH)
    }
]

if not complete_snapshots:
    raise ValueError("No complete order-items snapshots found")

# Filenames include sortable timestamps
latest_file = complete_snapshots[-1]
latest_csv_path = f"{VOLUME_PATH}/{latest_file}"
latest_json_path = latest_csv_path.replace(".csv", ".json")

print("Latest snapshot:", latest_file)
print("CSV path:", latest_csv_path)
print("Metadata path:", latest_json_path)


# COMMAND ----------


from pyspark.sql.types import (
    StructType, StructField, IntegerType, DecimalType
)
from pyspark.sql import functions as F

# Define the expected schema
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(12, 2), False)
])

# Read newest snapshot
latest_df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(latest_csv_path)
)

print("Snapshot records:", latest_df.count())

# Ensure we have unique composite keys
duplicates = (
    latest_df
    .groupBy("order_id", "product_id")
    .count()
    .filter("count > 1")
    .count()
)

if duplicates:
    raise ValueError("Duplicate order-item keys detected!")

# Validate basic business rules
invalid = latest_df.filter(
    F.col("order_id").isNull()
    | F.col("product_id").isNull()
    | F.col("quantity").isNull()
    | F.col("unit_price").isNull()
    | (F.col("quantity") <= 0)
    | (F.col("unit_price") < 0)
).count()

if invalid:
    raise ValueError("Invalid order-item records detected!")

# Create SQL view for snapshot replacement
latest_df.createOrReplaceTempView("latest_order_items")

# Replace Bronze snapshot
spark.sql("""
CREATE OR REPLACE TABLE workspace.bronze.order_items_raw
USING DELTA
AS SELECT * FROM latest_order_items
""")

# Replace Silver with validated records
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.order_items_clean
USING DELTA
AS SELECT * FROM latest_order_items
""")

print("Bronze records:",
      spark.table("workspace.bronze.order_items_raw").count())
print("Silver records:",
      spark.table("workspace.silver.order_items_clean").count())

display(
    spark.table("workspace.silver.order_items_clean")
    .filter("order_id = 1 AND product_id = 1")
)


# COMMAND ----------


import json
from decimal import Decimal
from pyspark.sql import functions as F

# Read reconciliation metadata for the selected snapshot
metadata_text = dbutils.fs.head(latest_json_path, 10000)
metadata = json.loads(metadata_text)

# Expected totals from PostgreSQL
expected_items = int(metadata["item_count"])
expected_units = int(metadata["unit_count"])
expected_value = Decimal(metadata["gross_value"])

# Actual totals from Databricks Silver
items = spark.table("workspace.silver.order_items_clean")

actual = items.agg(
    F.count("*").alias("item_count"),
    F.sum("quantity").alias("unit_count"),
    F.round(
        F.sum(F.col("quantity") * F.col("unit_price")),
        2
    ).alias("gross_value")
).first()

# Actual totals from Gold
gold_total = spark.table(
    "workspace.gold.daily_revenue_metrics"
).agg(
    F.round(F.sum("gross_order_value"), 2).alias("value")
).first().value

checks = {
    "Order item count": actual.item_count == expected_items,
    "Total units": actual.unit_count == expected_units,
    "Silver gross value": actual.gross_value == expected_value,
    "Gold gross value": gold_total == expected_value
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

if not all(checks.values()):
    raise ValueError("Snapshot reconciliation failed!")

print("\nSnapshot:", metadata["snapshot_id"])
print("Order items:", actual.item_count)
print("Total units:", actual.unit_count)
print("Gross order value:", actual.gross_value)
print("All reconciliation checks passed!")
