# Databricks notebook source

# Location of our raw PostgreSQL extracts
VOLUME_PATH = "/Volumes/workspace/bronze/retail_raw"

# Discover CSV files in the volume
csv_files = [
    file.path
    for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.endswith(".csv")
]

print("CSV files discovered:", len(csv_files))

for file in sorted(csv_files):
    print(file)


# COMMAND ----------


# Create a Delta table to track processed files
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.bronze.ingestion_log (
    file_path STRING,
    processed_at TIMESTAMP,
    record_count BIGINT
)
USING DELTA
""")

# See which files have already been logged
display(spark.table("workspace.bronze.ingestion_log"))


# COMMAND ----------


from pyspark.sql import functions as F

# These are the batches we already loaded manually
processed_batches = {
    "orders_20261024_130000_000000_92.csv": 500,
    "orders_20261118_154740_565718_8.csv": 1,
    "orders_20261118_163344_254415_15.csv": 1
}

# Register these files in our ingestion log
for filename, expected_count in processed_batches.items():

    file_path = f"{VOLUME_PATH}/{filename}"

    # Check whether the file is already registered
    existing = (
        spark.table("workspace.bronze.ingestion_log")
        .filter(F.col("file_path") == file_path)
        .count()
    )

    if existing == 0:
        spark.createDataFrame(
            [(file_path, expected_count)],
            ["file_path", "record_count"]
        ).withColumn(
            "processed_at", F.current_timestamp()
        ).select(
            "file_path", "processed_at", "record_count"
        ).write.mode("append").saveAsTable(
            "workspace.bronze.ingestion_log"
        )

print("Registered existing batches:")
display(spark.table("workspace.bronze.ingestion_log"))


# COMMAND ----------

VOLUME_PATH = "/Volumes/workspace/bronze/retail_raw"

# Find files that have already been processed
processed_files = {
    row.file_path
    for row in spark.table(
        "workspace.bronze.ingestion_log"
    ).select("file_path").collect()
}

# Discover CSV files in our volume

# Discover only order-update CSV files
all_files = [
    file.path
    for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.startswith("orders_")
    and file.name.endswith(".csv")
]


# Find files that are not in the ingestion log
new_files = [
    file for file in all_files
    if file.replace("dbfs:", "") not in processed_files
]

print("Total CSV files:", len(all_files))
print("Already processed:", len(processed_files))
print("New files to process:", len(new_files))

for file in new_files:
    print(file)


# COMMAND ----------


from pyspark.sql import functions as F

# Process CSV files that aren't in the ingestion log
for file_path in sorted(new_files):

    normalized_path = file_path.replace("dbfs:", "")

    print("Processing:", normalized_path)

    # Read the new CSV batch
    batch_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(normalized_path)
    )

    # Register the incoming batch as a temporary view
    batch_df.createOrReplaceTempView("incoming_orders")

    # Append only order versions not already in Bronze
    spark.sql("""
        INSERT INTO workspace.bronze.orders_raw
        SELECT s.*
        FROM incoming_orders s
        WHERE NOT EXISTS (
            SELECT 1
            FROM workspace.bronze.orders_raw b
            WHERE b.order_id = s.order_id
              AND b.updated_at = s.updated_at
        )
    """)

    # Count the records in the source file
    row_count = batch_df.count()

    # Record the processed file
    spark.createDataFrame(
        [(normalized_path, row_count)],
        ["file_path", "record_count"]
    ).withColumn(
        "processed_at", F.current_timestamp()
    ).select(
        "file_path", "processed_at", "record_count"
    ).write.mode("append").saveAsTable(
        "workspace.bronze.ingestion_log"
    )

    print(f"Processed {row_count} records")

print("Ingestion complete!")


# COMMAND ----------


# Check Bronze record count
bronze_count = spark.table(
    "workspace.bronze.orders_raw"
).count()

# Check the ingestion log
log_count = spark.table(
    "workspace.bronze.ingestion_log"
).count()

print("Bronze records:", bronze_count)
print("Processed files:", log_count)

# Check the latest version of order 25
display(
    spark.table("workspace.bronze.orders_raw")
    .filter("order_id = 25")
    .orderBy("updated_at", ascending=False)
)


# COMMAND ----------


# Get the latest version of every order from Bronze
spark.sql("""
CREATE OR REPLACE TEMP VIEW latest_orders AS
SELECT
    order_id,
    customer_id,
    ordered_at,
    status,
    updated_at
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM workspace.bronze.orders_raw
)
WHERE rn = 1
""")

# Merge changes into Silver
spark.sql("""
MERGE INTO workspace.silver.orders_clean AS target
USING latest_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.updated_at > target.updated_at
THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.ordered_at = source.ordered_at,
    target.status = source.status,
    target.updated_at = source.updated_at

WHEN NOT MATCHED
THEN INSERT (
    order_id, customer_id, ordered_at,
    status, updated_at
)
VALUES (
    source.order_id, source.customer_id, source.ordered_at,
    source.status, source.updated_at
)
""")

# Validate Silver
silver = spark.table("workspace.silver.orders_clean")

print("Silver records:", silver.count())
print("Unique orders:", silver.select("order_id").distinct().count())

display(silver.filter("order_id = 25"))


# COMMAND ----------


# Create daily order analytics from Silver
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.daily_order_metrics
USING DELTA
AS
SELECT
    CAST(ordered_at AS DATE) AS order_date,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END)
        AS delivered_orders,
    SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END)
        AS shipped_orders,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END)
        AS pending_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)
        AS cancelled_orders,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM workspace.silver.orders_clean
GROUP BY CAST(ordered_at AS DATE)
""")

print("Gold analytics table created successfully!")

display(
    spark.table("workspace.gold.daily_order_metrics")
    .orderBy("order_date", ascending=False)
    .limit(10)
)
