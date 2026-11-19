# End-to-End Retail Data Engineering Pipeline

### PostgreSQL | Python | Databricks | PySpark | Delta Lake | SQL

An end-to-end data engineering portfolio project that demonstrates incremental data extraction, automated cloud ingestion, Medallion Architecture, Delta Lake transformations, data-quality validation, and revenue analytics.

The pipeline processes a synthetic retail dataset containing customers, products, inventory, orders, and order items.

## Architecture

```text
                  PostgreSQL 18
                 Retail Database
                        |
                        v
                 Python Extraction
            /                       \
     Incremental Orders       Order Item Snapshots
     Timestamp Checkpoint     SHA-256 Change Detection
            \                       /
             v                     v
                  Databricks CLI
                  Automated Upload
                        |
                        v
                Unity Catalog Volume
                        |
                        v
                 BRONZE LAYER
             Raw Orders + Order Items
                        |
                        v
                 SILVER LAYER
             Deduplicated Current Orders
             Validated Order Items
                        |
                        v
                  GOLD LAYER
             Daily Order Metrics
             Daily Revenue Analytics
                        |
                        v
               Data Quality Checks
             Source Reconciliation
```

Databricks Jobs orchestrates the ingestion, analytics, and validation notebooks using serverless compute.

## Technology Stack

| Technology | Purpose |
|---|---|
| PostgreSQL 18 | Source relational database |
| Python 3 | Data extraction and pipeline automation |
| Psycopg | PostgreSQL connectivity |
| Databricks CLI | Automated file transfers and job execution |
| Databricks Free Edition | Cloud analytics environment |
| PySpark | Distributed data transformations |
| Delta Lake | Managed analytical tables |
| Unity Catalog | Data organization and governance |
| SQL | Data transformation and aggregation |
| Git / GitHub | Version control and source management |

## Features

### Incremental Data Extraction

The orders extraction pipeline uses an `updated_at` timestamp and order ID checkpoint to identify new or changed records.

Only changed records are exported, reducing unnecessary extraction work.

### Snapshot Change Detection

Order-item data is extracted using versioned full snapshots because the source table does not contain an update timestamp.

SHA-256 hashing identifies unchanged snapshots and prevents unnecessary snapshot generation.

Each versioned snapshot includes a JSON metadata file containing source row counts, total units, and gross order value for reconciliation.

### Automated Uploads

Python invokes the Databricks CLI to upload new CSV files and matching JSON metadata into a Unity Catalog volume.

Previously uploaded filenames are skipped during subsequent executions.

### Medallion Architecture

**Bronze:** Stores ingested order versions and the current order-item source snapshot.

**Silver:** Uses Delta MERGE to retain the latest version of each order. Order items are validated using the composite key `(order_id, product_id)` and refreshed from the latest complete snapshot.

**Gold:** Produces daily order activity and revenue metrics by joining validated datasets.

### Data Quality and Reconciliation

Automated checks verify:

- Unique order identifiers
- Valid order-item quantities and prices
- Referential integrity between orders and order items
- Row counts and total quantities
- Gross order value across PostgreSQL snapshot metadata, Silver, and Gold

The workflow raises an error when validation fails.

## Dataset

The project uses synthetic retail data.

| Table | Initial records |
|---|---:|
| Customers | 100 |
| Products | 30 |
| Inventory | 30 |
| Orders | 500 |
| Order Items | 1,253 |

The dataset is intended for demonstrating data engineering techniques and does not represent a real business.

## Example Validation Results

The pipeline was tested with an order-item quantity update.

| Metric | Verified result |
|---|---:|
| Order items | 1,253 |
| Total units | 2,534 |
| Gross order value | $220,730.47 |
| Silver / Gold reconciliation | Passed |

Gross order value represents the value of ordered items, including orders that may be pending or canceled. It should not be interpreted as recognized accounting revenue.

## Databricks Workflow

The Databricks job `retail_orders_ingestion` executes three dependent notebook tasks.

1. `process_retail_orders` — Discovers new order files, ingests Bronze records, and performs Silver MERGE.
2. `refresh_revenue_analytics` — Refreshes order-item snapshots and generates Gold metrics.
3. `validate_data_quality` — Executes data-quality and reconciliation checks.

The workflow runs on Databricks serverless compute and can be triggered through the CLI.

## Project Structure

```text
retail-data-engineering/
├── notebooks/
│   ├── 01_incremental_orders_ingestion.py
│   ├── 02_order_items_processing.py
│   └── 03_data_quality_checks.py
├── sql/
│   └── schema.sql
├── setup_project.py
├── extract_orders.py
├── incremental_extract.py
├── extract_order_items.py
├── upload_to_databricks.py
├── run_pipeline.py
├── requirements.txt
├── .gitignore
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.10 or newer
- PostgreSQL with a `retail_analytics` database
- Databricks workspace with Unity Catalog support
- Databricks CLI with an authenticated profile

### Install Dependencies

```powershell
python -m pip install -r requirements.txt
```

### Initialize PostgreSQL Data

```powershell
python setup_project.py
```

The script prompts for the PostgreSQL password.

### Configure Databricks

Authenticate using the Databricks CLI and configure the workspace-specific profile and job ID.

The project currently uses the Unity Catalog volume:

```text
/Volumes/workspace/bronze/retail_raw
```

The Databricks notebooks and corresponding job must be imported or configured in the target workspace.

### Run the Pipeline

```powershell
python run_pipeline.py
```

This executes the local extraction scripts, uploads new source files, and triggers the Databricks workflow.

## Limitations and Future Improvements

- Add stronger recovery handling for interrupted extraction or uploads.
- Verify checksums against uploaded files.
- Improve snapshot publication and atomicity.
- Support configurable Databricks catalogs, schemas, and volumes.
- Move critical validations before Gold publishing.
- Add automated integration tests.
- Schedule pipeline runs.
- Build a Databricks SQL dashboard for sales and revenue trends.

## Project Purpose

This project demonstrates practical data engineering concepts, including incremental loading, checkpoint management, data transformation, job orchestration, data-quality testing, and analytical modeling.

It was built as a hands-on portfolio project to demonstrate end-to-end data pipeline development.