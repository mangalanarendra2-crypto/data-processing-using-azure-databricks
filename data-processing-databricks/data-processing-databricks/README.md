# Data Processing Using Azure Databricks

A simple base project for a PySpark ETL pipeline on Azure Databricks,
following the **medallion architecture** (Bronze → Silver → Gold). Built so
the same code runs on Databricks *and* locally (for fast iteration/testing)
with no changes.

## Architecture

```
data/raw (CSV, upstream files)
        │
        ▼
┌───────────────────┐
│  01_bronze_ingestion │  Raw copy + schema enforcement + ingestion metadata
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ 02_silver_transformation │  Clean, dedupe, validate, type-cast
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  03_gold_aggregation  │  Business-level aggregates for BI/reporting
└───────────────────┘
```

- **Bronze** — raw, append-only, minimally-processed copy of the source data
  with lineage metadata (`_ingested_at`, `_source_file`). Nothing is dropped
  or "fixed" here — it's the audit trail.
- **Silver** — cleaned, deduplicated, validated data. This is where business
  rules like "drop rows with no ID", "fill missing region with UNKNOWN", or
  "reject negative amounts" live.
- **Gold** — aggregated, denormalized tables built for direct consumption by
  BI tools (Power BI, Databricks SQL dashboards) or downstream ML.

## Project Structure

```
data-processing-databricks/
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Databricks notebook-source format
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregation.py
├── src/
│   ├── utils.py                       # Spark session, config loader, helpers
│   └── transformations.py             # Pure PySpark functions (unit-testable)
├── tests/
│   └── test_transformations.py        # pytest unit tests for transformations.py
├── config/
│   └── config.yaml                    # paths, catalog/schema/table names, DQ thresholds
├── generate_sample_data.py            # creates a synthetic raw sales CSV to test with
├── databricks_job.json                # Databricks Workflow (Jobs) definition
├── requirements.txt                   # local dev/test dependencies only
└── data/                              # local sample data + lake output (gitignored on Databricks)
```

### Why split `notebooks/` and `src/`?

Notebooks are great for exploration but hard to unit test and easy to turn
into unmaintainable "notebook spaghetti." Here, notebooks are thin
orchestration layers — they read config, call functions from `src/`, and
write output. All actual transformation logic lives in
`src/transformations.py` as plain functions that take a DataFrame and return
a DataFrame, so they're trivial to test with pytest (see `tests/`) without
needing a live Databricks workspace.

## Running on Azure Databricks

1. **Import the repo**: Connect this repo via *Databricks Repos* (Workspace
   → Repos → Add Repo), or upload the `notebooks/` folder directly.
2. **Adjust `config/config.yaml`**: point `paths.raw/bronze/silver/gold` at
   your ADLS Gen2 path or Unity Catalog volume, e.g.
   `abfss://raw@yourstorageaccount.dfs.core.windows.net/sales` or
   `/Volumes/main/retail_sales/raw`. Set `catalog`/`schema` to match your
   Unity Catalog setup (or leave `catalog: null` to use a plain Hive
   metastore database).
3. **Run interactively**: open each notebook in order and run all cells —
   `spark` and `dbutils` are auto-injected by the Databricks runtime, so
   `get_spark()` and the widget-based parameters in `src/utils.py` and the
   notebooks work without modification.
4. **Or run as a scheduled Job**: import `databricks_job.json` as a
   Databricks Workflow (Workflows → Create Job → Edit as JSON, or
   `databricks jobs create --json @databricks_job.json` via the CLI). It
   wires the three notebooks together with dependencies, a nightly
   schedule (paused by default), a job cluster sized for a small workload,
   and failure email alerts — adjust node types, schedule, and paths for
   your workspace.

## Running Locally (for development/testing)

```bash
pip install -r requirements.txt

# 1. Generate a synthetic raw sales CSV (with some intentionally messy rows)
python generate_sample_data.py

# 2. Run the pipeline stages in order
python notebooks/01_bronze_ingestion.py
python notebooks/02_silver_transformation.py
python notebooks/03_gold_aggregation.py

# 3. Run unit tests for the transformation logic
pytest tests/ -v
```

Locally, `get_spark()` builds a local SparkSession (with Delta Lake support
if `delta-spark` is installed and reachable; otherwise it falls back to
Parquet automatically) and paths resolve to the `data/` folder — no Azure
resources needed.

## Data Quality

The silver notebook checks the null rate of key columns (`transaction_id`,
`sale_amount`) against a configurable threshold (`data_quality.max_null_rate`
in `config.yaml`) and logs a warning if exceeded — a starting point you can
extend into a hard failure, a dead-letter table for rejected rows, or an
integration with a framework like Great Expectations / Databricks' own
Lakehouse Monitoring.

## Next Steps (for extending this base project)

- Replace `generate_sample_data.py`'s output with a real source: Autoloader
  (`cloudFiles`) for incrementally-arriving files, a JDBC source, or an Event
  Hub / Kafka stream for near-real-time ingestion.
- Convert Bronze ingestion to **Structured Streaming** with Autoloader for
  true incremental loads instead of batch re-reads.
- Add `MERGE INTO` (Delta upserts) in the silver layer instead of a full
  overwrite, once you're handling incremental batches.
- Wire up Unity Catalog permissions and lineage instead of the Hive
  metastore fallback used here.
- Add Great Expectations or Databricks Lakehouse Monitoring for richer data
  quality checks and a dead-letter table for rejected rows.
- Add CI (e.g. GitHub Actions) to run `pytest tests/` on every PR before
  merging, and use Databricks Asset Bundles (DABs) to deploy notebooks/jobs
  instead of manual import.
