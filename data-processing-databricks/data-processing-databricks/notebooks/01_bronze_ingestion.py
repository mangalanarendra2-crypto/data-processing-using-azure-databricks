# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC Reads raw sales CSV files and lands them as a Bronze Delta table with an
# MAGIC enforced schema and ingestion metadata (source file, ingestion timestamp).
# MAGIC No business logic here — bronze is a raw, append-only copy of the source.

# COMMAND ----------

import sys, os
# Make `src` importable whether cwd is the project root (Databricks Repos,
# or `python notebooks/x.py` run from the project root) or notebooks/ itself.
for _candidate in (os.getcwd(), os.path.dirname(os.getcwd())):
    if os.path.isdir(os.path.join(_candidate, "src")):
        sys.path.append(_candidate)
        break

from src.utils import get_spark, load_config, resolve_path, full_table_name, log
from src.transformations import RAW_SALES_SCHEMA, add_ingestion_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets (job parameters)
# MAGIC On Databricks these become parameters you can set from a Job or the notebook UI.
# MAGIC Locally, `dbutils` doesn't exist, so we fall back to config.yaml defaults.

# COMMAND ----------

cfg = load_config()

try:
    dbutils.widgets.text("raw_path", cfg["paths"]["raw"], "Raw input path")
    dbutils.widgets.text("bronze_path", cfg["paths"]["bronze"], "Bronze output path")
    raw_path = dbutils.widgets.get("raw_path")
    bronze_path = dbutils.widgets.get("bronze_path")
except NameError:
    raw_path = resolve_path(cfg, "raw")
    bronze_path = resolve_path(cfg, "bronze")

spark = get_spark("01-bronze-ingestion")
log(f"Reading raw files from: {raw_path}")
log(f"Writing bronze table to: {bronze_path}")

# COMMAND ----------

raw_df = (
    spark.read.option("header", True)
    .schema(RAW_SALES_SCHEMA)
    .csv(raw_path)
)

bronze_df = add_ingestion_metadata(raw_df)

row_count = bronze_df.count()
log(f"Read {row_count} raw rows.")
display(bronze_df.limit(10)) if "display" in dir() else bronze_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Bronze (append-only, Delta with schema enforcement)

# COMMAND ----------

writer = bronze_df.write.mode("append").option("mergeSchema", "false")

try:
    writer.format("delta").save(bronze_path)
    log("Wrote bronze data as Delta.")
except Exception as e:
    # Falls back to Parquet if Delta isn't available (e.g. no delta-spark installed locally)
    log(f"Delta write failed ({e}); falling back to Parquet.")
    bronze_df.write.mode("append").parquet(bronze_path)

# Optionally register in the metastore / Unity Catalog so downstream notebooks
# and BI tools can query it by name instead of by path.
try:
    table_name = full_table_name(cfg, "bronze")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{bronze_path}'")
    log(f"Registered table: {table_name}")
except Exception as e:
    log(f"Skipping table registration (expected when running locally without a metastore): {e}")

log("Bronze ingestion complete.")
