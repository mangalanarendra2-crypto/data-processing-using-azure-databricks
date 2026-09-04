# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformation
# MAGIC Reads the Bronze table, applies cleaning/validation rules, and writes a
# MAGIC deduplicated, typed, analysis-ready Silver table.

# COMMAND ----------

import sys, os
for _candidate in (os.getcwd(), os.path.dirname(os.getcwd())):
    if os.path.isdir(os.path.join(_candidate, "src")):
        sys.path.append(_candidate)
        break

from src.utils import get_spark, load_config, resolve_path, full_table_name, log
from src.transformations import clean_sales_bronze_to_silver, null_rate

# COMMAND ----------

cfg = load_config()

try:
    dbutils.widgets.text("bronze_path", cfg["paths"]["bronze"], "Bronze input path")
    dbutils.widgets.text("silver_path", cfg["paths"]["silver"], "Silver output path")
    bronze_path = dbutils.widgets.get("bronze_path")
    silver_path = dbutils.widgets.get("silver_path")
except NameError:
    bronze_path = resolve_path(cfg, "bronze")
    silver_path = resolve_path(cfg, "silver")

spark = get_spark("02-silver-transformation")
log(f"Reading bronze from: {bronze_path}")
log(f"Writing silver to:   {silver_path}")

# COMMAND ----------

try:
    bronze_df = spark.read.format("delta").load(bronze_path)
except Exception:
    bronze_df = spark.read.parquet(bronze_path)

log(f"Bronze row count: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality check
# MAGIC Fail fast (or at least warn loudly) if key columns are null beyond an
# MAGIC acceptable threshold — better to catch upstream problems here than let
# MAGIC bad data flow into Gold/BI dashboards silently.

# COMMAND ----------

max_null_rate = cfg["data_quality"]["max_null_rate"]
for col in ["transaction_id", "sale_amount"]:
    rate = null_rate(bronze_df, col)
    log(f"Null rate for '{col}': {rate:.2%}")
    if rate > max_null_rate:
        log(f"WARNING: null rate for '{col}' ({rate:.2%}) exceeds threshold ({max_null_rate:.2%})")

# COMMAND ----------

silver_df = clean_sales_bronze_to_silver(
    bronze_df, min_valid_amount=cfg["data_quality"]["min_valid_amount"]
)

before, after = bronze_df.count(), silver_df.count()
log(f"Rows before cleaning: {before}, after cleaning: {after} (dropped {before - after})")
display(silver_df.limit(10)) if "display" in dir() else silver_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver (overwrite — silver always reflects the latest clean state)

# COMMAND ----------

writer = silver_df.write.mode("overwrite").option("overwriteSchema", "true")

try:
    writer.format("delta").save(silver_path)
    log("Wrote silver data as Delta.")
except Exception as e:
    log(f"Delta write failed ({e}); falling back to Parquet.")
    silver_df.write.mode("overwrite").parquet(silver_path)

try:
    table_name = full_table_name(cfg, "silver")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{silver_path}'")
    log(f"Registered table: {table_name}")
except Exception as e:
    log(f"Skipping table registration (expected when running locally without a metastore): {e}")

log("Silver transformation complete.")
