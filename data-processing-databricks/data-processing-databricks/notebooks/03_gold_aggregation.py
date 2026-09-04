# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Aggregation
# MAGIC Reads the Silver table and produces business-ready Gold tables:
# MAGIC daily revenue summary, and revenue by region. These are what a BI
# MAGIC tool (Power BI, Databricks SQL dashboards, etc.) would query directly.

# COMMAND ----------

import sys, os
for _candidate in (os.getcwd(), os.path.dirname(os.getcwd())):
    if os.path.isdir(os.path.join(_candidate, "src")):
        sys.path.append(_candidate)
        break

from src.utils import get_spark, load_config, resolve_path, full_table_name, log
from src.transformations import build_daily_summary, build_region_summary

# COMMAND ----------

cfg = load_config()

try:
    dbutils.widgets.text("silver_path", cfg["paths"]["silver"], "Silver input path")
    dbutils.widgets.text("gold_path", cfg["paths"]["gold"], "Gold output path")
    silver_path = dbutils.widgets.get("silver_path")
    gold_path = dbutils.widgets.get("gold_path")
except NameError:
    silver_path = resolve_path(cfg, "silver")
    gold_path = resolve_path(cfg, "gold")

spark = get_spark("03-gold-aggregation")
log(f"Reading silver from: {silver_path}")
log(f"Writing gold to:     {gold_path}")

# COMMAND ----------

try:
    silver_df = spark.read.format("delta").load(silver_path)
except Exception:
    silver_df = spark.read.parquet(silver_path)

log(f"Silver row count: {silver_df.count()}")

# COMMAND ----------

daily_summary_df = build_daily_summary(silver_df)
region_summary_df = build_region_summary(silver_df)

log("Daily summary:")
daily_summary_df.show(truncate=False)

log("Region summary:")
region_summary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Gold tables (overwrite — gold is fully rebuilt from silver each run)

# COMMAND ----------

daily_path = os.path.join(gold_path, "daily_summary")
region_path = os.path.join(gold_path, "region_summary")

for df, path, label in [
    (daily_summary_df, daily_path, "daily_summary"),
    (region_summary_df, region_path, "region_summary"),
]:
    try:
        df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(path)
        log(f"Wrote gold/{label} as Delta.")
    except Exception as e:
        log(f"Delta write failed for {label} ({e}); falling back to Parquet.")
        df.write.mode("overwrite").parquet(path)

try:
    daily_table = full_table_name(cfg, "gold_daily")
    region_table = full_table_name(cfg, "gold_by_region")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {daily_table} USING DELTA LOCATION '{daily_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {region_table} USING DELTA LOCATION '{region_path}'")
    log(f"Registered tables: {daily_table}, {region_table}")
except Exception as e:
    log(f"Skipping table registration (expected when running locally without a metastore): {e}")

log("Gold aggregation complete.")
