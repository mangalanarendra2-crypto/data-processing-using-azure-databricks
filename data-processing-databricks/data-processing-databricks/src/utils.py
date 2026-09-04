# src/utils.py
"""
Shared helpers used by every notebook in the pipeline.

On Databricks, `spark` and `dbutils` are already injected into the notebook
environment. `get_spark()` below detects that and reuses the existing
session; when running locally (e.g. via pytest, or `python notebooks/x.py`)
it builds a local SparkSession with Delta Lake support instead, so the same
code works in both places.
"""

import os
import yaml


def get_spark(app_name: str = "databricks-etl"):
    """Return a SparkSession, whether running on Databricks or locally."""
    try:
        # Databricks notebooks inject a global `spark` object automatically.
        return spark  # noqa: F821
    except NameError:
        pass

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # small value: fine for local/test data
    )

    # Enable Delta Lake locally too, so bronze/silver/gold tables behave the
    # same way as they would on Databricks (ACID writes, MERGE, time travel).
    try:
        from delta import configure_spark_with_delta_pip
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        # delta-spark not installed locally -> fall back to plain Spark/Parquet.
        return builder.getOrCreate()


def load_config(path: str = None) -> dict:
    """Load config/config.yaml (or a path override) as a plain dict."""
    if path is None:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(here, "config", "config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def resolve_path(cfg: dict, layer: str) -> str:
    """Resolve a data-lake layer ('raw' | 'bronze' | 'silver' | 'gold') to a path.

    On Databricks you'd typically point these at ADLS/Unity Catalog volume
    paths via config.yaml; locally they resolve relative to the repo root.
    """
    return cfg["paths"][layer]


def full_table_name(cfg: dict, table_key: str) -> str:
    """Build a catalog.schema.table (or schema.table) name from config."""
    catalog = cfg.get("catalog")
    schema = cfg["schema"]
    table = cfg["tables"][table_key]
    if catalog:
        return f"{catalog}.{schema}.{table}"
    return f"{schema}.{table}"


def log(msg: str):
    """Lightweight logger. Swap for `logging` or Databricks' native logging as needed."""
    print(f"[pipeline] {msg}")
