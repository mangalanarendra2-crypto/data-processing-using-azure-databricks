# src/transformations.py
"""
Pure(ish) PySpark transformation functions, deliberately kept separate from
the notebooks that call them. Notebooks are hard to unit test directly;
functions that take a DataFrame in and return a DataFrame out are easy to
test with pytest (see tests/test_transformations.py).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType


# Explicit schema for raw ingestion. On Databricks, letting Spark infer the
# schema of every file is slow and can silently drift; pinning it here makes
# the bronze layer predictable and gives clear errors on malformed input.
RAW_SALES_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("store_region", StringType(), nullable=True),
    StructField("product_category", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("sale_amount", DoubleType(), nullable=True),
    StructField("sale_timestamp", TimestampType(), nullable=True),
])


def add_ingestion_metadata(df: DataFrame, source_file_col: str = "_source_file") -> DataFrame:
    """Stamp every bronze row with ingestion metadata for lineage/auditing."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn(source_file_col, F.input_file_name())
    )


def clean_sales_bronze_to_silver(df: DataFrame, min_valid_amount: float = 0.0) -> DataFrame:
    """
    Bronze -> Silver cleaning rules:
      - drop rows missing a transaction_id (can't dedupe/trace those)
      - drop exact duplicate transaction_ids, keeping the most recently ingested
      - fill missing store_region / product_category with 'UNKNOWN'
      - drop rows with a negative or null sale_amount (bad data)
      - derive sale_date from sale_timestamp for easy daily aggregation
    """
    from pyspark.sql.window import Window

    cleaned = df.filter(F.col("transaction_id").isNotNull())

    dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
    cleaned = (
        cleaned.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    cleaned = (
        cleaned.withColumn(
            "store_region",
            F.when(F.col("store_region").isNull() | (F.col("store_region") == ""), "UNKNOWN")
            .otherwise(F.col("store_region")),
        )
        .withColumn(
            "product_category",
            F.when(F.col("product_category").isNull() | (F.col("product_category") == ""), "UNKNOWN")
            .otherwise(F.col("product_category")),
        )
    )

    cleaned = cleaned.filter(
        F.col("sale_amount").isNotNull() & (F.col("sale_amount") >= min_valid_amount)
    )

    cleaned = cleaned.withColumn("sale_date", F.to_date("sale_timestamp"))

    return cleaned


def build_daily_summary(silver_df: DataFrame) -> DataFrame:
    """Silver -> Gold: total revenue, order count, and avg order value per day."""
    return (
        silver_df.groupBy("sale_date")
        .agg(
            F.sum("sale_amount").alias("total_revenue"),
            F.count("transaction_id").alias("num_transactions"),
            F.round(F.avg("sale_amount"), 2).alias("avg_order_value"),
        )
        .orderBy("sale_date")
    )


def build_region_summary(silver_df: DataFrame) -> DataFrame:
    """Silver -> Gold: revenue and transaction count per region per day."""
    return (
        silver_df.groupBy("sale_date", "store_region")
        .agg(
            F.sum("sale_amount").alias("total_revenue"),
            F.count("transaction_id").alias("num_transactions"),
        )
        .orderBy("sale_date", "store_region")
    )


def null_rate(df: DataFrame, column: str) -> float:
    """Fraction of rows where `column` is null. Used for simple data-quality checks."""
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(column).isNull()).count()
    return nulls / total
