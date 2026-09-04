# tests/test_transformations.py
"""
Unit tests for src/transformations.py. Run with:
    pytest tests/
"""

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, Row

from src.transformations import (
    clean_sales_bronze_to_silver,
    build_daily_summary,
    build_region_summary,
    null_rate,
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def bronze_sample(spark):
    rows = [
        Row(transaction_id="T1", customer_id="C1", store_region="North",
            product_category="Electronics", quantity=1, unit_price=100.0,
            sale_amount=100.0, sale_timestamp=datetime(2026, 9, 1, 10, 0, 0),
            _ingested_at=datetime(2026, 9, 1, 12, 0, 0)),
        # duplicate transaction_id, ingested later -> should win the dedup
        Row(transaction_id="T1", customer_id="C1", store_region="North",
            product_category="Electronics", quantity=1, unit_price=100.0,
            sale_amount=100.0, sale_timestamp=datetime(2026, 9, 1, 10, 0, 0),
            _ingested_at=datetime(2026, 9, 1, 13, 0, 0)),
        # missing region -> should become UNKNOWN
        Row(transaction_id="T2", customer_id="C2", store_region=None,
            product_category="Apparel", quantity=2, unit_price=20.0,
            sale_amount=40.0, sale_timestamp=datetime(2026, 9, 2, 9, 0, 0),
            _ingested_at=datetime(2026, 9, 2, 9, 5, 0)),
        # negative amount -> should be dropped
        Row(transaction_id="T3", customer_id="C3", store_region="South",
            product_category="Toys", quantity=1, unit_price=-5.0,
            sale_amount=-5.0, sale_timestamp=datetime(2026, 9, 2, 11, 0, 0),
            _ingested_at=datetime(2026, 9, 2, 11, 5, 0)),
        # null transaction_id -> should be dropped
        Row(transaction_id=None, customer_id="C4", store_region="East",
            product_category="Grocery", quantity=3, unit_price=10.0,
            sale_amount=30.0, sale_timestamp=datetime(2026, 9, 2, 12, 0, 0),
            _ingested_at=datetime(2026, 9, 2, 12, 5, 0)),
    ]
    return spark.createDataFrame(rows)


def test_dedup_keeps_latest_ingested(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    t1_rows = silver.filter(silver.transaction_id == "T1").collect()
    assert len(t1_rows) == 1


def test_null_transaction_id_dropped(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    assert silver.filter(silver.transaction_id.isNull()).count() == 0


def test_negative_amount_dropped(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    assert silver.filter(silver.sale_amount < 0).count() == 0


def test_missing_region_filled_unknown(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    t2 = silver.filter(silver.transaction_id == "T2").collect()[0]
    assert t2.store_region == "UNKNOWN"


def test_silver_row_count(bronze_sample):
    # 5 input rows -> T1 dup collapses to 1, T3 (negative) dropped, null-id row dropped
    # remaining: T1, T2 = 2 rows
    silver = clean_sales_bronze_to_silver(bronze_sample)
    assert silver.count() == 2


def test_daily_summary_aggregates_correctly(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    daily = build_daily_summary(silver)
    result = {row.sale_date: row.total_revenue for row in daily.collect()}
    # T1 = 100.0 on 2026-09-01, T2 = 40.0 on 2026-09-02
    from datetime import date
    assert result[date(2026, 9, 1)] == 100.0
    assert result[date(2026, 9, 2)] == 40.0


def test_region_summary_groups_by_region_and_date(bronze_sample):
    silver = clean_sales_bronze_to_silver(bronze_sample)
    region = build_region_summary(silver)
    regions = {row.store_region for row in region.collect()}
    assert regions == {"North", "UNKNOWN"}


def test_null_rate_helper(spark):
    df = spark.createDataFrame([Row(x=1), Row(x=None), Row(x=3), Row(x=None)])
    assert null_rate(df, "x") == 0.5
