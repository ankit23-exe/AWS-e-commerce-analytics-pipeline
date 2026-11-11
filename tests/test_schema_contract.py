"""Contract test: the Parquet columns written by Glue must match the Redshift staging tables.

Redshift COPY maps Parquet columns by position, so a reordered or renamed column would load
silently into the wrong field. This test fails first.
"""
import re
from pathlib import Path

import pytest

import clean_transform as ct

DDL = Path(__file__).resolve().parents[1] / "sql" / "redshift" / "01_schema.sql"
PARTITION_COLUMNS = {"purchase_year", "purchase_month"}  # stored in folder names, not in the files


def staging_columns():
    text = DDL.read_text()
    tables = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS staging\.(\w+) \((.*?)\n\);", text, re.S):
        cols = []
        for line in body.splitlines():
            line = line.split("--")[0].strip()
            if line:
                cols.append(line.split()[0])
        tables[name] = cols
    return tables


@pytest.fixture(scope="module")
def curated(spark, sample_raw, tmp_path_factory):
    base = tmp_path_factory.mktemp("contract")
    ct.run_pipeline(spark, sample_raw, str(base / "curated"), str(base / "q"), str(base / "r"),
                    run_id="contract", max_bad_ratio=0.10)
    return base / "curated"


@pytest.mark.parametrize("table", ["customers", "sellers", "products", "orders", "order_items", "dim_date"])
def test_parquet_columns_match_redshift_staging(spark, curated, table):
    written = [c for c in spark.read.parquet(str(curated / table)).columns if c not in PARTITION_COLUMNS]
    assert written == staging_columns()[table]
