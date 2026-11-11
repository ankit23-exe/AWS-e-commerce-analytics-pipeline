"""Unit tests for the Glue job logic. They run on a local Spark session, no AWS needed."""
import datetime as dt
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import clean_transform as ct


# ----------------------------- generic helpers ----------------------------- #
def test_blank_to_null_trims_and_nulls(spark):
    df = spark.createDataFrame([("  a ", ""), ("b", "  ")], ["x", "y"])
    rows = [(r["x"], r["y"]) for r in ct.blank_to_null(df).collect()]
    assert rows == [("a", None), ("b", None)]


def test_apply_rules_names_each_failure_and_treats_null_as_failure(spark):
    df = spark.createDataFrame([("a", 5), ("b", -1), (None, 3), ("d", None)], "id string, qty int")
    valid, invalid = ct.apply_rules(
        df, {"id_missing": F.col("id").isNotNull(), "qty_negative": F.col("qty") >= 0}
    )
    assert [r["id"] for r in valid.collect()] == ["a"]
    assert {(r["id"], r["_failed_rules"]) for r in invalid.collect()} == {
        ("b", "qty_negative"), (None, "id_missing"), ("d", "qty_negative"),
    }


def test_dedupe_keeps_one_row_per_key_deterministically(spark):
    a = spark.createDataFrame([("k", "b"), ("k", "a"), ("j", "z")], ["key", "val"])
    b = spark.createDataFrame([("j", "z"), ("k", "a"), ("k", "b")], ["key", "val"])  # other input order
    out_a = sorted((r["key"], r["val"]) for r in ct.dedupe(a, ["key"]).collect())
    out_b = sorted((r["key"], r["val"]) for r in ct.dedupe(b, ["key"]).collect())
    assert out_a == out_b == [("j", "z"), ("k", "a")]


def test_split_by_reference_flags_orphans_including_null_keys(spark):
    child = spark.createDataFrame([("o1",), ("o2",), (None,)], "order_id string")
    parent = spark.createDataFrame([("o1",)], "order_id string")
    valid, orphans = ct.split_by_reference(child, parent, "order_id", "orphan_order_id")
    assert [r["order_id"] for r in valid.collect()] == ["o1"]
    assert {r["order_id"] for r in orphans.collect()} == {"o2", None}
    assert {r["_failed_rules"] for r in orphans.collect()} == {"orphan_order_id"}


# --------------------------- table cleaning rules --------------------------- #
def test_clean_products_fixes_typos_and_category_fallbacks(spark):
    raw = spark.createDataFrame(
        [("p1", "beleza_saude", "40", "300", "1", "200", "10", "5", "8"),
         ("p2", None, "40", "300", "1", "200", "10", "5", "8"),
         ("p3", "sem_traducao", "40", "300", "1", "200", "10", "5", "8")],
        ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
         "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm",
         "product_width_cm"],
    )
    translation = spark.createDataFrame(
        [("beleza_saude", "health_beauty")], ["product_category_name", "product_category_name_english"]
    )
    out = {r["product_id"]: r for r in ct.clean_products(raw, translation).collect()}
    assert "product_name_length" in ct.clean_products(raw, translation).columns
    assert out["p1"]["product_category_name_english"] == "health_beauty"
    assert out["p2"]["product_category_name_english"] == "unknown"
    assert out["p3"]["product_category_name_english"] == "sem_traducao"  # falls back to the original name
    assert out["p1"]["product_weight_g"] == 200  # cast to int


def test_clean_orders_parses_timestamps_and_adds_partition_columns(spark):
    raw = spark.createDataFrame(
        [("o1", "c1", "DELIVERED", "2018-01-15 10:00:00", None, None, None, "2018-01-30 10:00:00")],
        "order_id string, customer_id string, order_status string, order_purchase_timestamp string,"
        " order_approved_at string, order_delivered_carrier_date string,"
        " order_delivered_customer_date string, order_estimated_delivery_date string",
    )
    row = ct.clean_orders(raw).first()
    assert row["order_status"] == "delivered"
    assert row["order_purchase_timestamp"] == dt.datetime(2018, 1, 15, 10, 0, 0)
    assert (row["purchase_year"], row["purchase_month"]) == (2018, 1)


def test_clean_customers_keeps_leading_zero_zip_codes(spark):
    raw = spark.createDataFrame([("c1", "u1", "01046", " Sao Paulo ", "sp")],
                                ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                                 "customer_city", "customer_state"])
    row = ct.clean_customers(raw).first()
    assert row["customer_zip_code_prefix"] == "01046"
    assert (row["customer_city"], row["customer_state"]) == ("sao paulo", "SP")


def test_build_dim_date_is_contiguous_and_flags_weekends(spark):
    orders = spark.createDataFrame(
        [(dt.datetime(2018, 1, 5, 9, 0, 0),), (dt.datetime(2018, 1, 10, 9, 0, 0),)],
        ["order_purchase_timestamp"],
    )
    rows = ct.build_dim_date(orders).orderBy("full_date").collect()
    assert [r["date_key"] for r in rows] == [20180105, 20180106, 20180107, 20180108, 20180109, 20180110]
    weekend = {r["full_date"].isoformat(): r["is_weekend"] for r in rows}
    assert weekend["2018-01-06"] and weekend["2018-01-07"] and not weekend["2018-01-08"]  # Sat, Sun, Mon


# ------------------------------- full pipeline ------------------------------ #
def _run(spark, raw, tmp_path, max_bad_ratio=0.10):
    return ct.run_pipeline(
        spark, raw, str(tmp_path / "curated"), str(tmp_path / "quarantine"),
        str(tmp_path / "reports"), run_id="test", max_bad_ratio=max_bad_ratio,
    )


def test_pipeline_quarantines_exactly_the_planted_defects(spark, sample_raw, tmp_path):
    report = _run(spark, sample_raw, tmp_path)["tables"]

    # every table must reconcile: raw = duplicates + quarantined + valid
    assert all(t["reconciled"] for t in report.values())

    assert report["customers"]["duplicates_removed"] == 1
    assert report["customers"]["valid_rows"] == 30

    assert report["orders"]["duplicates_removed"] == 1
    assert report["orders"]["quarantined_rows"] == 3
    assert report["orders"]["quarantine_reasons"] == {
        "orphan_customer_id": 1, "order_status_invalid": 1, "purchase_ts_missing_or_invalid": 1,
    }

    assert report["order_items"]["raw_rows"] == 87
    assert report["order_items"]["duplicates_removed"] == 1
    assert report["order_items"]["valid_rows"] == 79
    assert report["order_items"]["quarantine_reasons"] == {
        "orphan_order_id": 4,        # 1 non-existent order + 3 items of quarantined orders
        "orphan_seller_id": 1, "price_invalid": 1, "product_id_missing": 1,
    }


def test_curated_zone_has_no_orphans_and_is_partitioned(spark, sample_raw, tmp_path):
    _run(spark, sample_raw, tmp_path)
    curated = tmp_path / "curated"

    orders = spark.read.parquet(str(curated / "orders"))
    items = spark.read.parquet(str(curated / "order_items"))
    customers = spark.read.parquet(str(curated / "customers"))
    dim_date = spark.read.parquet(str(curated / "dim_date"))

    assert orders.count() == 40 and items.count() == 79
    assert items.join(orders, "order_id", "left_anti").count() == 0          # no orphan items
    assert orders.join(customers, "customer_id", "left_anti").count() == 0    # no orphan orders
    assert {p.name for p in (curated / "orders").iterdir() if p.is_dir()} >= {"purchase_year=2017", "purchase_year=2018"}
    assert dim_date.count() == 118  # 2017-11-01 .. 2018-02-26 inclusive

    products = {r["product_id"]: r["product_category_name_english"]
                for r in spark.read.parquet(str(curated / "products")).collect()}
    assert products["p09"] == "unknown"
    assert products["p10"] == "categoria_sem_traducao"


def test_quarantine_files_and_report_are_written(spark, sample_raw, tmp_path):
    _run(spark, sample_raw, tmp_path)
    assert (tmp_path / "quarantine" / "order_items" / "run_id=test").exists()
    assert (tmp_path / "quarantine" / "orders" / "run_id=test").exists()
    assert not (tmp_path / "quarantine" / "customers").exists()  # nothing to quarantine there
    assert list((tmp_path / "reports" / "run_id=test").glob("part-*"))


def test_rerun_is_idempotent(spark, sample_raw, tmp_path):
    first = _run(spark, sample_raw, tmp_path)
    second = _run(spark, sample_raw, tmp_path)
    assert first["tables"] == second["tables"]
    assert spark.read.parquet(str(tmp_path / "curated" / "order_items")).count() == 79  # not 158


def test_pipeline_refuses_to_publish_when_data_is_too_dirty(spark, sample_raw, tmp_path):
    with pytest.raises(ct.DataQualityError):
        _run(spark, sample_raw, tmp_path, max_bad_ratio=0.0)
    assert (tmp_path / "quarantine").exists()    # the evidence is kept
    assert not (tmp_path / "curated").exists()   # nothing is published
