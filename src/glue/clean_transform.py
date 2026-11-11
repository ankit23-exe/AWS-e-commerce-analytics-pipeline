"""Glue PySpark job: raw CSV (bronze) -> validated, typed Parquet (silver).

Flow
----
1. Read every raw table as strings (nothing is silently inferred).
2. Clean and type each table (trim, blank -> NULL, cast, standardise).
3. Apply row-level rules and referential checks. Rows that fail go to the
   quarantine zone with the name of the failed rule, they are never dropped silently.
4. Write a data-quality report (row counts reconcile: raw = duplicates + quarantined + valid).
5. Publish the curated zone ONLY if the share of bad rows is under a threshold.
   If the job fails, the previous curated data stays untouched.

The transformation logic lives in plain functions (no Glue imports) so it can be
unit tested on a laptop. Glue-specific code is isolated in main().
"""
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TABLES = ("customers", "sellers", "products", "category_translation", "orders", "order_items")

VALID_ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
]

ORDER_TS_COLUMNS = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]


class DataQualityError(RuntimeError):
    """Raised when too many rows fail validation to publish the curated zone."""


# --------------------------------------------------------------------------- #
# Generic helpers
# --------------------------------------------------------------------------- #
def read_raw(spark: SparkSession, raw_path: str, table: str) -> DataFrame:
    """Read one raw table (all columns as strings). Layout: <raw_path>/<table>/*.csv"""
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"{raw_path.rstrip('/')}/{table}/")
    )
    # Some exports add a BOM or stray spaces to the header, normalise the names.
    return df.toDF(*[c.strip().lstrip("\ufeff") for c in df.columns])


def blank_to_null(df: DataFrame) -> DataFrame:
    """Trim every string column and turn empty strings into NULL."""
    return df.select(
        *[
            F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))).alias(c)
            for c in df.columns
        ]
    )


def dedupe(df: DataFrame, keys: list) -> DataFrame:
    """Keep exactly one row per key. Sorting on all columns makes the choice deterministic,
    so re-running the job on the same input always gives the same output."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).asc_nulls_last() for c in df.columns])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def apply_rules(df: DataFrame, rules: dict):
    """Split df into (valid, invalid).

    `rules` maps a failure name -> boolean Column that must be TRUE for a good row.
    A NULL result counts as a failure. Invalid rows get `_failed_rules` (comma separated).
    """
    flags = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    tagged = df.withColumn("_failed_rules", F.concat_ws(",", *flags))
    valid = tagged.filter(F.col("_failed_rules") == "").drop("_failed_rules")
    invalid = tagged.filter(F.col("_failed_rules") != "")
    return valid, invalid


def split_by_reference(df: DataFrame, ref: DataFrame, key: str, rule_name: str):
    """Referential integrity: rows whose `key` exists in `ref` stay, the rest are orphans."""
    ref_keys = ref.select(key).distinct()
    cols = df.columns  # a join on a column name moves that key first; restore the original order
    valid = df.join(ref_keys, on=key, how="left_semi").select(*cols)
    orphans = (
        df.join(ref_keys, on=key, how="left_anti").select(*cols).withColumn("_failed_rules", F.lit(rule_name))
    )
    return valid, orphans


# --------------------------------------------------------------------------- #
# Table-specific cleaning (typing and standardising only, no filtering)
# --------------------------------------------------------------------------- #
def clean_customers(raw: DataFrame) -> DataFrame:
    df = blank_to_null(raw)
    return df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",  # stays a string: zip prefixes can start with 0
        F.lower("customer_city").alias("customer_city"),
        F.upper("customer_state").alias("customer_state"),
    )


def clean_sellers(raw: DataFrame) -> DataFrame:
    df = blank_to_null(raw)
    return df.select(
        "seller_id",
        "seller_zip_code_prefix",
        F.lower("seller_city").alias("seller_city"),
        F.upper("seller_state").alias("seller_state"),
    )


def clean_category_translation(raw: DataFrame) -> DataFrame:
    return dedupe(blank_to_null(raw), ["product_category_name"])


def clean_products(raw: DataFrame, translation: DataFrame) -> DataFrame:
    df = blank_to_null(raw)
    # The source files spell "length" as "lenght". Fix it once, at the boundary.
    df = df.withColumnRenamed("product_name_lenght", "product_name_length").withColumnRenamed(
        "product_description_lenght", "product_description_length"
    )
    tr = translation.select("product_category_name", F.col("product_category_name_english").alias("_en"))
    df = df.join(F.broadcast(tr), on="product_category_name", how="left")
    int_cols = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    return df.select(
        "product_id",
        F.coalesce(F.col("product_category_name"), F.lit("unknown")).alias("product_category_name"),
        F.coalesce(F.col("_en"), F.col("product_category_name"), F.lit("unknown")).alias(
            "product_category_name_english"
        ),
        *[F.col(c).cast("int").alias(c) for c in int_cols],
    )


def clean_orders(raw: DataFrame) -> DataFrame:
    df = blank_to_null(raw)
    for c in ORDER_TS_COLUMNS:
        df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df.select(
        "order_id",
        "customer_id",
        F.lower("order_status").alias("order_status"),
        *ORDER_TS_COLUMNS,
        # partition columns go last: Spark moves them into the folder path
        F.year("order_purchase_timestamp").alias("purchase_year"),
        F.month("order_purchase_timestamp").alias("purchase_month"),
    )


def clean_order_items(raw: DataFrame) -> DataFrame:
    df = blank_to_null(raw)
    return df.select(
        "order_id",
        F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id",
        "seller_id",
        F.to_timestamp(F.col("shipping_limit_date")).alias("shipping_limit_date"),
        F.col("price").cast("decimal(10,2)").alias("price"),
        F.col("freight_value").cast("decimal(10,2)").alias("freight_value"),
    )


def build_dim_date(orders: DataFrame) -> DataFrame:
    """One row per calendar day between the first and last order."""
    bounds = orders.select(
        F.min(F.to_date("order_purchase_timestamp")).alias("d0"),
        F.max(F.to_date("order_purchase_timestamp")).alias("d1"),
    ).first()
    if bounds["d0"] is None:
        raise DataQualityError("No valid orders, cannot build dim_date")
    n_days = (bounds["d1"] - bounds["d0"]).days + 1
    days = orders.sparkSession.range(n_days).select(
        F.date_add(F.lit(bounds["d0"]), F.col("id").cast("int")).alias("full_date")
    )
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"),
        F.dayofweek("full_date").alias("day_of_week"),  # 1 = Sunday
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )


# --------------------------------------------------------------------------- #
# Validation of one table
# --------------------------------------------------------------------------- #
def process_table(name, typed, keys, rules, refs, report):
    """Apply rules, dedupe, check references. Returns (valid, invalid) and fills the report.

    refs: list of (key_column, parent_valid_dataframe, rule_name)
    """
    typed = typed.cache()
    raw_n = typed.count()

    valid, invalid = apply_rules(typed, rules)
    rule_valid_n = valid.count()

    valid = dedupe(valid, keys).cache()
    deduped_n = valid.count()

    for key, parent, rule in refs:
        valid, orphans = split_by_reference(valid, parent, key, rule)
        invalid = invalid.unionByName(orphans)

    valid, invalid = valid.cache(), invalid.cache()
    valid_n, invalid_n = valid.count(), invalid.count()
    dup_n = rule_valid_n - deduped_n

    reasons = {r["_failed_rules"]: r["count"] for r in invalid.groupBy("_failed_rules").count().collect()}
    report["tables"][name] = {
        "raw_rows": raw_n,
        "duplicates_removed": dup_n,
        "quarantined_rows": invalid_n,
        "valid_rows": valid_n,
        "reconciled": raw_n == dup_n + invalid_n + valid_n,
        "quarantine_reasons": reasons,
    }
    return valid, invalid


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
def run_pipeline(spark, raw_path, curated_path, quarantine_path, report_path, run_id, max_bad_ratio=0.05):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Redshift COPY reads TIMESTAMP_MICROS Parquet timestamps; Spark's INT96 default is legacy
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    raw = {t: read_raw(spark, raw_path, t) for t in TABLES}
    report = {"run_id": run_id, "max_bad_ratio": max_bad_ratio, "tables": {}}

    translation = clean_category_translation(raw["category_translation"])

    customers, bad_customers = process_table(
        "customers", clean_customers(raw["customers"]), ["customer_id"],
        {
            "customer_id_missing": F.col("customer_id").isNotNull(),
            "customer_state_invalid": F.length("customer_state") == 2,
        },
        [], report,
    )
    sellers, bad_sellers = process_table(
        "sellers", clean_sellers(raw["sellers"]), ["seller_id"],
        {
            "seller_id_missing": F.col("seller_id").isNotNull(),
            "seller_state_invalid": F.length("seller_state") == 2,
        },
        [], report,
    )
    products, bad_products = process_table(
        "products", clean_products(raw["products"], translation), ["product_id"],
        {
            "product_id_missing": F.col("product_id").isNotNull(),
            "product_weight_negative": F.col("product_weight_g").isNull() | (F.col("product_weight_g") >= 0),
        },
        [], report,
    )
    orders, bad_orders = process_table(
        "orders", clean_orders(raw["orders"]), ["order_id"],
        {
            "order_id_missing": F.col("order_id").isNotNull(),
            "customer_id_missing": F.col("customer_id").isNotNull(),
            "order_status_invalid": F.col("order_status").isin(VALID_ORDER_STATUSES),
            "purchase_ts_missing_or_invalid": F.col("order_purchase_timestamp").isNotNull(),
            "delivered_before_purchase": F.col("order_delivered_customer_date").isNull()
            | F.col("order_purchase_timestamp").isNull()
            | (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp")),
        },
        [("customer_id", customers, "orphan_customer_id")], report,
    )
    order_items, bad_items = process_table(
        "order_items", clean_order_items(raw["order_items"]), ["order_id", "order_item_id"],
        {
            "order_id_missing": F.col("order_id").isNotNull(),
            "order_item_id_missing": F.col("order_item_id").isNotNull(),
            "product_id_missing": F.col("product_id").isNotNull(),
            "seller_id_missing": F.col("seller_id").isNotNull(),
            "price_invalid": F.col("price") >= 0,
            "freight_invalid": F.col("freight_value") >= 0,
        },
        [
            ("order_id", orders, "orphan_order_id"),
            ("product_id", products, "orphan_product_id"),
            ("seller_id", sellers, "orphan_seller_id"),
        ],
        report,
    )

    # 1) quarantine + report are always written, so failures are inspectable
    base = quarantine_path.rstrip("/")
    for name, bad in [
        ("customers", bad_customers), ("sellers", bad_sellers), ("products", bad_products),
        ("orders", bad_orders), ("order_items", bad_items),
    ]:
        if report["tables"][name]["quarantined_rows"] > 0:
            bad.write.mode("overwrite").parquet(f"{base}/{name}/run_id={run_id}")

    report_json = json.dumps(report, sort_keys=True)
    spark.createDataFrame([(report_json,)], ["report"]).coalesce(1).write.mode("overwrite").text(
        f"{report_path.rstrip('/')}/run_id={run_id}"
    )
    print("DATA QUALITY REPORT: " + report_json)

    # 2) gate: do not publish the curated zone if the data is too dirty
    worst = max(
        t["quarantined_rows"] / t["raw_rows"] if t["raw_rows"] else 0 for t in report["tables"].values()
    )
    if worst > max_bad_ratio:
        raise DataQualityError(
            f"Quarantine ratio {worst:.2%} exceeds the {max_bad_ratio:.2%} limit. Curated zone not updated."
        )

    # 3) publish (overwrite = safe to re-run, same input gives the same output)
    out = curated_path.rstrip("/")
    customers.write.mode("overwrite").parquet(f"{out}/customers")
    sellers.write.mode("overwrite").parquet(f"{out}/sellers")
    products.write.mode("overwrite").parquet(f"{out}/products")
    order_items.write.mode("overwrite").parquet(f"{out}/order_items")
    orders.write.mode("overwrite").partitionBy("purchase_year", "purchase_month").parquet(f"{out}/orders")
    build_dim_date(orders).write.mode("overwrite").parquet(f"{out}/dim_date")
    return report


# --------------------------------------------------------------------------- #
# Glue entry point
# --------------------------------------------------------------------------- #
def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "raw_path", "curated_path", "quarantine_path", "report_path",
         "max_bad_ratio", "curated_crawler_name"],
    )
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_pipeline(
        spark, args["raw_path"], args["curated_path"], args["quarantine_path"],
        args["report_path"], run_id, float(args["max_bad_ratio"]),
    )

    # Refresh the Data Catalog so Athena sees the new data
    try:
        boto3.client("glue").start_crawler(Name=args["curated_crawler_name"])
    except Exception as exc:  # a crawler that is already running is not an error
        print(f"Crawler not started: {exc}")
    job.commit()


if __name__ == "__main__":
    main()
