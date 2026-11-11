# Design decisions (and how to explain them)

## Why Parquet, and why partition orders by month?
Parquet is columnar and compressed: a query that needs two columns reads two columns, not whole rows.
Partitioning `orders` by purchase year and month means a filter such as "January 2018" reads one folder.
Measure it: run both queries in `sql/athena/queries.sql` and compare "Data scanned".

## Why keep raw (bronze) untouched?
So any bug in the cleaning logic can be fixed and the pipeline re-run from the original data. It also lets
you trace a bad number in a dashboard back to the exact source row.

## Why read everything as strings first?
Type inference hides problems. Reading as strings and casting explicitly turns a bad date or price into a
NULL that a named rule catches and quarantines, instead of a silent wrong value.

## Why quarantine instead of dropping or failing?
- Dropping hides data loss.
- Failing on the first bad row blocks the whole pipeline for one typo.
- Quarantine keeps the good data flowing and stores the bad rows with the rule that rejected them.
The row-count reconciliation (`raw = duplicates + quarantined + valid`) proves nothing went missing.

## Why a publish gate (`max_bad_ratio`)?
A sudden jump in bad rows usually means an upstream change, not bad luck. The job fails before replacing
the curated zone, so dashboards keep showing the last good data. (Limitation: the six table writes are
not one atomic transaction. A table format such as Iceberg or Delta would add that.)

## How is the pipeline idempotent?
Writes use overwrite, duplicate removal is deterministic (sorted on all columns), and the warehouse load
runs in one transaction. Running it twice on the same input gives the same output. There is a test for this.

## Why a `_READY` marker file?
Uploading six CSVs fires six S3 events. The marker makes the upload "complete" explicitly, so exactly one
Glue run starts. The Glue job also allows one concurrent run, and the Lambda treats "already running" as
a skip, not a failure.

## Why staging and then a star schema in Redshift?
Staging is a transient copy of curated data (truncate and reload). The core layer holds the dimensional model.
Loading with `COPY` is much faster than row-by-row inserts, and the transform from staging to star is plain SQL.

## Star schema choices
- **Grain:** one row per order item. State it first, everything else follows from it.
- **Surrogate keys** on dimensions protect the fact table from changes to business keys.
- **DISTSTYLE ALL** for small dimensions: every node has a copy, so joins do not move data.
- **DISTKEY(order_id) / SORTKEY(order_date_key)** on the fact table: items of one order stay together, and
  date filters (the most common dashboard filter) skip blocks.
- **Slowly changing dimensions:** this project uses Type 1 (overwrite), which is the common default. For Type 2
  add `valid_from`, `valid_to`, `is_current`; when an attribute changes, expire the current row and insert a new one.

## Why is `dim_date` generated, not loaded?
It is a calendar, not source data. Generating it from the min and max order dates guarantees every fact row
has a matching date key.

## How would this scale or change?
- **10x to 100x data:** more Glue workers, partition `order_items` by month, switch to incremental loads
  (process only new month partitions and MERGE into the warehouse).
- **Near real time:** Kinesis or MSK for ingestion, and a table format with upserts.
- **Orchestration:** Step Functions or Airflow for retries, dependencies and alerting on failure.
- **Governance:** Lake Formation permissions on the catalog.

## Cost controls
Budget alert, Athena scan cap per query, Redshift off by default, small Glue worker count with a timeout,
and a lifecycle rule on query results.
