-- Run in Athena with the workgroup created by Terraform (it caps each query at 1 GB scanned).
-- Table names come from the crawlers: raw_* (CSV, all columns strings) and curated_* (Parquet).
-- After each query, note "Data scanned" under the results. These numbers belong in your README.

-- 1) The SAME question on raw CSV vs partitioned Parquet
SELECT COUNT(*) AS orders_jan_2018
FROM raw_orders
WHERE order_purchase_timestamp LIKE '2018-01%';

SELECT COUNT(*) AS orders_jan_2018
FROM curated_orders
WHERE purchase_year = 2018 AND purchase_month = 1;      -- partition pruning: only that folder is read

-- 2) Column pruning: Parquet reads only the columns the query needs
SELECT SUM(price) AS revenue FROM curated_order_items;

-- 3) Revenue by month straight from the lake (no warehouse needed for ad-hoc work)
SELECT o.purchase_year, o.purchase_month, ROUND(SUM(i.price), 2) AS revenue
FROM curated_order_items i
JOIN curated_orders o ON o.order_id = i.order_id
WHERE o.order_status <> 'canceled'
GROUP BY o.purchase_year, o.purchase_month
ORDER BY o.purchase_year, o.purchase_month;
