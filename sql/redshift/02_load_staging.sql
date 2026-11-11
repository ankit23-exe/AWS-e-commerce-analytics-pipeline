-- Replace the two placeholders first:
--   <LAKE_BUCKET>       terraform output lake_bucket
--   <REDSHIFT_ROLE_ARN> terraform output redshift_role_arn
-- Staging is transient: truncate, then load the latest curated Parquet.

TRUNCATE staging.customers;
COPY staging.customers   FROM 's3://<LAKE_BUCKET>/curated/customers/'   IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

TRUNCATE staging.sellers;
COPY staging.sellers     FROM 's3://<LAKE_BUCKET>/curated/sellers/'     IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

TRUNCATE staging.products;
COPY staging.products    FROM 's3://<LAKE_BUCKET>/curated/products/'    IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

TRUNCATE staging.orders;
COPY staging.orders      FROM 's3://<LAKE_BUCKET>/curated/orders/'      IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

TRUNCATE staging.order_items;
COPY staging.order_items FROM 's3://<LAKE_BUCKET>/curated/order_items/' IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

TRUNCATE staging.dim_date;
COPY staging.dim_date    FROM 's3://<LAKE_BUCKET>/curated/dim_date/'    IAM_ROLE '<REDSHIFT_ROLE_ARN>' FORMAT AS PARQUET;

-- Sanity check: row counts should match quality_reports/*/part-*.txt (valid_rows per table)
SELECT 'customers' AS tbl, COUNT(*) FROM staging.customers
UNION ALL SELECT 'sellers',     COUNT(*) FROM staging.sellers
UNION ALL SELECT 'products',    COUNT(*) FROM staging.products
UNION ALL SELECT 'orders',      COUNT(*) FROM staging.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM staging.order_items
UNION ALL SELECT 'dim_date',    COUNT(*) FROM staging.dim_date;
