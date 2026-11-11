-- staging -> star schema. Full reload inside ONE transaction, so BI never sees a half-loaded warehouse
-- and re-running the script gives the same result (idempotent).
-- SCD: dimensions are Type 1 (overwrite). To keep history (Type 2) add valid_from, valid_to and
-- is_current columns and expire the old row instead of deleting it. See docs/design_decisions.md.

BEGIN;

DELETE FROM dw.fact_order_items;
DELETE FROM dw.dim_customer;
DELETE FROM dw.dim_seller;
DELETE FROM dw.dim_product;
DELETE FROM dw.dim_date;

INSERT INTO dw.dim_date
SELECT date_key, full_date, year, quarter, month, month_name, day, day_of_week, is_weekend
FROM staging.dim_date;

INSERT INTO dw.dim_customer (customer_id, customer_unique_id, zip_prefix, city, state)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM staging.customers;

INSERT INTO dw.dim_seller (seller_id, zip_prefix, city, state)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM staging.sellers;

INSERT INTO dw.dim_product (product_id, category_name, category_name_en, name_length,
                            description_length, photos_qty, weight_g, length_cm, height_cm, width_cm)
SELECT product_id, product_category_name, product_category_name_english, product_name_length,
       product_description_length, product_photos_qty, product_weight_g, product_length_cm,
       product_height_cm, product_width_cm
FROM staging.products;

INSERT INTO dw.fact_order_items (
    order_id, order_item_id, customer_key, product_key, seller_key, order_date_key,
    order_status, price, freight_value, item_total,
    purchase_ts, delivered_ts, estimated_delivery_ts, delivery_days, delivery_delay_days)
SELECT i.order_id,
       i.order_item_id,
       c.customer_key,
       p.product_key,
       s.seller_key,
       d.date_key,
       o.order_status,
       i.price,
       i.freight_value,
       i.price + i.freight_value,
       o.order_purchase_timestamp,
       o.order_delivered_customer_date,
       o.order_estimated_delivery_date,
       DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date),
       DATEDIFF(day, o.order_estimated_delivery_date, o.order_delivered_customer_date)
FROM staging.order_items i
JOIN staging.orders o     ON o.order_id    = i.order_id
JOIN dw.dim_customer c    ON c.customer_id = o.customer_id
JOIN dw.dim_product p     ON p.product_id  = i.product_id
JOIN dw.dim_seller s      ON s.seller_id   = i.seller_id
JOIN dw.dim_date d        ON d.date_key    = CAST(TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD') AS INT);

COMMIT;

-- Every curated item must land in the fact table (the Glue job already removed orphans)
SELECT (SELECT COUNT(*) FROM staging.order_items) AS staged_items,
       (SELECT COUNT(*) FROM dw.fact_order_items) AS fact_rows;
