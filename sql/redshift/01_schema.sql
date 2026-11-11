-- Run once in Redshift query editor v2.
-- staging.* mirrors the curated Parquet files column for column (COPY maps Parquet columns BY POSITION,
-- so keep this column order identical to the Glue job output).
-- dw.* is the star schema served to BI.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

-- ---------- staging (transient: truncated and reloaded on every run) ----------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id              VARCHAR(32),
    customer_unique_id       VARCHAR(32),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id              VARCHAR(32),
    seller_zip_code_prefix VARCHAR(10),
    seller_city            VARCHAR(100),
    seller_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id                    VARCHAR(32),
    product_category_name         VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length           INT,
    product_description_length    INT,
    product_photos_qty            INT,
    product_weight_g              INT,
    product_length_cm             INT,
    product_height_cm             INT,
    product_width_cm              INT
);

-- purchase_year / purchase_month live in the S3 folder names, not in the files
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id                      VARCHAR(32),
    customer_id                   VARCHAR(32),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id            VARCHAR(32),
    order_item_id       INT,
    product_id          VARCHAR(32),
    seller_id           VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS staging.dim_date (
    date_key    INT,
    full_date   DATE,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR(10),
    day         INT,
    day_of_week INT,
    is_weekend  BOOLEAN
);

-- ---------- dimensions (small, so DISTSTYLE ALL copies them to every node: joins need no shuffling) ----------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key    INT NOT NULL PRIMARY KEY,
    full_date   DATE,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR(10),
    day         INT,
    day_of_week INT,
    is_weekend  BOOLEAN
) DISTSTYLE ALL SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key       INT IDENTITY(1,1) PRIMARY KEY,   -- surrogate key
    customer_id        VARCHAR(32) NOT NULL,            -- natural (business) key
    customer_unique_id VARCHAR(32),
    zip_prefix         VARCHAR(10),
    city               VARCHAR(100),
    state              CHAR(2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id  VARCHAR(32) NOT NULL,
    zip_prefix VARCHAR(10),
    city       VARCHAR(100),
    state      CHAR(2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key         INT IDENTITY(1,1) PRIMARY KEY,
    product_id          VARCHAR(32) NOT NULL,
    category_name       VARCHAR(100),
    category_name_en    VARCHAR(100),
    name_length         INT,
    description_length  INT,
    photos_qty          INT,
    weight_g            INT,
    length_cm           INT,
    height_cm           INT,
    width_cm            INT
) DISTSTYLE ALL;

-- ---------- fact: grain = one row per order item ----------
-- DISTKEY(order_id) keeps all items of one order on the same node; SORTKEY(order_date_key)
-- lets date-range filters (the most common dashboard filter) skip blocks.
CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_id              VARCHAR(32) NOT NULL,
    order_item_id         INT NOT NULL,
    customer_key          INT NOT NULL,
    product_key           INT NOT NULL,
    seller_key            INT NOT NULL,
    order_date_key        INT NOT NULL,
    order_status          VARCHAR(20),
    price                 DECIMAL(10,2),
    freight_value         DECIMAL(10,2),
    item_total            DECIMAL(11,2),
    purchase_ts           TIMESTAMP,
    delivered_ts          TIMESTAMP,
    estimated_delivery_ts TIMESTAMP,
    delivery_days         INT,        -- purchase -> delivered
    delivery_delay_days   INT         -- delivered - estimated (positive = late)
) DISTSTYLE KEY DISTKEY (order_id) SORTKEY (order_date_key);
