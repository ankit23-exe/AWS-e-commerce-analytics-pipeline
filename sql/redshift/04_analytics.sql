-- Queries behind the QuickSight dashboard. Run them in query editor v2 first.

-- 1) Monthly revenue and orders
SELECT d.year, d.month, d.month_name,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.price)               AS revenue,
       SUM(f.freight_value)       AS freight
FROM dw.fact_order_items f
JOIN dw.dim_date d ON d.date_key = f.order_date_key
WHERE f.order_status <> 'canceled'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 2) Top 10 product categories by revenue
SELECT p.category_name_en, SUM(f.price) AS revenue, COUNT(*) AS items_sold
FROM dw.fact_order_items f
JOIN dw.dim_product p ON p.product_key = f.product_key
WHERE f.order_status <> 'canceled'
GROUP BY p.category_name_en
ORDER BY revenue DESC
LIMIT 10;

-- 3) Delivery performance by customer state (positive delay = late)
SELECT c.state,
       COUNT(DISTINCT f.order_id)                                            AS delivered_orders,
       ROUND(AVG(f.delivery_days), 1)                                        AS avg_delivery_days,
       ROUND(AVG(f.delivery_delay_days), 1)                                  AS avg_delay_days,
       ROUND(100.0 * SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
FROM dw.fact_order_items f
JOIN dw.dim_customer c ON c.customer_key = f.customer_key
WHERE f.delivered_ts IS NOT NULL
GROUP BY c.state
ORDER BY avg_delay_days DESC;

-- 4) Top 3 sellers per state by revenue (window function)
SELECT *
FROM (
    SELECT s.state, s.seller_id, SUM(f.price) AS revenue,
           RANK() OVER (PARTITION BY s.state ORDER BY SUM(f.price) DESC) AS rnk
    FROM dw.fact_order_items f
    JOIN dw.dim_seller s ON s.seller_key = f.seller_key
    GROUP BY s.state, s.seller_id
) ranked
WHERE rnk <= 3
ORDER BY state, rnk;
