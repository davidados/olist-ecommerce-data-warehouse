


-- 1. Row counts

SELECT 'warehouse.dim_date' AS table_name, COUNT(*) AS row_count
FROM warehouse.dim_date
UNION ALL
SELECT 'warehouse.dim_customer', COUNT(*)
FROM warehouse.dim_customer
UNION ALL
SELECT 'warehouse.dim_product', COUNT(*)
FROM warehouse.dim_product
UNION ALL
SELECT 'warehouse.dim_seller', COUNT(*)
FROM warehouse.dim_seller
UNION ALL
SELECT 'warehouse.dim_order_status', COUNT(*)
FROM warehouse.dim_order_status
UNION ALL
SELECT 'warehouse.dim_payment_type', COUNT(*)
FROM warehouse.dim_payment_type
UNION ALL
SELECT 'warehouse.fact_order_item', COUNT(*)
FROM warehouse.fact_order_item;


-- 2. Fact grain check: order_id + order_item_id must be unique

SELECT
    order_id,
    order_item_id,
    COUNT(*) AS duplicate_count
FROM warehouse.fact_order_item
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;


-- 3. Missing foreign key checks

SELECT
    COUNT(*) AS missing_customer_sk
FROM warehouse.fact_order_item
WHERE customer_sk IS NULL;

SELECT
    COUNT(*) AS missing_product_sk
FROM warehouse.fact_order_item
WHERE product_sk IS NULL;

SELECT
    COUNT(*) AS missing_seller_sk
FROM warehouse.fact_order_item
WHERE seller_sk IS NULL;

SELECT
    COUNT(*) AS missing_order_status_sk
FROM warehouse.fact_order_item
WHERE order_status_sk IS NULL;


-- 4. Revenue sanity check

SELECT
    COUNT(*) AS invalid_revenue_rows
FROM warehouse.fact_order_item
WHERE item_revenue < 0
   OR freight_value < 0;


-- 5. Sample fact rows with dimensions

SELECT
    f.order_id,
    f.order_item_id,
    d.full_date AS purchase_date,
    c.customer_state,
    c.customer_region,
    p.product_category_name_english,
    s.seller_state,
    s.seller_performance_segment,
    os.order_status,
    f.price,
    f.freight_value,
    f.item_total_value,
    f.delivery_performance_status
FROM warehouse.fact_order_item f
LEFT JOIN warehouse.dim_date d
    ON f.order_purchase_date_sk = d.date_sk
LEFT JOIN warehouse.dim_customer c
    ON f.customer_sk = c.customer_sk
LEFT JOIN warehouse.dim_product p
    ON f.product_sk = p.product_sk
LEFT JOIN warehouse.dim_seller s
    ON f.seller_sk = s.seller_sk
LEFT JOIN warehouse.dim_order_status os
    ON f.order_status_sk = os.order_status_sk
LIMIT 20;