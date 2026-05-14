
-- Row counts

SELECT 'staging.stg_customers' AS table_name, COUNT(*) AS row_count
FROM staging.stg_customers
UNION ALL
SELECT 'staging.stg_sellers', COUNT(*)
FROM staging.stg_sellers
UNION ALL
SELECT 'staging.stg_products', COUNT(*)
FROM staging.stg_products
UNION ALL
SELECT 'staging.stg_orders', COUNT(*)
FROM staging.stg_orders
UNION ALL
SELECT 'staging.stg_order_items', COUNT(*)
FROM staging.stg_order_items
UNION ALL
SELECT 'staging.stg_payments', COUNT(*)
FROM staging.stg_payments
UNION ALL
SELECT 'staging.stg_reviews', COUNT(*)
FROM staging.stg_reviews
UNION ALL
SELECT 'staging.stg_geolocation', COUNT(*)
FROM staging.stg_geolocation;


-- Invalid order dates

SELECT
    COUNT(*) AS invalid_delivery_date_count
FROM staging.stg_orders
WHERE order_delivered_customer_date < order_purchase_timestamp;


-- Invalid prices

SELECT
    COUNT(*) AS invalid_price_count
FROM staging.stg_order_items
WHERE has_invalid_price = TRUE;


-- Invalid freight values

SELECT
    COUNT(*) AS invalid_freight_count
FROM staging.stg_order_items
WHERE has_invalid_freight = TRUE;


-- Missing product categories

SELECT
    COUNT(*) AS unknown_product_category_count
FROM staging.stg_products
WHERE product_category_name_english = 'unknown';


-- Late delivery count

SELECT
    delivery_performance_status,
    COUNT(*) AS order_count
FROM staging.stg_orders
GROUP BY delivery_performance_status
ORDER BY order_count DESC;


-- Review sentiment distribution

SELECT
    review_sentiment,
    COUNT(*) AS review_count
FROM staging.stg_reviews
GROUP BY review_sentiment
ORDER BY review_count DESC;


-- Payment type distribution

SELECT
    payment_type,
    COUNT(*) AS payment_count,
    SUM(payment_value) AS total_payment_value
FROM staging.stg_payments
GROUP BY payment_type
ORDER BY payment_count DESC;