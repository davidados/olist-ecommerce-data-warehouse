-- =====================================================
-- File: 09_check_additional_facts.sql
-- Purpose: Validate additional warehouse fact tables
-- =====================================================


-- 1. Row counts

SELECT 'warehouse.fact_order_item' AS table_name, COUNT(*) AS row_count
FROM warehouse.fact_order_item
UNION ALL
SELECT 'warehouse.fact_payment', COUNT(*)
FROM warehouse.fact_payment
UNION ALL
SELECT 'warehouse.fact_review', COUNT(*)
FROM warehouse.fact_review
UNION ALL
SELECT 'warehouse.fact_delivery', COUNT(*)
FROM warehouse.fact_delivery;


-- 2. Compare source and warehouse counts

SELECT
    'payments' AS object_name,
    (SELECT COUNT(*) FROM staging.stg_payments) AS staging_count,
    (SELECT COUNT(*) FROM warehouse.fact_payment) AS warehouse_count;

SELECT
    'reviews' AS object_name,
    (SELECT COUNT(*) FROM staging.stg_reviews) AS staging_count,
    (SELECT COUNT(*) FROM warehouse.fact_review) AS warehouse_count;

SELECT
    'deliveries / orders' AS object_name,
    (SELECT COUNT(*) FROM staging.stg_orders) AS staging_count,
    (SELECT COUNT(*) FROM warehouse.fact_delivery) AS warehouse_count;


-- 3. Missing foreign key checks

SELECT
    COUNT(*) AS fact_payment_missing_customer_sk
FROM warehouse.fact_payment
WHERE customer_sk IS NULL;

SELECT
    COUNT(*) AS fact_payment_missing_payment_type_sk
FROM warehouse.fact_payment
WHERE payment_type_sk IS NULL;

SELECT
    COUNT(*) AS fact_review_missing_customer_sk
FROM warehouse.fact_review
WHERE customer_sk IS NULL;

SELECT
    COUNT(*) AS fact_delivery_missing_customer_sk
FROM warehouse.fact_delivery
WHERE customer_sk IS NULL;


-- 4. Payment sanity check

SELECT
    COUNT(*) AS invalid_payment_value_count
FROM warehouse.fact_payment
WHERE payment_value IS NULL
   OR payment_value < 0;


-- 5. Review score distribution

SELECT
    review_score,
    review_sentiment,
    COUNT(*) AS review_count
FROM warehouse.fact_review
GROUP BY review_score, review_sentiment
ORDER BY review_score;


-- 6. Delivery performance distribution

SELECT
    delivery_performance_status,
    COUNT(*) AS order_count,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(delay_days), 2) AS avg_delay_days
FROM warehouse.fact_delivery
GROUP BY delivery_performance_status
ORDER BY order_count DESC;


-- 7. Sample joined payment rows

SELECT
    fp.order_id,
    d.full_date AS purchase_date,
    c.customer_state,
    c.customer_region,
    pt.payment_type,
    pt.installment_band,
    fp.payment_installments,
    fp.payment_value
FROM warehouse.fact_payment fp
LEFT JOIN warehouse.dim_date d
    ON fp.order_purchase_date_sk = d.date_sk
LEFT JOIN warehouse.dim_customer c
    ON fp.customer_sk = c.customer_sk
LEFT JOIN warehouse.dim_payment_type pt
    ON fp.payment_type_sk = pt.payment_type_sk
LIMIT 20;