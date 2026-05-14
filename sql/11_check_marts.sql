-- 1. Mart row counts

SELECT 'mart.sales_performance' AS table_name, COUNT(*) AS row_count
FROM mart.sales_performance
UNION ALL
SELECT 'mart.delivery_performance', COUNT(*)
FROM mart.delivery_performance
UNION ALL
SELECT 'mart.customer_satisfaction', COUNT(*)
FROM mart.customer_satisfaction
UNION ALL
SELECT 'mart.seller_scorecard', COUNT(*)
FROM mart.seller_scorecard;


-- 2. Total revenue check:
-- fact_order_item revenue should match mart.sales_performance revenue

SELECT
    'fact_order_item' AS source_name,
    ROUND(SUM(price), 2) AS total_revenue
FROM warehouse.fact_order_item
WHERE has_invalid_price = FALSE
  AND has_invalid_freight = FALSE

UNION ALL

SELECT
    'mart.sales_performance' AS source_name,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM mart.sales_performance;


-- 3. Top 10 product categories by revenue

SELECT
    product_category_name_english,
    SUM(order_count) AS order_count,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(SUM(total_freight_value), 2) AS total_freight_value
FROM mart.sales_performance
GROUP BY product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Sales by customer region

SELECT
    customer_region,
    SUM(order_count) AS order_count,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(SUM(total_freight_value), 2) AS total_freight_value
FROM mart.sales_performance
GROUP BY customer_region
ORDER BY total_revenue DESC;


-- 5. Delivery performance overview

SELECT
    delivery_performance_status,
    SUM(order_count) AS order_count,
    ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(avg_delay_days), 2) AS avg_delay_days,
    ROUND(AVG(late_delivery_rate), 4) AS avg_late_delivery_rate
FROM mart.delivery_performance
GROUP BY delivery_performance_status
ORDER BY order_count DESC;


-- 6. Customer satisfaction overview

SELECT
    review_sentiment,
    SUM(review_count) AS review_count,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score,
    ROUND(AVG(negative_review_rate), 4) AS avg_negative_review_rate,
    ROUND(AVG(late_delivery_rate_for_reviewed_orders), 4) AS avg_late_delivery_rate
FROM mart.customer_satisfaction
GROUP BY review_sentiment
ORDER BY review_count DESC;


-- 7. Top 10 sellers by revenue

SELECT
    seller_id,
    seller_state,
    seller_region,
    seller_performance_segment,
    seller_scorecard_segment,
    order_count,
    ROUND(total_revenue, 2) AS total_revenue,
    avg_review_score,
    late_delivery_rate,
    revenue_rank
FROM mart.seller_scorecard
ORDER BY total_revenue DESC NULLS LAST
LIMIT 10;


-- 8. Sellers needing attention

SELECT
    seller_id,
    seller_state,
    seller_region,
    seller_scorecard_segment,
    order_count,
    ROUND(total_revenue, 2) AS total_revenue,
    avg_review_score,
    late_delivery_rate,
    negative_review_rate
FROM mart.seller_scorecard
WHERE seller_scorecard_segment = 'Needs Attention'
ORDER BY total_revenue DESC NULLS LAST
LIMIT 20;