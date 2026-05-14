

DROP TABLE IF EXISTS mart.sales_performance;
DROP TABLE IF EXISTS mart.delivery_performance;
DROP TABLE IF EXISTS mart.customer_satisfaction;
DROP TABLE IF EXISTS mart.seller_scorecard;



CREATE TABLE mart.sales_performance AS
SELECT
    d.year,
    d.month,
    TRIM(d.month_name) AS month_name,

    c.customer_state,
    c.customer_region,

    p.product_category_name_english,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS order_item_count,
    COUNT(DISTINCT f.customer_sk) AS customer_count,
    COUNT(DISTINCT f.product_sk) AS product_count,
    COUNT(DISTINCT f.seller_sk) AS seller_count,

    SUM(f.price) AS total_revenue,
    SUM(f.freight_value) AS total_freight_value,
    SUM(f.item_total_value) AS total_order_item_value,

    ROUND(AVG(f.price), 2) AS avg_item_price,
    ROUND(AVG(f.freight_value), 2) AS avg_freight_value,

    ROUND(
        SUM(f.price) / NULLIF(COUNT(DISTINCT f.order_id), 0),
        2
    ) AS average_order_value,

    ROUND(
        SUM(f.freight_value) / NULLIF(SUM(f.price), 0),
        4
    ) AS freight_to_revenue_ratio

FROM warehouse.fact_order_item f
LEFT JOIN warehouse.dim_date d
    ON f.order_purchase_date_sk = d.date_sk
LEFT JOIN warehouse.dim_customer c
    ON f.customer_sk = c.customer_sk
LEFT JOIN warehouse.dim_product p
    ON f.product_sk = p.product_sk
WHERE f.has_invalid_price = FALSE
  AND f.has_invalid_freight = FALSE
GROUP BY
    d.year,
    d.month,
    TRIM(d.month_name),
    c.customer_state,
    c.customer_region,
    p.product_category_name_english;


CREATE INDEX idx_mart_sales_year_month
ON mart.sales_performance(year, month);

CREATE INDEX idx_mart_sales_category
ON mart.sales_performance(product_category_name_english);

CREATE INDEX idx_mart_sales_region
ON mart.sales_performance(customer_region);




CREATE TABLE mart.delivery_performance AS
SELECT
    d.year,
    d.month,
    TRIM(d.month_name) AS month_name,

    c.customer_state,
    c.customer_region,

    fd.delivery_performance_status,

    COUNT(DISTINCT fd.order_id) AS order_count,

    SUM(CASE WHEN fd.is_late_delivery = TRUE THEN 1 ELSE 0 END) AS late_order_count,
    SUM(CASE WHEN fd.delivery_performance_status = 'delivered_on_time' THEN 1 ELSE 0 END) AS on_time_order_count,
    SUM(CASE WHEN fd.delivery_performance_status = 'not_delivered' THEN 1 ELSE 0 END) AS not_delivered_order_count,

    ROUND(AVG(fd.delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(fd.delay_days), 2) AS avg_delay_days,

    ROUND(
        SUM(CASE WHEN fd.is_late_delivery = TRUE THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fd.order_id), 0),
        4
    ) AS late_delivery_rate,

    ROUND(
        SUM(CASE WHEN fd.delivery_performance_status = 'delivered_on_time' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fd.order_id), 0),
        4
    ) AS on_time_delivery_rate,

    SUM(fd.total_order_value) AS total_order_value,
    SUM(fd.total_freight_value) AS total_freight_value,

    ROUND(AVG(fd.item_count), 2) AS avg_items_per_order,
    ROUND(AVG(fd.seller_count), 2) AS avg_sellers_per_order

FROM warehouse.fact_delivery fd
LEFT JOIN warehouse.dim_date d
    ON fd.order_purchase_date_sk = d.date_sk
LEFT JOIN warehouse.dim_customer c
    ON fd.customer_sk = c.customer_sk
GROUP BY
    d.year,
    d.month,
    TRIM(d.month_name),
    c.customer_state,
    c.customer_region,
    fd.delivery_performance_status;


CREATE INDEX idx_mart_delivery_year_month
ON mart.delivery_performance(year, month);

CREATE INDEX idx_mart_delivery_region
ON mart.delivery_performance(customer_region);

CREATE INDEX idx_mart_delivery_status
ON mart.delivery_performance(delivery_performance_status);




CREATE TABLE mart.customer_satisfaction AS
SELECT
    d.year,
    d.month,
    TRIM(d.month_name) AS month_name,

    c.customer_state,
    c.customer_region,

    fr.review_sentiment,

    COUNT(DISTINCT fr.review_id) AS review_count,
    COUNT(DISTINCT fr.order_id) AS reviewed_order_count,

    ROUND(AVG(fr.review_score), 2) AS avg_review_score,

    SUM(CASE WHEN fr.review_score >= 4 THEN 1 ELSE 0 END) AS positive_review_count,
    SUM(CASE WHEN fr.review_score = 3 THEN 1 ELSE 0 END) AS neutral_review_count,
    SUM(CASE WHEN fr.review_score <= 2 THEN 1 ELSE 0 END) AS negative_review_count,

    ROUND(
        SUM(CASE WHEN fr.review_score >= 4 THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fr.review_id), 0),
        4
    ) AS positive_review_rate,

    ROUND(
        SUM(CASE WHEN fr.review_score <= 2 THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fr.review_id), 0),
        4
    ) AS negative_review_rate,

    SUM(CASE WHEN fr.has_review_comment = TRUE THEN 1 ELSE 0 END) AS review_with_comment_count,

    ROUND(AVG(fr.review_response_days), 2) AS avg_review_response_days,

    ROUND(AVG(fd.delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(fd.delay_days), 2) AS avg_delay_days,

    ROUND(
        SUM(CASE WHEN fd.is_late_delivery = TRUE THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fr.order_id), 0),
        4
    ) AS late_delivery_rate_for_reviewed_orders

FROM warehouse.fact_review fr
LEFT JOIN warehouse.fact_delivery fd
    ON fr.order_id = fd.order_id
LEFT JOIN warehouse.dim_date d
    ON fr.review_creation_date_sk = d.date_sk
LEFT JOIN warehouse.dim_customer c
    ON fr.customer_sk = c.customer_sk
GROUP BY
    d.year,
    d.month,
    TRIM(d.month_name),
    c.customer_state,
    c.customer_region,
    fr.review_sentiment;


CREATE INDEX idx_mart_satisfaction_year_month
ON mart.customer_satisfaction(year, month);

CREATE INDEX idx_mart_satisfaction_region
ON mart.customer_satisfaction(customer_region);

CREATE INDEX idx_mart_satisfaction_sentiment
ON mart.customer_satisfaction(review_sentiment);




CREATE TABLE mart.seller_scorecard AS
WITH seller_sales AS (
    SELECT
        f.seller_sk,

        COUNT(DISTINCT f.order_id) AS order_count,
        COUNT(*) AS order_item_count,
        COUNT(DISTINCT f.product_sk) AS product_count,
        COUNT(DISTINCT f.customer_sk) AS customer_count,

        SUM(f.price) AS total_revenue,
        SUM(f.freight_value) AS total_freight_value,
        SUM(f.item_total_value) AS total_item_value,

        ROUND(AVG(f.price), 2) AS avg_item_price,
        ROUND(AVG(f.freight_value), 2) AS avg_freight_value
    FROM warehouse.fact_order_item f
    WHERE f.has_invalid_price = FALSE
      AND f.has_invalid_freight = FALSE
    GROUP BY f.seller_sk
),

seller_orders AS (
    SELECT DISTINCT
        seller_sk,
        order_id
    FROM warehouse.fact_order_item
),

seller_delivery_review AS (
    SELECT
        so.seller_sk,

        ROUND(AVG(fd.delivery_days), 2) AS avg_delivery_days,
        ROUND(AVG(fd.delay_days), 2) AS avg_delay_days,

        ROUND(
            SUM(CASE WHEN fd.is_late_delivery = TRUE THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT fd.order_id), 0),
            4
        ) AS late_delivery_rate,

        ROUND(AVG(fr.review_score), 2) AS avg_review_score,

        ROUND(
            SUM(CASE WHEN fr.review_score <= 2 THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT fr.review_id), 0),
            4
        ) AS negative_review_rate,

        COUNT(DISTINCT fr.review_id) AS review_count

    FROM seller_orders so
    LEFT JOIN warehouse.fact_delivery fd
        ON so.order_id = fd.order_id
    LEFT JOIN warehouse.fact_review fr
        ON so.order_id = fr.order_id
    GROUP BY so.seller_sk
)

SELECT
    s.seller_sk,
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    s.seller_performance_segment,

    ss.order_count,
    ss.order_item_count,
    ss.product_count,
    ss.customer_count,

    ss.total_revenue,
    ss.total_freight_value,
    ss.total_item_value,

    ss.avg_item_price,
    ss.avg_freight_value,

    sdr.avg_delivery_days,
    sdr.avg_delay_days,
    sdr.late_delivery_rate,
    sdr.avg_review_score,
    sdr.negative_review_rate,
    sdr.review_count,

    RANK() OVER (ORDER BY ss.total_revenue DESC) AS revenue_rank,

    CASE
        WHEN ss.total_revenue >= 50000
         AND COALESCE(sdr.late_delivery_rate, 0) <= 0.15
         AND COALESCE(sdr.avg_review_score, 5) >= 4 THEN 'Excellent'
        WHEN ss.total_revenue >= 10000
         AND COALESCE(sdr.avg_review_score, 5) >= 3.5 THEN 'Good'
        WHEN COALESCE(sdr.late_delivery_rate, 0) > 0.30
          OR COALESCE(sdr.avg_review_score, 5) < 3 THEN 'Needs Attention'
        ELSE 'Standard'
    END AS seller_scorecard_segment

FROM warehouse.dim_seller s
LEFT JOIN seller_sales ss
    ON s.seller_sk = ss.seller_sk
LEFT JOIN seller_delivery_review sdr
    ON s.seller_sk = sdr.seller_sk
WHERE s.is_current = TRUE;


CREATE INDEX idx_mart_seller_scorecard_seller_id
ON mart.seller_scorecard(seller_id);

CREATE INDEX idx_mart_seller_scorecard_region
ON mart.seller_scorecard(seller_region);

CREATE INDEX idx_mart_seller_scorecard_segment
ON mart.seller_scorecard(seller_scorecard_segment);