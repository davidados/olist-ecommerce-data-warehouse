

DROP TABLE IF EXISTS staging.stg_customers;

CREATE TABLE staging.stg_customers AS
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    INITCAP(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,

    CASE
        WHEN UPPER(TRIM(customer_state)) IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
        WHEN UPPER(TRIM(customer_state)) IN ('PR', 'SC', 'RS') THEN 'South'
        WHEN UPPER(TRIM(customer_state)) IN ('DF', 'GO', 'MT', 'MS') THEN 'Central-West'
        WHEN UPPER(TRIM(customer_state)) IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
        WHEN UPPER(TRIM(customer_state)) IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
        ELSE 'Unknown'
    END AS customer_region

FROM raw.olist_customers;
DROP TABLE IF EXISTS staging.stg_sellers;

CREATE TABLE staging.stg_sellers AS
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    INITCAP(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,

    CASE
        WHEN UPPER(TRIM(seller_state)) IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
        WHEN UPPER(TRIM(seller_state)) IN ('PR', 'SC', 'RS') THEN 'South'
        WHEN UPPER(TRIM(seller_state)) IN ('DF', 'GO', 'MT', 'MS') THEN 'Central-West'
        WHEN UPPER(TRIM(seller_state)) IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
        WHEN UPPER(TRIM(seller_state)) IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
        ELSE 'Unknown'
    END AS seller_region

FROM raw.olist_sellers;


DROP TABLE IF EXISTS staging.stg_products;

CREATE TABLE staging.stg_products AS
SELECT DISTINCT
    p.product_id,

    COALESCE(NULLIF(TRIM(p.product_category_name), ''), 'unknown') AS product_category_name,

    COALESCE(
        NULLIF(TRIM(t.product_category_name_english), ''),
        COALESCE(NULLIF(TRIM(p.product_category_name), ''), 'unknown')
    ) AS product_category_name_english,

    p.product_name_lenght AS product_name_length,
    p.product_description_lenght AS product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    COALESCE(p.product_length_cm, 0)
        * COALESCE(p.product_height_cm, 0)
        * COALESCE(p.product_width_cm, 0) AS product_volume_cm3,

    CASE
        WHEN p.product_weight_g IS NULL THEN 'Unknown'
        WHEN p.product_weight_g < 500 THEN 'Light'
        WHEN p.product_weight_g < 5000 THEN 'Medium'
        ELSE 'Heavy'
    END AS product_weight_band,

    CASE
        WHEN p.product_length_cm IS NULL
          OR p.product_height_cm IS NULL
          OR p.product_width_cm IS NULL THEN 'Unknown'
        WHEN p.product_length_cm * p.product_height_cm * p.product_width_cm < 1000 THEN 'Small'
        WHEN p.product_length_cm * p.product_height_cm * p.product_width_cm < 10000 THEN 'Medium'
        ELSE 'Large'
    END AS product_size_band

FROM raw.olist_products p
LEFT JOIN raw.product_category_translation t
    ON p.product_category_name = t.product_category_name;


DROP TABLE IF EXISTS staging.stg_orders;

CREATE TABLE staging.stg_orders AS
SELECT DISTINCT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) AS order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    CAST(order_purchase_timestamp AS DATE) AS order_purchase_date,
    CAST(order_approved_at AS DATE) AS order_approved_date,
    CAST(order_delivered_carrier_date AS DATE) AS order_delivered_carrier_date_only,
    CAST(order_delivered_customer_date AS DATE) AS order_delivered_customer_date_only,
    CAST(order_estimated_delivery_date AS DATE) AS order_estimated_delivery_date_only,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN ROUND(
            EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 86400.0,
            2
        )
        ELSE NULL
    END AS delivery_days,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN ROUND(
            EXTRACT(EPOCH FROM (order_delivered_customer_date - order_estimated_delivery_date)) / 86400.0,
            2
        )
        ELSE NULL
    END AS delay_days,

    CASE
        WHEN order_delivered_customer_date IS NULL THEN FALSE
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS is_late_delivery,

    CASE
        WHEN order_delivered_customer_date IS NULL THEN 'not_delivered'
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'delivered_late'
        ELSE 'delivered_on_time'
    END AS delivery_performance_status

FROM raw.olist_orders;

DROP TABLE IF EXISTS staging.stg_order_items;

CREATE TABLE staging.stg_order_items AS
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,

    price AS item_revenue,
    price + freight_value AS item_total_value,

    CASE
        WHEN price IS NULL OR price < 0 THEN TRUE
        ELSE FALSE
    END AS has_invalid_price,

    CASE
        WHEN freight_value IS NULL OR freight_value < 0 THEN TRUE
        ELSE FALSE
    END AS has_invalid_freight

FROM raw.olist_order_items;


DROP TABLE IF EXISTS staging.stg_payments;

CREATE TABLE staging.stg_payments AS
SELECT
    order_id,
    payment_sequential,

    CASE
        WHEN payment_type IS NULL OR TRIM(payment_type) = '' THEN 'unknown'
        ELSE LOWER(TRIM(payment_type))
    END AS payment_type,

    payment_installments,
    payment_value,

    CASE
        WHEN payment_installments IS NULL THEN 'Unknown'
        WHEN payment_installments = 1 THEN 'Single Payment'
        WHEN payment_installments BETWEEN 2 AND 3 THEN '2-3 Installments'
        WHEN payment_installments BETWEEN 4 AND 6 THEN '4-6 Installments'
        ELSE '7+ Installments'
    END AS installment_band

FROM raw.olist_order_payments;


DROP TABLE IF EXISTS staging.stg_reviews;

CREATE TABLE staging.stg_reviews AS
SELECT
    review_id,
    order_id,
    review_score,

    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,

    CAST(review_creation_date AS DATE) AS review_creation_date_only,
    CAST(review_answer_timestamp AS DATE) AS review_answer_date_only,

    CASE
        WHEN review_comment_message IS NULL
          OR TRIM(review_comment_message) = '' THEN FALSE
        ELSE TRUE
    END AS has_review_comment,

    CASE
        WHEN review_score >= 4 THEN 'Positive'
        WHEN review_score = 3 THEN 'Neutral'
        WHEN review_score <= 2 THEN 'Negative'
        ELSE 'Unknown'
    END AS review_sentiment,

    CASE
        WHEN review_answer_timestamp IS NOT NULL
        THEN ROUND(
            EXTRACT(EPOCH FROM (review_answer_timestamp - review_creation_date)) / 86400.0,
            2
        )
        ELSE NULL
    END AS review_response_days

FROM raw.olist_order_reviews;


DROP TABLE IF EXISTS staging.stg_geolocation;

CREATE TABLE staging.stg_geolocation AS
SELECT
    geolocation_zip_code_prefix,
    INITCAP(TRIM(MIN(geolocation_city))) AS geolocation_city,
    UPPER(TRIM(MIN(geolocation_state))) AS geolocation_state,
    AVG(geolocation_lat) AS avg_latitude,
    AVG(geolocation_lng) AS avg_longitude,
    COUNT(*) AS raw_geolocation_records

FROM raw.olist_geolocation
GROUP BY geolocation_zip_code_prefix;



CREATE INDEX IF NOT EXISTS idx_stg_customers_customer_id
ON staging.stg_customers(customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_order_id
ON staging.stg_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id
ON staging.stg_orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_id
ON staging.stg_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_product_id
ON staging.stg_order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_seller_id
ON staging.stg_order_items(seller_id);

CREATE INDEX IF NOT EXISTS idx_stg_products_product_id
ON staging.stg_products(product_id);

CREATE INDEX IF NOT EXISTS idx_stg_sellers_seller_id
ON staging.stg_sellers(seller_id);

CREATE INDEX IF NOT EXISTS idx_stg_payments_order_id
ON staging.stg_payments(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_reviews_order_id
ON staging.stg_reviews(order_id);