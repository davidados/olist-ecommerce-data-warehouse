
DROP TABLE IF EXISTS warehouse.fact_order_item;

DROP TABLE IF EXISTS warehouse.dim_date;
DROP TABLE IF EXISTS warehouse.dim_customer;
DROP TABLE IF EXISTS warehouse.dim_product;
DROP TABLE IF EXISTS warehouse.dim_seller;
DROP TABLE IF EXISTS warehouse.dim_order_status;
DROP TABLE IF EXISTS warehouse.dim_payment_type;

CREATE TABLE warehouse.dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    weekday_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

INSERT INTO warehouse.dim_date (
    date_sk,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    weekday_name,
    is_weekend
)
WITH all_dates AS (
    SELECT order_purchase_date AS date_value
    FROM staging.stg_orders
    WHERE order_purchase_date IS NOT NULL

    UNION

    SELECT order_approved_date
    FROM staging.stg_orders
    WHERE order_approved_date IS NOT NULL

    UNION

    SELECT order_delivered_carrier_date_only
    FROM staging.stg_orders
    WHERE order_delivered_carrier_date_only IS NOT NULL

    UNION

    SELECT order_delivered_customer_date_only
    FROM staging.stg_orders
    WHERE order_delivered_customer_date_only IS NOT NULL

    UNION

    SELECT order_estimated_delivery_date_only
    FROM staging.stg_orders
    WHERE order_estimated_delivery_date_only IS NOT NULL

    UNION

    SELECT CAST(shipping_limit_date AS DATE)
    FROM staging.stg_order_items
    WHERE shipping_limit_date IS NOT NULL

    UNION

    SELECT review_creation_date_only
    FROM staging.stg_reviews
    WHERE review_creation_date_only IS NOT NULL

    UNION

    SELECT review_answer_date_only
    FROM staging.stg_reviews
    WHERE review_answer_date_only IS NOT NULL
)
SELECT
    TO_CHAR(date_value, 'YYYYMMDD')::INTEGER AS date_sk,
    date_value AS full_date,
    EXTRACT(YEAR FROM date_value)::INTEGER AS year,
    EXTRACT(QUARTER FROM date_value)::INTEGER AS quarter,
    EXTRACT(MONTH FROM date_value)::INTEGER AS month,
    TO_CHAR(date_value, 'Month') AS month_name,
    EXTRACT(DAY FROM date_value)::INTEGER AS day,
    EXTRACT(ISODOW FROM date_value)::INTEGER AS day_of_week,
    TO_CHAR(date_value, 'Day') AS weekday_name,
    CASE
        WHEN EXTRACT(ISODOW FROM date_value)::INTEGER IN (6, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM all_dates
ORDER BY date_value;


CREATE TABLE warehouse.dim_customer (
    customer_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT,
    customer_region TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO warehouse.dim_customer (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_region
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_region
FROM staging.stg_customers;


CREATE UNIQUE INDEX idx_dim_customer_customer_id
ON warehouse.dim_customer(customer_id);


CREATE TABLE warehouse.dim_product (
    product_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_category_name TEXT,
    product_category_name_english TEXT,
    product_name_length NUMERIC,
    product_description_length NUMERIC,
    product_photos_qty NUMERIC,
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC,
    product_volume_cm3 NUMERIC,
    product_weight_band TEXT,
    product_size_band TEXT,

    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    record_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO warehouse.dim_product (
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    product_weight_band,
    product_size_band,
    valid_from,
    valid_to,
    is_current,
    record_hash
)
SELECT
    p.product_id,
    p.product_category_name,
    p.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
    p.product_weight_band,
    p.product_size_band,

    COALESCE(MIN(o.order_purchase_date), DATE '2016-01-01') AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,

    MD5(
        COALESCE(p.product_category_name_english, '') || '|' ||
        COALESCE(p.product_weight_band, '') || '|' ||
        COALESCE(p.product_size_band, '')
    ) AS record_hash

FROM staging.stg_products p
LEFT JOIN staging.stg_order_items oi
    ON p.product_id = oi.product_id
LEFT JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
GROUP BY
    p.product_id,
    p.product_category_name,
    p.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
    p.product_weight_band,
    p.product_size_band;


CREATE INDEX idx_dim_product_product_id_current
ON warehouse.dim_product(product_id, is_current);


CREATE TABLE warehouse.dim_seller (
    seller_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_id TEXT NOT NULL,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT,
    seller_region TEXT,
    seller_performance_segment TEXT,

    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    record_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO warehouse.dim_seller (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_region,
    seller_performance_segment,
    valid_from,
    valid_to,
    is_current,
    record_hash
)
WITH seller_metrics AS (
    SELECT
        s.seller_id,
        SUM(oi.price) AS seller_revenue,
        COUNT(DISTINCT oi.order_id) AS seller_order_count,
        AVG(CASE WHEN o.is_late_delivery THEN 1.0 ELSE 0.0 END) AS late_delivery_rate
    FROM staging.stg_sellers s
    LEFT JOIN staging.stg_order_items oi
        ON s.seller_id = oi.seller_id
    LEFT JOIN staging.stg_orders o
        ON oi.order_id = o.order_id
    GROUP BY s.seller_id
),
seller_segment AS (
    SELECT
        seller_id,
        CASE
            WHEN seller_revenue >= 50000 AND late_delivery_rate <= 0.15 THEN 'Top Seller'
            WHEN seller_revenue >= 10000 THEN 'Standard Seller'
            WHEN seller_revenue IS NULL THEN 'No Sales'
            ELSE 'Low Volume Seller'
        END AS seller_performance_segment
    FROM seller_metrics
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    ss.seller_performance_segment,

    COALESCE(MIN(o.order_purchase_date), DATE '2016-01-01') AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,

    MD5(
        COALESCE(s.seller_region, '') || '|' ||
        COALESCE(ss.seller_performance_segment, '')
    ) AS record_hash

FROM staging.stg_sellers s
LEFT JOIN seller_segment ss
    ON s.seller_id = ss.seller_id
LEFT JOIN staging.stg_order_items oi
    ON s.seller_id = oi.seller_id
LEFT JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
GROUP BY
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    ss.seller_performance_segment;


CREATE INDEX idx_dim_seller_seller_id_current
ON warehouse.dim_seller(seller_id, is_current);

CREATE TABLE warehouse.dim_order_status (
    order_status_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_status TEXT NOT NULL,
    order_status_description TEXT
);

INSERT INTO warehouse.dim_order_status (
    order_status,
    order_status_description
)
SELECT DISTINCT
    order_status,
    CASE
        WHEN order_status = 'delivered' THEN 'Order delivered to customer'
        WHEN order_status = 'shipped' THEN 'Order shipped but not yet delivered'
        WHEN order_status = 'canceled' THEN 'Order canceled'
        WHEN order_status = 'invoiced' THEN 'Order invoiced'
        WHEN order_status = 'processing' THEN 'Order being processed'
        WHEN order_status = 'unavailable' THEN 'Order unavailable'
        WHEN order_status = 'approved' THEN 'Order approved'
        WHEN order_status = 'created' THEN 'Order created'
        ELSE 'Unknown status'
    END AS order_status_description
FROM staging.stg_orders;


CREATE UNIQUE INDEX idx_dim_order_status
ON warehouse.dim_order_status(order_status);


-- =====================================================
-- 6. dim_payment_type
-- Grain: one row per payment type and installment band
-- =====================================================

CREATE TABLE warehouse.dim_payment_type (
    payment_type_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_type TEXT NOT NULL,
    installment_band TEXT NOT NULL
);

INSERT INTO warehouse.dim_payment_type (
    payment_type,
    installment_band
)
SELECT DISTINCT
    payment_type,
    installment_band
FROM staging.stg_payments;


CREATE UNIQUE INDEX idx_dim_payment_type
ON warehouse.dim_payment_type(payment_type, installment_band);

CREATE TABLE warehouse.fact_order_item (
    order_item_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,

    order_purchase_date_sk INTEGER,
    order_approved_date_sk INTEGER,
    order_delivered_customer_date_sk INTEGER,
    order_estimated_delivery_date_sk INTEGER,
    shipping_limit_date_sk INTEGER,

    customer_sk INTEGER,
    product_sk INTEGER,
    seller_sk INTEGER,
    order_status_sk INTEGER,

    price NUMERIC,
    freight_value NUMERIC,
    item_revenue NUMERIC,
    item_total_value NUMERIC,

    delivery_days NUMERIC,
    delay_days NUMERIC,
    is_late_delivery BOOLEAN,
    delivery_performance_status TEXT,

    has_invalid_price BOOLEAN,
    has_invalid_freight BOOLEAN,

    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_fact_order_item_customer
        FOREIGN KEY (customer_sk)
        REFERENCES warehouse.dim_customer(customer_sk),

    CONSTRAINT fk_fact_order_item_product
        FOREIGN KEY (product_sk)
        REFERENCES warehouse.dim_product(product_sk),

    CONSTRAINT fk_fact_order_item_seller
        FOREIGN KEY (seller_sk)
        REFERENCES warehouse.dim_seller(seller_sk),

    CONSTRAINT fk_fact_order_item_status
        FOREIGN KEY (order_status_sk)
        REFERENCES warehouse.dim_order_status(order_status_sk)
);

INSERT INTO warehouse.fact_order_item (
    order_id,
    order_item_id,

    order_purchase_date_sk,
    order_approved_date_sk,
    order_delivered_customer_date_sk,
    order_estimated_delivery_date_sk,
    shipping_limit_date_sk,

    customer_sk,
    product_sk,
    seller_sk,
    order_status_sk,

    price,
    freight_value,
    item_revenue,
    item_total_value,

    delivery_days,
    delay_days,
    is_late_delivery,
    delivery_performance_status,

    has_invalid_price,
    has_invalid_freight
)
SELECT
    oi.order_id,
    oi.order_item_id,

    TO_CHAR(o.order_purchase_date, 'YYYYMMDD')::INTEGER AS order_purchase_date_sk,
    TO_CHAR(o.order_approved_date, 'YYYYMMDD')::INTEGER AS order_approved_date_sk,
    TO_CHAR(o.order_delivered_customer_date_only, 'YYYYMMDD')::INTEGER AS order_delivered_customer_date_sk,
    TO_CHAR(o.order_estimated_delivery_date_only, 'YYYYMMDD')::INTEGER AS order_estimated_delivery_date_sk,
    TO_CHAR(CAST(oi.shipping_limit_date AS DATE), 'YYYYMMDD')::INTEGER AS shipping_limit_date_sk,

    c.customer_sk,
    p.product_sk,
    s.seller_sk,
    os.order_status_sk,

    oi.price,
    oi.freight_value,
    oi.item_revenue,
    oi.item_total_value,

    o.delivery_days,
    o.delay_days,
    o.is_late_delivery,
    o.delivery_performance_status,

    oi.has_invalid_price,
    oi.has_invalid_freight

FROM staging.stg_order_items oi
LEFT JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
LEFT JOIN warehouse.dim_customer c
    ON o.customer_id = c.customer_id
LEFT JOIN warehouse.dim_product p
    ON oi.product_id = p.product_id
   AND p.is_current = TRUE
LEFT JOIN warehouse.dim_seller s
    ON oi.seller_id = s.seller_id
   AND s.is_current = TRUE
LEFT JOIN warehouse.dim_order_status os
    ON o.order_status = os.order_status;


CREATE UNIQUE INDEX idx_fact_order_item_natural_key
ON warehouse.fact_order_item(order_id, order_item_id);

CREATE INDEX idx_fact_order_item_customer_sk
ON warehouse.fact_order_item(customer_sk);

CREATE INDEX idx_fact_order_item_product_sk
ON warehouse.fact_order_item(product_sk);

CREATE INDEX idx_fact_order_item_seller_sk
ON warehouse.fact_order_item(seller_sk);

CREATE INDEX idx_fact_order_item_purchase_date_sk
ON warehouse.fact_order_item(order_purchase_date_sk);