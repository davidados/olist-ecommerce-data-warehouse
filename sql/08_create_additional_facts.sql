

DROP TABLE IF EXISTS warehouse.fact_payment;
DROP TABLE IF EXISTS warehouse.fact_review;
DROP TABLE IF EXISTS warehouse.fact_delivery;


-- =====================================================
-- 1. fact_payment
-- Grain: one row per order payment transaction
-- Natural grain: order_id + payment_sequential
-- =====================================================

CREATE TABLE warehouse.fact_payment (
    payment_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    order_id TEXT NOT NULL,
    payment_sequential INTEGER NOT NULL,

    order_purchase_date_sk INTEGER,
    customer_sk INTEGER,
    payment_type_sk INTEGER,
    order_status_sk INTEGER,

    payment_installments INTEGER,
    payment_value NUMERIC,

    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_fact_payment_customer
        FOREIGN KEY (customer_sk)
        REFERENCES warehouse.dim_customer(customer_sk),

    CONSTRAINT fk_fact_payment_payment_type
        FOREIGN KEY (payment_type_sk)
        REFERENCES warehouse.dim_payment_type(payment_type_sk),

    CONSTRAINT fk_fact_payment_order_status
        FOREIGN KEY (order_status_sk)
        REFERENCES warehouse.dim_order_status(order_status_sk)
);

INSERT INTO warehouse.fact_payment (
    order_id,
    payment_sequential,
    order_purchase_date_sk,
    customer_sk,
    payment_type_sk,
    order_status_sk,
    payment_installments,
    payment_value
)
SELECT
    p.order_id,
    p.payment_sequential,

    TO_CHAR(o.order_purchase_date, 'YYYYMMDD')::INTEGER AS order_purchase_date_sk,

    c.customer_sk,
    pt.payment_type_sk,
    os.order_status_sk,

    p.payment_installments,
    p.payment_value

FROM staging.stg_payments p
LEFT JOIN staging.stg_orders o
    ON p.order_id = o.order_id
LEFT JOIN warehouse.dim_customer c
    ON o.customer_id = c.customer_id
LEFT JOIN warehouse.dim_payment_type pt
    ON p.payment_type = pt.payment_type
   AND p.installment_band = pt.installment_band
LEFT JOIN warehouse.dim_order_status os
    ON o.order_status = os.order_status;


CREATE UNIQUE INDEX idx_fact_payment_natural_key
ON warehouse.fact_payment(order_id, payment_sequential);

CREATE INDEX idx_fact_payment_customer_sk
ON warehouse.fact_payment(customer_sk);

CREATE INDEX idx_fact_payment_type_sk
ON warehouse.fact_payment(payment_type_sk);

CREATE INDEX idx_fact_payment_purchase_date_sk
ON warehouse.fact_payment(order_purchase_date_sk);


-- =====================================================
-- 2. fact_review
-- Grain: one row per review
-- Natural grain: review_id + order_id
-- =====================================================

CREATE TABLE warehouse.fact_review (
    review_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    review_id TEXT NOT NULL,
    order_id TEXT NOT NULL,

    review_creation_date_sk INTEGER,
    review_answer_date_sk INTEGER,
    order_purchase_date_sk INTEGER,

    customer_sk INTEGER,
    order_status_sk INTEGER,

    review_score INTEGER,
    review_sentiment TEXT,
    has_review_comment BOOLEAN,
    review_response_days NUMERIC,

    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_fact_review_customer
        FOREIGN KEY (customer_sk)
        REFERENCES warehouse.dim_customer(customer_sk),

    CONSTRAINT fk_fact_review_order_status
        FOREIGN KEY (order_status_sk)
        REFERENCES warehouse.dim_order_status(order_status_sk)
);

INSERT INTO warehouse.fact_review (
    review_id,
    order_id,

    review_creation_date_sk,
    review_answer_date_sk,
    order_purchase_date_sk,

    customer_sk,
    order_status_sk,

    review_score,
    review_sentiment,
    has_review_comment,
    review_response_days
)
SELECT
    r.review_id,
    r.order_id,

    TO_CHAR(r.review_creation_date_only, 'YYYYMMDD')::INTEGER AS review_creation_date_sk,
    TO_CHAR(r.review_answer_date_only, 'YYYYMMDD')::INTEGER AS review_answer_date_sk,
    TO_CHAR(o.order_purchase_date, 'YYYYMMDD')::INTEGER AS order_purchase_date_sk,

    c.customer_sk,
    os.order_status_sk,

    r.review_score,
    r.review_sentiment,
    r.has_review_comment,
    r.review_response_days

FROM staging.stg_reviews r
LEFT JOIN staging.stg_orders o
    ON r.order_id = o.order_id
LEFT JOIN warehouse.dim_customer c
    ON o.customer_id = c.customer_id
LEFT JOIN warehouse.dim_order_status os
    ON o.order_status = os.order_status;


CREATE INDEX idx_fact_review_order_id
ON warehouse.fact_review(order_id);

CREATE INDEX idx_fact_review_customer_sk
ON warehouse.fact_review(customer_sk);

CREATE INDEX idx_fact_review_score
ON warehouse.fact_review(review_score);

CREATE INDEX idx_fact_review_creation_date_sk
ON warehouse.fact_review(review_creation_date_sk);


-- =====================================================
-- 3. fact_delivery
-- Grain: one row per order delivery
-- Natural grain: order_id
-- =====================================================

CREATE TABLE warehouse.fact_delivery (
    delivery_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    order_id TEXT NOT NULL,

    order_purchase_date_sk INTEGER,
    order_approved_date_sk INTEGER,
    order_delivered_carrier_date_sk INTEGER,
    order_delivered_customer_date_sk INTEGER,
    order_estimated_delivery_date_sk INTEGER,

    customer_sk INTEGER,
    order_status_sk INTEGER,

    delivery_days NUMERIC,
    delay_days NUMERIC,
    is_late_delivery BOOLEAN,
    delivery_performance_status TEXT,

    item_count INTEGER,
    seller_count INTEGER,
    total_order_value NUMERIC,
    total_freight_value NUMERIC,

    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_fact_delivery_customer
        FOREIGN KEY (customer_sk)
        REFERENCES warehouse.dim_customer(customer_sk),

    CONSTRAINT fk_fact_delivery_order_status
        FOREIGN KEY (order_status_sk)
        REFERENCES warehouse.dim_order_status(order_status_sk)
);

INSERT INTO warehouse.fact_delivery (
    order_id,

    order_purchase_date_sk,
    order_approved_date_sk,
    order_delivered_carrier_date_sk,
    order_delivered_customer_date_sk,
    order_estimated_delivery_date_sk,

    customer_sk,
    order_status_sk,

    delivery_days,
    delay_days,
    is_late_delivery,
    delivery_performance_status,

    item_count,
    seller_count,
    total_order_value,
    total_freight_value
)
WITH order_item_summary AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        COUNT(DISTINCT seller_id) AS seller_count,
        SUM(price) AS total_order_value,
        SUM(freight_value) AS total_freight_value
    FROM staging.stg_order_items
    GROUP BY order_id
)
SELECT
    o.order_id,

    TO_CHAR(o.order_purchase_date, 'YYYYMMDD')::INTEGER AS order_purchase_date_sk,
    TO_CHAR(o.order_approved_date, 'YYYYMMDD')::INTEGER AS order_approved_date_sk,
    TO_CHAR(o.order_delivered_carrier_date_only, 'YYYYMMDD')::INTEGER AS order_delivered_carrier_date_sk,
    TO_CHAR(o.order_delivered_customer_date_only, 'YYYYMMDD')::INTEGER AS order_delivered_customer_date_sk,
    TO_CHAR(o.order_estimated_delivery_date_only, 'YYYYMMDD')::INTEGER AS order_estimated_delivery_date_sk,

    c.customer_sk,
    os.order_status_sk,

    o.delivery_days,
    o.delay_days,
    o.is_late_delivery,
    o.delivery_performance_status,

    COALESCE(ois.item_count, 0) AS item_count,
    COALESCE(ois.seller_count, 0) AS seller_count,
    COALESCE(ois.total_order_value, 0) AS total_order_value,
    COALESCE(ois.total_freight_value, 0) AS total_freight_value

FROM staging.stg_orders o
LEFT JOIN order_item_summary ois
    ON o.order_id = ois.order_id
LEFT JOIN warehouse.dim_customer c
    ON o.customer_id = c.customer_id
LEFT JOIN warehouse.dim_order_status os
    ON o.order_status = os.order_status;


CREATE UNIQUE INDEX idx_fact_delivery_order_id
ON warehouse.fact_delivery(order_id);

CREATE INDEX idx_fact_delivery_customer_sk
ON warehouse.fact_delivery(customer_sk);

CREATE INDEX idx_fact_delivery_purchase_date_sk
ON warehouse.fact_delivery(order_purchase_date_sk);

CREATE INDEX idx_fact_delivery_status
ON warehouse.fact_delivery(delivery_performance_status);