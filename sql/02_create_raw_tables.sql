DROP TABLE IF EXISTS raw.olist_customers;
CREATE TABLE raw.olist_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
);

DROP TABLE IF EXISTS raw.olist_geolocation;
CREATE TABLE raw.olist_geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat NUMERIC,
    geolocation_lng NUMERIC,
    geolocation_city TEXT,
    geolocation_state TEXT
);

DROP TABLE IF EXISTS raw.olist_order_items;
CREATE TABLE raw.olist_order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC
);

DROP TABLE IF EXISTS raw.olist_order_payments;
CREATE TABLE raw.olist_order_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC
);

DROP TABLE IF EXISTS raw.olist_order_reviews;
CREATE TABLE raw.olist_order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS raw.olist_orders;
CREATE TABLE raw.olist_orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

DROP TABLE IF EXISTS raw.olist_products;
CREATE TABLE raw.olist_products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght NUMERIC,
    product_description_lenght NUMERIC,
    product_photos_qty NUMERIC,
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC
);

DROP TABLE IF EXISTS raw.olist_sellers;
CREATE TABLE raw.olist_sellers (
    seller_id TEXT,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
);

DROP TABLE IF EXISTS raw.product_category_translation;
CREATE TABLE raw.product_category_translation (
    product_category_name TEXT,
    product_category_name_english TEXT
);