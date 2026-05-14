# Olist E-commerce Data Warehouse Project Documentation

## 1. Introduction

The goal of this project is to design and implement a complete end-to-end data lifecycle based on the Olist Brazilian E-Commerce Public Dataset. The project starts from raw CSV files and builds a layered data warehouse architecture in a PostgreSQL database. The final goal is to create a management-level Power BI dashboard that supports the analysis of sales performance, delivery performance, customer satisfaction, and seller performance.

The project was developed with a data engineering mindset. It does not only contain visualizations, but also demonstrates data ingestion, a staging layer, data cleaning, multidimensional modeling, an SCD strategy, data marts, and audit logging.

---

## 2. Technologies Used

The project uses the following technologies:

- PostgreSQL: database and data warehouse
- pgAdmin: PostgreSQL administration interface
- Python: running ETL scripts
- psycopg2: PostgreSQL connection from Python
- Pandas: export and supporting data processing tasks
- Power BI Desktop: management dashboard and OLAP visualization
- VS Code: development environment

The main data storage system of the project is PostgreSQL. Power BI connects directly to the mart tables created in PostgreSQL.

---

## 3. Dataset Overview

The dataset used in the project is the Olist Brazilian E-Commerce Public Dataset. It is a publicly available Kaggle dataset containing order data from a Brazilian e-commerce marketplace.

The source consists of multiple CSV files:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

The source data covers several business areas: orders, payments, delivery, products, customers, sellers, geographical data, and customer reviews.

---

## 4. Business Process Definition

The business process analyzed in this project is the following:

**E-commerce order, payment, delivery, and customer satisfaction process.**

The main steps of the process are:

1. The customer places an order.
2. The order contains one or more order items.
3. The customer completes a payment transaction.
4. The seller fulfills the order.
5. The order is delivered to the customer.
6. The customer may leave a review about the purchase experience.

The main business questions of the project are:

- Which product categories generate the highest revenue?
- In which regions and states is sales performance the strongest?
- How accurate is the delivery process?
- What is the relationship between delivery delays and customer review scores?
- Which sellers perform exceptionally well?
- Which sellers require attention due to delivery or review-related issues?

---

## 5. Grain Definition

The main fact table is `warehouse.fact_order_item`.

The grain is defined as follows:

**One row in the fact table represents one order item within an order.**

Natural business grain:

```text
order_id + order_item_id
```

This means that each record describes one specific product line within a given order. This grain enables analysis at the product, seller, region, and time level.

Additional fact tables were also created in the project:

| Fact Table | Grain | Purpose |
|---|---|---|
| `fact_order_item` | one order item | sales, product, seller, freight analysis |
| `fact_payment` | one payment transaction | payment method analysis |
| `fact_review` | one review record | customer satisfaction analysis |
| `fact_delivery` | one order delivery process | delivery performance analysis |

---

## 6. Data Architecture

The project uses a layered data architecture in PostgreSQL.

Created schemas:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;
```

Data flow:

```text
Kaggle CSV files
    ↓
raw schema
    ↓
staging schema
    ↓
warehouse schema
    ↓
mart schema
    ↓
Power BI dashboard
```

Role of each layer:

| Layer | Role |
|---|---|
| `raw` | stores raw CSV data in unchanged form |
| `staging` | data cleaning, standardization, and calculated fields |
| `warehouse` | star-schema data warehouse with fact and dimension tables |
| `mart` | aggregated business views optimized for dashboards |
| `audit` | logging ETL runs and data quality checks |

---

## 7. Raw Layer

The purpose of the raw layer is to preserve the original source data in unchanged form. This ensures auditability and traceability.

Tables created in the raw layer:

- `raw.olist_customers`
- `raw.olist_geolocation`
- `raw.olist_order_items`
- `raw.olist_order_payments`
- `raw.olist_order_reviews`
- `raw.olist_orders`
- `raw.olist_products`
- `raw.olist_sellers`
- `raw.product_category_translation`

No corrections are applied in the raw layer. For example, the original Olist column name typo `product_name_lenght` is kept in this layer. Corrections are applied only in the staging layer.

---

## 8. ETL Process

The ETL process starts from Python. The CSV files are loaded into PostgreSQL by the script `scripts/01_load_raw.py`.

Main characteristics of the loading process:

- CSV files are located in the `data/raw/` folder;
- the Python script uses a PostgreSQL connection;
- the data is loaded into the raw schema;
- the ETL run is logged in audit tables;
- during development, raw tables can be reloaded.

The database connection is stored in the `.env` file:

```env
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=olist_dw
DB_USER=postgres
DB_PASSWORD=password
```

After loading, the audit tables show when the pipeline ran, what status it finished with, and how many rows were loaded.

---

## 9. Audit Layer

The purpose of the audit layer is to track pipeline runs and data quality checks.

Created audit tables:

- `audit.etl_run_log`
- `audit.file_load_log`
- `audit.data_quality_results`

The `audit.etl_run_log` table stores:

- pipeline name,
- start time,
- finish time,
- status,
- number of rows read,
- number of rows loaded,
- number of rejected rows,
- error message.

The audit table is important from a data engineering perspective because it shows that not only the final result, but also the process that produced the result, can be checked and monitored.

---

## 10. Staging Layer

The staging layer is where data cleaning, standardization, and business preparation take place.

Created staging tables:

- `staging.stg_customers`
- `staging.stg_sellers`
- `staging.stg_products`
- `staging.stg_orders`
- `staging.stg_order_items`
- `staging.stg_payments`
- `staging.stg_reviews`
- `staging.stg_geolocation`

### 10.1. Standardization of Text Data

For city and state names, unnecessary spaces were removed and letter casing was standardized.

Example:

```sql
INITCAP(TRIM(customer_city)) AS customer_city,
UPPER(TRIM(customer_state)) AS customer_state
```

This ensures that values such as `sp`, `SP`, and ` SP ` appear consistently as `SP`.

### 10.2. Handling Missing Values

Missing or empty category values were replaced with descriptive values:

```sql
COALESCE(NULLIF(TRIM(product_category_name), ''), 'unknown')
```

This prevents `NULL` or empty fields from reducing the interpretability of the dashboard.

### 10.3. Mapping and Category Translation

Portuguese product category names were translated into English using the `product_category_name_translation` table.

In addition, Brazilian state codes were mapped into regions:

- `SP`, `RJ`, `MG`, `ES` → Southeast
- `PR`, `SC`, `RS` → South
- `BA`, `PE`, `CE`, etc. → Northeast
- `AM`, `PA`, etc. → North
- `DF`, `GO`, `MT`, `MS` → Central-West

### 10.4. Calculated Fields

Several calculated business fields were created in the staging layer:

- `delivery_days`
- `delay_days`
- `is_late_delivery`
- `delivery_performance_status`
- `item_revenue`
- `item_total_value`
- `product_volume_cm3`
- `product_weight_band`
- `product_size_band`
- `review_sentiment`
- `review_response_days`

Example delivery status logic:

```sql
CASE
    WHEN order_delivered_customer_date IS NULL THEN 'not_delivered'
    WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'delivered_late'
    ELSE 'delivered_on_time'
END AS delivery_performance_status
```

### 10.5. Handling Invalid Values

Invalid or suspicious values were not deleted automatically. Instead, they were marked with flag fields.

Example:

```sql
CASE
    WHEN price IS NULL OR price < 0 THEN TRUE
    ELSE FALSE
END AS has_invalid_price
```

Flagging invalid data is better than automatically deleting it, because the data remains auditable and can be filtered out from reports if necessary.

---

## 11. Integration and Single Point of Truth

In this project, the Single Point of Truth is implemented by building a unified warehouse and mart layer from separate CSV files.

The raw sources provide only partial views individually:

- `orders` contains order dates and statuses;
- `order_items` contains prices, products, and sellers;
- `customers` contains customer location;
- `products` contains product categories;
- `reviews` contains customer reviews;
- `payments` contains payment information.

The warehouse layer integrates these sources into a unified model. The central fact table is `warehouse.fact_order_item`, which connects to customer, product, seller, status, and date dimensions.

KPIs are not calculated separately from individual CSV files. Instead, they are calculated from the unified warehouse and mart layers. This ensures that every Power BI dashboard page is based on the same business definitions.

Standard KPI definitions:

| KPI | Definition |
|---|---|
| Total Revenue | sum of `price` for valid order item rows |
| Total Freight | sum of `freight_value` |
| Average Order Value | Total Revenue / Total Orders |
| Late Delivery Rate | late orders / total orders |
| Review Sentiment | Positive, Neutral, or Negative based on review score |

---

## 12. Warehouse Layer and Multidimensional Model

The warehouse layer follows star-schema logic. Fact tables are at the center of the model and connect to dimension tables using surrogate keys.

### 12.1. Dimension Tables

Created dimensions:

- `warehouse.dim_date`
- `warehouse.dim_customer`
- `warehouse.dim_product`
- `warehouse.dim_seller`
- `warehouse.dim_order_status`
- `warehouse.dim_payment_type`

### 12.2. Fact Tables

Created fact tables:

- `warehouse.fact_order_item`
- `warehouse.fact_payment`
- `warehouse.fact_review`
- `warehouse.fact_delivery`

### 12.3. Surrogate Keys

Artificial keys are used in the dimensions:

- `customer_sk`
- `product_sk`
- `seller_sk`
- `order_status_sk`
- `payment_type_sk`

The fact tables reference these surrogate keys. This provides a more stable warehouse model than directly using natural keys.

---

## 13. SCD Strategy

The project applies a hybrid SCD strategy.

Not every field is historized. Data quality corrections and standardizations are handled with Type 1 logic, while business-critical attributes that may change over time are handled using SCD Type 2.

### 13.1. SCD Type 2 Attributes

The following attributes were selected for SCD Type 2 handling:

1. `dim_product.product_category_name_english`
2. `dim_seller.seller_performance_segment`

Additional Type 2 attributes:

- `dim_product.product_weight_band`
- `dim_product.product_size_band`
- `dim_seller.seller_region`

### 13.2. SCD Type 2 Justification: Product Category

A product's business category may change over time. For example, a product may be classified as `housewares` in the past and later reclassified as `home_decor`. Historical reports must preserve the category that was valid at the time.

Without SCD Type 2, old reports would also show the new category, which would distort historical analysis.

### 13.3. SCD Type 2 Justification: Seller Performance Segment

A seller's performance segment may change over time. For example, a seller may initially be a `Standard Seller` and later become a `Top Seller`.

If this value were overwritten, earlier orders would also appear as if they belonged to the new seller status. For this reason, historizing the seller performance segment is justified.

### 13.4. SCD Type 2 Technical Fields

The dimensions handled with Type 2 include the following technical fields:

- `valid_from`
- `valid_to`
- `is_current`
- `record_hash`

The `record_hash` is generated from the historized attributes. If the hash changes, a new version is inserted with a new surrogate key.

Example:

```sql
MD5(
    COALESCE(product_category_name_english, '') || '|' ||
    COALESCE(product_weight_band, '') || '|' ||
    COALESCE(product_size_band, '')
) AS record_hash
```

### 13.5. SCD Type 2 Logic

When a change occurs:

1. The previous current record's `valid_to` value is closed.
2. The `is_current` value becomes `FALSE`.
3. A new record is inserted with a new surrogate key.
4. The new record's `valid_from` field is set to the change date.
5. The new record's `valid_to` value is set to `9999-12-31`.
6. The new record has `is_current = TRUE`.

---

## 14. Data Mart Layer

The purpose of the mart layer is to provide aggregated business tables prepared for the Power BI dashboard.

Created mart tables:

- `mart.sales_performance`
- `mart.delivery_performance`
- `mart.customer_satisfaction`
- `mart.seller_scorecard`
- `mart.olap_sales_cube`

### 14.1. Sales Performance Mart

Table:

```text
mart.sales_performance
```

Grain:

```text
year + month + customer_state + customer_region + product_category
```

Main metrics:

- `order_count`
- `order_item_count`
- `customer_count`
- `total_revenue`
- `total_freight_value`
- `average_order_value`
- `freight_to_revenue_ratio`

Business question:

**Which regions and product categories generate the most revenue?**

### 14.2. Delivery Performance Mart

Table:

```text
mart.delivery_performance
```

Main metrics:

- `order_count`
- `late_order_count`
- `on_time_order_count`
- `avg_delivery_days`
- `avg_delay_days`
- `late_delivery_rate`
- `on_time_delivery_rate`

Business question:

**How accurate is delivery by time and region?**

### 14.3. Customer Satisfaction Mart

Table:

```text
mart.customer_satisfaction
```

Main metrics:

- `review_count`
- `avg_review_score`
- `positive_review_rate`
- `negative_review_rate`
- `avg_review_response_days`
- `late_delivery_rate_for_reviewed_orders`

Business question:

**How does customer satisfaction change, and how is it related to delivery performance?**

### 14.4. Seller Scorecard Mart

Table:

```text
mart.seller_scorecard
```

Main metrics:

- `order_count`
- `total_revenue`
- `avg_review_score`
- `late_delivery_rate`
- `negative_review_rate`
- `revenue_rank`
- `seller_scorecard_segment`

Business question:

**Which sellers perform well, and which sellers require attention?**

---

## 15. Power BI Dashboard

The Power BI dashboard is based on the mart tables created in PostgreSQL.

Dashboard pages:

1. Sales Overview
2. Delivery Performance
3. Customer Satisfaction
4. Seller Scorecard
5. OLAP Demo

### 15.1. Sales Overview Page

Source table:

```text
mart.sales_performance
```

Main KPIs:

- Total Revenue
- Total Orders
- Average Order Value
- Revenue by Month
- Top 10 Product Categories by Revenue
- Revenue by Customer Region

### 15.2. Delivery Performance Page

Source table:

```text
mart.delivery_performance
```

Main KPIs:

- Delivery Orders
- Late Delivery Rate
- On-Time Delivery Rate
- Average Delivery Days
- Orders by Delivery Status
- Late Delivery Rate by Region

### 15.3. Customer Satisfaction Page

Source table:

```text
mart.customer_satisfaction
```

Main KPIs:

- Total Reviews
- Average Review Score
- Negative Review Rate
- Positive Review Rate
- Review Sentiment Distribution
- Delivery Delay vs Review Score

### 15.4. Seller Scorecard Page

Source table:

```text
mart.seller_scorecard
```

Main KPIs:

- Seller Count
- Seller Total Revenue
- Avg Seller Review Score
- Avg Seller Late Delivery Rate
- Top 10 Sellers by Revenue
- Seller Revenue vs Review Score
- Seller Scorecard Segment Distribution

---

## 16. Power BI DAX Measure Examples

Several DAX measures were created in the project.

### 16.1. Sales Measures

```DAX
Total Revenue = SUM('mart sales_performance'[total_revenue])
```

```DAX
Total Orders = SUM('mart sales_performance'[order_count])
```

```DAX
Average Order Value KPI = DIVIDE([Total Revenue], [Total Orders])
```

### 16.2. Delivery Measures

```DAX
Delivery Orders = SUM('mart delivery_performance'[order_count])
```

```DAX
Late Orders = SUM('mart delivery_performance'[late_order_count])
```

```DAX
Late Delivery Rate KPI = DIVIDE([Late Orders], [Delivery Orders])
```

### 16.3. Customer Satisfaction Measures

```DAX
Total Reviews = SUM('mart customer_satisfaction'[review_count])
```

```DAX
Average Review Score KPI = AVERAGE('mart customer_satisfaction'[avg_review_score])
```

```DAX
Negative Review Rate KPI = DIVIDE([Negative Reviews], [Total Reviews])
```

### 16.4. Seller Scorecard Measures

```DAX
Seller Total Revenue = SUM('mart seller_scorecard'[total_revenue])
```

```DAX
Seller Count = DISTINCTCOUNT('mart seller_scorecard'[seller_id])
```

```DAX
Avg Seller Review Score KPI = AVERAGE('mart seller_scorecard'[avg_review_score])
```

---

## 17. Project Result

As a result of the project, a complete e-commerce data warehouse was created. It:

- integrates multiple raw CSV files;
- cleans and standardizes the data through a staging layer;
- builds a star-schema warehouse model;
- uses surrogate keys;
- applies an SCD Type 2 strategy to product and seller attributes;
- creates data marts for management reporting;
- demonstrates OLAP operations;
- supports business decision-making with a Power BI dashboard.

The project can be interpreted not only as an academic assignment, but also as a data engineering portfolio project, because it demonstrates the full lifecycle of data from raw sources to management dashboards.

---

## 18. Summary

The project satisfies the main requirements of the assignment:

| Requirement | Implementation |
|---|---|
| Dataset selection | Olist Brazilian E-Commerce Public Dataset |
| Business process | e-commerce order, payment, delivery, and review process |
| Grain declaration | `fact_order_item`: one order item |
| Staging area | `staging` schema |
| Data cleaning | TRIM, mapping, unknown values, data quality flags |
| Integration | warehouse and mart layers as Single Point of Truth |
| Star schema | fact and dimension tables in PostgreSQL |
| SCD strategy | Type 2 attributes in `dim_product` and `dim_seller` |
| Surrogate keys | artificial `_sk` keys |
| Data mart | sales, delivery, satisfaction, and seller marts |
| Visualization | Power BI dashboard with at least 6 KPIs |

The main business value of the project is that it transforms raw e-commerce data into consistent, reliable, and dashboard-ready business information.

