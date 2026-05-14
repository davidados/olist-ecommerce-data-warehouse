\# End-to-End Brazilian E-Commerce Data Warehouse



\## Project Overview



This project builds an end-to-end data warehouse using the Olist Brazilian E-Commerce Public Dataset.  

The goal is to transform raw Kaggle CSV files into a clean, integrated and analytics-ready PostgreSQL data warehouse, then visualize the business insights in Power BI.


## Dashboard Preview

### Sales Overview
![Sales Overview](dashboard/screenshots/01_sales_overview.png)

### Delivery Performance
![Delivery Performance](dashboard/screenshots/02_delivery_performance.png)

### Customer Satisfaction
![Customer Satisfaction](dashboard/screenshots/03_customer_satisfaction.png)

### Seller Scorecard
![Seller Scorecard](dashboard/screenshots/04_seller_scorecard.png)


\## Business Process



The analyzed business process is the e-commerce order lifecycle:



1\. Customer places an order

2\. Payment is processed

3\. Seller fulfills the order

4\. Product is delivered

5\. Customer leaves a review



The project covers sales, delivery performance, customer satisfaction and seller performance analytics.



\## Tech Stack



\- PostgreSQL

\- Python

\- Pandas

\- psycopg2

\- Power BI Desktop

\- pgAdmin

\- VS Code



\## Data Architecture



```text

Kaggle CSV files

&#x20;   ↓

raw schema

&#x20;   ↓

staging schema

&#x20;   ↓

warehouse schema

&#x20;   ↓

mart schema

&#x20;   ↓

Power BI dashboard



\## Key Features

Raw, staging, warehouse, mart and audit layers
Python ETL pipeline
PostgreSQL dimensional data warehouse
Star schema design
Surrogate keys
SCD Type 2 strategy for product and seller dimensions
Data quality flags
Audit logging
Business data marts
Power BI dashboard
Main Fact Table Grain

The main fact table is warehouse.fact_order_item.

One row represents one order item within an order.

\## Natural grain:

order_id + order_item_id
Main Data Marts
mart.sales_performance
mart.delivery_performance
mart.customer_satisfaction
mart.seller_scorecard
mart.olap_sales_cube
Dashboard Pages
Sales Overview
Delivery Performance
Customer Satisfaction
Seller Scorecard
OLAP Demo
How to Run
Create PostgreSQL database:
CREATE DATABASE olist_dw;
Install Python dependencies:
pip install -r requirements.txt
Create .env file based on .env.example.
Place Kaggle CSV files into:
data/raw/

\## Run SQL and ETL scripts in this order:
psql -U postgres -d olist_dw -f sql/01_create_schemas.sql
psql -U postgres -d olist_dw -f sql/02_create_raw_tables.sql
psql -U postgres -d olist_dw -f sql/03_create_audit_tables.sql
python scripts/01_load_raw.py
psql -U postgres -d olist_dw -f sql/04_create_staging_tables.sql
psql -U postgres -d olist_dw -f sql/06_create_warehouse_tables.sql
psql -U postgres -d olist_dw -f sql/08_create_additional_facts.sql
psql -U postgres -d olist_dw -f sql/10_create_marts.sql


\##  Dataset
Dataset: Olist Brazilian E-Commerce Public Dataset
Source: Kaggle

Raw data files are  included in this repository. It is located in the data/raw/ folder.

