# ShopStream — End-to-End Databricks Lakehouse Project

## 📌 Project Overview

**ShopStream** is an end-to-end e-commerce Data Engineering project built on **Databricks**. The project demonstrates how raw order, customer, and product data can be ingested, cleaned, transformed, processed incrementally, and exposed through business-ready Gold tables and a Databricks dashboard.

The project uses a **Medallion Architecture**:

**Raw Data → Bronze → Silver → Gold → Analytics Dashboard**

It also includes a separate **streaming pipeline using Databricks Auto Loader** to demonstrate incremental file ingestion and streaming transformations.

## 🎯 Project Objectives

* Build a practical lakehouse pipeline on Databricks.
* Ingest e-commerce CSV data into Delta tables.
* Apply data cleansing and validation in the Silver layer.
* Create business-ready Gold datasets.
* Demonstrate Delta Lake time travel and table history.
* Implement incremental file ingestion using Auto Loader.
* Create streaming Bronze, Silver, and Gold tables.
* Build a Databricks dashboard for sales analysis.

## 🏗️ Architecture

```text
Source Data
    │
    ├── Batch CSV Files ──→ Bronze ──→ Silver ──→ Gold
    │                                      │
    │                                      ▼
    │                              ShopStream Sales
    │                                Dashboard
    │
    └── Event Files ──→ Auto Loader
                              │
                              ▼
                       Bronze Streaming
                              │
                              ▼
                       Silver Streaming
                              │
                              ▼
                        Gold Streaming
```

## 🛠️ Technology Stack

| Technology                  | Purpose                                |
| --------------------------- | -------------------------------------- |
| Databricks                  | Lakehouse development and execution    |
| Apache Spark / PySpark      | Data transformation and streaming      |
| SQL                         | Batch transformations and analytics    |
| Delta Lake                  | Reliable table storage and time travel |
| Databricks Auto Loader      | Incremental file ingestion             |
| Unity Catalog               | Catalog and table organization         |
| Databricks SQL / Dashboards | Business analytics                     |
| Git / GitHub                | Version control                        |

## 📂 Source Data

The batch pipeline works with:

* `orders_2026_h1.csv`
* `customers.csv`
* `products.csv`

The project uses the Unity Catalog namespace:

```text
shopstream.core
```

Source data is stored in Databricks Volumes.

## 🥉 Bronze Layer

Notebook:

```text
01_bronze_batch_ingestion.sql
```

### Tables

```text
shopstream.core.bronze_orders
shopstream.core.bronze_customers
shopstream.core.bronze_products
```

The Bronze layer uses Databricks SQL `COPY INTO` to ingest CSV files into Delta tables.

The pipeline also performs row-count checks after ingestion.

## 🥈 Silver Layer

Notebook:

```text
02_silver_layer.sql
```

### Tables

```text
shopstream.core.silver_orders
shopstream.core.silver_customers
shopstream.core.silver_products
```

The Silver layer performs:

* Duplicate detection
* Deduplication using `ROW_NUMBER()`
* Quantity validation
* Data type conversion
* Revenue calculation
* Status standardization
* Empty coupon-code handling
* Removal of cancelled orders

Order-line revenue is calculated as:

```text
line_revenue = quantity × unit_price
```

Product margin is calculated as:

```text
unit_margin = unit_price - unit_cost
```

## 🥇 Gold Layer

Notebook:

```text
03_gold_layer.sql
```

### Gold Tables

```text
shopstream.core.gold_daily_revenue
shopstream.core.gold_category_performance
shopstream.core.gold_customer_ltv
```

### Daily Revenue

Provides:

* Order date
* Number of orders
* Units sold
* Revenue

Only completed orders are included.

### Category Performance

Provides:

* Category
* Orders
* Units sold
* Revenue
* Gross margin

### Customer LTV

Provides:

* Customer ID
* Customer name
* Country
* Signup channel
* Lifetime orders
* Lifetime revenue

## 🔄 Streaming Pipeline

Notebook:

```text
04_Auto_Loader_Streaming.py
```

The project implements a separate streaming pipeline using **Databricks Auto Loader**.

```text
Incoming Event Files
        ↓
Databricks Auto Loader
        ↓
bronze_orders_stream
        ↓
silver_orders_stream
        ↓
gold_daily_revenue_stream
```

The streaming implementation demonstrates:

* Auto Loader / `cloudFiles`
* Schema inference
* Schema location
* Checkpointing
* Structured Streaming
* Delta tables
* `availableNow` trigger
* Streaming deduplication
* Streaming transformations

### Streaming Bronze

```text
shopstream.core.bronze_orders_stream
```

Incoming files are incrementally loaded into a Delta table.

### Streaming Silver

```text
shopstream.core.silver_orders_stream
```

The streaming Silver transformation:

* Standardizes status values
* Validates quantity
* Keeps `completed` and `returned` statuses
* Calculates `line_revenue`
* Removes duplicate `order_line_id` values

The implemented quantity validation keeps values between **1 and 3**.

### Streaming Gold

```text
shopstream.core.gold_daily_revenue_stream
```

Daily revenue is calculated from the streaming Silver table by grouping records by order date.

## ⏪ Delta Lake Time Travel

The project demonstrates Delta Lake table history and time travel using:

```sql
DESCRIBE HISTORY
```

and:

```sql
VERSION AS OF
```

Example:

```sql
SELECT *
FROM shopstream.core.gold_daily_revenue VERSION AS OF 0;
```

This demonstrates the ability to inspect and query previous versions of Delta tables.

## 📊 ShopStream Sales Dashboard

The project includes a Databricks dashboard named:

**ShopStream Sales**

The dashboard provides a business-facing view of the Gold-layer data.

### Revenue by Category

Shows revenue performance across categories including:

* Beauty
* Books
* Electronics
* Fashion
* Fitness
* Grocery
* Home & Kitchen
* Toys

### Daily Revenue

Shows the revenue trend across the available months in the dashboard.

The final analytics flow is:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
ShopStream Sales Dashboard
```

## 📓 Project Structure

```text
ShopStream: End-to-End Databricks Lakehouse Project/
│
├── 01_bronze_batch_ingestion.sql
├── 02_silver_layer.sql
├── 03_gold_layer.sql
└── 04_Auto_Loader_Streaming.py
```

## 🔍 Data Engineering Concepts Demonstrated

* Medallion Architecture
* Lakehouse architecture
* Batch ingestion
* `COPY INTO`
* Delta Lake
* Unity Catalog
* Databricks Volumes
* SQL transformations
* PySpark
* Structured Streaming
* Databricks Auto Loader
* Checkpointing
* Schema inference
* Data cleansing
* Deduplication
* Data validation
* Aggregations
* Joins
* Incremental file processing
* Delta Lake time travel
* Databricks dashboards


## 👨‍💻 Author

**Anshu Gupta**

## Connect with Me

- **GitHub:** https://github.com/anshu02042002
- **LinkedIn:** https://www.linkedin.com/in/anshu-gupta-de
