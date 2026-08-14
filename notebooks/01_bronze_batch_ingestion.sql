-- Databricks notebook source
SELECT * FROM read_files(
    '/Volumes/shopstream/core/raw/orders_2026_h1.csv',
    format => 'csv',
    header => true
)
LIMIT 10

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS shopstream.core.bronze_orders;

COPY INTO shopstream.core.bronze_orders
FROM '/Volumes/shopstream/core/raw/orders_2026_h1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS shopstream.core.bronze_customers;

COPY INTO shopstream.core.bronze_customers
FROM '/Volumes/shopstream/core/raw/customers.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


CREATE TABLE IF NOT EXISTS shopstream.core.bronze_products;

COPY INTO shopstream.core.bronze_products
FROM '/Volumes/shopstream/core/raw/products.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')


-- COMMAND ----------

SELECT 'bronze_orders' AS table_name, COUNT(*) AS row_count FROM shopstream.core.bronze_orders

UNION ALL

SELECT 'bronze_customers', COUNT(*) FROM shopstream.core.bronze_customers

UNION ALL

SELECT 'bronze_products', COUNT(*) FROM shopstream.core.bronze_products


-- COMMAND ----------

