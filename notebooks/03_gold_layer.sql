-- Databricks notebook source
CREATE OR REPLACE TABLE shopstream.core.gold_daily_revenue AS
SELECT
  DATE(order_ts) AS order_date,
  COUNT(DISTINCT order_id) AS orders,
  SUM(quantity) AS units_sold,
  ROUND(SUM(line_revenue), 2) AS revenue
FROM shopstream.core.silver_orders
WHERE status = 'completed'
GROUP BY DATE(order_ts);

SELECT * FROM shopstream.core.gold_daily_revenue ORDER BY order_date LIMIT 7


-- COMMAND ----------

CREATE OR REPLACE TABLE shopstream.core.gold_category_performance AS
SELECT
  p.category,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(o.quantity) AS units_sold,
  ROUND(SUM(o.line_revenue), 2) AS revenue,
  ROUND(SUM(o.quantity * p.unit_margin), 2) AS gross_margin
FROM shopstream.core.silver_orders o
JOIN shopstream.core.silver_products p USING (product_id)
WHERE o.status = 'completed'
GROUP BY p.category;

SELECT * FROM shopstream.core.gold_category_performance ORDER BY revenue DESC

-- COMMAND ----------

CREATE OR REPLACE TABLE shopstream.core.gold_customer_ltv AS
SELECT
  c.customer_id,
  c.name,
  c.country,
  c.signup_channel,
  COUNT(DISTINCT o.order_id) AS lifetime_orders,
  ROUND(SUM(o.line_revenue), 2) AS lifetime_revenue
FROM shopstream.core.silver_orders o
JOIN shopstream.core.silver_customers c USING (customer_id)
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.name, c.country, c.signup_channel;

SELECT * FROM shopstream.core.gold_customer_ltv ORDER BY lifetime_revenue DESC LIMIT 10
