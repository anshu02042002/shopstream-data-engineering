# Databricks notebook source

source_path = "/Volumes/shopstream/core/events/"

schema_path = "/Volumes/shopstream/core/events/_schema/orders"

checkpoint_path = "/Volumes/shopstream/core/events/_checkpoint/orders"

print(source_path)
print(schema_path)
print(checkpoint_path)

# COMMAND ----------

streaming_orders = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", schema_path)
         .option("cloudFiles.inferColumnTypes", "true")
         .option("header", "true")
         .load(source_path)
)

# COMMAND ----------

streaming_orders.printSchema()

# COMMAND ----------

streaming_query = (
    streaming_orders.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable("shopstream.core.bronze_orders_stream")
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS row_count
FROM shopstream.core.bronze_orders_stream
""").show()

# COMMAND ----------

spark.sql("""
SELECT *
FROM shopstream.core.bronze_orders_stream
LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM shopstream.core.silver_orders
# MAGIC LIMIT 10;

# COMMAND ----------

spark.sql("DESCRIBE TABLE shopstream.core.silver_orders")

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower

streaming_silver = (
    spark.readStream
        .table("shopstream.core.bronze_orders_stream")
        .withColumn(
            "status",
            lower(trim(col("status")))
        )
        .filter(
            (col("quantity") >= 1) &
            (col("quantity") <= 3)
        )
        .filter(
            col("status").isin("completed", "returned")
        )
        .withColumn(
            "line_revenue",
            col("quantity") * col("unit_price")
        )
        .dropDuplicates(["order_line_id"])
)

# COMMAND ----------

silver_checkpoint = "/Volumes/shopstream/core/events/_checkpoint/silver_orders"

silver_query = (
    streaming_silver.writeStream
        .format("delta")
        .option("checkpointLocation", silver_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable("shopstream.core.silver_orders_stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT order_line_id) AS distinct_order_lines,
# MAGIC     MIN(quantity) AS min_quantity,
# MAGIC     MAX(quantity) AS max_quantity
# MAGIC FROM shopstream.core.silver_orders_stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, COUNT(*) AS count
# MAGIC FROM shopstream.core.silver_orders_stream
# MAGIC GROUP BY status
# MAGIC ORDER BY status;

# COMMAND ----------

from pyspark.sql.functions import col, to_date, sum

streaming_daily_revenue = (
    spark.readStream
        .table("shopstream.core.silver_orders_stream")
        .withColumn("order_date", to_date(col("order_ts")))
        .groupBy("order_date")
        .agg(
            sum("line_revenue").alias("daily_revenue")
        )
)

# COMMAND ----------

gold_checkpoint = "/Volumes/shopstream/core/events/_checkpoint/gold_daily_revenue"

gold_query = (
    streaming_daily_revenue.writeStream
        .format("delta")
        .option("checkpointLocation", gold_checkpoint)
        .outputMode("complete")
        .trigger(availableNow=True)
        .toTable("shopstream.core.gold_daily_revenue_stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM shopstream.core.gold_daily_revenue_stream
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS days
# MAGIC FROM shopstream.core.gold_daily_revenue_stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY shopstream.core.gold_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM shopstream.core.gold_daily_revenue VERSION AS OF 0
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS current_rows
# MAGIC FROM shopstream.core.gold_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS historical_rows
# MAGIC FROM shopstream.core.gold_daily_revenue VERSION AS OF 0;

# COMMAND ----------

