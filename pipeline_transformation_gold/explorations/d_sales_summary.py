# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
    # order_date DATE PRIMARY KEY,
    # total_orders BIGINT,
    # total_revenue DECIMAL(12,2),
    # avg_order_value DECIMAL(10,2),
    # unique_customers BIGINT,
    # unique_restaurants BIGINT,
    # dine_in_orders BIGINT,
    # takeaway_orders BIGINT,
    # delivery_orders BIGINT

# COMMAND ----------

from pyspark.sql.functions  import *

# COMMAND ----------





df_daily_agg = (
    # Read order data from the Silver fact table
    spark.table("ws_dbxproject.`02_silver`.fact_orders")
    # Group the orders by each date
    .groupBy("order_date")
    # Calculate daily order and revenue metrics
    .agg(
        # Count distinct orders for each day
        countDistinct(col("order_id")).alias("total_orders"),
        # Calculate total revenue for each day
        sum(col("total_amount")).cast("decimal(10,2)").alias("total_revenue"),
        # Calculate average order value for each day
        avg("total_amount").cast("decimal(10,2)").alias("avg_order_value"),
        # Count distinct customers for each day
        countDistinct("customer_id").alias("unique_customers"),
        # Count distinct restaurants with orders each day
        countDistinct("restaurant_id").alias("unique_restaurants"),
        # Count distinct dine-in orders for each day
        # when(condition, col("order_id")) → if the condition is true, return that order's order_id
        countDistinct(when(col("order_type") == "dine_in", col("order_id")).otherwise(None)).alias("dine_in_orders"),
        # Count distinct takeaway orders for each day
        countDistinct(when(col("order_type") == "take_away", col("order_id")).otherwise(None)).alias("takeaway_orders"),
        # Count distinct delivery orders for each day
        countDistinct(when(col("order_type") == "delivery", col("order_id")).otherwise(None)).alias("delivery_orders")
    )
    # Select the required daily aggregation columns
    .select(
        "order_date",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "unique_customers",
        "unique_restaurants",
        "dine_in_orders",
        "takeaway_orders",
        "delivery_orders"
    )
)

# Display the daily aggregated results
display(df_daily_agg)

# COMMAND ----------

