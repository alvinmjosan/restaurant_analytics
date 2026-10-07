from pyspark.sql.functions import *
from pyspark import pipelines as dp
# The Silver layer can continuously receive data while the Gold layer stores an updated aggregated result
@dp.materialized_view(
    name="03_gold.d_sales_summary",  # Create a Gold materialized view for the sales summary
    partition_cols=["order_date"],  # Partition the table by order date for more efficient queries
    table_properties={"quality": "gold"}  # Mark this dataset as Gold quality
)
def d_sales_summary():
    df_daily_agg = (
        dp.read("ws_dbxproject.`02_silver`.fact_orders")  # Read the Silver fact_orders table as a batch/static dataset
        .groupBy("order_date")
        .agg(
            countDistinct (col("order_id")).alias("total_orders"),
            sum(col("total_amount")).cast("decimal(10,2)").alias("total_revenue"),
            avg("total_amount").cast("decimal(10,2)").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("restaurant_id").alias("unique_restaurants"),
            countDistinct(
                when(col("order_type")=="dine_in", col("order_id")).otherwise(None)
            ).alias("dine_in_orders"),
            countDistinct(
                when(col("order_type")=="take_away", col("order_id")).otherwise(None)
            ).alias("takeaway_orders"),
            countDistinct(
                when(col("order_type")=="delivery", col('order_id')).otherwise(None)
            ).alias("delivery_orders")

        )
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
    return df_daily_agg
