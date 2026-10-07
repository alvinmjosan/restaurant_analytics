from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name="ws_dbxproject.`03_gold`.d_customer_360",
    table_properties={
        "quality" : "gold"
    }
)
def cusotmer_360():
    df_orders=(
        dp.read("ws_dbxproject.`02_silver`.fact_orders")
    )


    df_order_stats=(
        df_orders
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            round(sum(col("total_amount")),2).alias("lifetime_spend"),
            round(avg(col("total_amount")),2).alias("avg_order_value"),
            max("order_date").alias("last_order_date")
        )
        .withColumn(
            "loyalty_tier",
            when(col("lifetime_spend")>=5000,"platinum")
            .when(col("lifetime_spend")>=2000, "gold")
            .when(col("lifetime_spend")>=1000, "silver")
            .otherwise("bronze")
        )
        
    )

    df_reviews=(
        dp.read("ws_dbxproject.`02_silver`.fact_reviews")
    )


    df_review_stats=(
        df_reviews
        .groupBy("customer_id")
        .agg(
            countDistinct("review_id").alias("total_reviews"),
            round(avg(col("rating")),2).alias("avg_rating_given")

        )

    )

    df_restaurant=(
        dp.read("ws_dbxproject.`02_silver`.dim_restaurants")
    )

    df_fav_restaurant=(
        df_orders
        .join(df_restaurant,on="restaurant_id",how="inner")
        .groupBy("customer_id","name")
        .agg(
            count("order_id").alias("order_count")
        )
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("customer_id") # Creates a separate  window/group for each customer for the purpose of ranking
                .orderBy(desc("order_count"))  # Sorts restaurants from highest orders to lowest
            )  #  row_number() -> Assigns rank numbers 1, 2, 3...
        )
        .filter(col("rn")==1)  # gets the rank 1
        .drop("rn")
        .select(
            "customer_id",
            col("name").alias("favorite_restaurant")
        )
    )

    df_fact_order_items=(
        dp.read("ws_dbxproject.`02_silver`.fact_order_items")
    )

    df_fav_item=(
        df_orders.join(df_fact_order_items,on="order_id",how="inner")
        .groupBy("customer_id","item_name")
        .agg(
            sum("quantity").alias("item_quantity")
        )
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("customer_id")
                .orderBy(desc("item_quantity"))
            )
        )
        .filter(col("rn")==1)
        .drop("rn")
        .select(
            "customer_id",
            col("item_name").alias("fav_item")
        )
    )
    

    df_c360=(
        dp.read("ws_dbxproject.`02_silver`.dim_customers")
        .join(df_order_stats,on="customer_id",how="left")
        .join(df_review_stats,on="customer_id",how="left")
        .join(df_fav_restaurant,on="customer_id",how="left")
        .join(df_fav_item,on="customer_id",how="left")
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            "email",
            "city",
            "join_date",
            "loyalty_tier",
            coalesce(col("total_orders"),lit(0)).alias("total_orders"),
            coalesce(col("lifetime_spend"),lit(0)).cast("decimal(10,2)").alias("lifetime_spend"),
        coalesce(col("avg_order_value"),lit(0)).cast("decimal(10,2)").alias("avg_order_value"),
            "last_order_date",
            coalesce(col("avg_rating_given"),lit(0)).cast("decimal(10,2)").alias("avg_rating_given"),
            coalesce(col("total_reviews"), lit(0)).cast("decimal(10,2)").alias("total_reviews"),

            "favorite_restaurant",
            "fav_item",
            when(col("lifetime_spend")>=5000,True).otherwise(False).alias("VIP")
        )
    )

    return df_c360

















