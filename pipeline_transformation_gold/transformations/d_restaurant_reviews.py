from pyspark.sql.functions import *  # Import PySpark functions
from pyspark import pipelines as dp  # Import Databricks pipeline functions

@dp.materialized_view(
    name="ws_dbxproject.`03_gold`.d_restaurant_reviews",  # Create the Gold materialized view
    table_properties={
        "quality": "gold"  # Mark the table as Gold quality
    }
)
def d_restaurant_reviews():
    # Read Silver reviews and calculate review statistics for each restaurant
    df_review_stats = (
        dp.read("ws_dbxproject.`02_silver`.fact_reviews")  # Read the Silver reviews table
        .groupBy("restaurant_id")  # Group reviews by restaurant
        .agg(
            countDistinct("review_id").alias("total_reviews"),  # Count the total unique reviews
            round(avg("rating"), 2).alias("avg_rating"),  # Calculate the average rating rounded to 2 decimals
            sum(when(col("rating") == 5, 1).otherwise(0)).alias("rating_5_count"),  # Count 5-star ratings
            sum(when(col("rating") == 4, 1).otherwise(0)).alias("rating_4_count"),  # Count 4-star ratings
            sum(when(col("rating") == 3, 1).otherwise(0)).alias("rating_3_count"),  # Count 3-star ratings
            sum(when(col("rating") == 2, 1).otherwise(0)).alias("rating_2_count"),  # Count 2-star ratings
            sum(when(col("rating") == 1, 1).otherwise(0)).alias("rating_1_count"),  # Count 1-star ratings
            sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_count"),  # Count positive reviews
            sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_count"),  # Count negative reviews
            sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_count")  # Count neutral reviews
        )
    )

    # Read restaurant details from the Silver restaurant dimension table
    df_restaurant = dp.read("ws_dbxproject.`02_silver`.dim_restaurants")

    # Join restaurant details with the calculated review statistics
    df_restaurant_reviews = (
        df_restaurant.join(df_review_stats, on="restaurant_id", how="left")  # Keep all restaurants and add their review statistics
        .select(
            "restaurant_id",  # Select the restaurant ID
            col("name").alias("restaurant_name"),  # Rename name to restaurant_name
            "city",  # Select the restaurant city
            coalesce(col("total_reviews"), lit(0)).alias("total_reviews"),  # Replace NULL review count with 0
            coalesce(col("avg_rating"), lit(0)).alias("avg_rating"),  # Replace NULL average rating with 0
            coalesce(col("rating_5_count"), lit(0)).alias("rating_5_count"),  # Replace NULL 5-star count with 0
            coalesce(col("rating_4_count"), lit(0)).alias("rating_4_count"),  # Replace NULL 4-star count with 0
            coalesce(col("rating_3_count"), lit(0)).alias("rating_3_count"),  # Replace NULL 3-star count with 0
            coalesce(col("rating_2_count"), lit(0)).alias("rating_2_count"),  # Replace NULL 2-star count with 0
            coalesce(col("rating_1_count"), lit(0)).alias("rating_1_count"),  # Replace NULL 1-star count with 0
            coalesce(col("sentiment_positive_count"), lit(0)).alias("sentiment_positive_count"),  # Replace NULL positive count with 0
            coalesce(col("sentiment_neutral_count"), lit(0)).alias("sentiment_neutral_count"),  # Replace NULL neutral count with 0
            coalesce(col("sentiment_negative_count"), lit(0)).alias("sentiment_negative_count")  # Replace NULL negative count with 0
        )
    )

    return df_restaurant_reviews  # Return the final Gold restaurant review dataset