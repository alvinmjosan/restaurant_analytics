from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

# go to settings and add configuration for EH_NAMESPACE, EH_NAME, EH_CONN_STR
EH_NAMESPACE="eh-ns-dbxprojects"
EH_NAME ="orders"
EH_CONN_STR="Endpoint=sb://eh-ns-dbxprojects.servicebus.windows.net/;SharedAccessKeyName=databricks-read-policy;SharedAccessKey=puENMFW0yjjwjWUKbXeZkjgevkzj+0F5B+AEhKYkYYs="

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
 "kafka.request.timeout.ms": "60000",   # Max wait for broker response
 "kafka.session.timeout.ms": "30000",   # Heartbeat timeout before rebalance
 "maxOffsetsPerTrigger": "50000",       # Rate limit: max messages per micro-batch
 "failOnDataLoss": "false",              # Fail stream if data is missing (safe default)
 "startingOffsets": "earliest",         # First run: read from beginning of topic
}


@dp.table(
    name="orders", 
    table_properties={"quality": "bronze"}
) # Defines a streaming table "orders" tagged as bronze quality (raw/ingested data layer)

def orders():
    df_raw = (
        spark.readStream          # Creates a streaming (not batch) DataFrame reader
        .format("kafka")          # Selects the Kafka connector as the streaming source
        .options(**KAFKA_OPTIONS) # Unpacks a dict of Kafka config options
        .load()                   # Connects and starts reading the stream
    )

    order_schema = StructType([
        StructField("order_id",       StringType(),    True),
        StructField("timestamp",      StringType(),    True),
        StructField("restaurant_id",  StringType(),    True),
        StructField("customer_id",    StringType(),    True),
        StructField("order_type",     StringType(),    True),
        StructField("items",          StringType(),    True),      
        StructField("total_amount",   DoubleType(),    True),
        StructField("payment_method", StringType(),    True),
        StructField("order_status",   StringType(),    True)
        
    ])

    df_parsed = (
        df_raw
        .withColumn("value_str", col("value").cast("string"))   # Convert Kafka binary payload to string
        .withColumn("key_str",   col("key").cast("string"))     # Convert Kafka binary key to string
        .withColumn("data", from_json("value_str", order_schema))  # Parse JSON string into struct using schema
        .select("data.*")                                       # Flatten struct fields into columns
        .withColumnRenamed("timestamp", "order_timestamp")      # Rename to avoid clash with Kafka's timestamp
    )

    return df_parsed