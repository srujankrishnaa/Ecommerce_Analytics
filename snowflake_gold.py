from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as _sum, when, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, BooleanType
)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "clean_events"
CHECKPOINT_PATH = "/opt/spark-data/checkpoints/gold_daily_metrics"

# Read private key with error handling
def read_private_key_full():
    """Read full private key with PEM headers"""
    key_path = "/opt/spark-secrets/snowflake_key.pem"
    with open(key_path, "r") as f:
        return f.read().strip()

def read_private_key_clean():
    """Read private key content without PEM headers"""
    key_path = "/opt/spark-secrets/snowflake_key.pem"
    with open(key_path, "r") as f:
        key_content = f.read().strip()
        lines = key_content.split('\n')
        key_lines = [line for line in lines if not line.startswith('-----')]
        return ''.join(key_lines)

# Try the clean key first
try:
    private_key_content = read_private_key_clean()
    print(f"🔍 Using clean private key (length: {len(private_key_content)})")
except:
    private_key_content = read_private_key_full()
    print(f"🔍 Using full PEM private key (length: {len(private_key_content)})")

SNOWFLAKE_OPTIONS = {
    "sfURL": "si29297.ap-southeast-1.snowflakecomputing.com",
    "sfUser": "KAFKA_SERVICE",
    "sfDatabase": "KAFKA_DB",
    "sfSchema": "STREAMING",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "KAFKA_LOADER",

    # 🔐 Authentication
    "pem_private_key": private_key_content,
    
    # 📦 Required streaming stage for Snowflake connector
    "streaming_stage": "KAFKA_STREAMING_STAGE"
}

SNOWFLAKE_TABLE = "CUSTOMER_DAILY_METRICS"

# -------------------------------------------------
# SPARK SESSION
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GoldDailyMetrics")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# SCHEMA (MUST MATCH SILVER)
# -------------------------------------------------
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("is_valid", BooleanType()),
    StructField("event_timestamp", TimestampType())
])

# -------------------------------------------------
# READ FROM KAFKA (SILVER)
# -------------------------------------------------
silver_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    silver_df
    .select(from_json(col("value").cast("string"), event_schema).alias("event"))
    .select("event.*")
)

# -------------------------------------------------
# GOLD AGGREGATION WITH WATERMARK
# -------------------------------------------------
gold_df = (
    parsed_df
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withWatermark("event_timestamp", "10 minutes")  # Handle late data up to 10 minutes
    .groupBy("customer_id", "event_date")
    .agg(
        count(when(col("event_type") == "PAGE_VIEW", True)).alias("page_views"),
        count(when(col("event_type") == "ADD_TO_CART", True)).alias("add_to_cart_count"),
        count(when(col("event_type") == "PURCHASE", True)).alias("purchase_count"),
        _sum(when(col("event_type") == "PURCHASE", col("amount"))).alias("total_revenue")
    )
)

# -------------------------------------------------
# WRITE TO SNOWFLAKE (MICRO-BATCH APPROACH)
# -------------------------------------------------
def write_to_snowflake(batch_df, batch_id):
    """Write each micro-batch to Snowflake using batch mode"""
    if batch_df.count() > 0:
        print(f"📦 Writing batch {batch_id} with {batch_df.count()} records to Snowflake")
        
        # Write using batch mode (which supports upsert/merge operations)
        batch_df.write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", SNOWFLAKE_TABLE) \
            .mode("append") \
            .save()
        
        print(f"✅ Batch {batch_id} written successfully")
    else:
        print(f"⏭️ Batch {batch_id} is empty, skipping")

query = (
    gold_df
    .writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Snowflake Gold Streaming Job Started (Micro-batch Mode)")
query.awaitTermination()