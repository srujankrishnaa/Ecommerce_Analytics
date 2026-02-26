from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_json,
    struct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)

BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "clean_events"
CHECKPOINT_PATH = "/opt/spark-data/checkpoints/silver_clean_events"

VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

# -------------------------------------
# 1. Spark Session
# -------------------------------------
spark = (
    SparkSession.builder
    .appName("SilverStreamProcessor")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------
# 2. Event Schema (NO timestamp here)
# -------------------------------------
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("is_valid", BooleanType())
])

# -------------------------------------
# 3. Read Bronze (Kafka)
# -------------------------------------
bronze_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# -------------------------------------
# 4. Parse JSON + ADD EVENT TIMESTAMP
# -------------------------------------
parsed_df = (
    bronze_df
    .select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), event_schema).alias("event")
    )
    .select("key", "event.*")
    .withColumn("event_timestamp", current_timestamp())  
)

# -------------------------------------
# 5. Silver Filters + Watermark
# -------------------------------------
silver_df = (
    parsed_df
    .withWatermark("event_timestamp", "10 minutes")  
    .filter(col("customer_id").isNotNull())
    .filter(col("event_type").isin(VALID_EVENT_TYPES))
    .filter(col("amount").isNotNull() & (col("amount") > 0))
    .filter(col("currency").isNotNull())
    .filter(col("is_valid") == True)
)

# -------------------------------------
# 6. Write to Silver (Kafka)
# -------------------------------------
query = (
    silver_df
    .selectExpr(
        "CAST(key AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

print("✅ Silver Stream Processor Started")
print(f"📥 Reading from: {INPUT_TOPIC}")
print(f"📤 Writing to: {OUTPUT_TOPIC}")
print(f"💾 Checkpoint: {CHECKPOINT_PATH}")

query.awaitTermination()