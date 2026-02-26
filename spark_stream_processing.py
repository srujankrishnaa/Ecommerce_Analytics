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

# -------------------------------------
# Config
# -------------------------------------
BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "clean_events"
CHECKPOINT_PATH = "/opt/spark-data/checkpoints/silver_clean_events"

VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

# -------------------------------------
# Spark Session
# -------------------------------------
spark = (
    SparkSession.builder
    .appName("SilverStreamProcessor")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------
# Event Schema (no timestamp from producer)
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
# Read Bronze (Kafka)
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
# Parse JSON + processing timestamp
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
# Silver filters + watermark
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
# Batch metrics logger
# -------------------------------------
def log_and_write_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"🧮 Silver batch {batch_id}: {count} valid events")

    (
        batch_df
        .select(
            col("key").cast("string").alias("key"),
            to_json(struct(*batch_df.columns)).alias("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

# -------------------------------------
# Write to Silver Kafka topic
# -------------------------------------
query = (
    silver_df
    .writeStream
    .foreachBatch(log_and_write_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

print("✅ Silver Stream Processor Started")
print(f"📥 Reading from: {INPUT_TOPIC}")
print(f"📤 Writing to: {OUTPUT_TOPIC}")
print(f"💾 Checkpoint: {CHECKPOINT_PATH}")

query.awaitTermination()