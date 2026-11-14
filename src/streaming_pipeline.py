"""Spark Structured Streaming pipeline for Wikimedia change events.

Reads the ``wiki-changes`` Kafka topic, parses the JSON payload, applies a
10-minute watermark on the event timestamp to tolerate late data, and writes
the enriched events to a Parquet output with checkpointing.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-changes")
OUTPUT_PATH = os.getenv("STREAM_OUTPUT", "data/streaming/output/wiki_changes")
CHECKPOINT_PATH = os.getenv("STREAM_CHECKPOINT", "data/streaming/checkpoints/wiki_changes")
WATERMARK_DELAY = os.getenv("STREAM_WATERMARK", "10 minutes")

SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("comment", StringType()),
    StructField("wiki", StringType()),
    StructField("_ingested_at", StringType()),
])


def build_spark():
    return (
        SparkSession.builder.appName("WikiStreamingPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def parse_events(df):
    """Parse the raw Kafka payload into a structured DataFrame.

    Events whose JSON payload cannot be parsed are silently dropped
    by filtering on a required field (``title``).
    """
    return (
        df.select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("title").isNotNull())
    )


def with_event_time(df):
    """Convert the Unix timestamp into a Spark timestamp column."""
    return df.withColumn("event_time", to_timestamp(col("timestamp").cast("long")))


def run_pipeline(spark):
    """Build and start the streaming query."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = with_event_time(parse_events(raw))
    watermarked = parsed.withWatermark("event_time", WATERMARK_DELAY)

    query = (
        watermarked.writeStream.format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()