"""
PySpark Structured Streaming Consumer — Reads orders from Kafka/MSK topic
and writes raw JSON data to S3 in micro-batches.

This script can run:
  - As an AWS Glue Streaming Job (paste into Glue Console)
  - Locally with PySpark (for development/testing)

Usage (local):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        stream_to_s3.py \
        --bootstrap-servers localhost:9092 \
        --topic raw-orders \
        --output-path s3a://<bucket>/raw-data/orders/ \
        --checkpoint-path s3a://<bucket>/checkpoints/spark-consumer/

Usage (MSK with IAM):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
        software.amazon.msk:aws-msk-iam-auth:1.1.9 \
        stream_to_s3.py \
        --bootstrap-servers <msk-endpoint>:9098 \
        --topic raw-orders \
        --output-path s3a://<bucket>/raw-data/orders/ \
        --checkpoint-path s3a://<bucket>/checkpoints/spark-consumer/ \
        --use-iam
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


# Schema matching the orders produced by fetch_to_kafka.py
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("order_date", StringType(), False),
    StructField("order_timestamp", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("discount_pct", IntegerType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("region", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("product_rating", DoubleType(), True),
])


def create_spark_session(app_name: str, use_iam: bool) -> SparkSession:
    """Create a Spark session with appropriate configurations."""
    builder = SparkSession.builder.appName(app_name)

    if use_iam:
        # MSK IAM authentication requires these Kafka configs
        builder = builder \
            .config("spark.kafka.sasl.mechanism", "AWS_MSK_IAM") \
            .config("spark.kafka.security.protocol", "SASL_SSL") \
            .config("spark.kafka.sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required;") \
            .config("spark.kafka.sasl.client.callback.handler.class",
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler")

    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="PySpark Kafka consumer — stream orders to S3")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="raw-orders",
                        help="Kafka topic to consume from (default: raw-orders)")
    parser.add_argument("--output-path", type=str, required=True,
                        help="S3 output path for raw data (e.g., s3a://<bucket>/raw-data/orders/)")
    parser.add_argument("--checkpoint-path", type=str, required=True,
                        help="S3 checkpoint path (e.g., s3a://<bucket>/checkpoints/spark-consumer/)")
    parser.add_argument("--trigger-interval", type=str, default="60 seconds",
                        help="Micro-batch trigger interval (default: '60 seconds')")
    parser.add_argument("--use-iam", action="store_true",
                        help="Use IAM authentication for MSK Serverless")
    parser.add_argument("--run-once", action="store_true",
                        help="Process all available data and stop (batch mode for testing)")
    args = parser.parse_args()

    print(f"Starting Spark Streaming Consumer")
    print(f"  Bootstrap servers: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"  Output: {args.output_path}")
    print(f"  Checkpoint: {args.checkpoint_path}")
    print(f"  IAM auth: {args.use_iam}")

    spark = create_spark_session("KafkaOrdersToS3", args.use_iam)

    # ──────────────────────────────────────────────
    # READ: Subscribe to Kafka topic
    # ──────────────────────────────────────────────

    kafka_options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "subscribe": args.topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }

    if args.use_iam:
        kafka_options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "kafka.sasl.client.callback.handler.class":
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        })

    raw_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # ──────────────────────────────────────────────
    # PARSE: Deserialize JSON from Kafka value
    # ──────────────────────────────────────────────

    # Kafka messages have: key (binary), value (binary), topic, partition, offset, timestamp
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_string",
                    "topic", "partition", "offset",
                    "timestamp as kafka_timestamp") \
        .select(
            F.from_json(F.col("json_string"), ORDER_SCHEMA).alias("order"),
            F.col("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        ) \
        .select("order.*", "kafka_timestamp", "kafka_partition", "kafka_offset") \
        .withColumn("ingested_at", F.current_timestamp())

    # ──────────────────────────────────────────────
    # WRITE: Output to S3 as JSON (partitioned by date)
    # ──────────────────────────────────────────────

    if args.run_once:
        # Batch mode: process everything available and stop
        print("Running in batch mode (--run-once)...")
        query = parsed_stream.writeStream \
            .format("json") \
            .option("path", args.output_path) \
            .option("checkpointLocation", args.checkpoint_path) \
            .partitionBy("order_date") \
            .trigger(availableNow=True) \
            .start()
    else:
        # Streaming mode: continuous micro-batches
        print(f"Running in streaming mode (trigger: {args.trigger_interval})...")
        query = parsed_stream.writeStream \
            .format("json") \
            .option("path", args.output_path) \
            .option("checkpointLocation", args.checkpoint_path) \
            .partitionBy("order_date") \
            .trigger(processingTime=args.trigger_interval) \
            .start()

    print("Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
