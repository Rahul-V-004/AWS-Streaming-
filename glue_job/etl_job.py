"""
AWS Glue ETL Job — E-Commerce Sales Analytics
Reads raw JSON order data from S3 (via Glue Data Catalog), transforms it,
and writes enriched Parquet output back to S3.

Pipeline: S3 (JSON from Kafka) → Glue Crawler (catalog) → Glue ETL Job (this script) → S3 (Parquet)

This script is meant to be copy-pasted into the AWS Glue Console script editor.
It uses PySpark + AWS Glue libraries (both pre-installed in the Glue runtime).
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ──────────────────────────────────────────────
# SETUP: Initialize Glue context and job
# ──────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "DATABASE_NAME",
    "TABLE_NAME",
    "OUTPUT_BUCKET",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DATABASE_NAME = args["DATABASE_NAME"]
TABLE_NAME = args["TABLE_NAME"]
OUTPUT_BUCKET = args["OUTPUT_BUCKET"]

print(f"Starting ETL job: {args['JOB_NAME']}")
print(f"Source: {DATABASE_NAME}.{TABLE_NAME}")
print(f"Output: s3://{OUTPUT_BUCKET}/processed-data/")


# ──────────────────────────────────────────────
# EXTRACT: Read from Glue Data Catalog
# ──────────────────────────────────────────────

print("EXTRACT: Reading data from Glue Data Catalog...")

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABLE_NAME,
    transformation_ctx="datasource",
)

# Convert to Spark DataFrame for easier transformations
df = dynamic_frame.toDF()
record_count = df.count()
print(f"EXTRACT: Read {record_count} records")
df.printSchema()


# ──────────────────────────────────────────────
# TRANSFORM: Clean, enrich, aggregate
# ──────────────────────────────────────────────

print("TRANSFORM: Applying transformations...")

# --- Step 1: Cast types and clean data ---
df_clean = df \
    .withColumn("product_id", F.col("product_id").cast(IntegerType())) \
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType())) \
    .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
    .withColumn("subtotal", F.col("subtotal").cast(DoubleType())) \
    .withColumn("discount_pct", F.col("discount_pct").cast(IntegerType())) \
    .withColumn("discount_amount", F.col("discount_amount").cast(DoubleType())) \
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType())) \
    .withColumn("product_rating", F.col("product_rating").cast(DoubleType())) \
    .filter(F.col("order_id").isNotNull()) \
    .filter(F.col("order_date").isNotNull())

# Drop Kafka metadata columns if present (from streaming consumer)
kafka_cols = ["kafka_timestamp", "kafka_partition", "kafka_offset", "ingested_at"]
for col_name in kafka_cols:
    if col_name in df_clean.columns:
        df_clean = df_clean.drop(col_name)

# --- Step 2: Add derived columns ---
df_enriched = df_clean \
    .withColumn("order_date_parsed", F.to_date(F.col("order_date"), "yyyy-MM-dd")) \
    .withColumn("order_year", F.year("order_date_parsed")) \
    .withColumn("order_month", F.month("order_date_parsed")) \
    .withColumn("order_day", F.dayofmonth("order_date_parsed")) \
    .withColumn("day_of_week", F.dayofweek("order_date_parsed")) \
    .withColumn("day_name", F.date_format("order_date_parsed", "EEEE")) \
    .withColumn(
        "price_tier",
        F.when(F.col("unit_price") < 20, "Budget")
         .when(F.col("unit_price") < 50, "Mid-Range")
         .when(F.col("unit_price") < 100, "Premium")
         .otherwise("Luxury")
    ) \
    .withColumn(
        "order_size",
        F.when(F.col("quantity") == 1, "Single")
         .when(F.col("quantity") <= 3, "Small")
         .otherwise("Bulk")
    ) \
    .withColumn("has_discount", F.when(F.col("discount_pct") > 0, True).otherwise(False)) \
    .withColumn("profit_estimate", F.round(F.col("total_amount") * 0.30, 2)) \
    .withColumn(
        "rating_tier",
        F.when(F.col("product_rating") >= 4.5, "Top Rated")
         .when(F.col("product_rating") >= 3.5, "Well Rated")
         .when(F.col("product_rating") >= 2.5, "Average")
         .otherwise("Below Average")
    ) \
    .drop("order_date_parsed", "day_of_week")

# --- Step 3: Print transform summary ---
total_after = df_enriched.count()
completed = df_enriched.filter(F.col("order_status") == "completed").count()
returned = df_enriched.filter(F.col("order_status") == "returned").count()
cancelled = df_enriched.filter(F.col("order_status") == "cancelled").count()

print(f"TRANSFORM: {total_after} records after cleaning")
print(f"  Completed: {completed} | Returned: {returned} | Cancelled: {cancelled}")


# ──────────────────────────────────────────────
# LOAD: Write enriched data to S3 as Parquet
# ──────────────────────────────────────────────

print("LOAD: Writing enriched order data to S3...")

# --- Output 1: Enriched orders (partitioned by category and order_status) ---
enriched_output = f"s3://{OUTPUT_BUCKET}/processed-data/enriched-orders/"

df_enriched.write \
    .mode("overwrite") \
    .partitionBy("category", "order_status") \
    .parquet(enriched_output)

print(f"LOAD: Wrote enriched orders to {enriched_output}")


# --- Output 2: Daily sales summary (aggregation) ---
print("TRANSFORM: Building daily sales summary...")

df_daily = df_enriched \
    .filter(F.col("order_status") == "completed") \
    .groupBy("order_date", "category", "region") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("discount_amount").alias("total_discounts"),
        F.sum("profit_estimate").alias("estimated_profit"),
        F.countDistinct("customer_id").alias("unique_customers"),
    ) \
    .withColumn("total_revenue", F.round("total_revenue", 2)) \
    .withColumn("avg_order_value", F.round("avg_order_value", 2)) \
    .withColumn("total_discounts", F.round("total_discounts", 2)) \
    .withColumn("estimated_profit", F.round("estimated_profit", 2)) \
    .orderBy("order_date", "category")

daily_output = f"s3://{OUTPUT_BUCKET}/processed-data/daily-summary/"

df_daily.write \
    .mode("overwrite") \
    .partitionBy("category") \
    .parquet(daily_output)

print(f"LOAD: Wrote daily summary to {daily_output}")


# --- Output 3: Category performance summary ---
print("TRANSFORM: Building category performance summary...")

df_category = df_enriched \
    .filter(F.col("order_status") == "completed") \
    .groupBy("category") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_name").alias("unique_products"),
    ) \
    .withColumn("total_revenue", F.round("total_revenue", 2)) \
    .withColumn("avg_order_value", F.round("avg_order_value", 2)) \
    .withColumn("avg_discount_pct", F.round("avg_discount_pct", 2)) \
    .orderBy(F.desc("total_revenue"))

category_output = f"s3://{OUTPUT_BUCKET}/processed-data/category-summary/"

df_category.write \
    .mode("overwrite") \
    .parquet(category_output)

print(f"LOAD: Wrote category summary to {category_output}")


# --- Output 4: Product performance summary ---
print("TRANSFORM: Building product performance summary...")

df_product = df_enriched \
    .filter(F.col("order_status") == "completed") \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        F.count("order_id").alias("times_ordered"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.sum("quantity").alias("total_units_sold"),
        F.first("brand").alias("brand"),
        F.first("product_rating").alias("api_rating"),
        F.first("rating_tier").alias("rating_tier"),
    ) \
    .withColumn("total_revenue", F.round("total_revenue", 2)) \
    .withColumn("avg_selling_price", F.round("avg_selling_price", 2)) \
    .orderBy(F.desc("total_revenue"))

product_output = f"s3://{OUTPUT_BUCKET}/processed-data/product-summary/"

df_product.write \
    .mode("overwrite") \
    .parquet(product_output)

print(f"LOAD: Wrote product summary to {product_output}")

# Print final summary
print("\n" + "=" * 60)
print("ETL JOB COMPLETE")
print("=" * 60)
print(f"Input records:          {record_count}")
print(f"Enriched records:       {total_after}")
print(f"Daily summary rows:     {df_daily.count()}")
print(f"Category summary rows:  {df_category.count()}")
print(f"Product summary rows:   {df_product.count()}")
print(f"Output location:        s3://{OUTPUT_BUCKET}/processed-data/")
print("=" * 60)

job.commit()
print("Job committed successfully.")
