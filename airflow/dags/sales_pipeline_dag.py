"""
Airflow DAG — E-Commerce Sales Pipeline Orchestration

This DAG orchestrates the full pipeline:
  1. Run Glue Crawler (catalog raw data from S3)
  2. Run Glue ETL Job (transform raw → Parquet)
  3. Run output Crawlers (catalog Parquet data)
  4. Load processed data to RDS PostgreSQL

Deployed to AWS MWAA (Managed Workflows for Apache Airflow).

Setup:
  - Upload this file to s3://<mwaa-bucket>/dags/
  - Set Airflow Variables in the MWAA console:
      * s3_bucket          — your S3 bucket name
      * glue_database      — Glue Data Catalog database name (e.g., ecommerce_sales_db)
      * glue_table          — Glue table name (e.g., orders)
      * glue_crawler        — raw data crawler name
      * glue_etl_job        — ETL job name
      * glue_role           — IAM role for Glue
      * rds_host            — RDS endpoint
      * rds_database        — RDS database name (e.g., ecommerce_analytics)
      * rds_user            — RDS username
      * rds_password_secret — AWS Secrets Manager secret name for RDS password
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook


# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────

S3_BUCKET = Variable.get("s3_bucket", default_var="ecommerce-streaming-pipeline-demo")
GLUE_DATABASE = Variable.get("glue_database", default_var="ecommerce_sales_db")
GLUE_TABLE = Variable.get("glue_table", default_var="orders")
GLUE_CRAWLER = Variable.get("glue_crawler", default_var="ecommerce-raw-data-crawler")
GLUE_ETL_JOB = Variable.get("glue_etl_job", default_var="ecommerce-sales-etl")
GLUE_ROLE = Variable.get("glue_role", default_var="ecommerce-glue-etl-role")
RDS_HOST = Variable.get("rds_host", default_var="")
RDS_DATABASE = Variable.get("rds_database", default_var="ecommerce_analytics")
RDS_USER = Variable.get("rds_user", default_var="admin")
RDS_PASSWORD_SECRET = Variable.get("rds_password_secret", default_var="ecommerce-rds-password")

# Output crawlers for Parquet data
OUTPUT_CRAWLERS = [
    "ecommerce-enriched-orders-crawler",
    "ecommerce-daily-summary-crawler",
    "ecommerce-category-summary-crawler",
    "ecommerce-product-summary-crawler",
]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ──────────────────────────────────────────────
# Python callable: Load data to RDS
# ──────────────────────────────────────────────

def load_to_rds(**kwargs):
    """Load processed Parquet data from S3 into RDS PostgreSQL."""
    import io
    import time
    import boto3
    import pandas as pd
    import pyarrow.parquet as pq
    import psycopg2
    from psycopg2.extras import execute_values

    # Retrieve RDS password from Secrets Manager
    secrets_hook = SecretsManagerHook()
    rds_password = secrets_hook.get_secret_value(secret_id=RDS_PASSWORD_SECRET)

    # Table mapping: S3 prefix → (pg_table, columns)
    table_config = {
        "enriched-orders": {
            "pg_table": "enriched_orders",
            "columns": [
                "order_id", "customer_id", "customer_name", "customer_city",
                "order_date", "order_timestamp", "product_id", "product_name",
                "brand", "category", "unit_price", "quantity", "subtotal",
                "discount_pct", "discount_amount", "total_amount", "payment_method",
                "region", "order_status", "product_rating", "order_year", "order_month",
                "order_day", "day_name", "price_tier", "order_size", "has_discount",
                "profit_estimate", "rating_tier",
            ],
        },
        "daily-summary": {
            "pg_table": "daily_summary",
            "columns": [
                "order_date", "category", "region", "total_orders", "total_revenue",
                "avg_order_value", "total_units_sold", "total_discounts",
                "estimated_profit", "unique_customers",
            ],
        },
        "category-summary": {
            "pg_table": "category_summary",
            "columns": [
                "category", "total_orders", "total_revenue", "avg_order_value",
                "avg_discount_pct", "total_units_sold", "unique_customers",
                "unique_products",
            ],
        },
        "product-summary": {
            "pg_table": "product_summary",
            "columns": [
                "product_id", "product_name", "category", "times_ordered",
                "total_revenue", "avg_selling_price", "total_units_sold",
                "brand", "api_rating", "rating_tier",
            ],
        },
    }

    s3_client = boto3.client("s3")
    conn = psycopg2.connect(
        host=RDS_HOST, port=5432, database=RDS_DATABASE,
        user=RDS_USER, password=rds_password, connect_timeout=10,
    )
    conn.autocommit = False

    total_rows = 0
    start_time = time.time()

    try:
        for s3_prefix, config in table_config.items():
            pg_table = config["pg_table"]
            columns = config["columns"]
            full_prefix = f"processed-data/{s3_prefix}/"

            print(f"Loading: s3://{S3_BUCKET}/{full_prefix} → {pg_table}")

            # List Parquet files
            parquet_keys = []
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        parquet_keys.append(obj["Key"])

            if not parquet_keys:
                print(f"  WARNING: No Parquet files found at {full_prefix}")
                continue

            # Read all Parquet files
            dfs = []
            for key in parquet_keys:
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                table = pq.read_table(io.BytesIO(response["Body"].read()))
                dfs.append(table.to_pandas())
            combined_df = pd.concat(dfs, ignore_index=True)

            # Build load DataFrame with expected columns
            load_df = pd.DataFrame()
            for col in columns:
                load_df[col] = combined_df[col] if col in combined_df.columns else None
            load_df = load_df.where(pd.notnull(load_df), None)

            if "has_discount" in load_df.columns:
                load_df["has_discount"] = load_df["has_discount"].apply(
                    lambda x: bool(x) if x is not None else False
                )

            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {pg_table}")

            col_names = ", ".join(columns)
            insert_sql = f"INSERT INTO {pg_table} ({col_names}) VALUES %s"
            values = [tuple(row) for row in load_df.itertuples(index=False, name=None)]

            chunk_size = 1000
            inserted = 0
            for i in range(0, len(values), chunk_size):
                execute_values(cursor, insert_sql, values[i:i + chunk_size], page_size=chunk_size)
                inserted += len(values[i:i + chunk_size])

            conn.commit()
            cursor.close()
            total_rows += inserted
            print(f"  Loaded {inserted} rows into {pg_table}")

    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"LOAD COMPLETE: {total_rows} total rows in {elapsed:.1f}s")
    return {"total_rows": total_rows, "elapsed_seconds": round(elapsed, 1)}


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_sales_pipeline",
    default_args=default_args,
    description="End-to-end e-commerce sales ETL: Glue Crawler → Glue ETL → RDS Load",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce", "glue", "rds"],
) as dag:

    # ── Step 1: Run the raw data Glue Crawler ──
    run_raw_crawler = GlueCrawlerOperator(
        task_id="run_raw_data_crawler",
        config={"Name": GLUE_CRAWLER},
        wait_for_completion=True,
        poll_interval=30,
    )

    # ── Step 2: Run the Glue ETL Job ──
    run_etl_job = GlueJobOperator(
        task_id="run_glue_etl_job",
        job_name=GLUE_ETL_JOB,
        script_args={
            "--DATABASE_NAME": GLUE_DATABASE,
            "--TABLE_NAME": GLUE_TABLE,
            "--OUTPUT_BUCKET": S3_BUCKET,
        },
        wait_for_completion=True,
        poll_interval=30,
        num_of_dpus=2,
    )

    # ── Step 3: Run output Glue Crawlers (in parallel) ──
    output_crawler_tasks = []
    for crawler_name in OUTPUT_CRAWLERS:
        task = GlueCrawlerOperator(
            task_id=f"run_{crawler_name.replace('-', '_')}",
            config={"Name": crawler_name},
            wait_for_completion=True,
            poll_interval=30,
        )
        output_crawler_tasks.append(task)

    # ── Step 4: Load to RDS ──
    load_rds = PythonOperator(
        task_id="load_to_rds",
        python_callable=load_to_rds,
        provide_context=True,
    )

    # ── DAG Dependencies ──
    # Crawler → ETL → Output Crawlers (parallel) → Load RDS
    run_raw_crawler >> run_etl_job >> output_crawler_tasks >> load_rds
