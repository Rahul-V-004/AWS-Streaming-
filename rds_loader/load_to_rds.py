"""
RDS Loader — Reads processed Parquet data from S3 and loads it into
AWS RDS PostgreSQL tables.

Called by the Airflow DAG as the final step of the pipeline.
Can also be run standalone for testing.

Usage:
    python load_to_rds.py \
        --bucket <s3-bucket> \
        --rds-host <rds-endpoint> \
        --rds-db ecommerce_analytics \
        --rds-user admin \
        --rds-password <password>

    # Load only a specific table
    python load_to_rds.py --bucket <bucket> --rds-host <host> --table enriched-orders
"""

import argparse
import io
import os
import sys
import time

import boto3
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values


# Mapping from S3 prefix → PostgreSQL table name
TABLE_MAP = {
    "enriched-orders": "enriched_orders",
    "daily-summary": "daily_summary",
    "category-summary": "category_summary",
    "product-summary": "product_summary",
}

# Columns to load per table (order matters — must match the INSERT statement)
TABLE_COLUMNS = {
    "enriched_orders": [
        "order_id", "customer_id", "customer_name", "customer_city",
        "order_date", "order_timestamp", "product_id", "product_name",
        "brand", "category", "unit_price", "quantity", "subtotal",
        "discount_pct", "discount_amount", "total_amount", "payment_method",
        "region", "order_status", "product_rating", "order_year", "order_month",
        "order_day", "day_name", "price_tier", "order_size", "has_discount",
        "profit_estimate", "rating_tier",
    ],
    "daily_summary": [
        "order_date", "category", "region", "total_orders", "total_revenue",
        "avg_order_value", "total_units_sold", "total_discounts",
        "estimated_profit", "unique_customers",
    ],
    "category_summary": [
        "category", "total_orders", "total_revenue", "avg_order_value",
        "avg_discount_pct", "total_units_sold", "unique_customers",
        "unique_products",
    ],
    "product_summary": [
        "product_id", "product_name", "category", "times_ordered",
        "total_revenue", "avg_selling_price", "total_units_sold",
        "brand", "api_rating", "rating_tier",
    ],
}


def get_rds_connection(host: str, database: str, user: str, password: str, port: int = 5432):
    """Create a PostgreSQL connection to RDS."""
    print(f"Connecting to RDS: {host}:{port}/{database}")
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


def list_parquet_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """List all Parquet files under an S3 prefix (handles partitioned data)."""
    parquet_keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                parquet_keys.append(key)
    return parquet_keys


def read_parquet_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Read a single Parquet file from S3 into a pandas DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    parquet_bytes = response["Body"].read()
    table = pq.read_table(io.BytesIO(parquet_bytes))
    return table.to_pandas()


def load_table(
    s3_client,
    conn,
    bucket: str,
    s3_prefix: str,
    pg_table: str,
    columns: list[str],
    truncate: bool = True,
):
    """Load all Parquet files from an S3 prefix into a PostgreSQL table."""
    full_prefix = f"processed-data/{s3_prefix}/"
    print(f"\n{'='*60}")
    print(f"Loading: s3://{bucket}/{full_prefix} → {pg_table}")

    # List Parquet files
    parquet_keys = list_parquet_files(s3_client, bucket, full_prefix)
    if not parquet_keys:
        print(f"  WARNING: No Parquet files found at s3://{bucket}/{full_prefix}")
        return 0

    print(f"  Found {len(parquet_keys)} Parquet file(s)")

    # Read all Parquet files into a single DataFrame
    dfs = []
    for key in parquet_keys:
        df = read_parquet_from_s3(s3_client, bucket, key)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)

    print(f"  Total rows: {len(combined_df)}")

    # Select and reorder columns (only include columns that exist in the DataFrame)
    available_columns = [c for c in columns if c in combined_df.columns]
    missing_columns = [c for c in columns if c not in combined_df.columns]
    if missing_columns:
        print(f"  NOTE: Missing columns (will be NULL): {missing_columns}")

    # Build DataFrame with expected columns, filling missing with None
    load_df = pd.DataFrame()
    for col in columns:
        if col in combined_df.columns:
            load_df[col] = combined_df[col]
        else:
            load_df[col] = None

    # Replace NaN with None for PostgreSQL compatibility
    load_df = load_df.where(pd.notnull(load_df), None)

    # Convert boolean columns
    if "has_discount" in load_df.columns:
        load_df["has_discount"] = load_df["has_discount"].apply(
            lambda x: bool(x) if x is not None else False
        )

    cursor = conn.cursor()

    try:
        # Truncate existing data (idempotent loads)
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {pg_table}")
            print(f"  Truncated {pg_table}")

        # Batch insert using execute_values (much faster than row-by-row)
        col_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {pg_table} ({col_names}) VALUES %s"

        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in load_df.itertuples(index=False, name=None)]

        # Insert in chunks of 1000
        chunk_size = 1000
        total_inserted = 0
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            execute_values(cursor, insert_sql, chunk, page_size=chunk_size)
            total_inserted += len(chunk)
            if total_inserted % 5000 == 0 or total_inserted == len(values):
                print(f"  Inserted {total_inserted}/{len(values)} rows")

        conn.commit()
        print(f"  Successfully loaded {total_inserted} rows into {pg_table}")
        return total_inserted

    except Exception as e:
        conn.rollback()
        print(f"  ERROR loading {pg_table}: {e}")
        raise
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Load processed Parquet from S3 to RDS PostgreSQL")
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--rds-host", type=str,
                        default=os.environ.get("RDS_HOST", ""),
                        help="RDS endpoint hostname")
    parser.add_argument("--rds-port", type=int,
                        default=int(os.environ.get("RDS_PORT", "5432")),
                        help="RDS port (default: 5432)")
    parser.add_argument("--rds-db", type=str,
                        default=os.environ.get("RDS_DATABASE", "ecommerce_analytics"),
                        help="RDS database name")
    parser.add_argument("--rds-user", type=str,
                        default=os.environ.get("RDS_USER", "admin"),
                        help="RDS username")
    parser.add_argument("--rds-password", type=str,
                        default=os.environ.get("RDS_PASSWORD", ""),
                        help="RDS password (prefer env var RDS_PASSWORD)")
    parser.add_argument("--table", type=str, default=None,
                        choices=list(TABLE_MAP.keys()),
                        help="Load only a specific table (default: all)")
    parser.add_argument("--no-truncate", action="store_true",
                        help="Append data instead of truncating first")
    args = parser.parse_args()

    if not args.rds_host:
        parser.error("--rds-host is required (or set RDS_HOST env var)")
    if not args.rds_password:
        parser.error("--rds-password is required (or set RDS_PASSWORD env var)")

    s3_client = boto3.client("s3")
    conn = get_rds_connection(args.rds_host, args.rds_db, args.rds_user, args.rds_password, args.rds_port)

    # Determine which tables to load
    if args.table:
        tables_to_load = {args.table: TABLE_MAP[args.table]}
    else:
        tables_to_load = TABLE_MAP

    total_rows = 0
    start_time = time.time()

    try:
        for s3_prefix, pg_table in tables_to_load.items():
            rows = load_table(
                s3_client, conn, args.bucket, s3_prefix, pg_table,
                TABLE_COLUMNS[pg_table], truncate=not args.no_truncate,
            )
            total_rows += rows
    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"LOAD COMPLETE: {total_rows} total rows loaded in {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
