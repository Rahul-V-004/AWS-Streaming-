# E-Commerce Streaming ETL Pipeline — Kafka (MSK) → Spark → S3 → Glue → RDS → BI

A full end-to-end serverless data engineering pipeline that fetches real product data from the **[DummyJSON API](https://dummyjson.com/)** (free, no API key), streams it through **Amazon MSK (Kafka)**, processes it with **PySpark Structured Streaming**, transforms it with **AWS Glue ETL**, orchestrates everything with **Amazon MWAA (Airflow)**, and loads the final data into **AWS RDS PostgreSQL** for BI/reporting.

## Architecture

```
DummyJSON API (free)                    AWS Cloud
┌──────────┐      ┌─────────────────────────────────────────────────────────────┐
│ Products │      │                                                             │
│ & Users  │      │  ┌─────────────┐   produce   ┌───────────────┐             │
│ (JSON)   │─────▶│  │   Lambda    │────────────▶│ MSK Serverless│             │
│          │      │  │  Producer   │             │ (Kafka Topic) │             │
└──────────┘      │  └──────┬──────┘             │ "raw-orders"  │             │
                  │         ▲                    └───────┬───────┘             │
                  │         │ trigger                    │ consume             │
                  │  ┌──────┴──────┐             ┌──────▼─────────┐            │
                  │  │ EventBridge │             │ PySpark Consumer│            │
                  │  │ (Schedule)  │             │ (Glue Streaming)│            │
                  │  │ every 5 min │             └──────┬─────────┘            │
                  │  └─────────────┘                    │ write JSON           │
                  │                              ┌──────▼─────────┐            │
                  │                              │ S3 /raw-data/  │            │
                  │                              └──────┬─────────┘            │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ Glue Crawler     │           │
                  │                              └──────┬───────────┘           │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ Glue ETL Job     │           │
                  │                              └──────┬───────────┘           │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ S3 /processed/   │           │
                  │                              │ (Parquet)        │           │
                  │                              └──────┬───────────┘           │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ MWAA (Airflow)   │           │
                  │                              │ Orchestration    │           │
                  │                              └──────┬───────────┘           │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ RDS PostgreSQL   │           │
                  │                              └──────┬───────────┘           │
                  │                                     ▼                      │
                  │                              ┌──────────────────┐           │
                  │                              │ BI / Reporting   │           │
                  │                              └──────────────────┘           │
                  └─────────────────────────────────────────────────────────────┘
```

## What the Pipeline Does

### 1. **Produce** (Lambda → DummyJSON API → Kafka)
- **AWS Lambda** function triggered by **EventBridge** on a schedule (e.g., every 5 minutes)
- Fetches **real product data** (50 products) and **real user data** (30 users) from `dummyjson.com`
- Generates realistic e-commerce orders (configurable count, default 500 per invocation)
- Publishes each order as a JSON message to an MSK Serverless Kafka topic (`raw-orders`)
- Uses customer ID as the partition key (same customer → same partition)
- Lambda runs inside the VPC to reach MSK; uses IAM auth (no passwords)

### 2. **Consume and Land** (Kafka → PySpark → S3)
- PySpark Structured Streaming reads from the Kafka topic
- Parses JSON messages, validates schema
- Writes raw data as JSON files to `s3://<bucket>/raw-data/orders/`, partitioned by `order_date`
- Adds Kafka metadata (partition, offset, timestamp) for lineage

### 3. **Catalog** (Glue Crawler → Data Catalog)
- Glue Crawler scans the raw JSON files in S3
- Infers schema and registers the table in the Glue Data Catalog

### 4. **Transform** (Glue ETL Job — PySpark)
- Reads from the Glue Data Catalog
- Casts types, cleans data
- Adds derived columns: `order_year/month/day`, `day_name`, `price_tier`, `order_size`, `rating_tier`, `has_discount`, `profit_estimate`
- Produces **4 Parquet output datasets**: Enriched Orders, Daily Summary, Category Performance, Product Performance

### 5. **Orchestrate** (Airflow / MWAA)
- DAG runs daily: Crawler → ETL Job → Output Crawlers → RDS Load
- Uses Airflow Variables for all config (bucket name, Glue job name, RDS credentials)
- RDS password retrieved from AWS Secrets Manager

### 6. **Load** (S3 Parquet → RDS PostgreSQL)
- Reads Parquet from S3, loads into 4 PostgreSQL tables
- Truncate-and-load pattern (idempotent)
- Batch inserts for performance (1000 rows/batch)

### 7. **Query / BI** (RDS PostgreSQL)
- Connect any BI tool (Metabase, Grafana, Tableau, Power BI) to RDS
- Or query directly with `psql` / pgAdmin / DBeaver

## Data Source

Uses the **[DummyJSON API](https://dummyjson.com/)** — a free REST API:
- **No API key required**
- `/products` — 50 real-looking products across 20+ categories
- `/users` — 30 users with names, addresses, and cities
- Each product has a real name, price, brand, category, rating, and stock info

## Prerequisites

- An **AWS account** (Free Tier eligible — [sign up](https://aws.amazon.com/))
- **Python 3.11+** with dependencies installed on your local machine
- **AWS CLI** installed and configured

## Project Structure

```
aws-streaming-pipeline/
├── lambda_producer/
│   ├── kafka_producer.py            # Lambda function: DummyJSON API → Kafka/MSK
│   └── build_layer.sh               # Script to build Lambda Layer (kafka-python + IAM signer)
├── fetch_to_kafka.py                # Local testing utility (optional, for dev/debug)
├── spark_consumer/
│   └── stream_to_s3.py              # PySpark: Kafka → S3 raw data (JSON)
├── glue_job/
│   └── etl_job.py                   # Glue ETL: S3 raw → S3 processed (Parquet)
├── airflow/
│   └── dags/
│       └── sales_pipeline_dag.py    # MWAA DAG: orchestrates full pipeline
├── rds_loader/
│   └── load_to_rds.py              # S3 Parquet → RDS PostgreSQL
├── sql/
│   └── create_tables.sql           # RDS PostgreSQL schema
├── requirements.txt                 # Python dependencies (local testing)
└── README.md
```

---

# Step-by-Step Setup (AWS Console)

## Step 1: Create an AWS Account

1. Go to https://aws.amazon.com/ and click **"Create an AWS Account"**
2. Follow the sign-up wizard (email, password, payment method)
3. Select the **Free Tier** plan
4. Sign in to the **AWS Management Console** at https://console.aws.amazon.com/

---

## Step 2: Create the S3 Bucket

1. Go to **AWS Console** → search **"S3"** → click **S3**
2. Click **"Create bucket"**
3. Fill in:
   - **Bucket name**: `ecommerce-streaming-pipeline-demo` (must be globally unique — add your name, e.g. `ecommerce-streaming-demo-john`)
   - **AWS Region**: `us-east-1` (recommended — MSK Serverless has best support here)
4. Leave all other settings as default
5. Click **"Create bucket"**
6. Open the bucket and create these folders:
   - `raw-data/` — for raw JSON from Spark consumer
   - `processed-data/` — for Parquet from Glue ETL
   - `checkpoints/` — for Spark streaming checkpoints
   - `athena-results/` — for Athena query results (optional)
   - `mwaa-dags/` — for Airflow DAG files (MWAA reads from here)

---

## Step 3: Create the IAM Roles

We need three IAM roles — one for the Lambda producer, one for Glue, one for MWAA.

### Role 1: Lambda Producer Role

1. Go to **IAM** → **Roles** → **"Create role"**
2. **Trusted entity**: AWS service → **Lambda** → **Next**
3. Add policies:
   - `CloudWatchLogsFullAccess`
4. Click **"Next"**
5. **Role name**: `ecommerce-streaming-lambda-role`
6. Click **"Create role"**
7. After creation, click on the role → **"Add inline policy"** → **JSON** tab
8. Paste this policy (grants Kafka + VPC access):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "kafka-cluster:Connect",
           "kafka-cluster:DescribeTopic",
           "kafka-cluster:CreateTopic",
           "kafka-cluster:WriteData",
           "kafka-cluster:DescribeGroup"
         ],
         "Resource": "*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "ec2:CreateNetworkInterface",
           "ec2:DescribeNetworkInterfaces",
           "ec2:DeleteNetworkInterface",
           "ec2:DescribeSecurityGroups",
           "ec2:DescribeSubnets",
           "ec2:DescribeVpcs"
         ],
         "Resource": "*"
       }
     ]
   }
   ```
9. **Policy name**: `msk-vpc-access` → Click **"Create policy"**

### Role 2: Glue Role

1. Go to **IAM** → **Roles** → **"Create role"**
2. **Trusted entity**: AWS service → **Glue** → **Next**
3. Add policies:
   - `AmazonS3FullAccess`
   - `AWSGlueServiceRole`
   - `CloudWatchLogsFullAccess`
4. **Role name**: `ecommerce-streaming-glue-role`
5. Click **"Create role"**
6. After creation, click on the role → **"Add inline policy"** → **JSON** tab
7. Paste this policy (grants Kafka read access for the Spark consumer):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "kafka-cluster:Connect",
           "kafka-cluster:DescribeTopic",
           "kafka-cluster:ReadData",
           "kafka-cluster:DescribeGroup",
           "kafka-cluster:AlterGroup"
         ],
         "Resource": "*"
       }
     ]
   }
   ```
8. **Policy name**: `msk-read-access` → Click **"Create policy"**

### Role 3: MWAA Execution Role

1. Go to **IAM** → **Roles** → **"Create role"**
2. **Trusted entity**: AWS service → select **EC2** → **Next** (we'll fix the trust policy after creation)
3. Add policies:
   - `AmazonS3FullAccess`
   - `AWSGlueConsoleFullAccess`
   - `AmazonRDSFullAccess`
   - `SecretsManagerReadWrite`
   - `CloudWatchLogsFullAccess`
4. **Role name**: `ecommerce-streaming-mwaa-role`
5. Click **"Create role"**
6. After creation, click on the role → **Trust relationships** → **Edit trust policy**
7. Set the trust policy to:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Service": [
             "airflow.amazonaws.com",
             "airflow-env.amazonaws.com"
           ]
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```
8. Click **"Update policy"**

---

## Step 4: Create the MSK Serverless Cluster

1. Go to **AWS Console** → search **"MSK"** → click **Amazon MSK**
2. Click **"Create cluster"**
3. Select **"Serverless"** (simplest, pay-per-use)
4. Fill in:
   - **Cluster name**: `ecommerce-streaming-cluster`
5. Under **Networking**:
   - Select the **default VPC**
   - Select **at least 2 subnets** in different Availability Zones
6. Under **Security**:
   - Authentication: **IAM role-based authentication** (default)
7. Click **"Create cluster"**
8. Wait until status is **"Active"** (~5-10 minutes)
9. Click on the cluster → **"View client information"**
10. Copy the **Bootstrap servers** endpoint (you'll need this for the producer and consumer)

> The endpoint looks like: `boot-xxxxxxxx.c1.kafka-serverless.us-east-1.amazonaws.com:9098`

### Create the Kafka Topic

MSK Serverless **auto-creates topics** when you first produce to them. No manual topic creation needed. The producer script will create `raw-orders` automatically on first use.

---

## Step 5: Create the RDS PostgreSQL Instance

1. Go to **AWS Console** → search **"RDS"** → click **RDS**
2. Click **"Create database"**
3. Select **"Standard create"**
4. Fill in:
   - **Engine**: PostgreSQL
   - **Engine version**: PostgreSQL 15 (or latest)
   - **Templates**: **Free tier**
   - **DB instance identifier**: `ecommerce-analytics-db`
   - **Master username**: `admin`
   - **Master password**: Choose a password (save it!)
5. **Instance configuration**:
   - **DB instance class**: `db.t3.micro` (Free Tier)
6. **Storage**:
   - **Allocated storage**: 20 GB
   - **Storage autoscaling**: Disable (for Free Tier)
7. **Connectivity**:
   - **VPC**: Default VPC
   - **Public access**: **Yes** (so you can connect from your local machine)
   - **VPC security group**: Create new → name it `ecommerce-rds-sg`
8. **Database authentication**: Password authentication
9. **Additional configuration**:
   - **Initial database name**: `ecommerce_analytics`
10. Click **"Create database"**
11. Wait until status is **"Available"** (~5-10 minutes)

### Configure Security Group

1. Click on the RDS instance → **"Connectivity & security"** tab
2. Click on the **VPC security group** link
3. Click **"Edit inbound rules"**
4. Add rule:
   - **Type**: PostgreSQL
   - **Port**: 5432
   - **Source**: `0.0.0.0/0` (for demo — restrict in production!)
5. Click **"Save rules"**

### Store Password in Secrets Manager

1. Go to **AWS Console** → search **"Secrets Manager"**
2. Click **"Store a new secret"**
3. **Secret type**: Other type of secret
4. **Key/value**: Key = `password`, Value = `<your-rds-password>`
5. **Secret name**: `ecommerce-rds-password`
6. Click through and **"Store"**

### Create the Tables

```bash
# Connect to RDS from your local machine
psql -h <rds-endpoint> -U admin -d ecommerce_analytics -f sql/create_tables.sql

# Or paste the contents of sql/create_tables.sql into pgAdmin/DBeaver
```

The RDS endpoint is on the RDS instance details page under **"Endpoint"**.

---

## Step 6: Create the Glue Database and Crawler

### Create the Database

1. Go to **AWS Glue** → **Databases** → **"Add database"**
2. **Database name**: `ecommerce_sales_db`
3. Click **"Create database"**

### Create the Raw Data Crawler

1. Go to **AWS Glue** → **Crawlers** → **"Create crawler"**
2. **Name**: `ecommerce-raw-data-crawler`
3. **Data source**: S3 → `s3://<your-bucket>/raw-data/orders/`
4. **IAM role**: `ecommerce-streaming-glue-role`
5. **Target database**: `ecommerce_sales_db`
6. Click **"Create crawler"**

> Don't run it yet — we need data first.

---

## Step 7: Create the Glue ETL Job

1. Go to **AWS Glue** → **ETL jobs** → **"Script editor"**
2. Select **Spark** → **"Create script"**
3. Delete all default code
4. Copy-paste the contents of `glue_job/etl_job.py`
5. Click **"Job details"** tab:
   - **Name**: `ecommerce-sales-etl`
   - **IAM Role**: `ecommerce-streaming-glue-role`
   - **Glue version**: Glue 4.0
   - **Worker type**: G.1X
   - **Number of workers**: 2
   - **Job timeout**: 10 minutes
6. Add **Job parameters**:
   - `--DATABASE_NAME` → `ecommerce_sales_db`
   - `--TABLE_NAME` → `orders` (table name the Crawler will create)
   - `--OUTPUT_BUCKET` → `<your-bucket-name>`
7. Click **"Save"**

---

## Step 8: Set Up MWAA (Airflow)

### Upload the DAG

```bash
# Upload the DAG to S3
aws s3 cp airflow/dags/sales_pipeline_dag.py s3://<your-bucket>/mwaa-dags/dags/
```

### Create the MWAA Environment

1. Go to **AWS Console** → search **"MWAA"** → click **Amazon MWAA**
2. Click **"Create environment"**
3. Fill in:
   - **Environment name**: `ecommerce-pipeline-airflow`
   - **Airflow version**: 2.8 (or latest)
4. **DAG code in Amazon S3**:
   - **S3 Bucket**: `s3://<your-bucket>`
   - **DAGs folder**: `s3://<your-bucket>/mwaa-dags/dags/`
5. **Networking**:
   - **VPC**: Default VPC
   - Select **2 private subnets** (MWAA requires private subnets)
   - **Web server access**: Public network
6. **Environment class**: `mw1.small` (smallest)
7. **Execution role**: `ecommerce-streaming-mwaa-role`
8. Click **"Create environment"**
9. Wait until status is **"Available"** (~20-30 minutes)

### Configure Airflow Variables

1. Open the **MWAA Console** → click **"Open Airflow UI"**
2. Go to **Admin** → **Variables**
3. Add these variables:

| Key | Value |
|-----|-------|
| `s3_bucket` | `<your-bucket-name>` |
| `glue_database` | `ecommerce_sales_db` |
| `glue_table` | `orders` |
| `glue_crawler` | `ecommerce-raw-data-crawler` |
| `glue_etl_job` | `ecommerce-sales-etl` |
| `glue_role` | `ecommerce-streaming-glue-role` |
| `rds_host` | `<rds-endpoint>` |
| `rds_database` | `ecommerce_analytics` |
| `rds_user` | `admin` |
| `rds_password_secret` | `ecommerce-rds-password` |

---

## Step 9: Create the Lambda Producer

### 9a. Build the Lambda Layer

The Lambda function needs `kafka-python` and `aws-msk-iam-sasl-signer-python`, which aren't in the Lambda runtime. We package them as a **Lambda Layer**.

On your **local machine**:

```bash
cd lambda_producer
chmod +x build_layer.sh
./build_layer.sh
```

This creates `kafka_layer.zip` (~5 MB).

### 9b. Upload the Lambda Layer

1. Go to **Lambda** → **Layers** (left sidebar) → **"Create layer"**
2. Fill in:
   - **Name**: `kafka-python-layer`
   - **Upload**: Choose `kafka_layer.zip`
   - **Compatible runtimes**: Python 3.11
3. Click **"Create"**
4. Note the **Layer ARN** — you'll need it next

### 9c. Create the Lambda Function

1. Go to **Lambda** → **"Create function"**
2. Select **"Author from scratch"**
3. Fill in:
   - **Function name**: `ecommerce-kafka-producer`
   - **Runtime**: Python 3.11
   - **Architecture**: x86_64
4. Under **Permissions** → **"Use an existing role"**:
   - Choose: `ecommerce-streaming-lambda-role`
5. Click **"Create function"**

### Upload the Code

1. Scroll to the **"Code"** section
2. Delete all default code
3. Open `lambda_producer/kafka_producer.py` from this project
4. **Copy the entire contents** and paste into the Lambda editor
5. **Rename the file** to `kafka_producer.py`
6. Click **"Deploy"**

### Update the Handler

1. Scroll to **"Runtime settings"** → **"Edit"**
2. Change handler to: `kafka_producer.lambda_handler`
3. Click **"Save"**

### Attach the Lambda Layer

1. Scroll to **"Layers"** section → **"Add a layer"**
2. Select **"Custom layers"** → choose `kafka-python-layer` → select the version
3. Click **"Add"**

### Configure Environment Variables

1. Go to **"Configuration"** tab → **"Environment variables"** → **"Edit"**
2. Add:
   - `MSK_BOOTSTRAP_SERVERS` → `<msk-bootstrap-endpoint>:9098`
   - `KAFKA_TOPIC` → `raw-orders`
   - `NUM_ORDERS` → `500`
   - `ORDER_DAYS_SPAN` → `30`
3. Click **"Save"**

### Configure VPC (Required for MSK)

Lambda must be in the **same VPC** as MSK to connect.

1. Go to **"Configuration"** tab → **"VPC"** → **"Edit"**
2. Select the **same VPC** as your MSK cluster (default VPC)
3. Select **at least 2 subnets** (same as MSK)
4. **Security groups**: Select the **default security group** (or create one that allows outbound traffic)
5. Click **"Save"**

> **Note:** Since the Lambda is in a VPC, it needs a **NAT Gateway** to reach the DummyJSON API (internet). If your VPC doesn't have one, see the note at the bottom of this section.

### Increase Timeout and Memory

1. Go to **"Configuration"** → **"General configuration"** → **"Edit"**
2. **Timeout**: 2 minutes (2 min 0 sec)
3. **Memory**: 512 MB
4. Click **"Save"**

### Test the Lambda

1. Click **"Test"** → **"Create new test event"**
2. Event name: `test500`
3. Event JSON: `{"num_orders": 500}`
4. Click **"Save"** → **"Test"**
5. Check the logs — you should see:
   ```
   Fetched 50 products
   Fetched 30 users
   Progress: 100/500 orders sent
   ...
   Producer complete: {"orders_sent": 500, ...}
   ```

### 9d. Create the EventBridge Schedule

This triggers the Lambda automatically on a schedule.

1. Go to **AWS Console** → search **"EventBridge"** → click **EventBridge**
2. Click **"Schedules"** in the left sidebar → **"Create schedule"**
3. Fill in:
   - **Schedule name**: `ecommerce-order-producer`
   - **Schedule type**: **Recurring schedule**
   - **Schedule expression**: `rate(5 minutes)` (or `rate(1 hour)` for less data)
4. Click **"Next"**
5. **Target**: **AWS Lambda** → select `ecommerce-kafka-producer`
6. **Payload**: `{"num_orders": 500}`
7. Click **"Next"** → **"Next"** → **"Create schedule"**

> The Lambda will now run every 5 minutes, producing 500 orders each time to the Kafka topic.

> **VPC + Internet Note:** If your Lambda can't reach the DummyJSON API (timeout errors), your VPC needs a NAT Gateway for outbound internet. Go to **VPC** → **NAT Gateways** → **"Create NAT Gateway"** → select a public subnet → allocate an Elastic IP → update your private subnet route table to route `0.0.0.0/0` through the NAT Gateway. (NAT Gateway costs ~$0.045/hr — delete after demo.)

---

## Step 10: Run the Spark Consumer (Kafka → S3)

You can run this as a **Glue Streaming Job** or locally with `spark-submit`:

**Option A: Glue Streaming Job** (recommended)
1. Go to **AWS Glue** → **ETL jobs** → **"Script editor"**
2. Select **Spark** → **"Create script"**
3. Delete all default code and paste `spark_consumer/stream_to_s3.py`
4. Click **"Job details"** tab:
   - **Name**: `ecommerce-kafka-to-s3`
   - **IAM Role**: `ecommerce-streaming-glue-role`
   - **Type**: **Spark Streaming**
   - **Glue version**: Glue 4.0
   - **Worker type**: G.1X
   - **Number of workers**: 2
5. Add **Job parameters**:
   - `--bootstrap-servers` → `<msk-endpoint>:9098`
   - `--topic` → `raw-orders`
   - `--output-path` → `s3a://<your-bucket>/raw-data/orders/`
   - `--checkpoint-path` → `s3a://<your-bucket>/checkpoints/spark-consumer/`
   - `--use-iam` → `true`
6. Click **"Save"** → **"Run"**

**Option B: Local spark-submit** (for testing)
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
software.amazon.msk:aws-msk-iam-auth:1.1.9 \
    spark_consumer/stream_to_s3.py \
    --bootstrap-servers <msk-endpoint>:9098 \
    --topic raw-orders \
    --output-path s3a://<your-bucket>/raw-data/orders/ \
    --checkpoint-path s3a://<your-bucket>/checkpoints/spark-consumer/ \
    --use-iam \
    --run-once
```

---

## Step 11: Trigger the Airflow DAG

1. Open the **MWAA Airflow UI**
2. Find the DAG `ecommerce_sales_pipeline`
3. Toggle it **ON**
4. Click **"Trigger DAG"** (play button)
5. Monitor the tasks:
   - `run_raw_data_crawler` — catalogs raw JSON data
   - `run_glue_etl_job` — transforms to Parquet
   - `run_ecommerce_*_crawler` (x4) — catalogs Parquet output
   - `load_to_rds` — loads to PostgreSQL

---

## Step 12: Verify the Results

### Check S3 Output

1. Go to **S3** → your bucket → `processed-data/`
2. You should see 4 folders:
   - `enriched-orders/` — partitioned by `category/` and `order_status/`
   - `daily-summary/` — partitioned by `category/`
   - `category-summary/` — Parquet files
   - `product-summary/` — Parquet files

### Check RDS Data

```bash
psql -h <rds-endpoint> -U admin -d ecommerce_analytics
```

```sql
-- Count rows in each table
SELECT 'enriched_orders' as tbl, COUNT(*) FROM enriched_orders
UNION ALL SELECT 'daily_summary', COUNT(*) FROM daily_summary
UNION ALL SELECT 'category_summary', COUNT(*) FROM category_summary
UNION ALL SELECT 'product_summary', COUNT(*) FROM product_summary;

-- Top 5 products by revenue
SELECT product_name, category, price_tier,
       COUNT(*) as orders, ROUND(SUM(total_amount)::numeric, 2) as revenue
FROM enriched_orders
WHERE order_status = 'completed'
GROUP BY product_name, category, price_tier
ORDER BY revenue DESC
LIMIT 5;

-- Revenue by day of week
SELECT day_name, COUNT(*) as orders,
       ROUND(SUM(total_amount)::numeric, 2) as revenue,
       ROUND(AVG(total_amount)::numeric, 2) as avg_order_value
FROM enriched_orders
WHERE order_status = 'completed'
GROUP BY day_name
ORDER BY revenue DESC;

-- Category return rates
SELECT category,
       COUNT(*) as total_orders,
       SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) as returns,
       ROUND(100.0 * SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END)
             / COUNT(*), 1) as return_rate_pct
FROM enriched_orders
GROUP BY category
ORDER BY return_rate_pct DESC;

-- Daily revenue trend
SELECT order_date, SUM(total_orders) as orders,
       ROUND(SUM(total_revenue)::numeric, 2) as revenue
FROM daily_summary
GROUP BY order_date
ORDER BY order_date;

-- Discount impact
SELECT has_discount, COUNT(*) as orders,
       ROUND(AVG(total_amount)::numeric, 2) as avg_order_value,
       ROUND(SUM(total_amount)::numeric, 2) as total_revenue
FROM enriched_orders
WHERE order_status = 'completed'
GROUP BY has_discount;
```

### Check Airflow

1. Open the **MWAA Airflow UI**
2. Click on the DAG → **Graph** view
3. All tasks should be green (success)

---

## Cleanup (After Demo)

To avoid charges, delete everything in this order:

1. **EventBridge Schedule**: Go to EventBridge → Schedules → select `ecommerce-order-producer` → **Delete**
2. **Lambda**: Go to Lambda → select `ecommerce-kafka-producer` → **Actions** → **Delete**
3. **Lambda Layer**: Go to Lambda → Layers → select `kafka-python-layer` → **Delete**
4. **MWAA**: Go to MWAA → select environment → **Delete** (~10 min to delete)
5. **MSK**: Go to MSK → select cluster → **Actions** → **Delete**
6. **Glue ETL Jobs**: Go to Glue → ETL jobs → select both jobs → **Delete**
7. **Glue Crawlers**: Go to Glue → Crawlers → select all → **Delete**
8. **Glue Database**: Go to Glue → Databases → select `ecommerce_sales_db` → **Delete**
9. **RDS**: Go to RDS → select instance → **Actions** → **Delete** (uncheck "Create final snapshot")
10. **Secrets Manager**: Go to Secrets Manager → select secret → **Delete**
11. **S3**: Open bucket → select all → **Delete** → then **Delete bucket**
12. **IAM Roles**: Go to IAM → Roles → delete all 3 roles
13. **Security Groups**: Go to VPC → Security Groups → delete `ecommerce-rds-sg`
14. **NAT Gateway** (if created): Go to VPC → NAT Gateways → delete + release Elastic IP

## Cost Estimate

| Service | Pricing | Est. Demo Cost |
|---------|---------|----------------|
| **Lambda** | 1M free requests/month | $0 (Free Tier) |
| **MSK Serverless** | ~$0.10/hr per cluster-hour + data | ~$0.50 for a few hours |
| **Glue ETL** | $0.44/DPU-hour, 2 DPUs, ~3 min | ~$0.04/run |
| **Glue Streaming** | $0.44/DPU-hour, 2 DPUs | ~$0.15/run |
| **Glue Crawler** | $0.44/DPU-hour, ~1 min | ~$0.01/run |
| **RDS** (db.t3.micro) | Free Tier (12 months) | $0 |
| **MWAA** (mw1.small) | ~$0.49/hr | ~$2-5 if left running a few hours |
| **S3** | Free Tier (5 GB) | $0 |
| **Secrets Manager** | $0.40/secret/month | ~$0.01 |
| **NAT Gateway** (if needed) | ~$0.045/hr + data | ~$0.20 for a few hours |
| **Total** | | **~$3-6 for a full demo session** |

> **Tip:** MWAA is the most expensive component. Create it last, run the demo, and delete it first. You can also test the DAG logic locally with a standalone Airflow Docker container before deploying to MWAA.

## Testing Locally (Without AWS)

You can test the producer logic locally before deploying to Lambda:

```bash
# 1. Preview generated order data (no Kafka, no AWS needed)
python fetch_to_kafka.py --dry-run --orders 10

# 2. Test Kafka connectivity against a running MSK cluster
pip install kafka-python aws-msk-iam-sasl-signer-python
python fetch_to_kafka.py \
    --bootstrap-servers <msk-endpoint>:9098 \
    --topic raw-orders \
    --orders 100 \
    --use-iam

# 3. Test the RDS loader (requires a local PostgreSQL)
#    Start PostgreSQL locally, create the database and tables, then:
python rds_loader/load_to_rds.py \
    --bucket <bucket> \
    --rds-host localhost \
    --rds-db ecommerce_analytics \
    --rds-user postgres \
    --rds-password postgres
```
