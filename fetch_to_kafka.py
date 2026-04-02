"""
Local Testing Utility — Kafka Producer

For local development/testing ONLY. The primary producer runs as an AWS Lambda
function (see lambda_producer/kafka_producer.py).

This script lets you test Kafka connectivity or preview generated order data
without deploying to Lambda.

Data Source: https://dummyjson.com — free, open, no authentication needed.

Usage:
    # Preview generated orders (no Kafka needed)
    python fetch_to_kafka.py --dry-run --orders 10

    # Test against MSK Serverless (IAM auth)
    python fetch_to_kafka.py --bootstrap-servers <msk-endpoint>:9098 --topic raw-orders --orders 100 --use-iam

    # Test against local Kafka (no auth)
    python fetch_to_kafka.py --bootstrap-servers localhost:9092 --topic raw-orders --orders 100
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# Seed for reproducibility
random.seed(42)

DUMMYJSON_API = "https://dummyjson.com"

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card", "buy_now_pay_later"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "returned", "cancelled"]

CATEGORY_RETURN_RATES = {
    "smartphones": 0.08, "laptops": 0.06, "fragrances": 0.12, "skincare": 0.10,
    "groceries": 0.02, "home-decoration": 0.07, "furniture": 0.09, "tops": 0.14,
    "womens-dresses": 0.15, "womens-shoes": 0.13, "mens-shirts": 0.12,
    "mens-shoes": 0.11, "mens-watches": 0.06, "womens-watches": 0.06,
    "womens-bags": 0.08, "womens-jewellery": 0.07, "sunglasses": 0.05,
    "automotive": 0.04, "motorcycle": 0.03, "lighting": 0.06, "beauty": 0.10,
}


def fetch_products() -> list[dict]:
    """Fetch products from the DummyJSON API."""
    url = f"{DUMMYJSON_API}/products?limit=50"
    print(f"  Fetching: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Streaming-Pipeline-Demo/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    products = data.get("products", [])
    categories = set(p["category"] for p in products)
    print(f"  Fetched {len(products)} products across {len(categories)} categories")
    return products


def fetch_users() -> list[dict]:
    """Fetch users from the DummyJSON API for realistic customer data."""
    url = f"{DUMMYJSON_API}/users?limit=30&select=id,firstName,lastName,address"
    print(f"  Fetching: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Streaming-Pipeline-Demo/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    users = data.get("users", [])
    print(f"  Fetched {len(users)} users")
    return users


def generate_order(products, customer_ids, customer_names, customer_cities, days) -> dict:
    """Generate a single realistic e-commerce order."""
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)

    product = random.choice(products)
    product_name = product["title"]
    category = product["category"]
    base_price = product["price"]
    rating = product.get("rating", 0)
    brand = product.get("brand", "Unknown")

    unit_price = round(base_price * random.uniform(0.85, 1.15), 2)
    quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
    subtotal = round(unit_price * quantity, 2)

    discount_pct = random.choice([0, 0, 0, 0, 0, 0, 0, 5, 10, 15, 20, 25])
    discount_amount = round(subtotal * discount_pct / 100, 2)
    total = round(subtotal - discount_amount, 2)

    return_rate = CATEGORY_RETURN_RATES.get(category, 0.05)
    status = random.choice(ORDER_STATUSES)
    if status == "returned" and random.random() > return_rate * 5:
        status = "completed"

    customer_id = random.choice(customer_ids)
    order_time = start_date + timedelta(
        seconds=random.randint(0, int((now - start_date).total_seconds()))
    )

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "customer_name": customer_names.get(customer_id, "Unknown"),
        "customer_city": customer_cities.get(customer_id, "Unknown"),
        "order_date": order_time.strftime("%Y-%m-%d"),
        "order_timestamp": order_time.isoformat(),
        "product_id": product["id"],
        "product_name": product_name,
        "brand": brand,
        "category": category,
        "unit_price": unit_price,
        "quantity": quantity,
        "subtotal": subtotal,
        "discount_pct": discount_pct,
        "discount_amount": discount_amount,
        "total_amount": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "region": random.choice(REGIONS),
        "order_status": status,
        "product_rating": rating,
    }


def create_msk_producer(bootstrap_servers: str, use_iam: bool) -> "KafkaProducer":
    """Create a Kafka producer configured for MSK Serverless (IAM) or plain Kafka."""
    if KafkaProducer is None:
        raise RuntimeError("kafka-python is required. Install with: pip install kafka-python")

    if use_iam:
        # MSK Serverless requires IAM authentication via SASL/OAUTHBEARER
        # This uses the AWS MSK IAM auth library
        try:
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        except ImportError:
            raise RuntimeError(
                "aws-msk-iam-sasl-signer-python is required for MSK IAM auth.\n"
                "Install with: pip install aws-msk-iam-sasl-signer-python"
            )

        # Region from the bootstrap server endpoint or env
        region = os.environ.get("AWS_REGION", "us-east-1")

        def msk_token_provider(config):
            token, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return token

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=msk_token_provider,
            request_timeout_ms=30000,
            retry_backoff_ms=500,
            acks="all",
        )
    else:
        # Plain Kafka (local Docker, no auth)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
        )

    return producer


def main():
    parser = argparse.ArgumentParser(description="Produce e-commerce orders to Kafka/MSK")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", type=str, default="raw-orders",
                        help="Kafka topic name (default: raw-orders)")
    parser.add_argument("--orders", type=int, default=5000,
                        help="Number of orders to generate (default: 5000)")
    parser.add_argument("--days", type=int, default=30,
                        help="Span of days for order dates (default: 30)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Number of orders to send per batch with progress logging (default: 100)")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Delay in seconds between each order (simulates streaming, default: 0)")
    parser.add_argument("--use-iam", action="store_true",
                        help="Use IAM authentication for MSK Serverless")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print orders to stdout instead of sending to Kafka")
    args = parser.parse_args()

    # Fetch real data
    print("Fetching real product and user data from DummyJSON API...\n")
    products = fetch_products()
    users = fetch_users()

    customer_ids = [f"CUST-{u['id']:04d}" for u in users]
    customer_names = {f"CUST-{u['id']:04d}": f"{u['firstName']} {u['lastName']}" for u in users}
    customer_cities = {
        f"CUST-{u['id']:04d}": u.get("address", {}).get("city", "Unknown") for u in users
    }

    if args.dry_run:
        print(f"\n[DRY RUN] Generating {args.orders} orders...\n")
        for i in range(args.orders):
            order = generate_order(products, customer_ids, customer_names, customer_cities, args.days)
            print(json.dumps(order, indent=2))
            if i >= 4:
                print(f"\n... ({args.orders - 5} more orders omitted)")
                break
        return

    # Create Kafka producer
    print(f"\nConnecting to Kafka: {args.bootstrap_servers} (IAM={args.use_iam})")
    producer = create_msk_producer(args.bootstrap_servers, args.use_iam)
    print(f"Connected. Producing {args.orders} orders to topic '{args.topic}'...\n")

    sent = 0
    errors = 0
    start_time = time.time()

    for i in range(args.orders):
        order = generate_order(products, customer_ids, customer_names, customer_cities, args.days)
        try:
            # Use customer_id as partition key (orders from same customer go to same partition)
            producer.send(args.topic, key=order["customer_id"], value=order)
            sent += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR sending order {i}: {e}")

        # Progress logging
        if (i + 1) % args.batch_size == 0:
            elapsed = time.time() - start_time
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"  Sent {sent}/{args.orders} orders ({rate:.0f} msgs/sec)")

        if args.delay > 0:
            time.sleep(args.delay)

    # Flush remaining messages
    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    print(f"\nDone! Sent {sent} orders to '{args.topic}' in {elapsed:.1f}s")
    if errors:
        print(f"  Errors: {errors}")

    # Summary
    print(f"\nNext steps:")
    print(f"  1. The PySpark consumer will read from '{args.topic}' and write raw data to S3")
    print(f"  2. The Glue ETL job will transform the raw data into Parquet")
    print(f"  3. The Airflow DAG will orchestrate the full pipeline")


if __name__ == "__main__":
    main()
