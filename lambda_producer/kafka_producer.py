"""
AWS Lambda Kafka Producer — Fetches real product/user data from the DummyJSON API,
generates realistic e-commerce orders, and publishes them to an MSK Serverless topic.

Triggered by Amazon EventBridge on a schedule (e.g., every 5 minutes).

Environment Variables:
    MSK_BOOTSTRAP_SERVERS  — MSK Serverless bootstrap endpoint (e.g., boot-xxx...kafka-serverless.us-east-1.amazonaws.com:9098)
    KAFKA_TOPIC            — Kafka topic name (default: raw-orders)
    NUM_ORDERS             — Number of orders to generate per invocation (default: 500)
    ORDER_DAYS_SPAN        — Span of days for order dates (default: 30)
    AWS_REGION             — AWS region (auto-set by Lambda)

IAM Auth:
    Uses the Lambda execution role for MSK IAM authentication (SASL/OAUTHBEARER).
    The execution role must have `kafka-cluster:*` permissions on the MSK cluster.

Deployment:
    This function requires a Lambda Layer with `kafka-python` and `aws-msk-iam-sasl-signer-python`.
    See README.md for layer build instructions.
"""

import json
import os
import random
import time
import uuid
import logging
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ──────────────────────────────────────────────
# Configuration from environment variables
# ──────────────────────────────────────────────

MSK_BOOTSTRAP_SERVERS = os.environ.get("MSK_BOOTSTRAP_SERVERS", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-orders")
NUM_ORDERS = int(os.environ.get("NUM_ORDERS", "500"))
ORDER_DAYS_SPAN = int(os.environ.get("ORDER_DAYS_SPAN", "30"))
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Seed for reproducibility within a single invocation
random.seed(int(time.time()))

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


# ──────────────────────────────────────────────
# Data fetching from DummyJSON API
# ──────────────────────────────────────────────

def fetch_products() -> list[dict]:
    """Fetch products from the DummyJSON API."""
    url = f"{DUMMYJSON_API}/products?limit=50"
    logger.info(f"Fetching products: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Streaming-Pipeline-Lambda/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    products = data.get("products", [])
    logger.info(f"Fetched {len(products)} products")
    return products


def fetch_users() -> list[dict]:
    """Fetch users from the DummyJSON API for realistic customer data."""
    url = f"{DUMMYJSON_API}/users?limit=30&select=id,firstName,lastName,address"
    logger.info(f"Fetching users: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Streaming-Pipeline-Lambda/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    users = data.get("users", [])
    logger.info(f"Fetched {len(users)} users")
    return users


# ──────────────────────────────────────────────
# Order generation
# ──────────────────────────────────────────────

def generate_order(products, customer_ids, customer_names, customer_cities, days) -> dict:
    """Generate a single realistic e-commerce order."""
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)

    product = random.choice(products)
    unit_price = round(product["price"] * random.uniform(0.85, 1.15), 2)
    quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
    subtotal = round(unit_price * quantity, 2)

    discount_pct = random.choice([0, 0, 0, 0, 0, 0, 0, 5, 10, 15, 20, 25])
    discount_amount = round(subtotal * discount_pct / 100, 2)
    total = round(subtotal - discount_amount, 2)

    category = product["category"]
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
        "product_name": product["title"],
        "brand": product.get("brand", "Unknown"),
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
        "product_rating": product.get("rating", 0),
    }


# ──────────────────────────────────────────────
# Kafka producer with MSK IAM auth
# ──────────────────────────────────────────────

def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer with MSK Serverless IAM authentication."""

    def msk_token_provider(config):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

    return KafkaProducer(
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


# ──────────────────────────────────────────────
# Lambda handler
# ──────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Main Lambda entry point.
    Triggered by EventBridge schedule or manual invocation.

    The event can optionally override:
      - "num_orders": number of orders to generate
      - "topic": Kafka topic name
    """
    num_orders = event.get("num_orders", NUM_ORDERS)
    topic = event.get("topic", KAFKA_TOPIC)

    logger.info(f"Starting Kafka producer: {num_orders} orders → topic '{topic}'")
    logger.info(f"MSK endpoint: {MSK_BOOTSTRAP_SERVERS}")

    if not MSK_BOOTSTRAP_SERVERS:
        raise ValueError("MSK_BOOTSTRAP_SERVERS environment variable is not set")

    # 1. Fetch real data from DummyJSON API
    products = fetch_products()
    users = fetch_users()

    customer_ids = [f"CUST-{u['id']:04d}" for u in users]
    customer_names = {f"CUST-{u['id']:04d}": f"{u['firstName']} {u['lastName']}" for u in users}
    customer_cities = {
        f"CUST-{u['id']:04d}": u.get("address", {}).get("city", "Unknown") for u in users
    }

    # 2. Create Kafka producer
    producer = create_producer(MSK_BOOTSTRAP_SERVERS)

    # 3. Generate and send orders
    sent = 0
    errors = 0
    start_time = time.time()

    for i in range(num_orders):
        order = generate_order(products, customer_ids, customer_names, customer_cities, ORDER_DAYS_SPAN)
        try:
            producer.send(topic, key=order["customer_id"], value=order)
            sent += 1
        except Exception as e:
            errors += 1
            logger.error(f"Error sending order {i}: {e}")

        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{num_orders} orders sent")

    # 4. Flush and close
    producer.flush()
    producer.close()

    elapsed = time.time() - start_time

    result = {
        "orders_sent": sent,
        "errors": errors,
        "topic": topic,
        "elapsed_seconds": round(elapsed, 1),
    }

    logger.info(f"Producer complete: {json.dumps(result)}")

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
