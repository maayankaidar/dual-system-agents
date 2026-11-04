import json
import os
import random
import time
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "system-logs"

# Realistic enterprise error scenarios per service
SCENARIOS = {
    "auth-service": {
        "INFO": [
            "User session authenticated successfully for user_id={user_id}",
            "OAuth token refreshed for client_id={client_id}",
            "MFA challenge validated successfully",
        ],
        "WARN": [
            "Failed login attempt: invalid password for user_id={user_id}",
            "Expired JWT token presented by client {client_ip}",
            "Rate limit reached for IP {client_ip} on /login endpoint",
        ],
        "ERROR": [
            "Redis session store connection refused (127.0.0.1:6379)",
            "PublicKey rotation failed: could not fetch JWKS keys from IAM",
            "Database pool exhausted while querying user credentials",
        ],
    },
    "payment-service": {
        "INFO": [
            "Payment authorization succeeded for order_id={order_id} amount=${amount}",
            "Webhook dispatched to merchant gateway",
            "Refund processed successfully for tx_id={tx_id}",
        ],
        "WARN": [
            "Payment declined by issuing bank: insufficient funds",
            "Card expiration check failed for user_id={user_id}",
            "Stripe API latency threshold exceeded (>800ms)",
        ],
        "ERROR": [
            "Stripe gateway timeout after 5000ms for tx_id={tx_id}",
            "Idempotency key collision on checkout endpoint",
            "Fraud detection service offline; dropping transaction tx_id={tx_id}",
        ],
    },
    "inventory-service": {
        "INFO": [
            "Stock reserved: item_id={item_id} quantity={qty} for order_id={order_id}",
            "Inventory sync with warehouse DB completed in {latency}ms",
            "Catalogue cache refreshed",
        ],
        "WARN": [
            "Item item_id={item_id} low on stock (remaining: {qty})",
            "Duplicate stock reservation attempted for order_id={order_id}",
        ],
        "ERROR": [
            "Optimistic lock failure during inventory decrement for item_id={item_id}",
            "PostgreSQL deadlock detected on inventory_items table",
            "Warehouse sync API returned 502 Bad Gateway",
        ],
    },
    "order-service": {
        "INFO": [
            "Order created: order_id={order_id} total=${amount}",
            "Order status updated to SHIPPED for order_id={order_id}",
        ],
        "WARN": [
            "Order order_id={order_id} abandoned during checkout step 2",
            "Promotion code expired for user_id={user_id}",
        ],
        "ERROR": [
            "Kafka publish failed: unable to send OrderCreated event",
            "Circuit breaker OPEN for payment-service dependency",
            "Unhandled NullPointerException in OrderProcessingWorker",
        ],
    },
    "recommendation-engine": {
        "INFO": [
            "Inference completed for user_id={user_id} top_k=5 in {latency}ms",
            "User embedding vector updated in Qdrant collection",
        ],
        "WARN": [
            "Cold start fallback: user_id={user_id} has no purchase history",
            "Vector search latency spike detected (>300ms)",
        ],
        "ERROR": [
            "Qdrant vector DB connection timeout (grpc://qdrant:6334)",
            "Model weights failed to load into GPU memory: CUDA OOM",
            "Feature store lookup returned empty payload for user_id={user_id}",
        ],
    },
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Starting realistic log generator -> Kafka topic '{TOPIC}' at {KAFKA_BROKER}...")

def make_log_entry(force_level=None):
    service = random.choice(list(SCENARIOS.keys()))
    level = force_level or random.choices(["INFO", "WARN", "ERROR"], weights=[0.75, 0.15, 0.10])[0]
    
    if level == "INFO":
        status_code = random.choice([200, 200, 201])
        latency_ms = random.randint(15, 120)
    elif level == "WARN":
        status_code = random.choice([400, 401, 403, 404, 429])
        latency_ms = random.randint(100, 450)
    else:  # ERROR
        status_code = random.choice([500, 502, 503, 504])
        latency_ms = random.randint(1200, 4500)

    template = random.choice(SCENARIOS[service][level])
    message = template.format(
        user_id=random.randint(1000, 9999),
        order_id=f"ORD-{random.randint(10000, 99999)}",
        client_id=f"client_{random.randint(10, 99)}",
        client_ip=fake.ipv4(),
        item_id=f"ITEM-{random.randint(100, 500)}",
        qty=random.randint(1, 5),
        amount=random.randint(10, 850),
        tx_id=f"tx_{fake.hexify(text='^^^^^^^^')}",
        latency=latency_ms,
    )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "level": level,
        "status_code": status_code,
        "response_time_ms": latency_ms,
        "client_ip": fake.ipv4(),
        "message": message,
    }

# Seed an initial batch of 50 logs on startup so bronze-worker flushes immediately
print("Seeding initial batch of 50 telemetry events...", flush=True)
for i in range(50):
    seed_level = "ERROR" if i in (10, 25, 40) else random.choices(["INFO", "WARN"], weights=[0.85, 0.15])[0]
    entry = make_log_entry(force_level=seed_level)
    producer.send(TOPIC, value=entry)
producer.flush()
print("Initial seed batch of 50 events flushed to Kafka.", flush=True)

while True:
    log_entry = make_log_entry()
    producer.send(TOPIC, value=log_entry)
    print(f"Sent: [{log_entry['level']}] {log_entry['service']} - {log_entry['status_code']} ({log_entry['response_time_ms']}ms) -> {log_entry['message']}", flush=True)
    
    time.sleep(5)