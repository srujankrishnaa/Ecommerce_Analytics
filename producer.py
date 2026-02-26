import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "host.docker.internal:29092"
TOPIC_NAME = "raw_events"

# -----------------------------
# Throughput configuration
# -----------------------------
EVENTS_PER_SECOND = 1000        # target rate
TEST_DURATION_SECONDS = 30      # benchmark window
SLEEP_INTERVAL = 1 / EVENTS_PER_SECOND

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past = now - timedelta(days=6)
    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_event():
    is_invalid = random.random() < 0.25

    customer_id = f"CUST_{random.randint(1,5)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10,500), 2)
    currency = "USD"

    invalid_field = None
    if is_invalid:
        invalid_field = random.choice(
            ["customer_id", "event_type", "amount", "currency"]
        )

    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": None if invalid_field == "customer_id" else customer_id,
        "event_type": (
            random.choice(INVALID_EVENT_TYPES)
            if invalid_field == "event_type"
            else event_type
        ),
        "amount": (
            random.uniform(-500, -10)
            if invalid_field == "amount"
            else amount
        ),
        "currency": None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }

    return event["customer_id"], event


print("🚀 Kafka producer started")
print(f"🎯 Target rate: {EVENTS_PER_SECOND} events/sec")

start_time = time.time()
event_count = 0

while True:
    key, event = generate_event()

    producer.send(
        topic=TOPIC_NAME,
        key=key,
        value=event
    )

    event_count += 1

    # Log every second
    elapsed = time.time() - start_time
    if elapsed >= 1:
        throughput = event_count / elapsed
        print(
            f"📊 Produced {event_count} events in {elapsed:.2f}s "
            f"→ {throughput:.2f} events/sec"
        )
        start_time = time.time()
        event_count = 0

    time.sleep(SLEEP_INTERVAL)