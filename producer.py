import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

BOOTSTRAP_SERVERS="host.docker.internal:29092"
Topic_NAME= "raw_events"

Producer = KafkaProducer(
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
    amount = round(random.uniform(10,500),2)
    currency = "USD"

    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])
    
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

print("Starting Kafka producer...")

while True:
    key, event = generate_event()

    Producer.send(
        topic=Topic_NAME,
        key=key,
        value=event
    )

    print(f"Produced event | key={key} | valid={event['is_valid']}")

    time.sleep(1)