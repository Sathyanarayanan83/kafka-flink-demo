"""
Kafka Producer — sends JSON events to:
  - user-events:    login, logout, page_view
  - user-activity:  clicks, searches, purchases
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# ── Config ─────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPICS = {
    "user_events": "user-events",
    "user_activity": "user-activity",
}

# ── Sample data ─────────────────────────────────────────────────────────────
USER_IDS = [f"user_{i}" for i in range(1, 21)]
PAGES = ["/home", "/dashboard", "/profile", "/settings", "/checkout", "/search"]
PRODUCTS = ["laptop", "phone", "headphones", "keyboard", "monitor", "tablet"]
SEARCH_TERMS = ["best deals", "python tutorials", "kafka flink", "streaming data", "AI tools"]

EVENT_TYPES = ["login", "logout", "page_view"]
ACTIVITY_TYPES = ["click", "search", "purchase", "add_to_cart", "remove_from_cart"]


def make_user_event() -> dict:
    """Schema: user-events topic — high-level session events."""
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id": f"evt_{random.randint(10000, 99999)}",
        "event_type": event_type,
        "user_id": random.choice(USER_IDS),
        "timestamp": datetime.utcnow().isoformat(),
        "session_id": f"sess_{random.randint(1000, 9999)}",
        "metadata": {
            "ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
            "user_agent": random.choice(["Chrome/120", "Firefox/121", "Safari/17"]),
            "page": random.choice(PAGES) if event_type == "page_view" else None,
        },
    }


def make_user_activity() -> dict:
    """Schema: user-activity topic — granular interaction events."""
    activity_type = random.choice(ACTIVITY_TYPES)
    payload = {
        "activity_id": f"act_{random.randint(10000, 99999)}",
        "activity_type": activity_type,
        "user_id": random.choice(USER_IDS),
        "timestamp": datetime.utcnow().isoformat(),
    }

    if activity_type == "click":
        payload["details"] = {"element": random.choice(["button", "link", "image"]), "page": random.choice(PAGES)}
    elif activity_type == "search":
        payload["details"] = {"query": random.choice(SEARCH_TERMS), "results_count": random.randint(0, 50)}
    elif activity_type in ("purchase", "add_to_cart", "remove_from_cart"):
        payload["details"] = {
            "product": random.choice(PRODUCTS),
            "price": round(random.uniform(9.99, 999.99), 2),
            "quantity": random.randint(1, 5),
        }

    return payload


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",          # wait for all replicas
        retries=3,
        linger_ms=10,        # batch messages for 10ms
    )


def on_send_success(record_metadata):
    print(
        f"  ✓ [{record_metadata.topic}] "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"  ✗ Error sending message: {excp}")


def run(interval_seconds: float = 1.0, max_messages: int = None):
    producer = create_producer()
    print(f"Producer connected to {BOOTSTRAP_SERVERS}")
    print(f"Publishing to: {list(TOPICS.values())}\n")

    count = 0
    try:
        while True:
            # Send to user-events
            event = make_user_event()
            producer.send(
                TOPICS["user_events"],
                key=event["user_id"],
                value=event,
            ).add_callback(on_send_success).add_errback(on_send_error)

            # Send to user-activity
            activity = make_user_activity()
            producer.send(
                TOPICS["user_activity"],
                key=activity["user_id"],
                value=activity,
            ).add_callback(on_send_success).add_errback(on_send_error)

            count += 2
            print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Sent event + activity (total: {count})")

            if max_messages and count >= max_messages:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Sent {count} messages total.")


if __name__ == "__main__":
    run(interval_seconds=1.0)
