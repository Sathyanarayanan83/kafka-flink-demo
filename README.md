# Kafka + PyFlink — User Events Pipeline

Two Kafka topics consumed by a PyFlink streaming job.

```
user-events     → session events (login, logout, page_view)
user-activity   → interaction events (click, search, purchase)
```

---

## Prerequisites

- Docker & Docker Compose
- Python 3.8+

---

## Project Structure

```
kafka-flink-demo/
├── docker-compose.yml          # Kafka + Zookeeper + Flink cluster
├── producer/
│   ├── producer.py             # Sends JSON events to both topics
│   └── requirements.txt
└── consumer/
    ├── flink_consumer.py       # PyFlink job: counts events, tracks spend
    └── requirements.txt
```

---

## Quickstart

### 1. Start the stack

```bash
docker compose up -d
```

This starts:
- Zookeeper on port 2181
- Kafka broker on port 9092
- Flink JobManager on port 8081 (UI: http://localhost:8081)
- Flink TaskManager
- A one-shot container that creates the two topics

Wait ~15 seconds for everything to be healthy.

### 2. Verify topics exist

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Expected:
# user-events
# user-activity
```

### 3. Run the producer (local)

```bash
cd producer
pip install -r requirements.txt
python producer.py
```

You'll see output like:
```
[14:02:01] Sent event + activity (total: 2)
  ✓ [user-events] partition=1 offset=0
  ✓ [user-activity] partition=0 offset=0
```

Press Ctrl+C to stop.

### 4. Run the Flink consumer

**Option A — locally (easiest for dev):**

```bash
cd consumer
pip install -r requirements.txt

# Point at localhost instead of the Docker hostname
# Edit flink_consumer.py line: BOOTSTRAP_SERVERS = "localhost:9092"

python flink_consumer.py
```

**Option B — on the Flink cluster:**

```bash
# Copy job into the JobManager container
docker cp consumer/flink_consumer.py flink-jobmanager:/opt/flink/jobs/

# Submit the job
docker exec flink-jobmanager \
  flink run -py /opt/flink/jobs/flink_consumer.py

# Watch logs
docker logs -f flink-taskmanager
```

Visit http://localhost:8081 to see the running job in the Flink UI.

---

## What the Flink job does

| Stream | Processing |
|--------|-----------|
| `user-events` | Parses events → running count per `event_type` |
| `user-activity` | Filters purchases → running total spend per `user_id` |

Sample output:
```
[user-events]    user=user_7       event=page_view   ts=2024-01-15T14:02:01
  → Running count  event_type=page_view   count=3
[user-activity]  user=user_3       PURCHASE  amount=$249.99
  → Total spend    user=user_3       total=$499.98
```

---

## Topic Schemas

### user-events
```json
{
  "event_id": "evt_12345",
  "event_type": "page_view",
  "user_id": "user_7",
  "timestamp": "2024-01-15T14:02:01.123456",
  "session_id": "sess_4321",
  "metadata": {
    "ip": "192.168.1.42",
    "user_agent": "Chrome/120",
    "page": "/dashboard"
  }
}
```

### user-activity
```json
{
  "activity_id": "act_67890",
  "activity_type": "purchase",
  "user_id": "user_3",
  "timestamp": "2024-01-15T14:02:01.456789",
  "details": {
    "product": "laptop",
    "price": 999.99,
    "quantity": 1
  }
}
```

---

## Tear down

```bash
docker compose down -v
```
