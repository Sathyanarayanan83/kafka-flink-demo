"""
PyFlink Consumer — reads JSON events from:
  - user-events    → counts events per type (tumbling window, 10s)
  - user-activity  → filters purchases, computes total spend per user

Run locally:
  pip install apache-flink kafka-python
  python consumer/flink_consumer.py

Run on the Flink cluster:
  docker cp consumer/flink_consumer.py flink-jobmanager:/opt/flink/jobs/
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_consumer.py
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import MapFunction, FilterFunction, ReduceFunction

import json
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "kafka:29092"   # inside Docker; use localhost:9092 locally
GROUP_ID_EVENTS = "flink-user-events-consumer"
GROUP_ID_ACTIVITY = "flink-user-activity-consumer"
TOPIC_EVENTS = "user-events"
TOPIC_ACTIVITY = "user-activity"


# ── Helpers ──────────────────────────────────────────────────────────────────
class ParseUserEvent(MapFunction):
    """Parse raw JSON string → (user_id, event_type, timestamp)."""

    def map(self, value: str):
        try:
            obj = json.loads(value)
            return (
                obj.get("user_id", "unknown"),
                obj.get("event_type", "unknown"),
                obj.get("timestamp", datetime.utcnow().isoformat()),
            )
        except Exception:
            return ("unknown", "parse_error", datetime.utcnow().isoformat())


class ParseUserActivity(MapFunction):
    """Parse raw JSON string → (user_id, activity_type, amount)."""

    def map(self, value: str):
        try:
            obj = json.loads(value)
            amount = 0.0
            if obj.get("activity_type") == "purchase":
                details = obj.get("details", {})
                amount = details.get("price", 0.0) * details.get("quantity", 1)
            return (
                obj.get("user_id", "unknown"),
                obj.get("activity_type", "unknown"),
                amount,
            )
        except Exception:
            return ("unknown", "parse_error", 0.0)


class IsPurchase(FilterFunction):
    """Keep only purchase events."""

    def filter(self, value):
        return value[1] == "purchase"


class PrintEventSink(MapFunction):
    """Log processed event-stream records."""

    def map(self, value):
        user_id, event_type, ts = value
        print(f"[user-events]    user={user_id:12s}  event={event_type:10s}  ts={ts}")
        return value


class PrintPurchaseSink(MapFunction):
    """Log purchase records with spend amount."""

    def map(self, value):
        user_id, _, amount = value
        print(f"[user-activity]  user={user_id:12s}  PURCHASE  amount=${amount:.2f}")
        return value


# ── Main job ─────────────────────────────────────────────────────────────────
def build_kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # ── Stream 1: user-events ────────────────────────────────────────────────
    events_source = build_kafka_source(TOPIC_EVENTS, GROUP_ID_EVENTS)

    events_stream = (
        env.from_source(
            events_source,
            WatermarkStrategy.no_watermarks(),
            "UserEventsSource",
        )
        .map(ParseUserEvent(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))
        .map(PrintEventSink(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))
    )

    # Count events per type (keyed reduce — stateful running count)
    event_counts = (
        events_stream
        .map(lambda t: (t[1], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda t: t[0])
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    )
    event_counts.map(
        lambda t: print(f"  → Running count  event_type={t[0]:10s}  count={t[1]}") or t
    )

    # ── Stream 2: user-activity ──────────────────────────────────────────────
    activity_source = build_kafka_source(TOPIC_ACTIVITY, GROUP_ID_ACTIVITY)

    activity_stream = (
        env.from_source(
            activity_source,
            WatermarkStrategy.no_watermarks(),
            "UserActivitySource",
        )
        .map(ParseUserActivity(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
    )

    # Filter purchases → running spend per user
    purchase_stream = (
        activity_stream
        .filter(IsPurchase())
        .map(PrintPurchaseSink(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
    )

    user_spend = (
        purchase_stream
        .map(lambda t: (t[0], t[2]), output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
        .key_by(lambda t: t[0])
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    )
    user_spend.map(
        lambda t: print(f"  → Total spend    user={t[0]:12s}  total=${t[1]:.2f}") or t
    )

    print("=" * 60)
    print("Flink job starting...")
    print(f"  Consuming '{TOPIC_EVENTS}'    → event type counts")
    print(f"  Consuming '{TOPIC_ACTIVITY}' → purchase spend per user")
    print("=" * 60)

    env.execute("Kafka-Flink User Events Pipeline")


if __name__ == "__main__":
    main()
