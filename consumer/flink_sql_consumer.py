"""
Flink SQL Consumer — same pipeline as flink_consumer.py, but written using
Flink's Table API / SQL instead of the DataStream API.

Reads JSON events from:
  - user-events    → counts events per type
  - user-activity   → filters purchases, computes total spend per user

Writes aggregated results as JSON to:
  - aggregated-results

Run on the Flink cluster:
  docker cp consumer/flink_sql_consumer.py flink-jobmanager:/opt/flink/jobs/
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_sql_consumer.py

Note: this is a STATEMENT SET job — it runs both aggregation queries
concurrently as part of the same Flink job, sharing one execution graph.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP_SERVERS = "kafka:29092"   # inside Docker; use localhost:9092 locally


def main():
    # ── Environment setup ────────────────────────────────────────────────────
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("parallelism.default", "2")

    # ── Source table: user-events ────────────────────────────────────────────
    # Raw JSON looks like:
    # {"event_id": "...", "event_type": "login", "user_id": "user_7",
    #  "timestamp": "...", "session_id": "...", "metadata": {...}}
    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            event_id     STRING,
            event_type   STRING,
            user_id      STRING,
            `timestamp`  STRING,
            session_id   STRING,
            proc_time    AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-sql-user-events-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ── Source table: user-activity ──────────────────────────────────────────
    # Raw JSON looks like:
    # {"activity_id": "...", "activity_type": "purchase", "user_id": "user_3",
    #  "timestamp": "...", "details": {"product": "...", "price": 999.99, "quantity": 1}}
    t_env.execute_sql(f"""
        CREATE TABLE user_activity (
            activity_id    STRING,
            activity_type  STRING,
            user_id        STRING,
            `timestamp`    STRING,
            details        ROW<product STRING, price DOUBLE, quantity INT>,
            proc_time      AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-activity',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-sql-user-activity-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ── Sink table: aggregated-results ───────────────────────────────────────
    # Both queries below write into this single results topic, each emitting
    # the same shape: (metric, key, value, updated_at). One column is kept
    # empty depending on which query produced the row.
    t_env.execute_sql(f"""
        CREATE TABLE aggregated_results (
            metric       STRING,
            metric_key   STRING,
            metric_value DOUBLE,
            updated_at   TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'aggregated-results',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # ── Query 1: event count per type, per 10-second tumbling window ─────────
    # TUMBLE window produces append-only output (one final row per group per
    # window when it closes) — compatible with the Kafka append-only sink.
    event_counts_insert = """
        INSERT INTO aggregated_results
        SELECT
            'event_type_count'       AS metric,
            event_type               AS metric_key,
            CAST(COUNT(*) AS DOUBLE) AS metric_value,
            TUMBLE_END(proc_time, INTERVAL '10' SECOND) AS updated_at
        FROM user_events
        GROUP BY
            event_type,
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    """

    # ── Query 2: total spend per user per 10-second tumbling window ──────────
    user_spend_insert = """
        INSERT INTO aggregated_results
        SELECT
            'user_total_spend' AS metric,
            user_id            AS metric_key,
            SUM(details.price * details.quantity) AS metric_value,
            TUMBLE_END(proc_time, INTERVAL '10' SECOND) AS updated_at
        FROM user_activity
        WHERE activity_type = 'purchase'
        GROUP BY
            user_id,
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    """

    # ── Run both queries as one statement set (single Flink job) ────────────
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(event_counts_insert)
    statement_set.add_insert_sql(user_spend_insert)

    print("=" * 60)
    print("Flink SQL job starting...")
    print("  Consuming 'user-events'    → event type counts")
    print("  Consuming 'user-activity'  → purchase spend per user")
    print("  Writing results to 'aggregated-results'")
    print("=" * 60)

    statement_set.execute().wait()


if __name__ == "__main__":
    main()