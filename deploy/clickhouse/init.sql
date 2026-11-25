CREATE DATABASE IF NOT EXISTS analytics;

-- Both tables share the event envelope schema.
--
-- Idempotency is enforced at two levels:
--   1. non_replicated_deduplication_window + the ingestor's per-batch
--      insert_deduplication_token: retrying an identical batch is a no-op.
--   2. ReplacingMergeTree keyed on the event identity: an event redelivered
--      in a *different* batch (e.g. after a crash before offset commit) is
--      collapsed to one row at merge time. Query with FINAL for exact counts.

CREATE TABLE IF NOT EXISTS analytics.page_view_events
(
    event_id        String,
    event_type      LowCardinality(String),
    user_id         String,
    event_time      DateTime64(3, 'UTC'),
    properties      String,
    kafka_topic     LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS analytics.order_events AS analytics.page_view_events
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_id)
SETTINGS non_replicated_deduplication_window = 1000;
