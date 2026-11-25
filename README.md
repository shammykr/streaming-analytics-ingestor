# Streaming Analytics Ingestor

A high-throughput Go service that consumes events from Kafka, validates them against a schema, batches them, and writes them to ClickHouse analytical tables. It is built for reliability: it applies backpressure when ClickHouse falls behind, retries transient failures with exponential backoff, and delivers idempotently, so duplicate events produce the same result. Prometheus metrics and a provisioned Grafana dashboard show what it is doing.

## Architecture

```
                 one pipeline per configured route (topic -> table)
             ┌───────────────────────────────────────────────────────────────┐
  Kafka      │  kafka-go reader      validate          bounded buffer        │
  topic ─────┼─► (consumer group) ─► (schema) ──────► [ chan, buffer_size ] ─┐ │
             │        ▲                 │ invalid: log + metric,            │ │
             │        │                 │ offset still committed            │ │
             │        │                                                     ▼ │
             │  commit offsets ◄─── write OK ◄── ClickHouse insert ◄── batcher│
             │  (after write)            (retry w/ backoff,      (max_size or │
             │                            dedup token)          flush_interval)
             └───────────────────────────────────────────────────────────────┘
                                   │
   lag monitor (committed offset vs high-water mark)      /metrics ──► Prometheus ──► Grafana
```

* **Routing**: each `routes` entry maps a Kafka topic to a ClickHouse table. Every route gets its own reader (all in the same consumer group), buffer, and batcher, so one slow table does not stall the others.
* **Validation** (`internal/event`): every message must be a JSON object with a non-empty `event_id` and `event_type`, an RFC3339 `timestamp` no more than 24h in the future, an optional `user_id`, and an optional `properties` object. Unknown fields are rejected. A rejected record is logged as structured JSON with its topic, partition, offset, key, reason, field, and detail, and it is counted in `ingestor_events_invalid_total{reason=...}`. Its offset is still committed, so a poison message never blocks a partition.
* **Batching** (`internal/pipeline`): records are flushed when `batch.max_size` is reached or every `batch.flush_interval`, whichever comes first.
* **Sink** (`internal/sink`): uses the native ClickHouse protocol with LZ4 compression. Each insert carries a deterministic `insert_deduplication_token`.

### Example event

```json
{
  "event_id": "4f1c2a9e7b3d4c11",
  "event_type": "order_placed",
  "user_id": "user-1234",
  "timestamp": "2024-06-01T12:34:56.789Z",
  "properties": { "order_id": "ord-77", "amount": 42.5, "currency": "USD" }
}
```

## Running it

Requirements: Docker with Compose. Go 1.26+ is needed only to build outside Docker.

```bash
docker compose up -d --build            # Kafka, ClickHouse, Prometheus, Grafana, ingestor

# publish 100k synthetic events (1% malformed, 2% duplicates)
docker compose --profile load run --rm producer -count 100000 -rate 10000

# check the result
docker compose exec clickhouse clickhouse-client --password clickhouse -q \
  "SELECT count() FROM analytics.page_view_events FINAL"
```

| Service    | URL                                                          |
|------------|--------------------------------------------------------------|
| Grafana    | http://localhost:3000 (dashboard: *Streaming Analytics Ingestor*) |
| Prometheus | http://localhost:9090                                        |
| Metrics    | http://localhost:9100/metrics                                |
| ClickHouse | localhost:9000 (native), localhost:8123 (HTTP), user `default` / `clickhouse` |
| Kafka      | localhost:9092                                               |

To run the ingestor on the host against the Compose infrastructure:

```bash
docker compose up -d kafka kafka-init clickhouse prometheus grafana
CLICKHOUSE_PASSWORD=clickhouse go run ./cmd/ingestor -config config.yaml
go run ./cmd/producer -count 50000
```

### Trying the failure paths

```bash
# ClickHouse outage: the buffer fills, fetching stops, lag grows, retries back off.
# When ClickHouse returns, everything drains with nothing lost and nothing duplicated.
docker compose stop clickhouse
docker compose --profile load run --rm producer -count 60000 -rate 20000
docker compose start clickhouse

# Graceful shutdown mid-stream: in-flight batches are written and committed before exit.
docker compose stop ingestor
```

## Configuration

`config.yaml` (the environment variables `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`, `CLICKHOUSE_ADDR`, `CLICKHOUSE_PASSWORD`, and `LOG_LEVEL` override it):

| Key                     | Default              | Meaning                                                        |
|-------------------------|----------------------|----------------------------------------------------------------|
| `kafka.brokers`         | `localhost:9092`     | Bootstrap brokers                                              |
| `kafka.consumer_group`  | `analytics-ingestor` | Consumer group shared by all routes                            |
| `routes[].topic/table`  | —                    | Topic → ClickHouse table routing (at least one route required) |
| `batch.max_size`        | `5000`               | Records per batch before a flush                               |
| `batch.flush_interval`  | `1s`                 | Upper bound on how long records wait in a partial batch        |
| `batch.buffer_size`     | `20000`              | Bounded queue between fetch and batcher (backpressure threshold) |
| `retry.max_attempts`    | `12`                 | Insert attempts before the pipeline gives up                   |
| `retry.initial_backoff` | `200ms`              | First backoff ceiling                                          |
| `retry.max_backoff`     | `30s`                | Backoff cap                                                    |
| `retry.multiplier`      | `2.0`                | Exponential growth factor                                      |
| `shutdown_timeout`      | `30s`                | Maximum time to drain on SIGTERM                               |

## Metrics

| Metric                                         | Type      | Labels             |
|------------------------------------------------|-----------|--------------------|
| `ingestor_events_consumed_total`               | counter   | topic              |
| `ingestor_events_ingested_total`               | counter   | topic, table       |
| `ingestor_events_invalid_total`                | counter   | topic, reason      |
| `ingestor_batch_write_duration_seconds`        | histogram | table, outcome     |
| `ingestor_batch_size_rows`                     | histogram | table              |
| `ingestor_write_errors_total`                  | counter   | table, kind (`transient`/`permanent`) |
| `ingestor_write_retries_total`                 | counter   | table              |
| `ingestor_commit_errors_total`                 | counter   | topic              |
| `ingestor_consumer_lag`                        | gauge     | topic, partition   |
| `ingestor_buffered_records`                    | gauge     | topic              |
| `ingestor_backpressure_seconds_total`          | counter   | topic              |

The Grafana dashboard (`deploy/grafana/dashboards/ingestor.json`) is provisioned automatically. It shows ingest rate, p50/p99 write latency, per-partition lag, backpressure, error breakdowns, and batch sizes.

## Design decisions around failure handling

**At-least-once delivery plus idempotent writes means duplicates have no effect.** Offsets are committed only after the batch containing them is in ClickHouse, so a crash at any point leads to redelivery, never loss. Two layers keep redelivery harmless:

1. *Per-batch dedup token.* The token is a SHA-256 over the batch's `(topic, partition, offset)` list and is sent as `insert_deduplication_token`. The tables set `non_replicated_deduplication_window`. If an insert times out on the client but succeeded on the server, the retry carries the same token and ClickHouse drops it.
2. *ReplacingMergeTree keyed on event identity.* A redelivered event can land in a different batch, for example after a restart, and an event can be published twice by a producer. In both cases the copies share an `ORDER BY` key, so they collapse to one row at merge time. Use `FINAL` (or `argMax`/`GROUP BY event_id`) when you need exact counts before merges run.

**Backpressure comes from a bounded queue, not from dropping data.** The fetch loop pushes into a channel of `buffer_size` records. When ClickHouse slows down or fails, the batcher stops draining it and the channel fills. The fetch loop then blocks, and the Kafka reader stops pulling. Memory stays bounded, events stay in Kafka, and the stall shows up in three metrics: `buffered_records` stays at capacity, `backpressure_seconds_total` climbs, and `consumer_lag`, which is measured from committed offsets against the broker, rises.

**Errors are classified before retrying.** Network errors, timeouts, `TOO_MANY_PARTS`, memory limits, and similar errors are treated as transient. They are retried with capped exponential backoff with full jitter, so many instances do not retry in lockstep. Schema and permission errors (unknown table or column, type mismatch, auth failures) are permanent: retrying the same insert cannot fix them, so they fail immediately.

**When retries are exhausted, the process fails fast.** A batch that cannot be written stops its pipeline, cancels the others, which first drain their own in-flight batches, and the process exits non-zero with the failed offsets uncommitted. The supervisor (Compose `restart: on-failure`, Kubernetes, etc.) restarts it, and consumption resumes from the last committed offset. This avoids two worse options: silently skipping data, or buffering it unboundedly in memory.

**Malformed events don't block partitions.** Validation failures are logged with full coordinates and counted by reason. Their offsets are committed with the surrounding batch, so a single bad record cannot halt a partition.

**Shutdown is graceful.** On SIGTERM or SIGINT the fetch loops stop, each batcher flushes everything already fetched, and offsets are committed. In-flight writes, including ones that are backing off, keep running on a context detached from the signal but bounded by `shutdown_timeout`. Compose's `stop_grace_period` is set longer than that timeout. Anything still unwritten at the deadline is left uncommitted and is redelivered on the next start.

## Layout

```
cmd/ingestor/       service entrypoint: config, metrics server, signal handling
cmd/producer/       synthetic load generator (valid, malformed, and duplicate events)
internal/config/    YAML config + env overrides + validation
internal/event/     envelope schema, validation, row conversion
internal/pipeline/  per-route fetch -> buffer -> batch -> write -> commit loop
internal/sink/      ClickHouse writer, error classification, dedup tokens
internal/retry/     exponential backoff with jitter
internal/lag/       consumer-group lag monitor
internal/metrics/   Prometheus collectors
deploy/             ClickHouse DDL, Prometheus config, Grafana provisioning + dashboard
```
