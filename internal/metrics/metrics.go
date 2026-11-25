// Package metrics holds the Prometheus instrumentation for the ingestor.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ns = "ingestor"

var (
	// EventsConsumed counts every message fetched from Kafka.
	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "events_consumed_total",
		Help: "Messages fetched from Kafka.",
	}, []string{"topic"})

	// EventsIngested counts rows successfully written to ClickHouse.
	EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "events_ingested_total",
		Help: "Rows successfully written to ClickHouse.",
	}, []string{"topic", "table"})

	// EventsInvalid counts records rejected by schema validation.
	EventsInvalid = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "events_invalid_total",
		Help: "Messages rejected by schema validation.",
	}, []string{"topic", "reason"})

	// BatchWriteDuration measures one successful or failed insert attempt.
	BatchWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "batch_write_duration_seconds",
		Help:    "Latency of a single ClickHouse batch insert attempt.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	}, []string{"table", "outcome"})

	// BatchSize records the number of rows per flushed batch.
	BatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "batch_size_rows",
		Help:    "Rows per flushed batch.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 16),
	}, []string{"table"})

	// WriteErrors counts failed insert attempts by classification.
	WriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "write_errors_total",
		Help: "Failed ClickHouse insert attempts (kind=transient|permanent).",
	}, []string{"table", "kind"})

	// WriteRetries counts insert attempts that were retried after backoff.
	WriteRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "write_retries_total",
		Help: "ClickHouse insert retries.",
	}, []string{"table"})

	// CommitErrors counts failed Kafka offset commits.
	CommitErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "commit_errors_total",
		Help: "Failed Kafka offset commits.",
	}, []string{"topic"})

	// ConsumerLag is high-water mark minus the group's committed offset.
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "consumer_lag",
		Help: "Messages between the consumer group's committed offset and the partition high-water mark.",
	}, []string{"topic", "partition"})

	// BufferedRecords is the current depth of the fetch -> batcher queue.
	BufferedRecords = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "buffered_records",
		Help: "Records queued between the Kafka fetcher and the batcher.",
	}, []string{"topic"})

	// BackpressureSeconds accumulates time the fetcher spent blocked
	// because the batcher (i.e. ClickHouse) could not keep up.
	BackpressureSeconds = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "backpressure_seconds_total",
		Help: "Time the Kafka fetch loop spent blocked on a full buffer.",
	}, []string{"topic"})
)
