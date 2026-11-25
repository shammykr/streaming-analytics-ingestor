// Package pipeline wires one Kafka topic to one ClickHouse table:
//
//	Kafka reader --> validate --> bounded buffer --> batcher --> ClickHouse --> commit offsets
//
// Offsets are committed only after the batch containing them has been
// written, giving at-least-once delivery; idempotent inserts on the
// ClickHouse side turn redeliveries into no-ops.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/shammykr/streaming-analytics-ingestor/internal/config"
	"github.com/shammykr/streaming-analytics-ingestor/internal/event"
	"github.com/shammykr/streaming-analytics-ingestor/internal/metrics"
	"github.com/shammykr/streaming-analytics-ingestor/internal/retry"
)

// Writer is the subset of the ClickHouse sink the pipeline depends on.
type Writer interface {
	Write(ctx context.Context, table string, rows []event.Row) error
}

// record is a fetched message plus its parsed row (nil if it failed
// validation). Invalid records still travel through the batcher so their
// offsets are committed in order with the valid ones around them.
type record struct {
	msg kafka.Message
	row *event.Row
}

type Pipeline struct {
	route  config.Route
	reader *kafka.Reader
	writer Writer
	batch  config.BatchConfig
	retry  retry.Policy
	drain  time.Duration
	log    *slog.Logger
}

func New(cfg config.Config, route config.Route, writer Writer, log *slog.Logger) *Pipeline {
	log = log.With("topic", route.Topic, "table", route.Table)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		GroupID:        cfg.Kafka.ConsumerGroup,
		Topic:          route.Topic,
		MinBytes:       cfg.Kafka.MinBytes,
		MaxBytes:       cfg.Kafka.MaxBytes,
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // synchronous commits, issued only after a successful write
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			log.Warn("kafka: " + fmt.Sprintf(msg, args...))
		}),
	})
	return &Pipeline{
		route:  route,
		reader: reader,
		writer: writer,
		batch:  cfg.Batch,
		retry: retry.Policy{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff,
			MaxBackoff:     cfg.Retry.MaxBackoff,
			Multiplier:     cfg.Retry.Multiplier,
		},
		drain: cfg.ShutdownTimeout,
		log:   log,
	}
}

// Run consumes until ctx is cancelled, then drains everything already
// fetched into ClickHouse and commits it before returning. A non-nil error
// means a batch could not be written; its offsets were not committed.
func (p *Pipeline) Run(ctx context.Context) error {
	defer p.reader.Close()

	// Writes and commits must outlive the shutdown signal so in-flight
	// batches can drain, but are bounded by the shutdown timeout.
	drainCtx, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()
	stopTimer := context.AfterFunc(ctx, func() { time.AfterFunc(p.drain, cancelDrain) })
	defer stopTimer()

	// fetchCtx stops the fetcher on shutdown or when the batcher fails.
	fetchCtx, stopFetch := context.WithCancel(ctx)
	defer stopFetch()

	records := make(chan record, p.batch.BufferSize)
	fetchErr := make(chan error, 1)
	go func() {
		defer close(records)
		fetchErr <- p.fetch(fetchCtx, records)
	}()

	batchErr := p.runBatcher(drainCtx, records)
	if batchErr != nil {
		stopFetch()
		for range records { // unblock the fetcher
		}
	}
	return errors.Join(batchErr, <-fetchErr)
}

// fetch reads messages, validates them and pushes them into the bounded
// buffer. When the buffer is full the send blocks, which stops fetching
// from Kafka: this is the backpressure path when ClickHouse is slow.
func (p *Pipeline) fetch(ctx context.Context, out chan<- record) error {
	topic := p.route.Topic
	consumed := metrics.EventsConsumed.WithLabelValues(topic)
	blocked := metrics.BackpressureSeconds.WithLabelValues(topic)
	buffered := metrics.BufferedRecords.WithLabelValues(topic)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", topic, err)
		}
		consumed.Inc()

		rec := record{msg: msg}
		env, ts, err := event.Parse(msg.Value, time.Now())
		if err != nil {
			var ve *event.ValidationError
			errors.As(err, &ve)
			metrics.EventsInvalid.WithLabelValues(topic, ve.Reason).Inc()
			p.log.Warn("rejected invalid event",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"reason", ve.Reason,
				"field", ve.Field,
				"detail", ve.Detail,
				"payload_bytes", len(msg.Value),
			)
		} else {
			row := event.ToRow(env, ts, msg.Topic, msg.Partition, msg.Offset)
			rec.row = &row
		}

		select {
		case out <- rec:
			buffered.Set(float64(len(out)))
			continue
		default:
		}
		// Buffer full: stop fetching until the batcher catches up. Blocked
		// time is reported every second so the metric moves during a stall.
		buffered.Set(float64(cap(out)))
		last := time.Now()
	wait:
		for {
			select {
			case out <- rec:
				break wait
			case <-tick.C:
				now := time.Now()
				blocked.Add(now.Sub(last).Seconds())
				last = now
			case <-ctx.Done():
				return nil // not committed; will be redelivered
			}
		}
		blocked.Add(time.Since(last).Seconds())
	}
}

// runBatcher accumulates records and flushes when the batch reaches
// MaxSize or every FlushInterval. When the input channel closes (shutdown)
// it flushes what is left and returns.
func (p *Pipeline) runBatcher(ctx context.Context, in <-chan record) error {
	ticker := time.NewTicker(p.batch.FlushInterval)
	defer ticker.Stop()

	pending := make([]record, 0, p.batch.MaxSize)
	for {
		select {
		case rec, ok := <-in:
			if !ok {
				if len(pending) > 0 {
					p.log.Info("draining final batch", "records", len(pending))
				}
				return p.flush(ctx, pending)
			}
			pending = append(pending, rec)
			if len(pending) >= p.batch.MaxSize {
				if err := p.flush(ctx, pending); err != nil {
					return err
				}
				pending = pending[:0]
			}
		case <-ticker.C:
			if err := p.flush(ctx, pending); err != nil {
				return err
			}
			pending = pending[:0]
		}
	}
}

func (p *Pipeline) flush(ctx context.Context, recs []record) error {
	if len(recs) == 0 {
		return nil
	}
	rows := make([]event.Row, 0, len(recs))
	msgs := make([]kafka.Message, len(recs))
	for i, r := range recs {
		msgs[i] = r.msg
		if r.row != nil {
			rows = append(rows, *r.row)
		}
	}

	if len(rows) > 0 {
		if err := p.writer.Write(ctx, p.route.Table, rows); err != nil {
			return fmt.Errorf("write %d rows to %s: %w", len(rows), p.route.Table, err)
		}
		metrics.BatchSize.WithLabelValues(p.route.Table).Observe(float64(len(rows)))
		metrics.EventsIngested.WithLabelValues(p.route.Topic, p.route.Table).Add(float64(len(rows)))
	}

	// Commit after the write. A failed commit is not fatal: the rows are
	// already stored, the next successful commit covers these offsets, and
	// any redelivery is absorbed by the idempotent table.
	err := p.retry.Do(ctx, func(ctx context.Context) error {
		return p.reader.CommitMessages(ctx, msgs...)
	}, nil)
	if err != nil {
		metrics.CommitErrors.WithLabelValues(p.route.Topic).Inc()
		p.log.Error("offset commit failed", "records", len(msgs), "err", err)
	}
	p.log.Debug("batch flushed", "rows", len(rows), "invalid", len(recs)-len(rows))
	return nil
}
