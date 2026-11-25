// Package sink writes batches of validated events into ClickHouse.
package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/shammykr/streaming-analytics-ingestor/internal/config"
	"github.com/shammykr/streaming-analytics-ingestor/internal/event"
	"github.com/shammykr/streaming-analytics-ingestor/internal/metrics"
	"github.com/shammykr/streaming-analytics-ingestor/internal/retry"
)

// ClickHouse error codes that will not go away by retrying the same insert.
// Everything else (network errors, TOO_MANY_PARTS, memory limits, timeouts,
// server restarts) is treated as transient.
var permanentCodes = map[int32]string{
	16:  "NO_SUCH_COLUMN_IN_TABLE",
	47:  "UNKNOWN_IDENTIFIER",
	53:  "TYPE_MISMATCH",
	60:  "UNKNOWN_TABLE",
	62:  "SYNTAX_ERROR",
	81:  "UNKNOWN_DATABASE",
	497: "ACCESS_DENIED",
	516: "AUTHENTICATION_FAILED",
}

type ClickHouse struct {
	conn   driver.Conn
	policy retry.Policy
	log    *slog.Logger
}

func NewClickHouse(ctx context.Context, cfg config.ClickHouseConfig, policy retry.Policy, log *slog.Logger) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addr,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		ConnMaxLifetime: time.Hour,
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	ch := &ClickHouse{conn: conn, policy: policy, log: log}
	// Wait for ClickHouse at startup using the same backoff policy.
	err = policy.Do(ctx, func(ctx context.Context) error { return conn.Ping(ctx) },
		func(attempt int, wait time.Duration, err error) {
			log.Warn("clickhouse not reachable, retrying", "attempt", attempt, "wait", wait, "err", err)
		})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return ch, nil
}

func (c *ClickHouse) Close() error { return c.conn.Close() }

// DedupToken derives a deterministic insert_deduplication_token from the
// Kafka coordinates of a batch. Retrying the same batch (for example after
// a timeout where the server actually committed the insert) produces the
// same token, so ClickHouse drops the second copy of the block.
func DedupToken(rows []event.Row) string {
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%s/%d/%d;", r.Topic, r.Partition, r.Offset)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Write inserts rows into table, retrying transient failures with
// exponential backoff. It returns an error only when the batch could not be
// written; the caller must then not commit the corresponding offsets.
func (c *ClickHouse) Write(ctx context.Context, table string, rows []event.Row) error {
	if len(rows) == 0 {
		return nil
	}
	token := DedupToken(rows)
	return c.policy.Do(ctx, func(ctx context.Context) error {
		start := time.Now()
		err := c.insert(ctx, table, rows, token)
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		metrics.BatchWriteDuration.WithLabelValues(table, outcome).Observe(time.Since(start).Seconds())
		if err == nil {
			return nil
		}
		if name, ok := permanent(err); ok {
			metrics.WriteErrors.WithLabelValues(table, "permanent").Inc()
			c.log.Error("permanent clickhouse error", "table", table, "code", name, "err", err)
			return retry.Permanent(err)
		}
		metrics.WriteErrors.WithLabelValues(table, "transient").Inc()
		return err
	}, func(attempt int, wait time.Duration, err error) {
		metrics.WriteRetries.WithLabelValues(table).Inc()
		c.log.Warn("batch insert failed, backing off",
			"table", table, "rows", len(rows), "attempt", attempt, "wait", wait, "err", err)
	})
}

func (c *ClickHouse) insert(ctx context.Context, table string, rows []event.Row, token string) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": token,
	}))
	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s (event_id, event_type, user_id, event_time, properties, kafka_topic, kafka_partition, kafka_offset)", table))
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, r := range rows {
		if err := batch.Append(r.EventID, r.EventType, r.UserID, r.EventTime, r.Properties, r.Topic, r.Partition, r.Offset); err != nil {
			return err
		}
	}
	return batch.Send()
}

func permanent(err error) (string, bool) {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		name, ok := permanentCodes[ex.Code]
		return name, ok
	}
	return "", false
}
