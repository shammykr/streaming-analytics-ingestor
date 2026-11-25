// Package lag periodically measures consumer group lag against the broker:
// lag = partition high-water mark - last committed offset. Because it is
// based on committed offsets it keeps rising while the ingestor is applying
// backpressure, which is exactly when it matters.
package lag

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/shammykr/streaming-analytics-ingestor/internal/metrics"
)

type Monitor struct {
	client   *kafka.Client
	group    string
	topics   []string
	interval time.Duration
	log      *slog.Logger
}

func NewMonitor(brokers []string, group string, topics []string, interval time.Duration, log *slog.Logger) *Monitor {
	return &Monitor{
		client:   &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second},
		group:    group,
		topics:   topics,
		interval: interval,
		log:      log,
	}
}

func (m *Monitor) Run(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		if err := m.poll(ctx); err != nil && ctx.Err() == nil {
			m.log.Debug("lag poll failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Monitor) poll(ctx context.Context) error {
	meta, err := m.client.Metadata(ctx, &kafka.MetadataRequest{Topics: m.topics})
	if err != nil {
		return err
	}
	parts := map[string][]int{}
	ends := map[string][]kafka.OffsetRequest{}
	for _, t := range meta.Topics {
		for _, p := range t.Partitions {
			parts[t.Name] = append(parts[t.Name], p.ID)
			ends[t.Name] = append(ends[t.Name], kafka.LastOffsetOf(p.ID), kafka.FirstOffsetOf(p.ID))
		}
	}

	committed, err := m.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: m.group, Topics: parts})
	if err != nil {
		return err
	}
	offsets, err := m.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: ends})
	if err != nil {
		return err
	}

	for topic, ps := range offsets.Topics {
		commit := map[int]int64{}
		for _, c := range committed.Topics[topic] {
			commit[c.Partition] = c.CommittedOffset
		}
		for _, p := range ps {
			if p.Error != nil {
				continue
			}
			pos, ok := commit[p.Partition]
			if !ok || pos < 0 { // nothing committed yet: consumption starts at the earliest offset
				pos = p.FirstOffset
			}
			metrics.ConsumerLag.WithLabelValues(topic, strconv.Itoa(p.Partition)).Set(float64(max(p.LastOffset-pos, 0)))
		}
	}
	return nil
}
