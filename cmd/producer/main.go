// Command producer publishes synthetic events to the demo topics so the
// ingestor can be exercised end to end. A configurable fraction of messages
// are malformed (to exercise validation) or exact duplicates (to exercise
// idempotent delivery).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	count := flag.Int("count", 100000, "number of events to send (0 = until interrupted)")
	rate := flag.Int("rate", 5000, "target events per second")
	invalid := flag.Float64("invalid", 0.01, "fraction of malformed messages")
	dupes := flag.Float64("dupes", 0.02, "fraction of messages re-sent as duplicates")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(*brokers, ",")...),
		Balancer:               &kafka.Hash{},
		BatchSize:              500,
		BatchTimeout:           20 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	const chunk = 500
	interval := time.Duration(float64(time.Second) * chunk / float64(*rate))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent, dupCount, badCount := 0, 0, 0
	start := time.Now()
	for *count == 0 || sent < *count {
		msgs := make([]kafka.Message, 0, chunk+chunk/10)
		for i := 0; i < chunk && (*count == 0 || sent < *count); i++ {
			m := makeMessage(*invalid, &badCount)
			msgs = append(msgs, m)
			if rand.Float64() < *dupes {
				msgs = append(msgs, m)
				dupCount++
			}
			sent++
		}
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Fatalf("write: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			break
		}
	}
	fmt.Fprintf(os.Stderr, "sent %d unique events (+%d duplicates, %d malformed) in %s\n",
		sent, dupCount, badCount, time.Since(start).Round(time.Millisecond))
}

func makeMessage(invalidRatio float64, bad *int) kafka.Message {
	topic := "page_views"
	if rand.IntN(4) == 0 {
		topic = "orders"
	}
	id := fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
	user := fmt.Sprintf("user-%d", rand.IntN(50000))

	if rand.Float64() < invalidRatio {
		*bad++
		return kafka.Message{Topic: topic, Key: []byte(user), Value: malformed(id)}
	}

	ev := map[string]any{
		"event_id":  id,
		"user_id":   user,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if topic == "orders" {
		ev["event_type"] = "order_placed"
		ev["properties"] = map[string]any{
			"order_id": "ord-" + id[:12],
			"amount":   float64(rand.IntN(50000)) / 100,
			"currency": "USD",
			"items":    1 + rand.IntN(5),
		}
	} else {
		ev["event_type"] = "page_view"
		ev["properties"] = map[string]any{
			"path":     []string{"/", "/pricing", "/docs", "/blog", "/signup"}[rand.IntN(5)],
			"referrer": []string{"google", "direct", "twitter", "newsletter"}[rand.IntN(4)],
		}
	}
	b, _ := json.Marshal(ev)
	return kafka.Message{Topic: topic, Key: []byte(user), Value: b}
}

func malformed(id string) []byte {
	switch rand.IntN(4) {
	case 0:
		return []byte(`{"event_id": "` + id + `", "event_type": "page_view"`) // truncated JSON
	case 1:
		return []byte(`{"event_id": "` + id + `", "timestamp": "2024-01-01T00:00:00Z"}`) // missing event_type
	case 2:
		return []byte(`{"event_id": "` + id + `", "event_type": "page_view", "timestamp": "yesterday"}`)
	default:
		return []byte(`{"event_id": 42, "event_type": "page_view", "timestamp": "2024-01-01T00:00:00Z"}`)
	}
}
