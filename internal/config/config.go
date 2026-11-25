// Package config loads the ingestor configuration from a YAML file, with a
// small set of environment variable overrides for container deployments.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Batch      BatchConfig      `yaml:"batch"`
	Retry      RetryConfig      `yaml:"retry"`
	// Routes maps a Kafka topic to the ClickHouse table its events land in.
	Routes          []Route       `yaml:"routes"`
	MetricsAddr     string        `yaml:"metrics_addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	ConsumerGroup string   `yaml:"consumer_group"`
	MinBytes      int      `yaml:"min_bytes"`
	MaxBytes      int      `yaml:"max_bytes"`
}

type ClickHouseConfig struct {
	Addr     []string `yaml:"addr"`
	Database string   `yaml:"database"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type BatchConfig struct {
	// MaxSize is the number of records that triggers a flush.
	MaxSize int `yaml:"max_size"`
	// FlushInterval flushes a partially filled batch after this long.
	FlushInterval time.Duration `yaml:"flush_interval"`
	// BufferSize bounds the number of validated records waiting for the
	// batcher. When it is full the Kafka fetch loop blocks (backpressure).
	BufferSize int `yaml:"buffer_size"`
}

type RetryConfig struct {
	MaxAttempts    int           `yaml:"max_attempts"`
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	Multiplier     float64       `yaml:"multiplier"`
}

type Route struct {
	Topic string `yaml:"topic"`
	Table string `yaml:"table"`
}

func defaults() Config {
	return Config{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			ConsumerGroup: "analytics-ingestor",
			MinBytes:      1,
			MaxBytes:      10 << 20,
		},
		ClickHouse: ClickHouseConfig{
			Addr:     []string{"localhost:9000"},
			Database: "analytics",
			Username: "default",
		},
		Batch: BatchConfig{
			MaxSize:       5000,
			FlushInterval: time.Second,
			BufferSize:    20000,
		},
		Retry: RetryConfig{
			MaxAttempts:    8,
			InitialBackoff: 200 * time.Millisecond,
			MaxBackoff:     15 * time.Second,
			Multiplier:     2.0,
		},
		MetricsAddr:     ":9100",
		ShutdownTimeout: 30 * time.Second,
	}
}

// Load reads the YAML file at path (if non-empty), applies environment
// overrides and validates the result.
func Load(path string) (Config, error) {
	cfg := defaults()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	applyEnv(&cfg)
	return cfg, cfg.validate()
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_CONSUMER_GROUP"); v != "" {
		cfg.Kafka.ConsumerGroup = v
	}
	if v := os.Getenv("CLICKHOUSE_ADDR"); v != "" {
		cfg.ClickHouse.Addr = strings.Split(v, ",")
	}
	if v := os.Getenv("CLICKHOUSE_PASSWORD"); v != "" {
		cfg.ClickHouse.Password = v
	}
}

func (c Config) validate() error {
	switch {
	case len(c.Kafka.Brokers) == 0:
		return fmt.Errorf("kafka.brokers must not be empty")
	case c.Kafka.ConsumerGroup == "":
		return fmt.Errorf("kafka.consumer_group must not be empty")
	case len(c.Routes) == 0:
		return fmt.Errorf("at least one route (topic -> table) is required")
	case c.Batch.MaxSize <= 0:
		return fmt.Errorf("batch.max_size must be > 0")
	case c.Batch.FlushInterval <= 0:
		return fmt.Errorf("batch.flush_interval must be > 0")
	case c.Batch.BufferSize < c.Batch.MaxSize:
		return fmt.Errorf("batch.buffer_size must be >= batch.max_size")
	case c.Retry.MaxAttempts <= 0:
		return fmt.Errorf("retry.max_attempts must be > 0")
	case c.Retry.Multiplier < 1:
		return fmt.Errorf("retry.multiplier must be >= 1")
	}
	seen := map[string]bool{}
	for _, r := range c.Routes {
		if r.Topic == "" || r.Table == "" {
			return fmt.Errorf("route %+v: topic and table are required", r)
		}
		if seen[r.Topic] {
			return fmt.Errorf("topic %q is routed more than once", r.Topic)
		}
		seen[r.Topic] = true
	}
	return nil
}
