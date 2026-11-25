// Package event defines the envelope schema every Kafka message must satisfy
// and turns raw message bytes into rows ready for ClickHouse.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	maxIDLen   = 128
	maxTypeLen = 64
	// Events with timestamps further than this in the future are rejected;
	// they almost always indicate a producer clock or unit bug.
	maxClockSkew = 24 * time.Hour
)

// Envelope is the wire schema. Unknown top-level fields are rejected.
type Envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	UserID     string          `json:"user_id"`
	Timestamp  string          `json:"timestamp"`
	Properties json.RawMessage `json:"properties"`
}

// Row is a validated event plus the Kafka coordinates it came from.
type Row struct {
	EventID    string
	EventType  string
	UserID     string
	EventTime  time.Time
	Properties string
	Topic      string
	Partition  int32
	Offset     int64
}

// ValidationError describes why a record was rejected. Reason is a short,
// low-cardinality code suitable for use as a metric label.
type ValidationError struct {
	Field  string
	Reason string
	Detail string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
	}
	return fmt.Sprintf("%s (%s): %s", e.Reason, e.Field, e.Detail)
}

func invalid(field, reason, format string, args ...any) error {
	return &ValidationError{Field: field, Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Parse decodes and validates a message value. The returned error is always
// a *ValidationError when non-nil.
func Parse(value []byte, now time.Time) (Envelope, time.Time, error) {
	var env Envelope
	if len(bytes.TrimSpace(value)) == 0 {
		return env, time.Time{}, invalid("", "empty_payload", "message value is empty")
	}

	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &typeErr):
			return env, time.Time{}, invalid(typeErr.Field, "wrong_type", "expected %s, got %s", typeErr.Type, typeErr.Value)
		case strings.HasPrefix(err.Error(), "json: unknown field"):
			return env, time.Time{}, invalid(strings.Trim(strings.TrimPrefix(err.Error(), "json: unknown field "), `"`), "unknown_field", "field is not part of the schema")
		default:
			return env, time.Time{}, invalid("", "malformed_json", "%v", err)
		}
	}
	if dec.More() {
		return env, time.Time{}, invalid("", "malformed_json", "trailing data after JSON object")
	}

	if err := requireString("event_id", env.EventID, maxIDLen); err != nil {
		return env, time.Time{}, err
	}
	if err := requireString("event_type", env.EventType, maxTypeLen); err != nil {
		return env, time.Time{}, err
	}
	if len(env.UserID) > maxIDLen {
		return env, time.Time{}, invalid("user_id", "too_long", "length %d exceeds %d", len(env.UserID), maxIDLen)
	}

	if env.Timestamp == "" {
		return env, time.Time{}, invalid("timestamp", "missing_field", "field is required")
	}
	ts, err := time.Parse(time.RFC3339Nano, env.Timestamp)
	if err != nil {
		return env, time.Time{}, invalid("timestamp", "bad_timestamp", "must be RFC3339: %v", err)
	}
	if ts.After(now.Add(maxClockSkew)) {
		return env, time.Time{}, invalid("timestamp", "bad_timestamp", "%s is too far in the future", env.Timestamp)
	}

	if len(env.Properties) > 0 && !bytes.Equal(env.Properties, []byte("null")) {
		trimmed := bytes.TrimSpace(env.Properties)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return env, time.Time{}, invalid("properties", "wrong_type", "must be a JSON object")
		}
	}
	return env, ts.UTC(), nil
}

func requireString(field, v string, max int) error {
	if strings.TrimSpace(v) == "" {
		return invalid(field, "missing_field", "field is required")
	}
	if len(v) > max {
		return invalid(field, "too_long", "length %d exceeds %d", len(v), max)
	}
	return nil
}

// ToRow converts a validated envelope into a ClickHouse row.
func ToRow(env Envelope, ts time.Time, topic string, partition int, offset int64) Row {
	props := "{}"
	if len(env.Properties) > 0 && !bytes.Equal(env.Properties, []byte("null")) {
		var buf bytes.Buffer
		if json.Compact(&buf, env.Properties) == nil {
			props = buf.String()
		}
	}
	return Row{
		EventID:    env.EventID,
		EventType:  env.EventType,
		UserID:     env.UserID,
		EventTime:  ts,
		Properties: props,
		Topic:      topic,
		Partition:  int32(partition),
		Offset:     offset,
	}
}
