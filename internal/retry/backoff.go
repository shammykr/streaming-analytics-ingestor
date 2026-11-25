// Package retry implements capped exponential backoff with full jitter.
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

type Policy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// permanentError marks an error that must not be retried.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent wraps err so Do stops retrying immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Backoff returns the sleep before retry number attempt (1-based), using
// full jitter: a uniform random duration in [0, min(max, initial*mult^(n-1))].
func (p Policy) Backoff(attempt int) time.Duration {
	ceiling := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt-1))
	if ceiling > float64(p.MaxBackoff) {
		ceiling = float64(p.MaxBackoff)
	}
	return time.Duration(rand.Float64() * ceiling)
}

// Do calls fn until it succeeds, returns a permanent error, the context is
// cancelled, or MaxAttempts is reached. onRetry is invoked before each sleep.
func (p Policy) Do(ctx context.Context, fn func(ctx context.Context) error, onRetry func(attempt int, wait time.Duration, err error)) error {
	var err error
	for attempt := 1; ; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if IsPermanent(err) || attempt >= p.MaxAttempts {
			return err
		}
		wait := p.Backoff(attempt)
		if onRetry != nil {
			onRetry(attempt, wait, err)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(err, ctx.Err())
		case <-t.C:
		}
	}
}
