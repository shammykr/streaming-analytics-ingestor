// Command ingestor consumes events from Kafka and writes them to ClickHouse.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/shammykr/streaming-analytics-ingestor/internal/config"
	"github.com/shammykr/streaming-analytics-ingestor/internal/pipeline"
	"github.com/shammykr/streaming-analytics-ingestor/internal/retry"
	"github.com/shammykr/streaming-analytics-ingestor/internal/sink"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to YAML config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(*cfgPath, log); err != nil {
		log.Error("ingestor exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("ingestor stopped cleanly")
}

func run(cfgPath string, log *slog.Logger) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: cfg.MetricsAddr, ReadHeaderTimeout: 5 * time.Second}
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()
	defer srv.Close()
	log.Info("metrics listening", "addr", cfg.MetricsAddr)

	ch, err := sink.NewClickHouse(ctx, cfg.ClickHouse, retry.Policy{
		MaxAttempts:    cfg.Retry.MaxAttempts,
		InitialBackoff: cfg.Retry.InitialBackoff,
		MaxBackoff:     cfg.Retry.MaxBackoff,
		Multiplier:     cfg.Retry.Multiplier,
	}, log)
	if err != nil {
		return err
	}
	defer ch.Close()

	// If any pipeline fails fatally, stop the others so the process exits
	// with uncommitted offsets intact and can be restarted.
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, route := range cfg.Routes {
		p := pipeline.New(cfg, route, ch, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("pipeline started", "topic", route.Topic, "table", route.Table, "group", cfg.Kafka.ConsumerGroup)
			if err := p.Run(pctx); err != nil {
				log.Error("pipeline failed", "topic", route.Topic, "err", err)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				cancel()
			}
		}()
	}

	<-pctx.Done()
	if ctx.Err() != nil {
		log.Info("shutdown signal received, draining in-flight batches", "timeout", cfg.ShutdownTimeout)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return l
}
