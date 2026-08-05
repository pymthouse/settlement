// Command worker is the long-lived settlement worker: it consumes the billing
// topics and drives OpenMeter invoices through Stripe.
package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/dedupe"
	"github.com/pymthouse/settlement/internal/kafkax"
	"github.com/pymthouse/settlement/internal/lifecycle"
	"github.com/pymthouse/settlement/internal/logging"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
	"github.com/pymthouse/settlement/internal/worker"
)

func main() {
	os.Exit(run())
}

// run owns Kafka and dedupe resources so deferred cleanup always runs before
// the process terminates, even on startup or runtime failure.
func run() int {
	log := logging.New("settlement-worker")

	cfg, err := config.LoadWorker()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		return 1
	}

	claims, err := dedupe.New(cfg.Dedupe)
	if err != nil {
		log.Error("dedupe store", "error", err)
		return 1
	}
	defer func() {
		if err := claims.Close(); err != nil {
			log.Error("dedupe store close", "error", err)
		}
	}()
	if cfg.Dedupe.RedisURL == "" {
		log.Warn("no SETTLEMENT_REDIS_URL: using in-process dedupe, which is only correct for a single replica")
	}

	dlq, err := kafkax.NewPublisher(cfg.Kafka)
	if err != nil {
		log.Error("kafka publisher", "error", err)
		return 1
	}
	defer func() {
		if err := dlq.Close(); err != nil {
			log.Error("kafka publisher close", "error", err)
		}
	}()

	readers := make(map[string]*kafka.Reader, 2)
	for _, topic := range []string{cfg.Kafka.TopicOpenMeter, cfg.Kafka.TopicStripe} {
		reader, err := kafkax.NewReader(cfg.Kafka, topic, cfg.StartOffset)
		if err != nil {
			log.Error("kafka reader", "topic", topic, "error", err)
			return 1
		}
		readers[topic] = reader
	}
	defer func() {
		for topic, reader := range readers {
			if err := reader.Close(); err != nil {
				log.Error("kafka reader close", "topic", topic, "error", err)
			}
		}
	}()

	om := openmeter.New(cfg.OpenMeter)
	settler := lifecycle.New(om, stripe.New(cfg.Stripe), cfg.Stripe, log)
	runner := worker.New(cfg, log, settler, claims, dlq, readers)
	reconciler := worker.NewReconciler(cfg, log, om, settler)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           observabilityRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reconciler.Run(ctx)
	}()

	log.Info("worker starting",
		"brokers", cfg.Kafka.Brokers,
		"group", cfg.Kafka.ConsumerGroup,
		"openmeter", cfg.OpenMeter.BaseURL,
		"charge_model", cfg.Stripe.DefaultChargeModel,
		"metrics_addr", cfg.MetricsAddr,
	)

	runErr := runner.Run(ctx)

	stop()
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics server shutdown", "error", err)
	}

	if runErr != nil {
		log.Error("worker stopped with error", "error", runErr)
		return 1
	}
	log.Info("worker stopped")
	return 0
}

func observabilityRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready")
	})
	return mux
}
