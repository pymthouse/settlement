// Command producer is the settlement doorman: it verifies inbound Stripe and
// OpenMeter webhooks and publishes the raw bodies to the billing topics.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/kafkax"
	"github.com/pymthouse/settlement/internal/logging"
	"github.com/pymthouse/settlement/internal/producer"
)

func main() {
	log := logging.New("settlement-producer")

	cfg, err := config.LoadProducer()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Idempotent and best-effort: a topic that already exists is left alone,
	// and a fresh environment (or a newly added topic, like the collect-
	// request lane) no longer depends on someone remembering to run
	// `settlementctl topics ensure` by hand before the first deploy. A
	// failure here is logged, not fatal — it usually means the topics
	// already exist under settings an operator deliberately customized via
	// the CLI, and crash-looping the doorman over that would be worse than
	// letting it start and surface a real problem on first publish.
	ensureCtx, cancelEnsure := context.WithTimeout(ctx, 10*time.Second)
	if err := kafkax.EnsureTopics(ensureCtx, cfg.Kafka, kafkax.DefaultBillingTopicSpecs(cfg.Kafka)); err != nil {
		log.Warn("ensure billing topics", "error", err)
	}
	cancelEnsure()

	publisher, err := kafkax.NewPublisher(cfg.Kafka)
	if err != nil {
		log.Error("kafka publisher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Error("kafka publisher close", "error", err)
		}
	}()

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: producer.New(cfg, log, publisher).Routes(),
		// Generous enough for a large webhook on a slow link, tight enough
		// that a stalled connection cannot pin a goroutine indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("producer listening",
			"addr", cfg.Addr,
			"brokers", cfg.Kafka.Brokers,
			"stripe_topic", cfg.Kafka.TopicStripe,
			"openmeter_topic", cfg.Kafka.TopicOpenMeter,
			"collect_request_topic", cfg.Kafka.TopicCollectRequest,
			"collect_requests_enabled", len(cfg.CollectRequestSecrets) > 0,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Error("http server", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown", "error", err)
		os.Exit(1)
	}
	log.Info("producer stopped")
}
