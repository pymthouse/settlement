// Package metrics holds the Prometheus instruments for both binaries.
//
// The series here are the ones an on-call engineer needs to answer "is money
// moving?": accepted vs rejected webhooks, events processed per outcome, how
// long the OpenMeter/Stripe calls take, and how deep the DLQ is.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// WebhooksReceived counts inbound webhooks by source and outcome
	// (accepted, bad_signature, bad_request, publish_error).
	WebhooksReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_webhooks_received_total",
		Help: "Inbound webhooks by source and outcome.",
	}, []string{"source", "outcome"})

	// WebhookDuration measures the doorman's end-to-end handler latency.
	WebhookDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "settlement_webhook_duration_seconds",
		Help:    "Webhook handler latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"source"})

	// EventsConsumed counts Kafka messages pulled by the worker.
	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_events_consumed_total",
		Help: "Kafka messages consumed by topic.",
	}, []string{"topic"})

	// EventsProcessed counts terminal processing outcomes.
	EventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_events_processed_total",
		Help: "Event processing outcomes by source, handler and outcome.",
	}, []string{"source", "handler", "outcome"})

	// EventDuration measures handler execution time.
	EventDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "settlement_event_duration_seconds",
		Help:    "Handler execution time in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"handler"})

	// EventRetries counts retry attempts, excluding the first try.
	EventRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_event_retries_total",
		Help: "Retry attempts by handler.",
	}, []string{"handler"})

	// DuplicatesDropped counts events suppressed by the idempotency store.
	DuplicatesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_duplicates_dropped_total",
		Help: "Events dropped because their id was already processed.",
	}, []string{"source"})

	// DeadLettered counts messages written to the DLQ. Alert on any increase.
	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_dead_lettered_total",
		Help: "Messages routed to the dead-letter topic by source and reason.",
	}, []string{"source", "reason"})

	// UpstreamRequests counts OpenMeter/Stripe API calls by outcome.
	UpstreamRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_upstream_requests_total",
		Help: "Upstream API calls by system, operation and outcome.",
	}, []string{"system", "operation", "outcome"})

	// UpstreamDuration measures OpenMeter/Stripe call latency.
	UpstreamDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "settlement_upstream_duration_seconds",
		Help:    "Upstream API call latency in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"system", "operation"})

	// InvoiceStateTransitions counts lifecycle advances we drove.
	InvoiceStateTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_invoice_transitions_total",
		Help: "Invoice lifecycle transitions driven by the worker.",
	}, []string{"transition", "outcome"})

	// InvoicesNeedingAttention reports invoices seen in a failed, overdue or
	// uncollectible state. This is the business alerting signal.
	InvoicesNeedingAttention = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_invoices_needing_attention_total",
		Help: "Invoices observed in a failed, overdue or uncollectible state.",
	}, []string{"status"})

	// ReconcileSweeps counts reconciliation passes and what they found.
	ReconcileSweeps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_reconcile_sweeps_total",
		Help: "Reconciliation sweeps by outcome.",
	}, []string{"outcome"})

	// ReconcileRedriven counts invoices the sweeper re-drove.
	ReconcileRedriven = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_reconcile_redriven_total",
		Help: "Stuck invoices re-driven by the reconciliation sweeper.",
	}, []string{"handler"})

	// CommittedOffset exposes the last committed offset per topic/partition.
	CommittedOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "settlement_committed_offset",
		Help: "Last committed Kafka offset by topic and partition.",
	}, []string{"topic", "partition"})

	// InFlight reports messages currently being handled.
	InFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "settlement_events_in_flight",
		Help: "Messages currently being processed.",
	})
)

// ObserveUpstream records the outcome and latency of one upstream call.
func ObserveUpstream(system, operation string, start time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	UpstreamRequests.WithLabelValues(system, operation, outcome).Inc()
	UpstreamDuration.WithLabelValues(system, operation).Observe(time.Since(start).Seconds())
}

// Handler serves the Prometheus scrape endpoint.
func Handler() http.Handler { return promhttp.Handler() }
