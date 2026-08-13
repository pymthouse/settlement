// Package config loads settlement service configuration from the environment.
//
// Every knob is an environment variable so the same image runs unchanged on
// Railway, Docker Compose and a laptop. Values are validated at startup: a
// misconfigured billing service should refuse to boot rather than discover the
// problem halfway through an invoice lifecycle.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ChargeModel selects how a connected account's money flows through Stripe.
type ChargeModel string

const (
	// ChargeModelDirect creates the invoice on the connected account itself
	// (Stripe-Account header). The developer is merchant of record and the
	// platform takes an application fee.
	ChargeModelDirect ChargeModel = "direct"
	// ChargeModelDestination creates the invoice on the platform account and
	// transfers the net to the connected account. The platform is merchant of
	// record.
	ChargeModelDestination ChargeModel = "destination"
	// ChargeModelPlatform bills the customer directly on the platform account
	// with no Connect involvement. This is the default no-Connect model where
	// a developer owner pays for their own end users' usage.
	ChargeModelPlatform ChargeModel = "platform"
)

// ValidChargeModel reports whether m is a charge model the worker implements.
func ValidChargeModel(m ChargeModel) bool {
	switch m {
	case ChargeModelDirect, ChargeModelDestination, ChargeModelPlatform:
		return true
	default:
		return false
	}
}

// RetryExhaustedPolicy decides what happens to a message whose retries ran out.
type RetryExhaustedPolicy string

const (
	// PolicyDLQ routes the message to the dead-letter topic and commits the
	// offset so the partition keeps flowing. Requires DLQ monitoring.
	PolicyDLQ RetryExhaustedPolicy = "dlq"
	// PolicyHalt stops the consumer without committing. Nothing is lost and
	// nothing proceeds — the safest choice for money, the loudest for uptime.
	PolicyHalt RetryExhaustedPolicy = "halt"
)

// Kafka holds the settings shared by the producer and the worker.
type Kafka struct {
	Brokers []string
	// TopicStripe carries verified Stripe webhook bodies.
	TopicStripe string
	// TopicOpenMeter carries verified OpenMeter notification bodies.
	TopicOpenMeter string
	// TopicDLQ receives messages that exhausted their retries.
	TopicDLQ string
	// TopicCollectRequest carries verified pymthouse "raise this customer now"
	// requests. Separate from TopicOpenMeter even though both ultimately raise
	// invoices — one is a third-party notification, the other pymthouse's own
	// business decision, and conflating them would make the audit trail read
	// as if OpenMeter asked for something it never did.
	TopicCollectRequest string
	// ConsumerGroup is the worker's Kafka consumer group id.
	ConsumerGroup string

	SASLMechanism string // "", "plain", "scram-sha-256", "scram-sha-512"
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool

	// RequiredAcks -1 (all) is the only durable setting for financial events;
	// it is configurable purely so single-broker dev clusters can relax it.
	RequiredAcks int
	WriteTimeout time.Duration
	DialTimeout  time.Duration
}

// Producer is the doorman's configuration.
type Producer struct {
	Addr            string
	ShutdownTimeout time.Duration
	MaxBodyBytes    int64

	// StripeWebhookSecrets are whsec_ values. Multiple entries let a secret be
	// rotated without dropping in-flight deliveries.
	StripeWebhookSecrets []string
	// StripeToleranceSeconds bounds webhook timestamp skew (replay defence).
	StripeToleranceSeconds int64

	// OpenMeterWebhookSecrets are the notification channel signing secrets
	// (Standard Webhooks / Svix format, optionally whsec_ prefixed).
	OpenMeterWebhookSecrets []string
	OpenMeterToleranceSecs  int64

	// CollectRequestSecrets authenticate pymthouse's own "raise this customer
	// now" requests. Not a webhook signature — pymthouse is a known first
	// party, not a third-party provider — but still a list so it can be
	// rotated without dropping a request. Compared with plain constant-time
	// equality, not HMAC'd over a body, since pymthouse is not signing a
	// payload it wants replay-verified months later the way Stripe does.
	CollectRequestSecrets []string

	Kafka Kafka
}

// OpenMeter is the worker's OpenMeter API configuration.
type OpenMeter struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration

	// DraftSyncStatuses / IssuingSyncStatuses / PaymentPendingStatuses are the
	// invoice extendedStatus values that pause the lifecycle waiting on us.
	// They are configurable because extendedStatus is a free-form string in the
	// OpenMeter API and has changed shape across releases.
	DraftSyncStatuses      []string
	IssuingSyncStatuses    []string
	PaymentPendingStatuses []string
}

// Stripe is the worker's Stripe API configuration.
type Stripe struct {
	APIBase            string
	SecretKey          string
	APIVersion         string
	Timeout            time.Duration
	MaxRetries         int
	DefaultChargeModel ChargeModel
	// ApplicationFeeBps is the platform's cut in basis points of the invoice
	// total; ApplicationFeeFlatMinor is added on top, in minor units.
	ApplicationFeeBps       int64
	ApplicationFeeFlatMinor int64
	// ConnectAccountMetadataKey is the OpenMeter customer metadata key holding
	// the developer's Connect account id (acct_...).
	ConnectAccountMetadataKey string
	// ChargeModelMetadataKey optionally overrides DefaultChargeModel per
	// customer via OpenMeter customer metadata.
	ChargeModelMetadataKey string
	// CustomerMetadataKey is the OpenMeter customer metadata key holding an
	// already-known Stripe customer id, which skips the search-then-create
	// dance entirely.
	CustomerMetadataKey string
	// StatementDescriptorSuffix is applied to Connect charges when set.
	StatementDescriptorSuffix string
	// CollectionMethod is "charge_automatically" (charge the saved payment
	// method) or "send_invoice" (email the customer a payable invoice).
	CollectionMethod string
	// AutoAdvance leaves Stripe's own dunning in charge after finalization.
	AutoAdvance bool
	// DaysUntilDue is used when the OpenMeter invoice carries no due date.
	DaysUntilDue int64
}

// Dedupe configures the idempotency store.
type Dedupe struct {
	// RedisURL enables the shared store. Empty falls back to an in-process
	// store, which is only correct for a single worker replica.
	RedisURL string
	TTL      time.Duration
	// KeyPrefix namespaces keys so several environments can share Redis.
	KeyPrefix string
	Timeout   time.Duration
}

// Worker is the settlement worker's configuration.
type Worker struct {
	MetricsAddr     string
	ShutdownTimeout time.Duration

	// Lanes is the number of ordered execution lanes. Messages are dispatched
	// by hash(key) so per-customer ordering survives concurrency.
	Lanes int
	// LaneBuffer is the queue depth of each lane.
	LaneBuffer int

	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	RetryJitter    float64

	CommitInterval   time.Duration
	OnRetryExhausted RetryExhaustedPolicy

	// StartOffset is "last" (default) or "first"; "first" replays a topic from
	// the beginning for a fresh consumer group.
	StartOffset string

	// ReconcileInterval drives the sweeper that re-drives invoices stuck in a
	// sync-pending state because an event was dropped. Zero disables it.
	ReconcileInterval time.Duration
	ReconcileMinAge   time.Duration
	ReconcilePageSize int

	Kafka     Kafka
	OpenMeter OpenMeter
	Stripe    Stripe
	Dedupe    Dedupe
	Admin     Admin
}

// Admin configures the worker's optional /admin ops console.
// An empty Token disables the console (routes return 404).
type Admin struct {
	Token              string
	ProducerURL        string
	OpenMeterUIURL     string
	StripeDashboardURL string
	RailwayURL         string
}

// LoadProducer reads the doorman configuration from the environment.
func LoadProducer() (Producer, error) {
	var errs []error
	p := Producer{
		Addr:                    env("SETTLEMENT_HTTP_ADDR", ":8080"),
		ShutdownTimeout:         envDuration("SETTLEMENT_SHUTDOWN_TIMEOUT", 15*time.Second, &errs),
		MaxBodyBytes:            int64(envInt("SETTLEMENT_MAX_BODY_BYTES", 1<<20, &errs)),
		StripeWebhookSecrets:    envList("SETTLEMENT_STRIPE_WEBHOOK_SECRETS"),
		StripeToleranceSeconds:  int64(envInt("SETTLEMENT_STRIPE_TOLERANCE_SECONDS", 300, &errs)),
		OpenMeterWebhookSecrets: envList("SETTLEMENT_OPENMETER_WEBHOOK_SECRETS"),
		OpenMeterToleranceSecs:  int64(envInt("SETTLEMENT_OPENMETER_TOLERANCE_SECONDS", 300, &errs)),
		CollectRequestSecrets:   envList("SETTLEMENT_COLLECT_REQUEST_SECRETS"),
		Kafka:                   loadKafka(&errs),
	}

	if len(p.StripeWebhookSecrets) == 0 && len(p.OpenMeterWebhookSecrets) == 0 {
		errs = append(errs, errors.New("no webhook secrets configured: set SETTLEMENT_STRIPE_WEBHOOK_SECRETS and/or SETTLEMENT_OPENMETER_WEBHOOK_SECRETS"))
	}
	if p.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("SETTLEMENT_MAX_BODY_BYTES must be positive"))
	}
	return p, errors.Join(errs...)
}

// LoadWorker reads the settlement worker configuration from the environment.
func LoadWorker() (Worker, error) {
	var errs []error
	w := Worker{
		MetricsAddr:      env("SETTLEMENT_METRICS_ADDR", ":8081"),
		ShutdownTimeout:  envDuration("SETTLEMENT_SHUTDOWN_TIMEOUT", 30*time.Second, &errs),
		Lanes:            envInt("SETTLEMENT_LANES", 16, &errs),
		LaneBuffer:       envInt("SETTLEMENT_LANE_BUFFER", 64, &errs),
		MaxAttempts:      envInt("SETTLEMENT_MAX_ATTEMPTS", 8, &errs),
		RetryBaseDelay:   envDuration("SETTLEMENT_RETRY_BASE_DELAY", 500*time.Millisecond, &errs),
		RetryMaxDelay:    envDuration("SETTLEMENT_RETRY_MAX_DELAY", 60*time.Second, &errs),
		RetryJitter:      envFloat("SETTLEMENT_RETRY_JITTER", 0.2, &errs),
		CommitInterval:   envDuration("SETTLEMENT_COMMIT_INTERVAL", time.Second, &errs),
		OnRetryExhausted: RetryExhaustedPolicy(strings.ToLower(env("SETTLEMENT_ON_RETRY_EXHAUSTED", string(PolicyDLQ)))),
		StartOffset:      strings.ToLower(env("SETTLEMENT_START_OFFSET", "last")),

		ReconcileInterval: envDuration("SETTLEMENT_RECONCILE_INTERVAL", 15*time.Minute, &errs),
		ReconcileMinAge:   envDuration("SETTLEMENT_RECONCILE_MIN_AGE", 10*time.Minute, &errs),
		ReconcilePageSize: envInt("SETTLEMENT_RECONCILE_PAGE_SIZE", 100, &errs),

		Kafka: loadKafka(&errs),
		OpenMeter: OpenMeter{
			BaseURL:                strings.TrimRight(env("SETTLEMENT_OPENMETER_URL", ""), "/"),
			APIKey:                 env("SETTLEMENT_OPENMETER_API_KEY", ""),
			Timeout:                envDuration("SETTLEMENT_OPENMETER_TIMEOUT", 20*time.Second, &errs),
			DraftSyncStatuses:      envListDefault("SETTLEMENT_OPENMETER_DRAFT_SYNC_STATUSES", []string{"draft.sync", "draft_sync", "draft.syncing"}),
			IssuingSyncStatuses:    envListDefault("SETTLEMENT_OPENMETER_ISSUING_SYNC_STATUSES", []string{"issuing.sync", "issuing_sync", "issuing.syncing"}),
			PaymentPendingStatuses: envListDefault("SETTLEMENT_OPENMETER_PAYMENT_PENDING_STATUSES", []string{"payment_processing.pending"}),
		},
		Stripe: Stripe{
			APIBase:                   strings.TrimRight(env("SETTLEMENT_STRIPE_API_BASE", "https://api.stripe.com"), "/"),
			SecretKey:                 env("SETTLEMENT_STRIPE_SECRET_KEY", ""),
			APIVersion:                env("SETTLEMENT_STRIPE_API_VERSION", ""),
			Timeout:                   envDuration("SETTLEMENT_STRIPE_TIMEOUT", 30*time.Second, &errs),
			MaxRetries:                envInt("SETTLEMENT_STRIPE_MAX_RETRIES", 3, &errs),
			DefaultChargeModel:        ChargeModel(strings.ToLower(env("SETTLEMENT_STRIPE_CHARGE_MODEL", string(ChargeModelPlatform)))),
			ApplicationFeeBps:         int64(envInt("SETTLEMENT_APPLICATION_FEE_BPS", 0, &errs)),
			ApplicationFeeFlatMinor:   int64(envInt("SETTLEMENT_APPLICATION_FEE_FLAT_MINOR", 0, &errs)),
			ConnectAccountMetadataKey: env("SETTLEMENT_CONNECT_ACCOUNT_METADATA_KEY", "stripe_connect_account_id"),
			ChargeModelMetadataKey:    env("SETTLEMENT_CHARGE_MODEL_METADATA_KEY", "stripe_charge_model"),
			CustomerMetadataKey:       env("SETTLEMENT_STRIPE_CUSTOMER_METADATA_KEY", "stripe_customer_id"),
			StatementDescriptorSuffix: env("SETTLEMENT_STRIPE_STATEMENT_DESCRIPTOR_SUFFIX", ""),
			CollectionMethod:          env("SETTLEMENT_STRIPE_COLLECTION_METHOD", "charge_automatically"),
			AutoAdvance:               envBool("SETTLEMENT_STRIPE_AUTO_ADVANCE", true, &errs),
			DaysUntilDue:              int64(envInt("SETTLEMENT_STRIPE_DAYS_UNTIL_DUE", 30, &errs)),
		},
		Dedupe: Dedupe{
			RedisURL:  env("SETTLEMENT_REDIS_URL", ""),
			TTL:       envDuration("SETTLEMENT_DEDUPE_TTL", 720*time.Hour, &errs), // 30 days
			KeyPrefix: env("SETTLEMENT_DEDUPE_PREFIX", "settlement:dedupe:"),
			Timeout:   envDuration("SETTLEMENT_DEDUPE_TIMEOUT", 3*time.Second, &errs),
		},
		Admin: Admin{
			Token:              env("SETTLEMENT_ADMIN_TOKEN", ""),
			ProducerURL:        strings.TrimRight(env("SETTLEMENT_ADMIN_PRODUCER_URL", ""), "/"),
			OpenMeterUIURL:     strings.TrimRight(env("SETTLEMENT_ADMIN_OPENMETER_UI_URL", ""), "/"),
			StripeDashboardURL: strings.TrimRight(env("SETTLEMENT_ADMIN_STRIPE_DASHBOARD_URL", "https://dashboard.stripe.com"), "/"),
			RailwayURL:         strings.TrimRight(env("SETTLEMENT_ADMIN_RAILWAY_URL", ""), "/"),
		},
	}

	if w.OpenMeter.BaseURL == "" {
		errs = append(errs, errors.New("SETTLEMENT_OPENMETER_URL is required"))
	}
	if w.Stripe.SecretKey == "" {
		errs = append(errs, errors.New("SETTLEMENT_STRIPE_SECRET_KEY is required"))
	}
	if !ValidChargeModel(w.Stripe.DefaultChargeModel) {
		errs = append(errs, fmt.Errorf("SETTLEMENT_STRIPE_CHARGE_MODEL %q must be one of direct, destination, platform", w.Stripe.DefaultChargeModel))
	}
	if w.Stripe.ApplicationFeeBps < 0 || w.Stripe.ApplicationFeeBps > 10_000 {
		errs = append(errs, errors.New("SETTLEMENT_APPLICATION_FEE_BPS must be between 0 and 10000"))
	}
	switch w.Stripe.CollectionMethod {
	case "charge_automatically", "send_invoice":
	default:
		errs = append(errs, fmt.Errorf("SETTLEMENT_STRIPE_COLLECTION_METHOD %q must be charge_automatically or send_invoice", w.Stripe.CollectionMethod))
	}
	if w.Lanes <= 0 {
		errs = append(errs, errors.New("SETTLEMENT_LANES must be positive"))
	}
	if w.MaxAttempts <= 0 {
		errs = append(errs, errors.New("SETTLEMENT_MAX_ATTEMPTS must be positive"))
	}
	switch w.OnRetryExhausted {
	case PolicyDLQ, PolicyHalt:
	default:
		errs = append(errs, fmt.Errorf("SETTLEMENT_ON_RETRY_EXHAUSTED %q must be dlq or halt", w.OnRetryExhausted))
	}
	switch w.StartOffset {
	case "first", "last":
	default:
		errs = append(errs, fmt.Errorf("SETTLEMENT_START_OFFSET %q must be first or last", w.StartOffset))
	}
	if w.Kafka.ConsumerGroup == "" {
		errs = append(errs, errors.New("SETTLEMENT_KAFKA_CONSUMER_GROUP is required"))
	}
	if w.RetryJitter < 0 || w.RetryJitter >= 1 {
		errs = append(errs, errors.New("SETTLEMENT_RETRY_JITTER must be in [0, 1)"))
	}
	if w.LaneBuffer < 0 {
		errs = append(errs, errors.New("SETTLEMENT_LANE_BUFFER must be >= 0"))
	}
	switch w.Kafka.RequiredAcks {
	case -1, 0, 1:
	default:
		errs = append(errs, errors.New("SETTLEMENT_KAFKA_REQUIRED_ACKS must be -1, 0, or 1"))
	}
	if w.ReconcilePageSize < 1 {
		errs = append(errs, errors.New("SETTLEMENT_RECONCILE_PAGE_SIZE must be >= 1"))
	}
	if w.Stripe.ApplicationFeeFlatMinor < 0 {
		errs = append(errs, errors.New("SETTLEMENT_APPLICATION_FEE_FLAT_MINOR must be >= 0"))
	}
	return w, errors.Join(errs...)
}

func loadKafka(errs *[]error) Kafka {
	k := Kafka{
		Brokers:             envListDefault("SETTLEMENT_KAFKA_BROKERS", []string{"localhost:9092"}),
		TopicStripe:         env("SETTLEMENT_KAFKA_TOPIC_STRIPE", "billing.stripe.events.v1"),
		TopicOpenMeter:      env("SETTLEMENT_KAFKA_TOPIC_OPENMETER", "billing.openmeter.invoices.v1"),
		TopicDLQ:            env("SETTLEMENT_KAFKA_TOPIC_DLQ", "billing.settlement.dlq.v1"),
		TopicCollectRequest: env("SETTLEMENT_KAFKA_TOPIC_COLLECT_REQUEST", "billing.collect.requests.v1"),
		ConsumerGroup:       env("SETTLEMENT_KAFKA_CONSUMER_GROUP", "settlement-worker"),
		SASLMechanism:       strings.ToLower(env("SETTLEMENT_KAFKA_SASL_MECHANISM", "")),
		SASLUsername:        env("SETTLEMENT_KAFKA_SASL_USERNAME", ""),
		SASLPassword:        env("SETTLEMENT_KAFKA_SASL_PASSWORD", ""),
		TLSEnabled:          envBool("SETTLEMENT_KAFKA_TLS", false, errs),
		RequiredAcks:        envInt("SETTLEMENT_KAFKA_REQUIRED_ACKS", -1, errs),
		WriteTimeout:        envDuration("SETTLEMENT_KAFKA_WRITE_TIMEOUT", 10*time.Second, errs),
		DialTimeout:         envDuration("SETTLEMENT_KAFKA_DIAL_TIMEOUT", 10*time.Second, errs),
	}
	switch k.SASLMechanism {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		*errs = append(*errs, fmt.Errorf("SETTLEMENT_KAFKA_SASL_MECHANISM %q is not supported", k.SASLMechanism))
	}
	if k.SASLMechanism != "" && (k.SASLUsername == "" || k.SASLPassword == "") {
		*errs = append(*errs, errors.New("SASL mechanism set but username/password missing"))
	}
	return k
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envList(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envListDefault(key string, def []string) []string {
	if v := envList(key); len(v) > 0 {
		return v
	}
	return def
}

func envInt(key string, def int, errs *[]error) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func envFloat(key string, def float64, errs *[]error) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return f
}

func envBool(key string, def bool, errs *[]error) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
