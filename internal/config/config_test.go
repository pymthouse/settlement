package config

import (
	"strings"
	"testing"
	"time"
)

// minimalWorkerEnv is the smallest set of variables that should boot a worker.
func minimalWorkerEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SETTLEMENT_OPENMETER_URL", "http://openmeter:8888")
	t.Setenv("SETTLEMENT_STRIPE_SECRET_KEY", "sk_test_123")
	t.Setenv("SETTLEMENT_KAFKA_CONSUMER_GROUP", "settlement-worker")
}

func TestLoadWorkerDefaults(t *testing.T) {
	minimalWorkerEnv(t)

	cfg, err := LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}

	if cfg.Stripe.DefaultChargeModel != ChargeModelPlatform {
		t.Errorf("default charge model = %q, want platform (the no-Connect default)", cfg.Stripe.DefaultChargeModel)
	}
	if cfg.Kafka.RequiredAcks != -1 {
		t.Errorf("required acks = %d, want -1 (all) for financial events", cfg.Kafka.RequiredAcks)
	}
	if cfg.OnRetryExhausted != PolicyDLQ {
		t.Errorf("retry policy = %q, want dlq", cfg.OnRetryExhausted)
	}
	if cfg.Dedupe.TTL != 720*time.Hour {
		t.Errorf("dedupe TTL = %s, want 30 days", cfg.Dedupe.TTL)
	}
	if cfg.Stripe.ApplicationFeeBps != 0 {
		t.Errorf("application fee defaults to %d bps; it must be opt-in", cfg.Stripe.ApplicationFeeBps)
	}
	if len(cfg.OpenMeter.DraftSyncStatuses) == 0 || cfg.OpenMeter.DraftSyncStatuses[0] != "draft.sync" {
		t.Errorf("draft sync statuses = %v", cfg.OpenMeter.DraftSyncStatuses)
	}
}

func TestLoadWorkerRequiresItsUpstreams(t *testing.T) {
	t.Setenv("SETTLEMENT_KAFKA_CONSUMER_GROUP", "g")

	_, err := LoadWorker()
	if err == nil {
		t.Fatal("a worker with no OpenMeter URL or Stripe key was accepted")
	}
	for _, want := range []string{"SETTLEMENT_OPENMETER_URL", "SETTLEMENT_STRIPE_SECRET_KEY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestLoadWorkerRejectsInvalidValues(t *testing.T) {
	cases := map[string]struct {
		key, value string
		wantIn     string
	}{
		"charge model":      {"SETTLEMENT_STRIPE_CHARGE_MODEL", "reseller", "CHARGE_MODEL"},
		"retry policy":      {"SETTLEMENT_ON_RETRY_EXHAUSTED", "ignore", "ON_RETRY_EXHAUSTED"},
		"start offset":      {"SETTLEMENT_START_OFFSET", "middle", "START_OFFSET"},
		"collection method": {"SETTLEMENT_STRIPE_COLLECTION_METHOD", "carrier_pigeon", "COLLECTION_METHOD"},
		"fee out of range":  {"SETTLEMENT_APPLICATION_FEE_BPS", "20000", "APPLICATION_FEE_BPS"},
		"lanes":             {"SETTLEMENT_LANES", "0", "LANES"},
		"max attempts":      {"SETTLEMENT_MAX_ATTEMPTS", "0", "MAX_ATTEMPTS"},
		"bad duration":      {"SETTLEMENT_RETRY_BASE_DELAY", "soon", "RETRY_BASE_DELAY"},
		"bad sasl":          {"SETTLEMENT_KAFKA_SASL_MECHANISM", "kerberos", "SASL_MECHANISM"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			minimalWorkerEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := LoadWorker()
			if err == nil {
				t.Fatalf("%s=%q was accepted", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("error does not mention %s: %v", tc.wantIn, err)
			}
		})
	}
}

// Bad configuration should report everything wrong at once, not one item per
// restart.
func TestLoadWorkerReportsAllProblemsTogether(t *testing.T) {
	t.Setenv("SETTLEMENT_KAFKA_CONSUMER_GROUP", "g")
	t.Setenv("SETTLEMENT_STRIPE_CHARGE_MODEL", "nonsense")

	_, err := LoadWorker()
	if err == nil {
		t.Fatal("expected an error")
	}
	message := err.Error()
	if !strings.Contains(message, "SETTLEMENT_OPENMETER_URL") || !strings.Contains(message, "CHARGE_MODEL") {
		t.Errorf("only some problems were reported: %v", err)
	}
}

func TestLoadProducerRequiresAtLeastOneSecret(t *testing.T) {
	if _, err := LoadProducer(); err == nil {
		t.Fatal("a doorman with no webhook secrets was accepted; it would forward unverified bodies")
	}

	t.Setenv("SETTLEMENT_STRIPE_WEBHOOK_SECRETS", "whsec_a")
	cfg, err := LoadProducer()
	if err != nil {
		t.Fatalf("LoadProducer: %v", err)
	}
	if len(cfg.StripeWebhookSecrets) != 1 {
		t.Errorf("secrets = %v", cfg.StripeWebhookSecrets)
	}
}

func TestSecretListsSupportRotation(t *testing.T) {
	t.Setenv("SETTLEMENT_STRIPE_WEBHOOK_SECRETS", " whsec_old , whsec_new ,, ")

	cfg, err := LoadProducer()
	if err != nil {
		t.Fatalf("LoadProducer: %v", err)
	}
	if len(cfg.StripeWebhookSecrets) != 2 {
		t.Fatalf("secrets = %v, want two entries with blanks trimmed", cfg.StripeWebhookSecrets)
	}
	if cfg.StripeWebhookSecrets[0] != "whsec_old" || cfg.StripeWebhookSecrets[1] != "whsec_new" {
		t.Errorf("secrets were not trimmed: %v", cfg.StripeWebhookSecrets)
	}
}

func TestKafkaSASLRequiresCredentials(t *testing.T) {
	t.Setenv("SETTLEMENT_STRIPE_WEBHOOK_SECRETS", "whsec_a")
	t.Setenv("SETTLEMENT_KAFKA_SASL_MECHANISM", "scram-sha-512")

	if _, err := LoadProducer(); err == nil {
		t.Fatal("SASL was enabled without credentials")
	}
}

func TestValidChargeModel(t *testing.T) {
	for _, model := range []ChargeModel{ChargeModelDirect, ChargeModelDestination, ChargeModelPlatform} {
		if !ValidChargeModel(model) {
			t.Errorf("%q should be valid", model)
		}
	}
	for _, model := range []ChargeModel{"", "reseller", "DIRECT"} {
		if ValidChargeModel(model) {
			t.Errorf("%q should be invalid", model)
		}
	}
}
