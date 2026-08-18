package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type settlementMetadataFixture struct {
	ChargeModelKey    string            `json:"charge_model_key"`
	ConnectAccountKey string            `json:"connect_account_key"`
	StripeCustomerKey string            `json:"stripe_customer_key"`
	LivemodeKey       string            `json:"livemode_key"`
	ConfigEnvDefaults map[string]string `json:"config_env_defaults"`
	E2E               e2eFixture        `json:"e2e"`
}

type e2eFixture struct {
	ChargeModel      string `json:"charge_model"`
	ConnectAccountID string `json:"connect_account_id"`
	Livemode         string `json:"livemode"`
}

func TestPymthouseSettlementMetadataContract(t *testing.T) {
	fixture := loadSettlementMetadataFixture(t)
	if fixture.ChargeModelKey != "stripe_charge_model" {
		t.Fatalf("charge_model_key = %q, want stripe_charge_model", fixture.ChargeModelKey)
	}
	if fixture.ConnectAccountKey != "stripe_connect_account_id" {
		t.Fatalf("connect_account_key = %q, want stripe_connect_account_id", fixture.ConnectAccountKey)
	}
	if fixture.StripeCustomerKey != "stripe_customer_id" {
		t.Fatalf("stripe_customer_key = %q, want stripe_customer_id", fixture.StripeCustomerKey)
	}
	if fixture.LivemodeKey != "stripe_livemode" {
		t.Fatalf("livemode_key = %q, want stripe_livemode", fixture.LivemodeKey)
	}
	if fixture.E2E.ChargeModel != "direct" {
		t.Fatalf("e2e.charge_model = %q, want direct", fixture.E2E.ChargeModel)
	}
	if fixture.E2E.ConnectAccountID != "acct_e2e_settlement" {
		t.Fatalf("e2e.connect_account_id = %q, want acct_e2e_settlement", fixture.E2E.ConnectAccountID)
	}
	if fixture.E2E.Livemode != "true" {
		t.Fatalf("e2e.livemode = %q, want true", fixture.E2E.Livemode)
	}

	minimalWorkerEnv(t)
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}

	wantDefaults := map[string]string{
		"SETTLEMENT_CONNECT_ACCOUNT_METADATA_KEY": fixture.ConfigEnvDefaults["SETTLEMENT_CONNECT_ACCOUNT_METADATA_KEY"],
		"SETTLEMENT_CHARGE_MODEL_METADATA_KEY":    fixture.ConfigEnvDefaults["SETTLEMENT_CHARGE_MODEL_METADATA_KEY"],
		"SETTLEMENT_STRIPE_CUSTOMER_METADATA_KEY": fixture.ConfigEnvDefaults["SETTLEMENT_STRIPE_CUSTOMER_METADATA_KEY"],
		"SETTLEMENT_LIVEMODE_METADATA_KEY":        fixture.ConfigEnvDefaults["SETTLEMENT_LIVEMODE_METADATA_KEY"],
	}
	if cfg.Stripe.ConnectAccountMetadataKey != wantDefaults["SETTLEMENT_CONNECT_ACCOUNT_METADATA_KEY"] {
		t.Fatalf("ConnectAccountMetadataKey = %q, want %q", cfg.Stripe.ConnectAccountMetadataKey, wantDefaults["SETTLEMENT_CONNECT_ACCOUNT_METADATA_KEY"])
	}
	if cfg.Stripe.ChargeModelMetadataKey != wantDefaults["SETTLEMENT_CHARGE_MODEL_METADATA_KEY"] {
		t.Fatalf("ChargeModelMetadataKey = %q, want %q", cfg.Stripe.ChargeModelMetadataKey, wantDefaults["SETTLEMENT_CHARGE_MODEL_METADATA_KEY"])
	}
	if cfg.Stripe.CustomerMetadataKey != wantDefaults["SETTLEMENT_STRIPE_CUSTOMER_METADATA_KEY"] {
		t.Fatalf("CustomerMetadataKey = %q, want %q", cfg.Stripe.CustomerMetadataKey, wantDefaults["SETTLEMENT_STRIPE_CUSTOMER_METADATA_KEY"])
	}
	if cfg.Stripe.LivemodeMetadataKey != wantDefaults["SETTLEMENT_LIVEMODE_METADATA_KEY"] {
		t.Fatalf("LivemodeMetadataKey = %q, want %q", cfg.Stripe.LivemodeMetadataKey, wantDefaults["SETTLEMENT_LIVEMODE_METADATA_KEY"])
	}
}

func loadSettlementMetadataFixture(t *testing.T) settlementMetadataFixture {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	path := filepath.Join(root, "testdata", "pymthouse-settlement-metadata.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var out settlementMetadataFixture
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	return out
}
