package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/openmeter"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		log.Printf("e2e config: %v", err)
		return 1
	}

	client := &http.Client{Timeout: 30 * time.Second}
	if err := healthCheck(ctx, client, cfg.ProducerURL, "/healthz"); err != nil {
		log.Printf("producer health: %v", err)
		return 1
	}
	if err := healthCheck(ctx, client, cfg.WorkerURL, "/healthz"); err != nil {
		log.Printf("worker health: %v", err)
		return 1
	}
	if err := healthCheck(ctx, client, cfg.StripefakeURL, "/healthz"); err != nil {
		log.Printf("stripefake health: %v", err)
		return 1
	}

	stripeBase := strings.TrimSpace(firstNonEmpty(os.Getenv("SETTLEMENT_STRIPE_API_BASE"), os.Getenv("E2E_STRIPE_API_BASE")))
	if stripeBase == "" {
		log.Printf("SETTLEMENT_STRIPE_API_BASE or E2E_STRIPE_API_BASE is required")
		return 1
	}
	if strings.Contains(strings.ToLower(stripeBase), "api.stripe.com") {
		log.Printf("refusing to run against production Stripe base %q", stripeBase)
		return 1
	}

	customerID, err := createKonnectCustomer(ctx, client, cfg)
	if err != nil {
		log.Printf("create customer: %v", err)
		return 1
	}
	if err := putBillingProfile(ctx, client, cfg, customerID); err != nil {
		log.Printf("billing profile override: %v", err)
		return 1
	}
	if err := createPendingLines(ctx, client, cfg, customerID); err != nil {
		log.Printf("pending lines: %v", err)
		return 1
	}

	invoiceID, err := createInvoice(ctx, client, cfg, customerID)
	if err != nil {
		log.Printf("create invoice: %v", err)
		return 1
	}
	if err := advanceInvoice(ctx, client, cfg, invoiceID); err != nil {
		log.Printf("advance invoice: %v", err)
		return 1
	}

	omClient := openmeter.New(config.OpenMeter{
		BaseURL: cfg.OpenMeterURL,
		APIKey:  cfg.OpenMeterAPIKey,
		Timeout: 30 * time.Second,
	})
	inv, err := waitForPaid(ctx, omClient, invoiceID)
	if err != nil {
		log.Printf("wait for paid: %v", err)
		return 1
	}

	state, err := stripeState(ctx, client, cfg)
	if err != nil {
		log.Printf("stripefake state: %v", err)
		return 1
	}
	if !stateHasAccount(state, cfg.ConnectAccountID) {
		log.Printf("stripefake never saw account %s", cfg.ConnectAccountID)
		return 1
	}
	if stateTotalFor(state, inv.ExternalIDs.Invoicing) <= 0 {
		log.Printf("stripefake state did not record a positive invoice total for %s", invoiceID)
		return 1
	}

	log.Printf("e2e ok: customer=%s invoice=%s stripe=%s paid", customerID, invoiceID, inv.ExternalIDs.Invoicing)
	return 0
}

type e2eConfig struct {
	OpenMeterURL     string
	OpenMeterAPIKey  string
	BillingProfileID string
	ProducerURL      string
	WorkerURL        string
	StripefakeURL    string
	ConnectAccountID string
}

func loadConfig() (e2eConfig, error) {
	cfg := e2eConfig{
		OpenMeterURL:     strings.TrimRight(os.Getenv("SETTLEMENT_OPENMETER_URL"), "/"),
		OpenMeterAPIKey:  strings.TrimSpace(os.Getenv("SETTLEMENT_OPENMETER_API_KEY")),
		BillingProfileID: strings.TrimSpace(os.Getenv("SETTLEMENT_E2E_BILLING_PROFILE_ID")),
		ProducerURL:      strings.TrimRight(os.Getenv("SETTLEMENT_E2E_PRODUCER_URL"), "/"),
		WorkerURL:        strings.TrimRight(os.Getenv("SETTLEMENT_E2E_WORKER_URL"), "/"),
		StripefakeURL:    strings.TrimRight(os.Getenv("SETTLEMENT_E2E_STRIPEFAKE_URL"), "/"),
		ConnectAccountID: firstNonEmpty(strings.TrimSpace(os.Getenv("SETTLEMENT_E2E_CONNECT_ACCOUNT_ID")), "acct_e2e_settlement"),
	}
	if cfg.OpenMeterURL == "" {
		return cfg, errors.New("SETTLEMENT_OPENMETER_URL is required")
	}
	if cfg.OpenMeterAPIKey == "" {
		return cfg, errors.New("SETTLEMENT_OPENMETER_API_KEY is required")
	}
	if cfg.BillingProfileID == "" {
		return cfg, errors.New("SETTLEMENT_E2E_BILLING_PROFILE_ID is required")
	}
	if cfg.ProducerURL == "" || cfg.WorkerURL == "" || cfg.StripefakeURL == "" {
		return cfg, errors.New("SETTLEMENT_E2E_PRODUCER_URL, SETTLEMENT_E2E_WORKER_URL and SETTLEMENT_E2E_STRIPEFAKE_URL are required")
	}
	return cfg, nil
}

func healthCheck(ctx context.Context, client *http.Client, baseURL, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func createKonnectCustomer(ctx context.Context, client *http.Client, cfg e2eConfig) (string, error) {
	payload := map[string]any{
		"metadata": map[string]string{
			"stripe_charge_model":       "direct",
			"stripe_connect_account_id": cfg.ConnectAccountID,
		},
	}
	var out map[string]any
	if err := postJSON(ctx, client, cfg.OpenMeterURL+"/api/v1/billing/customers", cfg.OpenMeterAPIKey, payload, &out); err != nil {
		return "", err
	}
	return extractID(out), nil
}

func putBillingProfile(ctx context.Context, client *http.Client, cfg e2eConfig, customerID string) error {
	payload := map[string]string{"billingProfileId": cfg.BillingProfileID}
	return putJSON(ctx, client, cfg.OpenMeterURL+"/api/v1/billing/customers/"+customerID, cfg.OpenMeterAPIKey, payload, nil)
}

func createPendingLines(ctx context.Context, client *http.Client, cfg e2eConfig, customerID string) error {
	payload := map[string]any{
		"lines": []map[string]any{{
			"name":        "E2E flat line",
			"description": "Konnect fake Stripe path",
			"currency":    "usd",
			"amountMinor": 1250,
			"quantity":    1,
		}},
	}
	return postJSON(ctx, client, cfg.OpenMeterURL+"/api/v1/billing/customers/"+customerID+"/invoices/pending-lines", cfg.OpenMeterAPIKey, payload, nil)
}

func createInvoice(ctx context.Context, client *http.Client, cfg e2eConfig, customerID string) (string, error) {
	payload := map[string]any{
		"customerId": customerID,
	}
	var out map[string]any
	if err := postJSON(ctx, client, cfg.OpenMeterURL+"/api/v1/billing/invoices/invoice", cfg.OpenMeterAPIKey, payload, &out); err != nil {
		return "", err
	}
	return extractID(out), nil
}

func advanceInvoice(ctx context.Context, client *http.Client, cfg e2eConfig, invoiceID string) error {
	return postJSON(ctx, client, cfg.OpenMeterURL+"/api/v1/billing/invoices/"+invoiceID+"/advance", cfg.OpenMeterAPIKey, map[string]any{}, nil)
}

func waitForPaid(ctx context.Context, omClient *openmeter.Client, invoiceID string) (*openmeter.Invoice, error) {
	deadline := time.NewTicker(2 * time.Second)
	defer deadline.Stop()

	for {
		inv, err := omClient.GetInvoice(ctx, invoiceID)
		if err == nil && strings.EqualFold(inv.Status, openmeter.StatusPaid) {
			return inv, nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return nil, err
			}
			return nil, ctx.Err()
		case <-deadline.C:
		}
	}
}

func stripeState(ctx context.Context, client *http.Client, cfg e2eConfig) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.StripefakeURL+"/_stripefake/v1/state", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("stripefake state: http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func postJSON(ctx context.Context, client *http.Client, url, apiKey string, payload, out any) error {
	return doJSON(ctx, client, http.MethodPost, url, apiKey, payload, out)
}

func putJSON(ctx context.Context, client *http.Client, url, apiKey string, payload, out any) error {
	return doJSON(ctx, client, http.MethodPut, url, apiKey, payload, out)
}

func doJSON(ctx context.Context, client *http.Client, method, url, apiKey string, payload, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func extractID(in map[string]any) string {
	if v, ok := in["id"].(string); ok && v != "" {
		return v
	}
	if data, ok := in["data"].(map[string]any); ok {
		if v, ok := data["id"].(string); ok && v != "" {
			return v
		}
		if customer, ok := data["customer"].(map[string]any); ok {
			if v, ok := customer["id"].(string); ok && v != "" {
				return v
			}
		}
	}
	return ""
}

func stateHasAccount(state map[string]any, account string) bool {
	entries, _ := state["invoices"].([]any)
	for _, entry := range entries {
		m, _ := entry.(map[string]any)
		if m["stripe_account"] == account {
			return true
		}
	}
	return false
}

func stateTotalFor(state map[string]any, invoiceID string) int64 {
	entries, _ := state["invoices"].([]any)
	for _, entry := range entries {
		m, _ := entry.(map[string]any)
		if m["id"] == invoiceID {
			switch v := m["total_minor"].(type) {
			case float64:
				return int64(v)
			case int64:
				return v
			}
		}
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
