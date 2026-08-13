package openmeter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
)

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return New(config.OpenMeter{BaseURL: server.URL, APIKey: "om_test_key", Timeout: 5 * time.Second})
}

// The three completion calls are the only writes settlement makes; their paths
// and bodies must match the Custom Invoicing contract exactly.
func TestCompletionCallsHitTheDocumentedEndpoints(t *testing.T) {
	var gotPath, gotAuth, gotBody string

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))

	ctx := context.Background()
	sentAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	t.Run("draft synchronized", func(t *testing.T) {
		err := client.DraftSynchronized(ctx, "inv_1", DraftSynchronizedRequest{
			Invoicing: &SyncResult{
				ExternalID:              "in_stripe_1",
				LineExternalIDs:         []LineExternalIDMapping{{LineID: "line_1", ExternalID: "ii_1"}},
				LineDiscountExternalIDs: []LineDiscountIDMapping{{LineDiscountID: "disc_1", ExternalID: "ii_1"}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if want := "/api/v1/apps/custom-invoicing/inv_1/draft/synchronized"; gotPath != want {
			t.Errorf("path = %q, want %q", gotPath, want)
		}
		if gotAuth != "Bearer om_test_key" {
			t.Errorf("authorization = %q", gotAuth)
		}

		var decoded map[string]any
		if err := json.Unmarshal([]byte(gotBody), &decoded); err != nil {
			t.Fatal(err)
		}
		invoicing, ok := decoded["invoicing"].(map[string]any)
		if !ok {
			t.Fatalf("body has no invoicing block: %s", gotBody)
		}
		if invoicing["externalId"] != "in_stripe_1" {
			t.Errorf("externalId = %v", invoicing["externalId"])
		}
		if _, ok := invoicing["lineExternalIds"]; !ok {
			t.Error("lineExternalIds missing; the mapping cannot be supplied later")
		}
		if _, ok := invoicing["lineDiscountExternalIds"]; !ok {
			t.Error("lineDiscountExternalIds missing")
		}
	})

	t.Run("issuing synchronized", func(t *testing.T) {
		err := client.IssuingSynchronized(ctx, "inv_1", FinalizedRequest{
			Invoicing: &FinalizedInvoicingRequest{InvoiceNumber: "STRIPE-1", SentToCustomerA: &sentAt},
			Payment:   &FinalizedPaymentRequest{ExternalID: "pi_1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if want := "/api/v1/apps/custom-invoicing/inv_1/issuing/synchronized"; gotPath != want {
			t.Errorf("path = %q, want %q", gotPath, want)
		}

		var decoded struct {
			Invoicing struct {
				InvoiceNumber   string `json:"invoiceNumber"`
				SentToCustomerA string `json:"sentToCustomerAt"`
			} `json:"invoicing"`
			Payment struct {
				ExternalID string `json:"externalId"`
			} `json:"payment"`
		}
		if err := json.Unmarshal([]byte(gotBody), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Invoicing.InvoiceNumber != "STRIPE-1" {
			t.Errorf("invoiceNumber = %q", decoded.Invoicing.InvoiceNumber)
		}
		if decoded.Payment.ExternalID != "pi_1" {
			t.Errorf("payment externalId = %q — the payment reference is stamped here, not at payment time", decoded.Payment.ExternalID)
		}
	})

	t.Run("payment status", func(t *testing.T) {
		if err := client.UpdatePaymentStatus(ctx, "inv_1", TriggerPaid); err != nil {
			t.Fatal(err)
		}
		if want := "/api/v1/apps/custom-invoicing/inv_1/payment/status"; gotPath != want {
			t.Errorf("path = %q, want %q", gotPath, want)
		}
		if gotBody := gotBody; gotBody != `{"trigger":"paid"}`+"\n" && gotBody != `{"trigger":"paid"}` {
			t.Errorf("body = %q, want a bare trigger", gotBody)
		}
	})
}

// Omitting the invoice number lets OpenMeter generate an INV- number; an empty
// string must not be sent, or it would fail the 1..256 character constraint.
func TestOmittedFieldsAreNotSerialized(t *testing.T) {
	var gotBody string
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))

	err := client.IssuingSynchronized(context.Background(), "inv_1", FinalizedRequest{
		Invoicing: &FinalizedInvoicingRequest{},
	})
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(gotBody), &decoded); err != nil {
		t.Fatal(err)
	}
	invoicing := decoded["invoicing"].(map[string]any)
	if _, present := invoicing["invoiceNumber"]; present {
		t.Errorf("an empty invoiceNumber was sent: %s", gotBody)
	}
	if _, present := decoded["payment"]; present {
		t.Errorf("an empty payment block was sent: %s", gotBody)
	}
}

func TestAPIErrorRetryability(t *testing.T) {
	cases := map[int]bool{
		http.StatusBadRequest:          false,
		http.StatusUnauthorized:        false,
		http.StatusForbidden:           false,
		http.StatusNotFound:            false,
		http.StatusUnprocessableEntity: false,
		http.StatusConflict:            true, // concurrent mutation; the next try usually wins
		http.StatusTooManyRequests:     true,
		http.StatusRequestTimeout:      true,
		http.StatusInternalServerError: true,
		http.StatusBadGateway:          true,
		http.StatusServiceUnavailable:  true,
	}

	for status, wantRetryable := range cases {
		err := &APIError{StatusCode: status, Operation: "test"}
		if got := err.Retryable(); got != wantRetryable {
			t.Errorf("status %d: Retryable() = %v, want %v", status, got, wantRetryable)
		}
	}
}

func TestErrorHelpers(t *testing.T) {
	notFound := &APIError{StatusCode: http.StatusNotFound}
	conflict := &APIError{StatusCode: http.StatusConflict}
	alreadyPaid := &APIError{
		StatusCode: http.StatusBadRequest,
		Body:       `{"message":"invoice is already paid; no leaving transition for trigger_paid"}`,
	}
	premature := &APIError{
		StatusCode: http.StatusBadRequest,
		Body:       `{"detail":"No valid leaving transitions are permitted from state 'issuing.syncing' for trigger 'trigger_paid'"}`,
	}
	otherBad := &APIError{StatusCode: http.StatusBadRequest, Body: `{"message":"invalid trigger"}`}

	if !IsNotFound(notFound) || IsNotFound(conflict) {
		t.Error("IsNotFound misclassified an error")
	}
	if !IsConflict(conflict) || IsConflict(notFound) {
		t.Error("IsConflict misclassified an error")
	}
	if IsNotFound(nil) || IsConflict(nil) {
		t.Error("nil should not classify as an API error")
	}
	if !IsAlreadyApplied(conflict) || !IsAlreadyApplied(alreadyPaid) {
		t.Error("IsAlreadyApplied should accept 409 and already-paid 4xx")
	}
	if IsAlreadyApplied(premature) {
		t.Error("IsAlreadyApplied must not swallow premature trigger_paid from issuing.syncing")
	}
	if !IsPrematurePaymentTrigger(premature) || IsPrematurePaymentTrigger(alreadyPaid) {
		t.Error("IsPrematurePaymentTrigger misclassified")
	}
	if IsAlreadyApplied(otherBad) || IsAlreadyApplied(nil) {
		t.Error("IsAlreadyApplied misclassified an unrelated error")
	}
}

func TestInvoicePendingLines(t *testing.T) {
	t.Run("posts the raise request and decodes the created invoices", func(t *testing.T) {
		var gotPath, gotMethod, gotBody string
		client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotMethod = r.Method
			body, _ := io.ReadAll(r.Body)
			gotBody = string(body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `[{"id":"inv_om_1"},{"id":"inv_om_2"}]`)
		}))

		results, err := client.InvoicePendingLines(context.Background(), "cus_om_1")
		if err != nil {
			t.Fatal(err)
		}
		if want := "/api/v1/billing/invoices/invoice"; gotPath != want {
			t.Errorf("path = %q, want %q", gotPath, want)
		}
		if gotMethod != http.MethodPost {
			t.Errorf("method = %q, want POST", gotMethod)
		}

		var decoded map[string]any
		if err := json.Unmarshal([]byte(gotBody), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["customerId"] != "cus_om_1" {
			t.Errorf("customerId = %v, want cus_om_1", decoded["customerId"])
		}
		if decoded["progressiveBillingOverride"] != true {
			t.Errorf("progressiveBillingOverride = %v, want true", decoded["progressiveBillingOverride"])
		}

		if len(results) != 2 || results[0].ID != "inv_om_1" || results[1].ID != "inv_om_2" {
			t.Errorf("results = %+v, want [inv_om_1 inv_om_2]", results)
		}
	})

	// This is the collision this whole feature exists to absorb: a second
	// raise landing on Konnect while the customer's prior invoice is still
	// mid-realization. It must come back as the sentinel error, not a bare
	// APIError, so HandleCollectRequest can treat it as a no-op instead of a
	// retryable failure.
	t.Run("classifies the stuck-invoice 400 as ErrRealizationRunActive", func(t *testing.T) {
		client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"message":"an ACTIVE Realization run already exists for this customer"}`)
		}))

		_, err := client.InvoicePendingLines(context.Background(), "cus_om_1")
		if !errors.Is(err, ErrRealizationRunActive) {
			t.Errorf("err = %v, want ErrRealizationRunActive", err)
		}
	})

	t.Run("leaves unrelated 400s as plain API errors", func(t *testing.T) {
		client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"message":"customer not found"}`)
		}))

		_, err := client.InvoicePendingLines(context.Background(), "cus_missing")
		if errors.Is(err, ErrRealizationRunActive) {
			t.Error("unrelated 400 must not be misclassified as ErrRealizationRunActive")
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
			t.Errorf("err = %v, want a plain *APIError(400)", err)
		}
	})
}

func TestGetInvoiceDecodesTheLifecycleFields(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("expand") != "lines" {
			t.Errorf("lines were not expanded: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"inv_1","currency":"USD","status":"draft","number":"INV-1",
			"statusDetails":{"immutable":false,"failed":false,"extendedStatus":"draft.sync",
				"availableActions":{"approve":{"resultingState":"payment_processing.pending"},
				                    "delete":{"resultingState":"deleted"}}},
			"customer":{"id":"cus_1","key":"owner-42"},
			"totals":{"total":"42.50"},
			"externalIds":{"invoicing":"in_stripe_1","payment":"pi_1"},
			"lines":[{"id":"line_1","totals":{"total":"30.00"},
				"children":[{"id":"line_1_child","totals":{"total":"30.00"}}],
				"discounts":{"amount":[{"id":"disc_1","amount":"5.00"}]}}]
		}`)
	}))

	invoice, err := client.GetInvoice(context.Background(), "inv_1")
	if err != nil {
		t.Fatal(err)
	}

	if invoice.StatusDetails.ExtendedStatus != "draft.sync" {
		t.Errorf("extendedStatus = %q", invoice.StatusDetails.ExtendedStatus)
	}
	if invoice.StatusDetails.AvailableActions.Approve == nil ||
		invoice.StatusDetails.AvailableActions.Approve.ResultingState != "payment_processing.pending" {
		t.Error("availableActions.approve was not decoded")
	}
	if invoice.ExternalIDs.Invoicing != "in_stripe_1" {
		t.Errorf("external invoicing id = %q", invoice.ExternalIDs.Invoicing)
	}
	if len(invoice.Lines) != 1 || len(invoice.Lines[0].Children) != 1 {
		t.Fatalf("lines were not decoded: %+v", invoice.Lines)
	}
	if got := invoice.Lines[0].LineDiscountIDs(); len(got) != 1 || got[0] != "disc_1" {
		t.Errorf("discount ids = %v", got)
	}
	if got := invoice.Lines[0].ChildIDs(); len(got) != 1 || got[0] != "line_1_child" {
		t.Errorf("child ids = %v", got)
	}
	if got := len(invoice.AllLines()); got != 2 {
		t.Errorf("AllLines returned %d lines, want parent and child", got)
	}
}

func TestNeedsAttention(t *testing.T) {
	cases := []struct {
		name string
		inv  Invoice
		want bool
	}{
		{"healthy draft", Invoice{Status: StatusDraft}, false},
		{"paid", Invoice{Status: StatusPaid}, false},
		{"failed sync", Invoice{Status: StatusIssuing, StatusDetails: StatusDetails{Failed: true}}, true},
		{"overdue", Invoice{Status: StatusOverdue}, true},
		{"uncollectible", Invoice{Status: StatusUncollectible}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.inv.NeedsAttention(); got != tc.want {
				t.Errorf("NeedsAttention() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBillableLinesSkipsDeletedLines(t *testing.T) {
	invoice := Invoice{Lines: []Line{
		{ID: "keep"},
		{ID: "gone", Status: "deleted"},
	}}
	billable := invoice.BillableLines()
	if len(billable) != 1 || billable[0].ID != "keep" {
		t.Fatalf("BillableLines = %+v, want only the live line", billable)
	}
}

func TestIsKonnectURL(t *testing.T) {
	cases := map[string]bool{
		"https://us.api.konghq.com/v3/openmeter": true,
		"https://eu.api.konghq.com/v3/openmeter": true,
		"https://localhost:8080":                 false,
		"http://openmeter:8888":                  false,
		"https://billing.konnect.example.com":    true,
	}
	for url, want := range cases {
		if got := isKonnectURL(url); got != want {
			t.Errorf("isKonnectURL(%q) = %v, want %v", url, got, want)
		}
	}
}

func TestKonnectRewritePath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/api/v1/billing/invoices/inv_1?expand=lines", "/billing/invoices/inv_1?expand=lines"},
		{"/api/v1/apps/custom-invoicing/inv_1/draft/synchronized", "/apps/custom-invoicing/inv_1/draft/synchronized"},
		{"/api/v1/customers/cust_1", "/customers/cust_1"},
		{"/api/v2/customers/cust_1/entitlements", "/customers/cust_1/entitlements"},
		{"/billing/invoices/inv_1", "/billing/invoices/inv_1"},
	}
	for _, tc := range cases {
		if got := konnectRewritePath(tc.in); got != tc.want {
			t.Errorf("konnectRewritePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestKonnectClientRewritesPaths(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"inv_1","status":"draft","currency":"USD",
			"customer":{"id":"c1"},"totals":{"total":"0"},
			"statusDetails":{"immutable":false,"failed":false,"extendedStatus":"draft","availableActions":{}},
			"externalIds":{},"lines":[]}`)
	}))
	t.Cleanup(server.Close)

	client := &Client{
		baseURL:       server.URL + "/v3/openmeter",
		meteringV1URL: server.URL + "/metering/v1",
		apiKey:        "test",
		isKonnect:     true,
		http:          &http.Client{Timeout: 5 * time.Second},
	}

	_, err := client.GetInvoice(context.Background(), "inv_1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/metering/v1/billing/invoices/inv_1"; gotPath != want {
		t.Errorf("Konnect path = %q, want %q (invoice GET must use metering/v1)", gotPath, want)
	}

	gotPath = ""
	if err := client.DraftSynchronized(context.Background(), "inv_1", DraftSynchronizedRequest{}); err != nil {
		t.Fatal(err)
	}
	if want := "/metering/v1/apps/custom-invoicing/inv_1/draft/synchronized"; gotPath != want {
		t.Errorf("Konnect draft sync path = %q, want %q", gotPath, want)
	}
}

func TestKonnectMeteringV1Base(t *testing.T) {
	got := konnectMeteringV1Base("https://us.api.konghq.com/v3/openmeter")
	if want := "https://us.api.konghq.com/metering/v1"; got != want {
		t.Errorf("konnectMeteringV1Base = %q, want %q", got, want)
	}
}

func TestKonnectNeedsMeteringV1(t *testing.T) {
	if !konnectNeedsMeteringV1("/billing/invoices/inv_1?expand=lines", http.MethodGet) {
		t.Error("invoice GET should use metering/v1")
	}
	if !konnectNeedsMeteringV1("/apps/custom-invoicing/inv_1/draft/synchronized", http.MethodPost) {
		t.Error("draft sync should use metering/v1")
	}
	if !konnectNeedsMeteringV1("/customers/cust_1", http.MethodGet) {
		t.Error("customer GET should use metering/v1 (metadata vs labels)")
	}
	if !konnectNeedsMeteringV1("/billing/invoices/invoice", http.MethodPost) {
		t.Error("invoice-pending-lines POST should use metering/v1")
	}
	if konnectNeedsMeteringV1("/billing/invoices/invoice", http.MethodDelete) {
		t.Error("the raise rule is POST-only; it should not match other verbs on the same literal path")
	}
}

func TestCustomerMetadataMergesLabels(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{
			"id":"cust_1",
			"metadata":{"stripe_charge_model":"direct"},
			"labels":{"stripe_connect_account_id":"acct_123","stripe_charge_model":"destination"}
		}`)
	}))
	t.Cleanup(server.Close)

	client := &Client{
		baseURL:       server.URL + "/v3/openmeter",
		meteringV1URL: server.URL + "/metering/v1",
		apiKey:        "test",
		isKonnect:     true,
		http:          &http.Client{Timeout: 5 * time.Second},
	}

	meta, err := client.CustomerMetadata(context.Background(), "cust_1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/metering/v1/customers/cust_1"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if meta.Get("stripe_connect_account_id") != "acct_123" {
		t.Errorf("connect account = %q, want acct_123 from labels", meta.Get("stripe_connect_account_id"))
	}
	// metadata wins over labels on key collision
	if meta.Get("stripe_charge_model") != "direct" {
		t.Errorf("charge model = %q, want direct from metadata", meta.Get("stripe_charge_model"))
	}
}

func TestNormalizedTypeStripsThePrefix(t *testing.T) {
	if got := (Notification{Type: "invoicing.invoice.updated"}).NormalizedType(); got != EventInvoiceUpdated {
		t.Errorf("NormalizedType = %q, want %q", got, EventInvoiceUpdated)
	}
	if got := (Notification{Type: "invoice.created"}).NormalizedType(); got != EventInvoiceCreated {
		t.Errorf("NormalizedType = %q", got)
	}
}
