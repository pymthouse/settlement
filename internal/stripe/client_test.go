package stripe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
)

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return New(config.Stripe{
		APIBase:    server.URL,
		SecretKey:  "sk_test_123",
		APIVersion: "2026-01-01",
		Timeout:    5 * time.Second,
		MaxRetries: 3,
	})
}

// Stripe-Account decides whose books the money lands on, and Idempotency-Key
// decides whether a retry creates a second invoice. Both must be sent.
func TestRequestOptionsBecomeHeaders(t *testing.T) {
	var account, idempotency, auth, version string

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account = r.Header.Get("Stripe-Account")
		idempotency = r.Header.Get("Idempotency-Key")
		auth = r.Header.Get("Authorization")
		version = r.Header.Get("Stripe-Version")
		writeJSON(w, `{"id":"in_1","status":"draft"}`)
	}))

	_, err := client.CreateInvoice(context.Background(), url.Values{"customer": {"cus_1"}},
		RequestOptions{Account: "acct_dev_1", IdempotencyKey: "settlement-invoice-inv_1"})
	if err != nil {
		t.Fatal(err)
	}

	if account != "acct_dev_1" {
		t.Errorf("Stripe-Account = %q", account)
	}
	if idempotency != "settlement-invoice-inv_1" {
		t.Errorf("Idempotency-Key = %q", idempotency)
	}
	if auth == "" {
		t.Error("no Authorization header was sent")
	}
	if version != "2026-01-01" {
		t.Errorf("Stripe-Version = %q", version)
	}
}

// Without an account header the call runs on the platform account, which is
// exactly what platform-mode invoices want.
func TestPlatformCallsSendNoAccountHeader(t *testing.T) {
	var account string
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account = r.Header.Get("Stripe-Account")
		writeJSON(w, `{"id":"in_1"}`)
	}))

	if _, err := client.CreateInvoice(context.Background(), url.Values{}, RequestOptions{}); err != nil {
		t.Fatal(err)
	}
	if account != "" {
		t.Errorf("Stripe-Account = %q, want empty for a platform call", account)
	}
}

func TestErrorsAreParsedFromStripesEnvelope(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Request-Id", "req_abc123")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, `{"error":{"type":"invalid_request_error","code":"parameter_invalid_integer",
			"param":"amount","message":"Invalid integer: abc"}}`)
	}))

	_, err := client.CreateInvoiceItem(context.Background(), url.Values{}, RequestOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}

	var sErr *Error
	if !asError(err, &sErr) {
		t.Fatalf("error type = %T, want *stripe.Error", err)
	}
	if sErr.Code != "parameter_invalid_integer" || sErr.Param != "amount" {
		t.Errorf("error not parsed: %+v", sErr)
	}
	if sErr.RequestID != "req_abc123" {
		t.Errorf("request id = %q; it is what support asks for", sErr.RequestID)
	}
	if !IsInvalidRequest(err) {
		t.Error("a 400 should classify as an invalid request")
	}
	if sErr.Retryable() {
		t.Error("a validation error must not be retried")
	}
}

func TestRetryability(t *testing.T) {
	cases := []struct {
		name string
		err  *Error
		want bool
	}{
		{"rate limited", &Error{StatusCode: http.StatusTooManyRequests}, true},
		{"server error", &Error{StatusCode: http.StatusBadGateway}, true},
		{"lock contention", &Error{StatusCode: http.StatusConflict, Code: "lock_timeout"}, true},
		{"validation", &Error{StatusCode: http.StatusBadRequest, Code: "parameter_missing"}, false},
		{"auth", &Error{StatusCode: http.StatusUnauthorized}, false},
		{"missing resource", &Error{StatusCode: http.StatusNotFound, Code: "resource_missing"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Retryable(); got != tc.want {
				t.Errorf("Retryable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTransientFailuresAreRetriedInternally(t *testing.T) {
	var calls atomic.Int32

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSON(w, `{"error":{"type":"api_error","message":"try later"}}`)
			return
		}
		writeJSON(w, `{"id":"in_1","status":"draft"}`)
	}))

	invoice, err := client.CreateInvoice(context.Background(), url.Values{},
		RequestOptions{IdempotencyKey: "settlement-invoice-inv_1"})
	if err != nil {
		t.Fatalf("transient failures were not ridden out: %v", err)
	}
	if invoice.ID != "in_1" {
		t.Errorf("invoice id = %q", invoice.ID)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("made %d attempts, want 3", got)
	}
}

func TestValidationFailuresAreNotRetried(t *testing.T) {
	var calls atomic.Int32

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, `{"error":{"type":"invalid_request_error","message":"no such customer"}}`)
	}))

	if _, err := client.CreateInvoice(context.Background(), url.Values{}, RequestOptions{}); err == nil {
		t.Fatal("expected an error")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("a permanent error was retried %d times", got)
	}
}

func TestIsNotFound(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, `{"error":{"type":"invalid_request_error","code":"resource_missing","message":"no such invoice"}}`)
	}))

	_, err := client.GetInvoice(context.Background(), "in_missing", RequestOptions{})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound = false for %v", err)
	}
}

func TestListInvoiceItemsPaginates(t *testing.T) {
	var calls atomic.Int32

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			if got := r.URL.Query().Get("invoice"); got != "in_1" {
				t.Errorf("invoice filter = %q", got)
			}
			writeJSON(w, `{"data":[{"id":"ii_1","amount":100},{"id":"ii_2","amount":200}],"has_more":true}`)
			return
		}
		if got := r.URL.Query().Get("starting_after"); got != "ii_2" {
			t.Errorf("starting_after = %q, want the last id of the previous page", got)
		}
		writeJSON(w, `{"data":[{"id":"ii_3","amount":300}],"has_more":false}`)
	}))

	items, err := client.ListInvoiceItems(context.Background(), "in_1", RequestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items across pages, want 3", len(items))
	}
}

func TestFinalizeInvoicePassesAutoAdvance(t *testing.T) {
	var gotPath, gotAutoAdvance string
	var gotExpand []string

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = r.ParseForm()
		gotAutoAdvance = r.PostForm.Get("auto_advance")
		gotExpand = r.PostForm["expand[]"]
		writeJSON(w, `{
			"id":"in_1","status":"open","number":"ABC-001",
			"payments":{"data":[{"id":"inpay_1","is_default":true,"payment":{"type":"payment_intent","payment_intent":"pi_1"}}]}
		}`)
	}))

	invoice, err := client.FinalizeInvoice(context.Background(), "in_1", true, RequestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/invoices/in_1/finalize" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAutoAdvance != "true" {
		t.Errorf("auto_advance = %q", gotAutoAdvance)
	}
	if len(gotExpand) == 0 || gotExpand[0] != "payments" {
		t.Errorf("expand[] = %v, want payments", gotExpand)
	}
	if invoice.Number != "ABC-001" {
		t.Errorf("finalized invoice number = %q", invoice.Number)
	}
	pi, err := invoice.PrimaryPaymentIntent()
	if err != nil || pi != "pi_1" {
		t.Errorf("PrimaryPaymentIntent() = %q, %v; want pi_1", pi, err)
	}
}

func TestUnkeyedWriteRetriesAreSkipped(t *testing.T) {
	var calls atomic.Int32

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, `{"error":{"type":"api_error","message":"try later"}}`)
	}))

	_, err := client.CreateInvoice(context.Background(), url.Values{}, RequestOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("unkeyed write was retried %d times; a 5xx without Idempotency-Key must not replay", got)
	}
}

// --- helpers -------------------------------------------------------------

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

func TestSecretForSelectsSandboxKey(t *testing.T) {
	live := true
	sandbox := false
	c := New(config.Stripe{
		SecretKey:        "sk_live_x",
		SandboxSecretKey: "sk_test_x",
		Timeout:          time.Second,
	})
	got, err := c.SecretFor(RequestOptions{Livemode: &live})
	if err != nil || got != "sk_live_x" {
		t.Fatalf("live: got %q err %v", got, err)
	}
	got, err = c.SecretFor(RequestOptions{Livemode: &sandbox})
	if err != nil || got != "sk_test_x" {
		t.Fatalf("sandbox: got %q err %v", got, err)
	}
	got, err = c.SecretFor(RequestOptions{})
	if err != nil || got != "sk_live_x" {
		t.Fatalf("default: got %q err %v", got, err)
	}
	c2 := New(config.Stripe{SecretKey: "sk_live_x", Timeout: time.Second})
	if _, err := c2.SecretFor(RequestOptions{Livemode: &sandbox}); err == nil {
		t.Fatal("expected error when sandbox key missing")
	}
}
