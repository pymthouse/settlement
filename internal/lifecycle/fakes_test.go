package lifecycle

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
	"github.com/pymthouse/settlement/internal/stripefake"
)

// fakeStripe is a thin test wrapper around internal/stripefake.
type fakeStripe struct {
	server *httptest.Server
	inner  *stripefake.Server
}

func newFakeStripe(t *testing.T) *fakeStripe {
	t.Helper()

	inner := stripefake.New(stripefake.Config{})
	server := httptest.NewServer(inner.Handler())
	t.Cleanup(server.Close)
	return &fakeStripe{
		server: server,
		inner:  inner,
	}
}

func (f *fakeStripe) URL() string {
	return f.server.URL
}

func (f *fakeStripe) FailNext(path string, count int) {
	f.inner.FailNext(path, count)
}

func (f *fakeStripe) SetInvoiceStatus(id, status string) {
	f.inner.SetInvoiceStatus(id, status)
}

func (f *fakeStripe) ItemsFor(invoiceID string) []stripe.InvoiceItem {
	return f.inner.ItemsFor(invoiceID)
}

func (f *fakeStripe) OnlyInvoice(t *testing.T) *stripe.Invoice {
	t.Helper()
	invoice := f.inner.OnlyInvoice()
	if invoice == nil {
		t.Fatal("expected exactly one Stripe invoice")
	}
	return invoice
}

func (f *fakeStripe) CallCount(method, path string) int {
	return f.inner.CallCount(method, path)
}

func (f *fakeStripe) AccountsSeen() map[string]bool {
	return f.inner.AccountsSeen()
}

func (f *fakeStripe) Snapshot() stripefake.Snapshot {
	return f.inner.Snapshot()
}

func (f *fakeStripe) Inspect() stripefake.Snapshot {
	return f.inner.Inspect()
}

func (f *fakeStripe) itemsFor(invoiceID string) []stripe.InvoiceItem {
	return f.ItemsFor(invoiceID)
}

func (f *fakeStripe) onlyInvoice(t *testing.T) *stripe.Invoice {
	return f.OnlyInvoice(t)
}

func (f *fakeStripe) callCount(method, path string) int {
	return f.CallCount(method, path)
}

func (f *fakeStripe) accountsSeen() map[string]bool {
	return f.AccountsSeen()
}

// fakeOpenMeter is an in-memory stand-in for the OpenMeter billing API.
type fakeOpenMeter struct {
	mu sync.Mutex

	server *httptest.Server

	invoices          map[string]openmeter.Invoice
	customerMetadata  map[string]openmeter.Metadata
	draftSynchronized map[string]openmeter.DraftSynchronizedRequest
	issuingSync       map[string]openmeter.FinalizedRequest
	paymentTriggers   map[string][]string
	// conflictOn makes a completion endpoint answer 409 once.
	conflictOn map[string]bool
	// listPages is what ListInvoices returns.
	listPages []openmeter.Invoice
}

func newFakeOpenMeter(t *testing.T) *fakeOpenMeter {
	t.Helper()

	f := &fakeOpenMeter{
		invoices:          map[string]openmeter.Invoice{},
		customerMetadata:  map[string]openmeter.Metadata{},
		draftSynchronized: map[string]openmeter.DraftSynchronizedRequest{},
		issuingSync:       map[string]openmeter.FinalizedRequest{},
		paymentTriggers:   map[string][]string{},
		conflictOn:        map[string]bool{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/billing/invoices", f.listInvoices)
	mux.HandleFunc("GET /api/v1/billing/invoices/{id}", f.getInvoice)
	mux.HandleFunc("GET /api/v1/customers/{id}", f.getCustomer)
	mux.HandleFunc("POST /api/v1/apps/custom-invoicing/{id}/draft/synchronized", f.draftSync)
	mux.HandleFunc("POST /api/v1/apps/custom-invoicing/{id}/issuing/synchronized", f.issuingSynchronized)
	mux.HandleFunc("POST /api/v1/apps/custom-invoicing/{id}/payment/status", f.paymentStatus)

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeOpenMeter) putInvoice(invoice openmeter.Invoice) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invoices[invoice.ID] = invoice
}

func (f *fakeOpenMeter) putCustomerMetadata(customerID string, metadata openmeter.Metadata) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.customerMetadata[customerID] = metadata
}

func (f *fakeOpenMeter) getInvoice(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	invoice, ok := f.invoices[r.PathValue("id")]
	if !ok {
		http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, invoice)
}

func (f *fakeOpenMeter) listInvoices(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	items := f.listPages
	if page > 1 {
		items = nil
	}
	writeJSON(w, openmeter.InvoiceList{TotalCount: len(items), Page: page, Items: items})
}

func (f *fakeOpenMeter) getCustomer(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	metadata, ok := f.customerMetadata[r.PathValue("id")]
	if !ok {
		http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]any{"metadata": metadata})
}

func (f *fakeOpenMeter) draftSync(w http.ResponseWriter, r *http.Request) {
	var body openmeter.DraftSynchronizedRequest
	decode(r, &body)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conflictOn["draft"] {
		f.conflictOn["draft"] = false
		http.Error(w, `{"message":"invoice already advanced"}`, http.StatusConflict)
		return
	}
	f.draftSynchronized[r.PathValue("id")] = body
	w.WriteHeader(http.StatusOK)
}

func (f *fakeOpenMeter) issuingSynchronized(w http.ResponseWriter, r *http.Request) {
	var body openmeter.FinalizedRequest
	decode(r, &body)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conflictOn["issuing"] {
		f.conflictOn["issuing"] = false
		http.Error(w, `{"message":"invoice already issued"}`, http.StatusConflict)
		return
	}
	f.issuingSync[r.PathValue("id")] = body
	w.WriteHeader(http.StatusOK)
}

func (f *fakeOpenMeter) paymentStatus(w http.ResponseWriter, r *http.Request) {
	var body openmeter.UpdatePaymentStatusRequest
	decode(r, &body)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conflictOn["payment"] {
		f.conflictOn["payment"] = false
		http.Error(w, `{"message":"already applied"}`, http.StatusConflict)
		return
	}
	id := r.PathValue("id")
	f.paymentTriggers[id] = append(f.paymentTriggers[id], body.Trigger)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeOpenMeter) draftFor(t *testing.T, invoiceID string) openmeter.DraftSynchronizedRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.draftSynchronized[invoiceID]
	if !ok {
		t.Fatalf("draft synchronized was never called for %s", invoiceID)
	}
	return body
}

func (f *fakeOpenMeter) issuingFor(t *testing.T, invoiceID string) openmeter.FinalizedRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.issuingSync[invoiceID]
	if !ok {
		t.Fatalf("issuing synchronized was never called for %s", invoiceID)
	}
	return body
}

func (f *fakeOpenMeter) triggersFor(invoiceID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paymentTriggers[invoiceID]...)
}

// newTestSettler wires a Settler against both fakes.
func newTestSettler(t *testing.T, om *fakeOpenMeter, sc *fakeStripe, mutate func(*config.Stripe)) *Settler {
	t.Helper()

	stripeCfg := config.Stripe{
		APIBase:                   sc.URL(),
		SecretKey:                 "sk_test_fake",
		Timeout:                   5 * time.Second,
		MaxRetries:                2,
		DefaultChargeModel:        config.ChargeModelPlatform,
		ConnectAccountMetadataKey: "stripe_connect_account_id",
		ChargeModelMetadataKey:    "stripe_charge_model",
		CustomerMetadataKey:       "stripe_customer_id",
		CollectionMethod:          "charge_automatically",
		AutoAdvance:               true,
		DaysUntilDue:              30,
	}
	if mutate != nil {
		mutate(&stripeCfg)
	}

	omClient := openmeter.New(config.OpenMeter{
		BaseURL:                om.server.URL,
		Timeout:                5 * time.Second,
		DraftSyncStatuses:      []string{"draft.sync"},
		IssuingSyncStatuses:    []string{"issuing.sync"},
		PaymentPendingStatuses: []string{"payment_processing.pending"},
	})

	settler := New(omClient, stripe.New(stripeCfg), stripeCfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	settler.SetClock(func() time.Time { return time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC) })
	return settler
}

// --- helpers -------------------------------------------------------------

func parseForm(r *http.Request) url.Values {
	if err := r.ParseForm(); err != nil {
		return url.Values{}
	}
	return r.PostForm
}

func metadataFrom(form url.Values) map[string]string {
	out := map[string]string{}
	for key, values := range form {
		if strings.HasPrefix(key, "metadata[") && strings.HasSuffix(key, "]") && len(values) > 0 {
			out[key[len("metadata["):len(key)-1]] = values[0]
		}
	}
	return out
}

// searchValue pulls the quoted value out of `metadata['k']:'v'`.
func searchValue(query string) string {
	_, rest, found := strings.Cut(query, ":")
	if !found {
		return ""
	}
	return strings.Trim(strings.TrimSpace(rest), "'")
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func decode(r *http.Request, out any) {
	defer func() { _ = r.Body.Close() }()
	_ = json.NewDecoder(r.Body).Decode(out)
}

func notFound(w http.ResponseWriter, resource string) {
	w.WriteHeader(http.StatusNotFound)
	_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":"resource_missing","message":"no such %s"}}`, resource)
}

func badRequest(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","message":%q}}`, message)
}
