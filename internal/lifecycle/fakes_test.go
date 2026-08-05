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
)

// fakeStripe is an in-memory stand-in for the Stripe API.
//
// It keeps enough state to catch the mistakes that matter: duplicate invoice
// items on a retry, an invoice total that does not match its items, a missing
// Stripe-Account header on a direct charge, an application fee applied after
// finalization.
type fakeStripe struct {
	mu sync.Mutex

	server *httptest.Server

	customers map[string]*stripe.Customer
	invoices  map[string]*stripe.Invoice
	items     map[string]*stripe.InvoiceItem

	// accountHeaders records the Stripe-Account header seen per request path.
	accountHeaders []string
	// idempotencyKeys records keys, so a test can assert a retry reused one.
	idempotencyKeys []string
	// requests counts calls by "METHOD /path".
	requests map[string]int

	// failNext, when set, makes the next matching request fail.
	failNext map[string]int
	nextID   int
}

func newFakeStripe(t *testing.T) *fakeStripe {
	t.Helper()

	f := &fakeStripe{
		customers: map[string]*stripe.Customer{},
		invoices:  map[string]*stripe.Invoice{},
		items:     map[string]*stripe.InvoiceItem{},
		requests:  map[string]int{},
		failNext:  map[string]int{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/customers", f.createCustomer)
	mux.HandleFunc("GET /v1/customers/search", f.searchCustomers)
	mux.HandleFunc("GET /v1/customers/{id}", f.getCustomer)
	mux.HandleFunc("POST /v1/invoices", f.createInvoice)
	mux.HandleFunc("GET /v1/invoices/search", f.searchInvoices)
	mux.HandleFunc("GET /v1/invoices/{id}", f.getInvoice)
	mux.HandleFunc("POST /v1/invoices/{id}", f.updateInvoice)
	mux.HandleFunc("POST /v1/invoices/{id}/finalize_invoice", f.finalizeInvoice)
	mux.HandleFunc("POST /v1/invoices/{id}/void", f.voidInvoice)
	mux.HandleFunc("POST /v1/invoiceitems", f.createInvoiceItem)
	mux.HandleFunc("GET /v1/invoiceitems", f.listInvoiceItems)
	mux.HandleFunc("DELETE /v1/invoiceitems/{id}", f.deleteInvoiceItem)

	f.server = httptest.NewServer(f.record(mux))
	t.Cleanup(f.server.Close)
	return f
}

// record captures cross-cutting request facts before dispatching.
func (f *fakeStripe) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests[r.Method+" "+r.URL.Path]++
		f.accountHeaders = append(f.accountHeaders, r.Header.Get("Stripe-Account"))
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			f.idempotencyKeys = append(f.idempotencyKeys, key)
		}
		remaining := f.failNext[r.URL.Path]
		if remaining > 0 {
			f.failNext[r.URL.Path] = remaining - 1
		}
		f.mu.Unlock()

		if remaining > 0 {
			w.Header().Set("Request-Id", "req_fail")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"type":"api_error","message":"simulated outage"}}`)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (f *fakeStripe) id(prefix string) string {
	f.nextID++
	return fmt.Sprintf("%s_%d", prefix, f.nextID)
}

func (f *fakeStripe) createCustomer(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)

	f.mu.Lock()
	defer f.mu.Unlock()

	customer := &stripe.Customer{
		ID:       f.id("cus"),
		Name:     form.Get("name"),
		Metadata: metadataFrom(form),
	}
	f.customers[customer.ID] = customer
	writeJSON(w, customer)
}

func (f *fakeStripe) getCustomer(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	customer, ok := f.customers[r.PathValue("id")]
	if !ok {
		notFound(w, "customer")
		return
	}
	writeJSON(w, customer)
}

func (f *fakeStripe) searchCustomers(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	wanted := searchValue(r.URL.Query().Get("query"))
	data := []*stripe.Customer{}
	for _, customer := range f.customers {
		if customer.Metadata[MetaCustomerID] == wanted {
			data = append(data, customer)
		}
	}
	writeJSON(w, map[string]any{"data": data})
}

func (f *fakeStripe) createInvoice(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)

	f.mu.Lock()
	defer f.mu.Unlock()

	invoice := &stripe.Invoice{
		ID:               f.id("in"),
		Status:           "draft",
		Currency:         form.Get("currency"),
		Customer:         form.Get("customer"),
		Metadata:         metadataFrom(form),
		CollectionMethod: form.Get("collection_method"),
	}
	f.invoices[invoice.ID] = invoice
	writeJSON(w, invoice)
}

func (f *fakeStripe) getInvoice(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	invoice, ok := f.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	f.retotalLocked(invoice)
	writeJSON(w, invoice)
}

func (f *fakeStripe) updateInvoice(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)

	f.mu.Lock()
	defer f.mu.Unlock()

	invoice, ok := f.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	if fee := form.Get("application_fee_amount"); fee != "" {
		if invoice.Status != "draft" {
			// Stripe rejects fee changes after finalization; so must the fake,
			// or the test would not catch us doing it in the wrong order.
			badRequest(w, "application_fee_amount cannot be updated on a finalized invoice")
			return
		}
		parsed, _ := strconv.ParseInt(fee, 10, 64)
		invoice.ApplicationFee = parsed
	}
	f.retotalLocked(invoice)
	writeJSON(w, invoice)
}

func (f *fakeStripe) finalizeInvoice(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	invoice, ok := f.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	if invoice.Status == "draft" {
		invoice.Status = "open"
		invoice.Number = "STRIPE-" + strings.ToUpper(invoice.ID)
		pi := f.id("pi")
		invoice.Payments = &stripe.InvoicePaymentList{
			Data: []stripe.InvoicePayment{{
				ID:        f.id("inpay"),
				IsDefault: true,
				Status:    "open",
				Payment: stripe.InvoicePaymentRef{
					Type:          "payment_intent",
					PaymentIntent: json.RawMessage(`"` + pi + `"`),
				},
			}},
		}
	}
	f.retotalLocked(invoice)
	writeJSON(w, invoice)
}

func (f *fakeStripe) voidInvoice(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	invoice, ok := f.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	invoice.Status = "void"
	writeJSON(w, invoice)
}

func (f *fakeStripe) searchInvoices(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	wanted := searchValue(r.URL.Query().Get("query"))
	data := []*stripe.Invoice{}
	for _, invoice := range f.invoices {
		if invoice.Metadata[MetaInvoiceID] == wanted {
			f.retotalLocked(invoice)
			data = append(data, invoice)
		}
	}
	writeJSON(w, map[string]any{"data": data})
}

func (f *fakeStripe) createInvoiceItem(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)

	f.mu.Lock()
	defer f.mu.Unlock()

	amount, _ := strconv.ParseInt(form.Get("amount"), 10, 64)
	item := &stripe.InvoiceItem{
		ID:          f.id("ii"),
		Amount:      amount,
		Currency:    form.Get("currency"),
		Description: form.Get("description"),
		Invoice:     form.Get("invoice"),
		Metadata:    metadataFrom(form),
	}
	f.items[item.ID] = item
	writeJSON(w, item)
}

func (f *fakeStripe) listInvoiceItems(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	invoiceID := r.URL.Query().Get("invoice")
	data := []*stripe.InvoiceItem{}
	for _, item := range f.items {
		if item.Invoice == invoiceID {
			data = append(data, item)
		}
	}
	writeJSON(w, map[string]any{"data": data, "has_more": false})
}

func (f *fakeStripe) deleteInvoiceItem(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := r.PathValue("id")
	if _, ok := f.items[id]; !ok {
		notFound(w, "invoiceitem")
		return
	}
	delete(f.items, id)
	writeJSON(w, map[string]any{"id": id, "deleted": true})
}

// retotalLocked keeps an invoice's total equal to the sum of its items, the
// way Stripe does.
func (f *fakeStripe) retotalLocked(invoice *stripe.Invoice) {
	var total int64
	for _, item := range f.items {
		if item.Invoice == invoice.ID {
			total += item.Amount
		}
	}
	invoice.Total = total
	invoice.AmountDue = total
}

// itemsFor returns the items attached to an invoice, for assertions.
func (f *fakeStripe) itemsFor(invoiceID string) []stripe.InvoiceItem {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []stripe.InvoiceItem
	for _, item := range f.items {
		if item.Invoice == invoiceID {
			out = append(out, *item)
		}
	}
	return out
}

func (f *fakeStripe) onlyInvoice(t *testing.T) *stripe.Invoice {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.invoices) != 1 {
		t.Fatalf("expected exactly one Stripe invoice, found %d", len(f.invoices))
	}
	for _, invoice := range f.invoices {
		return invoice
	}
	return nil
}

func (f *fakeStripe) callCount(method, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[method+" "+path]
}

func (f *fakeStripe) accountsSeen() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	seen := map[string]bool{}
	for _, account := range f.accountHeaders {
		seen[account] = true
	}
	return seen
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
		APIBase:                   sc.server.URL,
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
