package stripefake

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pymthouse/settlement/internal/stripe"
)

// Metadata keys used to locate OpenMeter-backed objects.
const (
	metaCustomerID = "openmeter_customer_id"
	metaInvoiceID  = "openmeter_invoice_id"
)

// Config controls the fake Stripe server.
type Config struct {
	WebhookURL    string
	WebhookSecret string
	Timeout       time.Duration
}

// Snapshot is a serialisable view of the fake's state for tests and e2e.
type Snapshot struct {
	Invoices     []InvoiceSnapshot   `json:"invoices"`
	AccountsSeen []string            `json:"accounts_seen"`
	Requests     map[string]int      `json:"requests"`
	Customers    []CustomerSnapshot  `json:"customers,omitempty"`
	Items        []InvoiceItemReport `json:"items,omitempty"`
}

// InvoiceSnapshot is the stable state we expose over /_stripefake/v1/state.
type InvoiceSnapshot struct {
	ID               string            `json:"id"`
	Status           string            `json:"status"`
	Number           string            `json:"number,omitempty"`
	Customer         string            `json:"customer,omitempty"`
	Total            int64             `json:"total_minor"`
	AmountDue        int64             `json:"amount_due_minor"`
	AmountPaid       int64             `json:"amount_paid_minor"`
	ApplicationFee   int64             `json:"application_fee_minor"`
	PaymentIntent    string            `json:"payment_intent,omitempty"`
	StripeAccount    string            `json:"stripe_account,omitempty"`
	CollectionMethod string            `json:"collection_method,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// CustomerSnapshot is the minimal customer state we expose.
type CustomerSnapshot struct {
	ID       string            `json:"id"`
	Name     string            `json:"name,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// InvoiceItemReport is included in the state snapshot for debugging.
type InvoiceItemReport struct {
	ID       string            `json:"id"`
	Invoice  string            `json:"invoice"`
	Amount   int64             `json:"amount_minor"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Server is an in-memory stand-in for Stripe.
type Server struct {
	mu sync.Mutex

	cfg Config

	customers map[string]*stripe.Customer
	invoices  map[string]*stripe.Invoice
	items     map[string]*stripe.InvoiceItem

	invoiceAccounts map[string]string
	paymentIDs      map[string]string
	customerOrder   []string
	invoiceOrder    []string
	itemOrder       []string

	accountHeaders   []string
	idempotencyKeys  []string
	requests         map[string]int
	failNext         map[string]int
	idempotencyIndex map[string]string
	nextID           int
	client           *http.Client
}

// New builds a fake Stripe server.
func New(cfg Config) *Server {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Server{
		cfg:              cfg,
		customers:        map[string]*stripe.Customer{},
		invoices:         map[string]*stripe.Invoice{},
		items:            map[string]*stripe.InvoiceItem{},
		invoiceAccounts:  map[string]string{},
		paymentIDs:       map[string]string{},
		requests:         map[string]int{},
		failNext:         map[string]int{},
		idempotencyIndex: map[string]string{},
		client:           &http.Client{Timeout: cfg.Timeout},
	}
}

// Handler returns the HTTP surface for the fake.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("GET /_stripefake/v1/state", s.handleState)
	mux.HandleFunc("POST /v1/customers", s.withAuth(s.createCustomer))
	mux.HandleFunc("GET /v1/customers/search", s.withAuth(s.searchCustomers))
	mux.HandleFunc("GET /v1/customers/{id}", s.withAuth(s.getCustomer))
	mux.HandleFunc("POST /v1/invoices", s.withAuth(s.createInvoice))
	mux.HandleFunc("GET /v1/invoices/search", s.withAuth(s.searchInvoices))
	mux.HandleFunc("GET /v1/invoices/{id}", s.withAuth(s.getInvoice))
	mux.HandleFunc("POST /v1/invoices/{id}", s.withAuth(s.updateInvoice))
	mux.HandleFunc("POST /v1/invoices/{id}/finalize_invoice", s.withAuth(s.finalizeInvoice))
	mux.HandleFunc("POST /v1/invoices/{id}/void", s.withAuth(s.voidInvoice))
	mux.HandleFunc("POST /v1/invoiceitems", s.withAuth(s.createInvoiceItem))
	mux.HandleFunc("GET /v1/invoiceitems", s.withAuth(s.listInvoiceItems))
	mux.HandleFunc("DELETE /v1/invoiceitems/{id}", s.withAuth(s.deleteInvoiceItem))
	return s.record(mux)
}

// FailNext makes the next count requests to path fail with a 500.
func (s *Server) FailNext(path string, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext[path] = count
}

// SetInvoiceStatus changes an invoice's status in place.
func (s *Server) SetInvoiceStatus(id, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inv, ok := s.invoices[id]; ok {
		inv.Status = status
		if status == "paid" {
			inv.AmountPaid = inv.Total
			inv.AmountDue = 0
		}
	}
}

// ItemsFor returns the items attached to invoiceID.
func (s *Server) ItemsFor(invoiceID string) []stripe.InvoiceItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]stripe.InvoiceItem, 0)
	for _, item := range s.items {
		if item.Invoice == invoiceID {
			out = append(out, *item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OnlyInvoice returns the only invoice when the fake has exactly one.
func (s *Server) OnlyInvoice() *stripe.Invoice {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.invoices) != 1 {
		return nil
	}
	for _, inv := range s.invoices {
		return inv
	}
	return nil
}

// CallCount reports how often the fake saw method path.
func (s *Server) CallCount(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[method+" "+path]
}

// AccountsSeen reports every Stripe-Account header the fake observed.
func (s *Server) AccountsSeen() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := map[string]bool{}
	for _, account := range s.accountHeaders {
		seen[account] = true
	}
	return seen
}

// Snapshot captures the current state for debugging and e2e assertions.
func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := Snapshot{
		AccountsSeen: append([]string(nil), uniqueStrings(s.accountHeaders)...),
		Requests:     copyCounts(s.requests),
	}
	for _, id := range s.customerOrder {
		if customer, ok := s.customers[id]; ok {
			out.Customers = append(out.Customers, CustomerSnapshot{
				ID:       customer.ID,
				Name:     customer.Name,
				Metadata: copyMap(customer.Metadata),
			})
		}
	}
	for _, id := range s.invoiceOrder {
		if invoice, ok := s.invoices[id]; ok {
			out.Invoices = append(out.Invoices, InvoiceSnapshot{
				ID:               invoice.ID,
				Status:           invoice.Status,
				Number:           invoice.Number,
				Customer:         invoice.Customer,
				Total:            invoice.Total,
				AmountDue:        invoice.AmountDue,
				AmountPaid:       invoice.AmountPaid,
				ApplicationFee:   invoice.ApplicationFee,
				PaymentIntent:    s.paymentIDs[id],
				StripeAccount:    s.invoiceAccounts[id],
				CollectionMethod: invoice.CollectionMethod,
				Metadata:         copyMap(invoice.Metadata),
			})
		}
	}
	for _, id := range s.itemOrder {
		if item, ok := s.items[id]; ok {
			out.Items = append(out.Items, InvoiceItemReport{
				ID:       item.ID,
				Invoice:  item.Invoice,
				Amount:   item.Amount,
				Metadata: copyMap(item.Metadata),
			})
		}
	}
	return out
}

// Inspect is an alias for Snapshot for callers that prefer a verb.
func (s *Server) Inspect() Snapshot { return s.Snapshot() }

func (s *Server) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests[r.Method+" "+r.URL.Path]++
		s.accountHeaders = append(s.accountHeaders, r.Header.Get("Stripe-Account"))
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			s.idempotencyKeys = append(s.idempotencyKeys, key)
		}
		remaining := s.failNext[r.URL.Path]
		if remaining > 0 {
			s.failNext[r.URL.Path] = remaining - 1
		}
		s.mu.Unlock()

		if remaining > 0 {
			w.Header().Set("Request-Id", "req_fail")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"type":"api_error","message":"simulated outage"}}`)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if user, _, ok := r.BasicAuth(); ok && strings.HasPrefix(user, "sk_test_") {
			next(w, r)
			return
		}
		if strings.HasPrefix(strings.TrimSpace(r.Header.Get("Authorization")), "Bearer sk_test_") {
			next(w, r)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"test key required"}}`)
	}
}

func (s *Server) createCustomer(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)
	key := r.Header.Get("Idempotency-Key")

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.idempotencyIndex["POST /v1/customers "+key]; ok {
		writeJSON(w, s.customers[id])
		return
	}

	customer := &stripe.Customer{
		ID:       s.id("cus"),
		Name:     form.Get("name"),
		Metadata: metadataFrom(form),
	}
	s.customers[customer.ID] = customer
	s.customerOrder = append(s.customerOrder, customer.ID)
	if key != "" {
		s.idempotencyIndex["POST /v1/customers "+key] = customer.ID
	}
	writeJSON(w, customer)
}

func (s *Server) getCustomer(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	customer, ok := s.customers[r.PathValue("id")]
	if !ok {
		notFound(w, "customer")
		return
	}
	writeJSON(w, customer)
}

func (s *Server) searchCustomers(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wanted := searchValue(r.URL.Query().Get("query"))
	data := []*stripe.Customer{}
	for _, customer := range s.customers {
		if customer.Metadata[metaCustomerID] == wanted {
			data = append(data, customer)
		}
	}
	writeJSON(w, map[string]any{"data": data})
}

func (s *Server) createInvoice(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)
	key := r.Header.Get("Idempotency-Key")

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.idempotencyIndex["POST /v1/invoices "+key]; ok {
		writeJSON(w, s.invoices[id])
		return
	}

	invoice := &stripe.Invoice{
		ID:               s.id("in"),
		Status:           "draft",
		Currency:         form.Get("currency"),
		Customer:         form.Get("customer"),
		Metadata:         metadataFrom(form),
		CollectionMethod: form.Get("collection_method"),
		AutoAdvance:      form.Get("auto_advance") == "true",
	}
	s.invoices[invoice.ID] = invoice
	s.invoiceOrder = append(s.invoiceOrder, invoice.ID)
	if account := r.Header.Get("Stripe-Account"); account != "" {
		s.invoiceAccounts[invoice.ID] = account
	}
	if key != "" {
		s.idempotencyIndex["POST /v1/invoices "+key] = invoice.ID
	}
	writeJSON(w, invoice)
}

func (s *Server) getInvoice(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	invoice, ok := s.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	s.retotalLocked(invoice)
	writeJSON(w, invoice)
}

func (s *Server) searchInvoices(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wanted := searchValue(r.URL.Query().Get("query"))
	data := []*stripe.Invoice{}
	for _, invoice := range s.invoices {
		if invoice.Metadata[metaInvoiceID] == wanted {
			s.retotalLocked(invoice)
			data = append(data, invoice)
		}
	}
	writeJSON(w, map[string]any{"data": data})
}

func (s *Server) updateInvoice(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)

	s.mu.Lock()
	defer s.mu.Unlock()

	invoice, ok := s.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	if fee := form.Get("application_fee_amount"); fee != "" {
		if invoice.Status != "draft" {
			badRequest(w, "application_fee_amount cannot be updated on a finalized invoice")
			return
		}
		parsed, _ := strconv.ParseInt(fee, 10, 64)
		invoice.ApplicationFee = parsed
	}
	s.retotalLocked(invoice)
	writeJSON(w, invoice)
}

func (s *Server) finalizeInvoice(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	invoice, ok := s.invoices[r.PathValue("id")]
	if !ok {
		s.mu.Unlock()
		notFound(w, "invoice")
		return
	}
	if account := r.Header.Get("Stripe-Account"); account != "" {
		s.invoiceAccounts[invoice.ID] = account
	}
	if invoice.Status != "draft" {
		if invoice.Status == "open" && invoice.AmountPaid == 0 && invoice.Total > 0 {
			invoice.AmountPaid = invoice.Total
			invoice.AmountDue = 0
			invoice.Status = "paid"
		}
		s.retotalLocked(invoice)
		out := invoice
		s.mu.Unlock()
		writeJSON(w, out)
		return
	}

	invoice.Status = "open"
	invoice.Number = "STRIPE-" + strings.ToUpper(invoice.ID)
	paymentID := s.id("pi")
	s.paymentIDs[invoice.ID] = paymentID
	invoice.Payments = &stripe.InvoicePaymentList{
		Data: []stripe.InvoicePayment{{
			ID:        s.id("inpay"),
			IsDefault: true,
			Status:    "open",
			Payment: stripe.InvoicePaymentRef{
				Type:          "payment_intent",
				PaymentIntent: json.RawMessage(`"` + paymentID + `"`),
			},
		}},
	}
	s.retotalLocked(invoice)
	account := s.invoiceAccounts[invoice.ID]
	payload := s.paymentWebhookPayloadLocked(invoice, accountFor(account), paymentID)
	paidPayload := paidWebhookPayload(payload, invoice.ID, paymentID)
	secret := s.cfg.WebhookSecret
	url := s.cfg.WebhookURL
	s.mu.Unlock()

	if url != "" && secret != "" {
		if err := postSignedStripeWebhook(s.client, url, secret, payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if err := postSignedStripeWebhook(s.client, url, secret, paidPayload); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		s.SetInvoiceStatus(invoice.ID, "paid")
	}

	s.mu.Lock()
	out := s.invoices[invoice.ID]
	s.mu.Unlock()
	writeJSON(w, out)
}

func (s *Server) voidInvoice(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	invoice, ok := s.invoices[r.PathValue("id")]
	if !ok {
		notFound(w, "invoice")
		return
	}
	invoice.Status = "void"
	writeJSON(w, invoice)
}

func (s *Server) createInvoiceItem(w http.ResponseWriter, r *http.Request) {
	form := parseForm(r)
	key := r.Header.Get("Idempotency-Key")

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.idempotencyIndex["POST /v1/invoiceitems "+key]; ok {
		writeJSON(w, s.items[id])
		return
	}

	amount, _ := strconv.ParseInt(form.Get("amount"), 10, 64)
	item := &stripe.InvoiceItem{
		ID:          s.id("ii"),
		Amount:      amount,
		Currency:    form.Get("currency"),
		Description: form.Get("description"),
		Invoice:     form.Get("invoice"),
		Metadata:    metadataFrom(form),
	}
	s.items[item.ID] = item
	s.itemOrder = append(s.itemOrder, item.ID)
	if key != "" {
		s.idempotencyIndex["POST /v1/invoiceitems "+key] = item.ID
	}
	writeJSON(w, item)
}

func (s *Server) listInvoiceItems(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	invoiceID := r.URL.Query().Get("invoice")
	data := []*stripe.InvoiceItem{}
	for _, item := range s.items {
		if item.Invoice == invoiceID {
			data = append(data, item)
		}
	}
	sort.Slice(data, func(i, j int) bool { return data[i].ID < data[j].ID })
	writeJSON(w, map[string]any{"data": data, "has_more": false})
}

func (s *Server) deleteInvoiceItem(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := r.PathValue("id")
	if _, ok := s.items[id]; !ok {
		notFound(w, "invoiceitem")
		return
	}
	delete(s.items, id)
	writeJSON(w, map[string]any{"id": id, "deleted": true})
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.Snapshot())
}

func (s *Server) id(prefix string) string {
	s.nextID++
	return fmt.Sprintf("%s_%d", prefix, s.nextID)
}

func (s *Server) retotalLocked(invoice *stripe.Invoice) {
	var total int64
	for _, item := range s.items {
		if item.Invoice == invoice.ID {
			total += item.Amount
		}
	}
	invoice.Total = total
	invoice.AmountDue = total
	if invoice.Status == "paid" {
		invoice.AmountPaid = total
		invoice.AmountDue = 0
	}
}

func (s *Server) maybePaidInvoice(id string) *stripe.Invoice {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv := s.invoices[id]
	if inv == nil {
		return nil
	}
	return inv
}

func (s *Server) paymentWebhookPayloadLocked(invoice *stripe.Invoice, account, paymentID string) []byte {
	body := map[string]any{
		"id":       fmt.Sprintf("evt_%s_payment_succeeded", invoice.ID),
		"type":     "invoice.payment_succeeded",
		"account":  account,
		"livemode": false,
		"data": map[string]any{
			"object": map[string]any{
				"id":                     invoice.ID,
				"object":                 "invoice",
				"status":                 invoice.Status,
				"customer":               invoice.Customer,
				"metadata":               invoice.Metadata,
				"amount_paid":            invoice.AmountPaid,
				"amount_due":             invoice.AmountDue,
				"total":                  invoice.Total,
				"payment_intent":         paymentID,
				"application_fee_amount": invoice.ApplicationFee,
			},
		},
	}
	payload, _ := json.Marshal(body)
	return payload
}

func paidWebhookPayload(prev []byte, invoiceID, paymentID string) []byte {
	var body map[string]any
	_ = json.Unmarshal(prev, &body)
	body["id"] = fmt.Sprintf("evt_%s_paid", invoiceID)
	body["type"] = "invoice.paid"
	if data, ok := body["data"].(map[string]any); ok {
		if obj, ok := data["object"].(map[string]any); ok {
			obj["status"] = "paid"
			obj["payment_intent"] = paymentID
			obj["amount_paid"] = obj["total"]
			obj["amount_due"] = 0
		}
	}
	payload, _ := json.Marshal(body)
	return payload
}

func accountFor(account string) string { return account }

func postSignedStripeWebhook(client *http.Client, targetURL, secret string, body []byte) error {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signStripeWebhook(secret, timestamp, body)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stripe-Signature", "t="+timestamp+",v1="+sig)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("stripefake webhook: http %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	return nil
}

func signStripeWebhook(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

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

func notFound(w http.ResponseWriter, resource string) {
	w.WriteHeader(http.StatusNotFound)
	_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":"resource_missing","message":"no such %s"}}`, resource)
}

func badRequest(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","message":%q}}`, message)
}

func copyMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyCounts(in map[string]int) map[string]int {
	if len(in) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
