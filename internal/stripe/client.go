// Package stripe is a focused Stripe REST client for the settlement worker.
//
// It is deliberately small: settlement only ever touches customers, invoices,
// invoice items and finalization, and every one of those calls needs precise
// control over two headers the generic SDK hides — `Stripe-Account`, which
// decides *whose* books the money lands on, and `Idempotency-Key`, which is the
// only thing standing between a Kafka redelivery and a customer being invoiced
// twice.
package stripe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/metrics"
)

// Client calls the Stripe API.
type Client struct {
	cfg  config.Stripe
	http *http.Client
}

// New builds a Stripe client with warm connections and a bounded timeout.
func New(cfg config.Stripe) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 32
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout, Transport: transport}}
}

// Error is a Stripe API error response.
type Error struct {
	StatusCode int
	Operation  string
	RequestID  string
	Type       string
	Code       string
	Param      string
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("stripe %s: http %d %s/%s: %s (request %s)",
		e.Operation, e.StatusCode, e.Type, e.Code, e.Message, e.RequestID)
}

// Retryable reports whether the same request could succeed on a later attempt.
//
// `lock_timeout` is Stripe telling us another request holds a lock on the same
// object — exactly the case where waiting and retrying is right. Ordinary 4xx
// validation errors are permanent and must go to the DLQ instead of spinning.
func (e *Error) Retryable() bool {
	switch {
	case e.StatusCode == http.StatusTooManyRequests:
		return true
	case e.StatusCode >= 500:
		return true
	case e.Code == "lock_timeout", e.Code == "rate_limit":
		return true
	default:
		return false
	}
}

// IsInvalidRequest reports whether err is a permanent 400-class Stripe error.
func IsInvalidRequest(err error) bool {
	var sErr *Error
	return errors.As(err, &sErr) && sErr.StatusCode >= 400 && sErr.StatusCode < 500 && !sErr.Retryable()
}

// IsNotFound reports whether err is a Stripe 404.
func IsNotFound(err error) bool {
	var sErr *Error
	return errors.As(err, &sErr) && sErr.StatusCode == http.StatusNotFound
}

// Customer is the subset of a Stripe customer we read.
type Customer struct {
	ID       string            `json:"id"`
	Email    string            `json:"email"`
	Name     string            `json:"name"`
	Metadata map[string]string `json:"metadata"`
	Deleted  bool              `json:"deleted"`
}

// Invoice is the subset of a Stripe invoice we read.
//
// As of API 2025-03-31.basil (and the pinned 2026-01-01 version), payment_intent
// and charge were removed from Invoice in favour of the payments list and
// confirmation_secret. Prefer PrimaryPaymentIntent over any legacy fields.
type Invoice struct {
	ID                 string              `json:"id"`
	Number             string              `json:"number"`
	Status             string              `json:"status"`
	Currency           string              `json:"currency"`
	Customer           string              `json:"customer"`
	Total              int64               `json:"total"`
	AmountDue          int64               `json:"amount_due"`
	AmountPaid         int64               `json:"amount_paid"`
	HostedInvoiceURL   string              `json:"hosted_invoice_url"`
	ApplicationFee     int64               `json:"application_fee_amount"`
	StatusTransitions  map[string]any      `json:"status_transitions"`
	Metadata           map[string]string   `json:"metadata"`
	Livemode           bool                `json:"livemode"`
	AutoAdvance        bool                `json:"auto_advance"`
	CollectionMethod   string              `json:"collection_method"`
	Payments           *InvoicePaymentList `json:"payments"`
	ConfirmationSecret *ConfirmationSecret `json:"confirmation_secret"`
}

// InvoicePaymentList is the expanded payments collection on an invoice.
type InvoicePaymentList struct {
	Data []InvoicePayment `json:"data"`
}

// InvoicePayment links a payment to an invoice.
type InvoicePayment struct {
	ID        string            `json:"id"`
	IsDefault bool              `json:"is_default"`
	Status    string            `json:"status"`
	Payment   InvoicePaymentRef `json:"payment"`
}

// InvoicePaymentRef holds the payment pointer. payment_intent may be a string
// id or an expanded object depending on the expand parameter.
type InvoicePaymentRef struct {
	Type          string          `json:"type"`
	PaymentIntent json.RawMessage `json:"payment_intent"`
}

// ConfirmationSecret carries the client secret used by Payment Element flows.
type ConfirmationSecret struct {
	Type         string `json:"type"`
	ClientSecret string `json:"client_secret"`
}

// PrimaryPaymentIntent returns the default payment intent id for the invoice.
//
// When multiple payments exist it prefers the is_default entry; when none are
// present it falls back to confirmation_secret. An error is returned when no
// payment reference can be resolved so callers never persist an empty id.
func (inv *Invoice) PrimaryPaymentIntent() (string, error) {
	if inv == nil {
		return "", fmt.Errorf("nil invoice")
	}

	if inv.Payments != nil && len(inv.Payments.Data) > 0 {
		var first string
		for _, p := range inv.Payments.Data {
			id := paymentIntentID(p.Payment.PaymentIntent)
			if id == "" {
				continue
			}
			if first == "" {
				first = id
			}
			if p.IsDefault {
				return id, nil
			}
		}
		if first != "" {
			if len(inv.Payments.Data) > 1 {
				// Multiple payments but none marked default: use the first
				// resolvable payment_intent and let the caller decide.
				return first, nil
			}
			return first, nil
		}
		return "", fmt.Errorf("invoice %s has %d payment(s) but no payment_intent reference", inv.ID, len(inv.Payments.Data))
	}

	if inv.ConfirmationSecret != nil {
		if id := paymentIntentIDFromClientSecret(inv.ConfirmationSecret.ClientSecret); id != "" {
			return id, nil
		}
	}

	return "", fmt.Errorf("invoice %s has no payment reference", inv.ID)
}

func paymentIntentID(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var id string
	if err := json.Unmarshal(raw, &id); err == nil {
		return id
	}
	var obj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj.ID
	}
	return ""
}

// paymentIntentIDFromClientSecret extracts "pi_xxx" from "pi_xxx_secret_yyy".
func paymentIntentIDFromClientSecret(secret string) string {
	const marker = "_secret_"
	if i := strings.Index(secret, marker); i > 0 {
		return secret[:i]
	}
	return ""
}

// InvoiceItem is the subset of a Stripe invoice item we read.
type InvoiceItem struct {
	ID          string            `json:"id"`
	Amount      int64             `json:"amount"`
	Currency    string            `json:"currency"`
	Description string            `json:"description"`
	Invoice     string            `json:"invoice"`
	Metadata    map[string]string `json:"metadata"`
}

// RequestOptions carry the per-call Connect account and idempotency key.
type RequestOptions struct {
	// Account sets `Stripe-Account`. Empty means the platform account.
	Account string
	// IdempotencyKey makes a retried create a no-op. Stripe remembers keys for
	// 24 hours; anything older is recovered by lookup instead.
	IdempotencyKey string
}

// CreateCustomer creates a customer, optionally on a connected account.
func (c *Client) CreateCustomer(ctx context.Context, params url.Values, opts RequestOptions) (*Customer, error) {
	var out Customer
	if err := c.do(ctx, "create_customer", http.MethodPost, "/v1/customers", params, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetCustomer retrieves a customer by id.
func (c *Client) GetCustomer(ctx context.Context, id string, opts RequestOptions) (*Customer, error) {
	var out Customer
	path := "/v1/customers/" + url.PathEscape(id)
	if err := c.do(ctx, "get_customer", http.MethodGet, path, nil, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SearchCustomers runs a Stripe search query.
//
// Stripe's search index lags writes by up to a minute, so a miss here is not
// proof the customer does not exist. Callers pair it with a deterministic
// idempotency key on create.
func (c *Client) SearchCustomers(ctx context.Context, query string, opts RequestOptions) ([]Customer, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("limit", "1")

	var out struct {
		Data []Customer `json:"data"`
	}
	if err := c.do(ctx, "search_customers", http.MethodGet, "/v1/customers/search?"+params.Encode(), nil, opts, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// SearchInvoices runs a Stripe invoice search query.
func (c *Client) SearchInvoices(ctx context.Context, query string, opts RequestOptions) ([]Invoice, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("limit", "10")

	var out struct {
		Data []Invoice `json:"data"`
	}
	if err := c.do(ctx, "search_invoices", http.MethodGet, "/v1/invoices/search?"+params.Encode(), nil, opts, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// CreateInvoice creates a draft invoice.
func (c *Client) CreateInvoice(ctx context.Context, params url.Values, opts RequestOptions) (*Invoice, error) {
	var out Invoice
	if err := c.do(ctx, "create_invoice", http.MethodPost, "/v1/invoices", params, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetInvoice retrieves an invoice by id, expanding payments so callers can
// resolve a payment intent under the post-basil Invoice shape.
func (c *Client) GetInvoice(ctx context.Context, id string, opts RequestOptions) (*Invoice, error) {
	var out Invoice
	path := "/v1/invoices/" + url.PathEscape(id) + "?expand[]=payments"
	if err := c.do(ctx, "get_invoice", http.MethodGet, path, nil, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateInvoice updates a draft invoice.
func (c *Client) UpdateInvoice(ctx context.Context, id string, params url.Values, opts RequestOptions) (*Invoice, error) {
	var out Invoice
	path := "/v1/invoices/" + url.PathEscape(id)
	if err := c.do(ctx, "update_invoice", http.MethodPost, path, params, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FinalizeInvoice moves a draft invoice to open and assigns its number.
//
// payments is expanded so the resulting PaymentIntent is available under the
// pinned API (payment_intent / charge were removed from Invoice).
func (c *Client) FinalizeInvoice(ctx context.Context, id string, autoAdvance bool, opts RequestOptions) (*Invoice, error) {
	params := url.Values{}
	params.Set("auto_advance", strconv.FormatBool(autoAdvance))
	params.Add("expand[]", "payments")

	var out Invoice
	// Stripe API is POST /v1/invoices/{id}/finalize (not finalize_invoice).
	path := "/v1/invoices/" + url.PathEscape(id) + "/finalize"
	if err := c.do(ctx, "finalize_invoice", http.MethodPost, path, params, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// VoidInvoice voids a finalized invoice.
func (c *Client) VoidInvoice(ctx context.Context, id string, opts RequestOptions) (*Invoice, error) {
	var out Invoice
	path := "/v1/invoices/" + url.PathEscape(id) + "/void"
	if err := c.do(ctx, "void_invoice", http.MethodPost, path, nil, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateInvoiceItem attaches a line to a draft invoice.
func (c *Client) CreateInvoiceItem(ctx context.Context, params url.Values, opts RequestOptions) (*InvoiceItem, error) {
	var out InvoiceItem
	if err := c.do(ctx, "create_invoice_item", http.MethodPost, "/v1/invoiceitems", params, opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteInvoiceItem removes an item from a draft invoice.
//
// Only used to replace a stale rounding adjustment: every other item is
// created idempotently and never needs to be taken back.
func (c *Client) DeleteInvoiceItem(ctx context.Context, id string, opts RequestOptions) error {
	path := "/v1/invoiceitems/" + url.PathEscape(id)
	return c.do(ctx, "delete_invoice_item", http.MethodDelete, path, nil, opts, nil)
}

// ListInvoiceItems lists the items already attached to an invoice.
//
// This is the recovery path: if the worker crashed midway through building a
// draft, the next attempt reads back what already exists instead of adding a
// second copy of every line.
func (c *Client) ListInvoiceItems(ctx context.Context, invoiceID string, opts RequestOptions) ([]InvoiceItem, error) {
	params := url.Values{}
	params.Set("invoice", invoiceID)
	params.Set("limit", "100")

	var items []InvoiceItem
	startingAfter := ""
	for {
		q := url.Values{}
		for k, v := range params {
			q[k] = v
		}
		if startingAfter != "" {
			q.Set("starting_after", startingAfter)
		}

		var page struct {
			Data    []InvoiceItem `json:"data"`
			HasMore bool          `json:"has_more"`
		}
		if err := c.do(ctx, "list_invoice_items", http.MethodGet, "/v1/invoiceitems?"+q.Encode(), nil, opts, &page); err != nil {
			return nil, err
		}
		items = append(items, page.Data...)
		if !page.HasMore || len(page.Data) == 0 {
			return items, nil
		}
		startingAfter = page.Data[len(page.Data)-1].ID
	}
}

// do performs one API call with bounded retries.
func (c *Client) do(ctx context.Context, operation, method, path string, form url.Values, opts RequestOptions, out any) error {
	start := time.Now()
	var err error
	defer func() { metrics.ObserveUpstream("stripe", operation, start, err) }()

	attempts := c.cfg.MaxRetries
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 1; ; attempt++ {
		err = c.attempt(ctx, operation, method, path, form, opts, out)
		if err == nil {
			return nil
		}

		// Writes without an idempotency key must not be retried: a 5xx or
		// transport blip after Stripe already applied the request would create
		// a second invoice / item. GETs and keyed writes are safe to replay.
		safeToRetry := method == http.MethodGet || opts.IdempotencyKey != ""
		var sErr *Error
		retryable := false
		if errors.As(err, &sErr) {
			retryable = sErr.Retryable() && safeToRetry
		} else {
			retryable = safeToRetry
		}

		if !retryable || attempt >= attempts || ctx.Err() != nil {
			return err
		}

		// Exponential backoff. Kept short because the caller has its own,
		// much longer retry loop; this only smooths over blips.
		delay := time.Duration(1<<uint(attempt-1)) * 200 * time.Millisecond
		if delay > 2*time.Second {
			delay = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (c *Client) attempt(ctx context.Context, operation, method, path string, form url.Values, opts RequestOptions, out any) error {
	var body io.Reader
	if form != nil && method != http.MethodGet {
		body = strings.NewReader(form.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, c.cfg.APIBase+path, body)
	if err != nil {
		return fmt.Errorf("stripe %s: build request: %w", operation, err)
	}
	req.SetBasicAuth(c.cfg.SecretKey, "")
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if c.cfg.APIVersion != "" {
		req.Header.Set("Stripe-Version", c.cfg.APIVersion)
	}
	if opts.Account != "" {
		req.Header.Set("Stripe-Account", opts.Account)
	}
	if opts.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", opts.IdempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("stripe %s: %w", operation, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("stripe %s: read response: %w", operation, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseError(operation, resp, payload)
	}
	if out != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("stripe %s: decode response: %w", operation, err)
		}
	}
	return nil
}

func parseError(operation string, resp *http.Response, payload []byte) error {
	e := &Error{
		StatusCode: resp.StatusCode,
		Operation:  operation,
		RequestID:  resp.Header.Get("Request-Id"),
		Message:    strings.TrimSpace(string(payload)),
	}
	var envelope struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Param   string `json:"param"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &envelope); err == nil && envelope.Error.Message != "" {
		e.Type = envelope.Error.Type
		e.Code = envelope.Error.Code
		e.Param = envelope.Error.Param
		e.Message = envelope.Error.Message
	}
	return e
}
