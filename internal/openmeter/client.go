package openmeter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/metrics"
)

// Client talks to the OpenMeter API.
//
// It is a hand-rolled HTTP client rather than a generated one so that the
// three Custom Invoicing completion calls — the only writes settlement makes
// to OpenMeter — are visible and auditable in one file.
type Client struct {
	baseURL   string
	apiKey    string
	isKonnect bool
	http      *http.Client
	cfg       config.OpenMeter
}

// New builds a client with a bounded timeout and a keep-alive pool. The worker
// is long-lived, so connections stay warm between invoice bursts.
func New(cfg config.OpenMeter) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 32
	base := strings.TrimRight(cfg.BaseURL, "/")
	return &Client{
		baseURL:   base,
		apiKey:    cfg.APIKey,
		isKonnect: isKonnectURL(base),
		http:      &http.Client{Timeout: cfg.Timeout, Transport: transport},
		cfg:       cfg,
	}
}

// isKonnectURL returns true when the base URL points at Konnect Metering &
// Billing rather than self-hosted OpenMeter. Konnect's v3 gateway already
// includes the version prefix, so the SDK-style /api/v1 must be stripped.
func isKonnectURL(base string) bool {
	return strings.Contains(base, "konghq.com") || strings.Contains(base, "konnect")
}

// konnectRewritePath strips the /api/v1 (or /api/v2) prefix that the
// OpenMeter SDK uses, mirroring pymthouse's rewriteKonnectPathname.
func konnectRewritePath(path string) string {
	for _, prefix := range []string{"/api/v1", "/api/v2"} {
		if strings.HasPrefix(path, prefix) {
			rest := path[len(prefix):]
			if rest == "" || rest[0] == '/' || rest[0] == '?' {
				return rest
			}
		}
	}
	return path
}

// APIError is a non-2xx response from OpenMeter.
type APIError struct {
	StatusCode int
	Operation  string
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("openmeter %s: http %d: %s", e.Operation, e.StatusCode, strings.TrimSpace(e.Body))
}

// Retryable reports whether retrying the same request could succeed.
//
// 409 is retryable on purpose: OpenMeter returns it when an invoice is being
// mutated concurrently, and by the next attempt the other writer is usually
// done. 429 and 5xx are the usual transient cases.
func (e *APIError) Retryable() bool {
	switch {
	case e.StatusCode == http.StatusTooManyRequests,
		e.StatusCode == http.StatusConflict,
		e.StatusCode == http.StatusRequestTimeout:
		return true
	case e.StatusCode >= 500:
		return true
	default:
		return false
	}
}

// IsNotFound reports whether err is a 404 from OpenMeter.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

// IsConflict reports whether err is a 409 — typically an invoice that has
// already advanced past the state we tried to complete.
func IsConflict(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
}

// DraftSynchronized completes the draft sync hook.
//
// POST /api/v1/apps/custom-invoicing/{invoiceId}/draft/synchronized
//
// This is the only call that accepts line and line-discount external id
// mappings; after it the invoice moves on and those mappings can no longer be
// supplied, so everything the rest of the lifecycle needs to reference in
// Stripe has to be reported here.
func (c *Client) DraftSynchronized(ctx context.Context, invoiceID string, body DraftSynchronizedRequest) error {
	path := fmt.Sprintf("/api/v1/apps/custom-invoicing/%s/draft/synchronized", url.PathEscape(invoiceID))
	return c.post(ctx, "draft_synchronized", path, body)
}

// IssuingSynchronized completes the issuing sync hook.
//
// POST /api/v1/apps/custom-invoicing/{invoiceId}/issuing/synchronized
//
// The payment external id is set here, not at payment time.
func (c *Client) IssuingSynchronized(ctx context.Context, invoiceID string, body FinalizedRequest) error {
	path := fmt.Sprintf("/api/v1/apps/custom-invoicing/%s/issuing/synchronized", url.PathEscape(invoiceID))
	return c.post(ctx, "issuing_synchronized", path, body)
}

// UpdatePaymentStatus advances a finalized invoice's payment state.
//
// POST /api/v1/apps/custom-invoicing/{invoiceId}/payment/status
func (c *Client) UpdatePaymentStatus(ctx context.Context, invoiceID, trigger string) error {
	path := fmt.Sprintf("/api/v1/apps/custom-invoicing/%s/payment/status", url.PathEscape(invoiceID))
	return c.post(ctx, "update_payment_status", path, UpdatePaymentStatusRequest{Trigger: trigger})
}

// GetInvoice re-reads an invoice, expanding its lines.
//
// Handlers re-read rather than trusting the invoice embedded in the event: by
// the time a message is processed the invoice may have advanced, and acting on
// a stale copy is how duplicate Stripe invoices get created.
func (c *Client) GetInvoice(ctx context.Context, invoiceID string) (*Invoice, error) {
	path := fmt.Sprintf("/api/v1/billing/invoices/%s?expand=lines", url.PathEscape(invoiceID))
	var inv Invoice
	if err := c.get(ctx, "get_invoice", path, &inv); err != nil {
		return nil, err
	}
	return &inv, nil
}

// ListInvoicesInput filters the invoice list used by the reconciliation sweep.
type ListInvoicesInput struct {
	Statuses         []string
	ExtendedStatuses []string
	PageSize         int
	Page             int
	IssuedBefore     *time.Time
}

// InvoiceList is one page of invoices.
type InvoiceList struct {
	TotalCount int       `json:"totalCount"`
	Page       int       `json:"page"`
	PageSize   int       `json:"pageSize"`
	Items      []Invoice `json:"items"`
}

// ListInvoices pages through invoices for the reconciliation sweep.
func (c *Client) ListInvoices(ctx context.Context, in ListInvoicesInput) (*InvoiceList, error) {
	q := url.Values{}
	for _, s := range in.Statuses {
		q.Add("statuses", s)
	}
	for _, s := range in.ExtendedStatuses {
		q.Add("extendedStatuses", s)
	}
	if in.PageSize > 0 {
		q.Set("pageSize", strconv.Itoa(in.PageSize))
	}
	if in.Page > 0 {
		q.Set("page", strconv.Itoa(in.Page))
	}
	if in.IssuedBefore != nil {
		q.Set("issuedBefore", in.IssuedBefore.UTC().Format(time.RFC3339Nano))
	}
	q.Set("expand", "lines")

	var list InvoiceList
	if err := c.get(ctx, "list_invoices", "/api/v1/billing/invoices?"+q.Encode(), &list); err != nil {
		return nil, err
	}
	return &list, nil
}

// CustomerMetadata fetches a customer's metadata, which is where the Connect
// account id and per-customer charge model live.
func (c *Client) CustomerMetadata(ctx context.Context, customerID string) (Metadata, error) {
	path := fmt.Sprintf("/api/v1/customers/%s", url.PathEscape(customerID))
	var out struct {
		Metadata Metadata `json:"metadata"`
	}
	if err := c.get(ctx, "get_customer", path, &out); err != nil {
		return nil, err
	}
	return out.Metadata, nil
}

// DraftSyncStatuses returns the extendedStatus values that mean "waiting on the
// draft sync hook".
func (c *Client) DraftSyncStatuses() []string { return c.cfg.DraftSyncStatuses }

// IssuingSyncStatuses returns the extendedStatus values that mean "waiting on
// the issuing sync hook".
func (c *Client) IssuingSyncStatuses() []string { return c.cfg.IssuingSyncStatuses }

// PaymentPendingStatuses returns the extendedStatus values that mean "waiting
// on a payment result".
func (c *Client) PaymentPendingStatuses() []string { return c.cfg.PaymentPendingStatuses }

func (c *Client) get(ctx context.Context, operation, path string, out any) error {
	return c.do(ctx, operation, http.MethodGet, path, nil, out)
}

func (c *Client) post(ctx context.Context, operation, path string, body any) error {
	return c.do(ctx, operation, http.MethodPost, path, body, nil)
}

func (c *Client) do(ctx context.Context, operation, method, path string, body, out any) error {
	start := time.Now()
	var err error
	defer func() { metrics.ObserveUpstream("openmeter", operation, start, err) }()

	if c.isKonnect {
		path = konnectRewritePath(path)
	}

	var reader io.Reader
	if body != nil {
		encoded, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			err = fmt.Errorf("openmeter %s: encode body: %w", operation, marshalErr)
			return err
		}
		reader = bytes.NewReader(encoded)
	}

	req, reqErr := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if reqErr != nil {
		err = fmt.Errorf("openmeter %s: build request: %w", operation, reqErr)
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, doErr := c.http.Do(req)
	if doErr != nil {
		err = fmt.Errorf("openmeter %s: %w", operation, doErr)
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	// Cap the read: a proxy returning an HTML error page should not be able to
	// balloon memory on a hot path.
	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if readErr != nil {
		err = fmt.Errorf("openmeter %s: read response: %w", operation, readErr)
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = &APIError{StatusCode: resp.StatusCode, Operation: operation, Body: truncateUTF8(string(payload), 512)}
		return err
	}

	if out != nil && len(payload) > 0 {
		if decodeErr := json.Unmarshal(payload, out); decodeErr != nil {
			err = fmt.Errorf("openmeter %s: decode response: %w", operation, decodeErr)
			return err
		}
	}
	return nil
}

func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}
