// Package openmeter is the client for OpenMeter's billing and Custom
// Invoicing APIs.
//
// The types here are pinned against OpenMeter v1.0.0-beta.228 (the release
// pymthouse runs, matching @openmeter/sdk of the same version). Only the
// fields settlement actually reads are modelled; the raw payload is kept
// alongside so nothing is lost when the upstream schema grows. See
// docs/openmeter-custom-invoicing.md for how these map to the published
// OpenAPI schema names.
package openmeter

import (
	"encoding/json"
	"strings"
	"time"
)

// Notification event types emitted by OpenMeter's notification channels.
const (
	EventInvoiceCreated = "invoice.created"
	EventInvoiceUpdated = "invoice.updated"
)

// Invoice statuses (OpenMeter `InvoiceStatus`).
const (
	StatusGathering         = "gathering"
	StatusDraft             = "draft"
	StatusIssuing           = "issuing"
	StatusIssued            = "issued"
	StatusPaymentProcessing = "payment_processing"
	StatusOverdue           = "overdue"
	StatusPaid              = "paid"
	StatusUncollectible     = "uncollectible"
	StatusVoided            = "voided"
)

// PaymentTrigger values accepted by the update-payment-status endpoint
// (OpenMeter `CustomInvoicingPaymentTrigger`).
const (
	TriggerPaid           = "paid"
	TriggerPaymentFailed  = "payment_failed"
	TriggerUncollectible  = "payment_uncollectible"
	TriggerOverdue        = "payment_overdue"
	TriggerActionRequired = "action_required"
	TriggerVoid           = "void"
)

// Notification is the body OpenMeter POSTs to a webhook channel for invoice
// events (`NotificationEventInvoiceCreatedPayload` /
// `NotificationEventInvoiceUpdatedPayload`).
type Notification struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Data      Invoice   `json:"data"`
}

// NormalizedType strips the `invoicing.` prefix seen in some documentation so
// callers can compare against EventInvoice* directly.
func (n Notification) NormalizedType() string {
	return strings.TrimPrefix(n.Type, "invoicing.")
}

// Invoice is the subset of OpenMeter's `Invoice` the worker needs.
//
// Note the absence of setters for most of it: an OpenMeter invoice is
// immutable once created — it clones the billing profile and customer data at
// creation time — so corrections belong on the invoice itself, never upstream
// on the customer.
type Invoice struct {
	ID            string        `json:"id"`
	Type          string        `json:"type"`
	Number        string        `json:"number"`
	Currency      string        `json:"currency"`
	Status        string        `json:"status"`
	StatusDetails StatusDetails `json:"statusDetails"`
	Customer      Customer      `json:"customer"`
	Supplier      Party         `json:"supplier"`
	Totals        Totals        `json:"totals"`
	Lines         []Line        `json:"lines"`
	ExternalIDs   ExternalIDs   `json:"externalIds"`
	Metadata      Metadata      `json:"metadata"`
	Description   string        `json:"description"`

	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
	IssuedAt        *time.Time `json:"issuedAt"`
	DueAt           *time.Time `json:"dueAt"`
	SentToCustomerA *time.Time `json:"sentToCustomerAt"`
	Period          *Period    `json:"period"`

	ValidationIssues []ValidationIssue `json:"validationIssues"`
}

// StatusDetails carries the fields OpenMeter asks integrators to branch on.
//
// ExtendedStatus is a free-form string whose vocabulary has shifted across
// releases; Immutable, Failed and AvailableActions are the stable contract.
type StatusDetails struct {
	Immutable        bool             `json:"immutable"`
	Failed           bool             `json:"failed"`
	ExtendedStatus   string           `json:"extendedStatus"`
	AvailableActions AvailableActions `json:"availableActions"`
}

// AvailableActions is OpenMeter's own answer to "what can happen next".
// Reading it beats hard-coding the state graph, which changes between releases.
type AvailableActions struct {
	Advance            *ActionDetails `json:"advance"`
	Approve            *ActionDetails `json:"approve"`
	Delete             *ActionDetails `json:"delete"`
	Retry              *ActionDetails `json:"retry"`
	SnapshotQuantities *ActionDetails `json:"snapshotQuantities"`
	Void               *ActionDetails `json:"void"`
	Invoice            *struct{}      `json:"invoice"`
}

// ActionDetails names the state an action would move the invoice into.
type ActionDetails struct {
	ResultingState string `json:"resultingState"`
}

// Customer is `BillingInvoiceCustomerExtendedDetails`.
type Customer struct {
	ID               string          `json:"id"`
	Key              string          `json:"key"`
	Name             string          `json:"name"`
	UsageAttribution json.RawMessage `json:"usageAttribution"`
	Metadata         Metadata        `json:"metadata"`
}

// Party is `BillingParty` — the supplier side of the invoice.
type Party struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

// Metadata is OpenMeter's string map.
type Metadata map[string]string

// Get returns the value for key, or "" when absent.
func (m Metadata) Get(key string) string {
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m[key])
}

// Totals is `InvoiceTotals`. Every amount is a decimal string, never a float.
type Totals struct {
	Amount              string `json:"amount"`
	ChargesTotal        string `json:"chargesTotal"`
	DiscountsTotal      string `json:"discountsTotal"`
	CreditsTotal        string `json:"creditsTotal"`
	TaxesInclusiveTotal string `json:"taxesInclusiveTotal"`
	TaxesExclusiveTotal string `json:"taxesExclusiveTotal"`
	TaxesTotal          string `json:"taxesTotal"`
	Total               string `json:"total"`
}

// Line is `InvoiceLine`, flattened across its usage-based and detailed forms.
type Line struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Status      string          `json:"status"`
	ManagedBy   string          `json:"managedBy"`
	Currency    string          `json:"currency"`
	Totals      Totals          `json:"totals"`
	Period      *Period         `json:"period"`
	Discounts   *LineDiscounts  `json:"discounts"`
	Children    []Line          `json:"children"`
	Quantity    string          `json:"quantity"`
	FeatureKey  string          `json:"featureKey"`
	ExternalIDs LineExternalIDs `json:"externalIds"`
	Metadata    Metadata        `json:"metadata"`
}

// LineDiscounts is `InvoiceLineDiscounts`.
type LineDiscounts struct {
	Amount []Discount `json:"amount"`
	Usage  []Discount `json:"usage"`
}

// Discount covers both `InvoiceLineAmountDiscount` and
// `InvoiceLineUsageDiscount`; only the id and description are read.
type Discount struct {
	ID          string `json:"id"`
	Amount      string `json:"amount"`
	Quantity    string `json:"quantity"`
	Description string `json:"description"`
	Reason      any    `json:"reason"`
}

// ExternalIDs is `InvoiceAppExternalIds`.
type ExternalIDs struct {
	Invoicing string `json:"invoicing"`
	Tax       string `json:"tax"`
	Payment   string `json:"payment"`
}

// LineExternalIDs is `InvoiceLineAppExternalIds`.
type LineExternalIDs struct {
	Invoicing string `json:"invoicing"`
	Tax       string `json:"tax"`
}

// Period is a billing period.
type Period struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// ValidationIssue is one problem OpenMeter recorded against the invoice.
type ValidationIssue struct {
	Severity  string `json:"severity"`
	Message   string `json:"message"`
	Code      string `json:"code"`
	Component string `json:"component"`
	Field     string `json:"field"`
}

// AllLines returns the invoice's lines in depth-first order, parents first.
func (i Invoice) AllLines() []Line {
	var out []Line
	var walk func([]Line)
	walk = func(lines []Line) {
		for _, l := range lines {
			out = append(out, l)
			walk(l.Children)
		}
	}
	walk(i.Lines)
	return out
}

// BillableLines returns the top-level lines that carry a total.
//
// Only top-level lines are billed to Stripe. A usage-based line's children are
// OpenMeter's internal breakdown of that same charge — billing both would
// double the invoice.
func (i Invoice) BillableLines() []Line {
	out := make([]Line, 0, len(i.Lines))
	for _, l := range i.Lines {
		if strings.EqualFold(l.Status, "deleted") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// LineDiscountIDs returns every discount id attached to a line and its
// children, which is the set the draft-sync response must map.
func (l Line) LineDiscountIDs() []string {
	var out []string
	var walk func(Line)
	walk = func(line Line) {
		if line.Discounts != nil {
			for _, d := range line.Discounts.Amount {
				if d.ID != "" {
					out = append(out, d.ID)
				}
			}
			for _, d := range line.Discounts.Usage {
				if d.ID != "" {
					out = append(out, d.ID)
				}
			}
		}
		for _, c := range line.Children {
			walk(c)
		}
	}
	walk(l)
	return out
}

// ChildIDs returns the ids of every descendant line.
func (l Line) ChildIDs() []string {
	var out []string
	var walk func([]Line)
	walk = func(lines []Line) {
		for _, c := range lines {
			out = append(out, c.ID)
			walk(c.Children)
		}
	}
	walk(l.Children)
	return out
}

// NeedsAttention reports whether the invoice is in a state a human should look
// at: a failed sync, an overdue balance, or a write-off.
func (i Invoice) NeedsAttention() bool {
	if i.StatusDetails.Failed {
		return true
	}
	switch i.Status {
	case StatusOverdue, StatusUncollectible:
		return true
	}
	return false
}

// --- Custom Invoicing request bodies -------------------------------------
//
// These mirror the OpenAPI schemas exactly. Field names matter: OpenMeter
// silently ignores unknown keys, so a typo here means an invoice that advances
// without its external ids ever being recorded.

// SyncResult is `CustomInvoicingSyncResult`.
type SyncResult struct {
	InvoiceNumber           string                  `json:"invoiceNumber,omitempty"`
	ExternalID              string                  `json:"externalId,omitempty"`
	LineExternalIDs         []LineExternalIDMapping `json:"lineExternalIds,omitempty"`
	LineDiscountExternalIDs []LineDiscountIDMapping `json:"lineDiscountExternalIds,omitempty"`
}

// LineExternalIDMapping is `CustomInvoicingLineExternalIdMapping`.
type LineExternalIDMapping struct {
	LineID     string `json:"lineId"`
	ExternalID string `json:"externalId"`
}

// LineDiscountIDMapping is `CustomInvoicingLineDiscountExternalIdMapping`.
type LineDiscountIDMapping struct {
	LineDiscountID string `json:"lineDiscountId"`
	ExternalID     string `json:"externalId"`
}

// DraftSynchronizedRequest is `CustomInvoicingDraftSynchronizedRequest`.
type DraftSynchronizedRequest struct {
	Invoicing *SyncResult `json:"invoicing,omitempty"`
}

// FinalizedInvoicingRequest is `CustomInvoicingFinalizedInvoicingRequest`.
type FinalizedInvoicingRequest struct {
	InvoiceNumber   string     `json:"invoiceNumber,omitempty"`
	SentToCustomerA *time.Time `json:"sentToCustomerAt,omitempty"`
}

// FinalizedPaymentRequest is `CustomInvoicingFinalizedPaymentRequest`.
type FinalizedPaymentRequest struct {
	ExternalID string `json:"externalId,omitempty"`
}

// FinalizedRequest is `CustomInvoicingFinalizedRequest`, the body of the
// issuing-synchronized call. When invoicing.invoiceNumber is omitted OpenMeter
// generates one with an INV- prefix.
type FinalizedRequest struct {
	Invoicing *FinalizedInvoicingRequest `json:"invoicing,omitempty"`
	Payment   *FinalizedPaymentRequest   `json:"payment,omitempty"`
}

// UpdatePaymentStatusRequest is `CustomInvoicingUpdatePaymentStatusRequest`.
//
// This is the third pause point. Unlike the first two it carries no external
// id — the Stripe payment reference is stamped during the issuing call — only
// the trigger that moves the invoice toward paid, failed or void.
type UpdatePaymentStatusRequest struct {
	Trigger string `json:"trigger"`
}
