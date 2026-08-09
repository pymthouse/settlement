package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
)

// Handler names, used for metrics labels and log fields.
const (
	HandlerDraftSync     = "draft_sync"
	HandlerIssuingSync   = "issuing_sync"
	HandlerPaymentStatus = "payment_status"
	HandlerNoop          = "noop"
)

// Settler drives invoices through the Custom Invoicing lifecycle.
type Settler struct {
	om     *openmeter.Client
	stripe *stripe.Client
	cfg    config.Stripe
	log    *slog.Logger
	now    func() time.Time

	customerCache *metadataCache
}

// New builds a Settler.
func New(om *openmeter.Client, sc *stripe.Client, cfg config.Stripe, log *slog.Logger) *Settler {
	return &Settler{
		om:            om,
		stripe:        sc,
		cfg:           cfg,
		log:           log,
		now:           func() time.Time { return time.Now().UTC() },
		customerCache: newMetadataCache(60 * time.Second),
	}
}

// SetClock overrides the clock. Tests only.
func (s *Settler) SetClock(now func() time.Time) { s.now = now }

// HandleOpenMeterNotification processes one raw OpenMeter notification body.
func (s *Settler) HandleOpenMeterNotification(ctx context.Context, raw []byte) (string, error) {
	var notification openmeter.Notification
	if err := json.Unmarshal(raw, &notification); err != nil {
		return HandlerNoop, faults.Wrap("unparseable_notification", err)
	}

	eventType := notification.NormalizedType()
	if eventType != openmeter.EventInvoiceCreated && eventType != openmeter.EventInvoiceUpdated {
		s.log.Debug("ignoring non-invoice notification", "type", notification.Type)
		return HandlerNoop, nil
	}
	if notification.Data.ID == "" {
		return HandlerNoop, faults.Permanentf("missing_invoice_id",
			"notification %s carries no invoice id", notification.ID)
	}

	// Re-read rather than trusting the embedded invoice. Notifications can
	// arrive out of order relative to the invoice's real state, and acting on
	// a stale draft is how a second Stripe invoice gets created.
	invoice, err := s.om.GetInvoice(ctx, notification.Data.ID)
	if err != nil {
		if openmeter.IsNotFound(err) {
			return HandlerNoop, faults.Wrap("invoice_deleted", err)
		}
		return HandlerNoop, fmt.Errorf("reload invoice %s: %w", notification.Data.ID, err)
	}

	return s.DriveInvoice(ctx, invoice)
}

// DriveInvoice advances an invoice from whatever state it is in.
//
// It is the single entry point for both live notifications and the
// reconciliation sweep, which is what makes the sweep trustworthy: a
// re-driven invoice takes exactly the same path a live event would have.
func (s *Settler) DriveInvoice(ctx context.Context, inv *openmeter.Invoice) (string, error) {
	if inv.NeedsAttention() {
		metrics.InvoicesNeedingAttention.WithLabelValues(attentionLabel(inv)).Inc()
		s.log.Warn("invoice needs attention",
			"invoice_id", inv.ID,
			"status", inv.Status,
			"extended_status", inv.StatusDetails.ExtendedStatus,
			"failed", inv.StatusDetails.Failed,
			"validation_issues", len(inv.ValidationIssues),
		)
	}

	extended := inv.StatusDetails.ExtendedStatus
	switch {
	case matchesAny(extended, s.om.DraftSyncStatuses()):
		return HandlerDraftSync, s.draftSync(ctx, inv)
	case matchesAny(extended, s.om.IssuingSyncStatuses()):
		return HandlerIssuingSync, s.issuingSync(ctx, inv)
	case matchesAny(extended, s.om.PaymentPendingStatuses()):
		return HandlerPaymentStatus, s.paymentPending(ctx, inv)
	}

	// Not at a pause point. Every other state either belongs to OpenMeter
	// alone (gathering, issued while Stripe collects) or is terminal.
	// Draft* noops (draft / draft.invalid) are Info so ops can see why a
	// webhook did not reach Connect — Debug left reconcile sweeps silent.
	msg := "invoice not at a sync hook"
	attrs := []any{
		"invoice_id", inv.ID,
		"status", inv.Status,
		"extended_status", extended,
	}
	if strings.HasPrefix(strings.ToLower(inv.Status), "draft") ||
		strings.HasPrefix(strings.ToLower(extended), "draft") {
		s.log.Info(msg, attrs...)
	} else {
		s.log.Debug(msg, attrs...)
	}
	return HandlerNoop, nil
}

// HandleStripeEvent processes one raw Stripe webhook body.
//
// This is the half of the reconciliation OpenMeter cannot do for itself: it
// never sees Stripe's webhooks, so an invoice sits in payment_processing
// forever unless the worker tells it money arrived.
func (s *Settler) HandleStripeEvent(ctx context.Context, raw []byte) (string, error) {
	var event struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Account string `json:"account"`
		Data    struct {
			Object struct {
				ID            string            `json:"id"`
				Object        string            `json:"object"`
				Invoice       string            `json:"invoice"`
				Metadata      map[string]string `json:"metadata"`
				Status        string            `json:"status"`
				PaymentIntent string            `json:"payment_intent"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return HandlerNoop, faults.Wrap("unparseable_stripe_event", err)
	}

	trigger, ok := triggerFor(event.Type)
	if !ok {
		s.log.Debug("ignoring stripe event", "event_id", event.ID, "type", event.Type)
		return HandlerNoop, nil
	}

	invoiceID := strings.TrimSpace(event.Data.Object.Metadata[MetaInvoiceID])
	if invoiceID == "" {
		// Not an invoice settlement raised — another integration's object, or
		// a platform charge unrelated to OpenMeter. Ignoring is correct.
		s.log.Debug("stripe event carries no openmeter invoice id",
			"event_id", event.ID, "type", event.Type, "object", event.Data.Object.ID)
		return HandlerNoop, nil
	}

	s.log.Info("advancing payment status",
		"event_id", event.ID,
		"type", event.Type,
		"trigger", trigger,
		"invoice_id", invoiceID,
		"stripe_object", event.Data.Object.ID,
		"stripe_account", event.Account,
	)

	if err := s.om.UpdatePaymentStatus(ctx, invoiceID, trigger); err != nil {
		if openmeter.IsConflict(err) {
			// The invoice already moved — a duplicate delivery, or the
			// reconciler got there first. The desired state holds either way.
			metrics.InvoiceStateTransitions.WithLabelValues(trigger, "already_applied").Inc()
			s.log.Info("payment status already applied", "invoice_id", invoiceID, "trigger", trigger)
			return HandlerPaymentStatus, nil
		}
		metrics.InvoiceStateTransitions.WithLabelValues(trigger, "error").Inc()
		return HandlerPaymentStatus, fmt.Errorf("update payment status %s -> %s: %w", invoiceID, trigger, err)
	}

	metrics.InvoiceStateTransitions.WithLabelValues(trigger, "ok").Inc()
	return HandlerPaymentStatus, nil
}

// triggerFor maps a Stripe event type to an OpenMeter payment trigger.
//
// Only events that change what the customer owes are mapped. Anything else
// (invoice.created, invoice.updated, customer.*) is Stripe narrating its own
// bookkeeping and must not move the OpenMeter invoice.
func triggerFor(eventType string) (string, bool) {
	switch eventType {
	case "invoice.payment_succeeded", "invoice.paid":
		return openmeter.TriggerPaid, true
	case "invoice.payment_failed":
		return openmeter.TriggerPaymentFailed, true
	case "invoice.marked_uncollectible":
		return openmeter.TriggerUncollectible, true
	case "invoice.voided":
		return openmeter.TriggerVoid, true
	case "invoice.payment_action_required":
		return openmeter.TriggerActionRequired, true
	case "invoice.overdue":
		return openmeter.TriggerOverdue, true
	default:
		return "", false
	}
}

// paymentPending handles an invoice parked at payment_processing.pending.
//
// The usual path is to do nothing: Stripe's webhook is the authority on
// payment and it will arrive on its own. This exists for the case where it did
// not — a webhook dropped, an endpoint misconfigured for a day — and asks
// Stripe directly rather than leaving the invoice parked forever.
func (s *Settler) paymentPending(ctx context.Context, inv *openmeter.Invoice) error {
	stripeInvoiceID := inv.ExternalIDs.Invoicing
	if stripeInvoiceID == "" {
		return faults.Permanentf("missing_stripe_invoice",
			"invoice %s is awaiting payment but has no Stripe invoice recorded", inv.ID)
	}

	tgt, err := s.resolveTarget(ctx, inv)
	if err != nil {
		return err
	}

	stripeInvoice, err := s.stripe.GetInvoice(ctx, stripeInvoiceID, tgt.requestOptions(""))
	if err != nil {
		if stripe.IsNotFound(err) {
			return faults.Wrap("stripe_invoice_missing", err)
		}
		return fmt.Errorf("read stripe invoice %s: %w", stripeInvoiceID, err)
	}

	trigger, ok := triggerForStripeInvoiceStatus(stripeInvoice)
	if !ok {
		s.log.Debug("payment still pending at stripe",
			"invoice_id", inv.ID, "stripe_invoice", stripeInvoiceID, "stripe_status", stripeInvoice.Status)
		return nil
	}

	s.log.Info("reconciling payment from stripe state",
		"invoice_id", inv.ID, "stripe_invoice", stripeInvoiceID,
		"stripe_status", stripeInvoice.Status, "trigger", trigger)

	if err := s.om.UpdatePaymentStatus(ctx, inv.ID, trigger); err != nil {
		if openmeter.IsConflict(err) {
			metrics.InvoiceStateTransitions.WithLabelValues(trigger, "already_applied").Inc()
			return nil
		}
		metrics.InvoiceStateTransitions.WithLabelValues(trigger, "error").Inc()
		return fmt.Errorf("update payment status %s -> %s: %w", inv.ID, trigger, err)
	}
	metrics.InvoiceStateTransitions.WithLabelValues(trigger, "ok").Inc()
	return nil
}

func triggerForStripeInvoiceStatus(inv *stripe.Invoice) (string, bool) {
	switch {
	case inv.Status == "paid":
		return openmeter.TriggerPaid, true
	case inv.Status == "void":
		return openmeter.TriggerVoid, true
	case inv.Status == "uncollectible":
		return openmeter.TriggerUncollectible, true
	default:
		return "", false
	}
}

// matchesAny reports whether status equals any candidate, case-insensitively.
func matchesAny(status string, candidates []string) bool {
	status = strings.TrimSpace(status)
	if status == "" {
		return false
	}
	for _, c := range candidates {
		if strings.EqualFold(status, strings.TrimSpace(c)) {
			return true
		}
	}
	return false
}

func attentionLabel(inv *openmeter.Invoice) string {
	if inv.StatusDetails.Failed {
		return "failed"
	}
	return inv.Status
}

// retryable reports whether an upstream error is worth another attempt.
func retryable(err error) bool {
	var omErr *openmeter.APIError
	if errors.As(err, &omErr) {
		return omErr.Retryable()
	}
	var sErr *stripe.Error
	if errors.As(err, &sErr) {
		return sErr.Retryable()
	}
	return true
}

// classify converts an upstream error into a permanent fault when retrying is
// pointless, so it reaches the DLQ instead of occupying a lane for hours.
func classify(reason string, err error) error {
	if err == nil {
		return nil
	}
	if faults.IsPermanent(err) {
		return err
	}
	if !retryable(err) {
		return faults.Wrap(reason, err)
	}
	return err
}
