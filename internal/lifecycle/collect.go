package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/openmeter"
)

// CollectRequest asks settlement to raise a customer's pending gathering
// lines into a real invoice.
//
// Deciding *whether* a customer is due — soft-negative ceiling, lead window,
// Stripe's minimum-charge floor — stays pymthouse's job; it already reads the
// billing config this depends on and settlement has no reason to duplicate
// that. This event means "pymthouse has already decided yes, execute it."
// CustomerID is pymthouse's already-resolved OpenMeter customer id, so this
// handler never needs to know how identity resolution works either.
type CollectRequest struct {
	ClientID       string `json:"clientId"`
	ExternalUserID string `json:"externalUserId"`
	CustomerID     string `json:"customerId"`
	// RequestID is the dedupe key (events.DescribeCollectRequest); carried
	// here too purely so log lines can be traced back to the originating
	// pymthouse decision.
	RequestID string `json:"requestId"`
	// Force marks an explicit "collect now" (pymthouse's /billing/collect
	// endpoint) as opposed to the automatic mid-cycle trigger. It decides how
	// hard we push a freshly raised invoice past OpenMeter's own collection
	// period and approval delay: see the snapshot/advance/approve sequence
	// below.
	Force bool `json:"force"`
}

// HandleCollectRequest processes one raw collect-request body.
//
// This is the one place settlement *raises* an invoice rather than driving
// one that already exists. Running it through the worker's per-customer
// lanes (shared with every other handler for the same customer) is the whole
// point: a second raise request for a customer already mid-raise waits its
// turn in the lane instead of reaching Konnect at the same time and racing
// the first one into "an active realization run already exists."
func (s *Settler) HandleCollectRequest(ctx context.Context, raw []byte) (string, error) {
	var req CollectRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return HandlerNoop, faults.Wrap("unparseable_collect_request", err)
	}
	if req.CustomerID == "" {
		return HandlerNoop, faults.Permanentf("missing_customer_id",
			"collect request for client=%s external_user=%s carries no customerId",
			req.ClientID, req.ExternalUserID)
	}

	results, err := s.om.InvoicePendingLines(ctx, req.CustomerID)
	if err != nil {
		if errors.Is(err, openmeter.ErrRealizationRunActive) {
			// Not our failure — a prior raise for this customer has not
			// finalized yet (most commonly: still waiting on the
			// payment-status report). Once it does, this customer's next
			// natural trigger (or a fresh request) succeeds normally.
			// Nothing to retry here and nothing to page anyone about.
			metrics.InvoiceStateTransitions.WithLabelValues(HandlerCollectRequest, "already_in_progress").Inc()
			s.log.Info("collect request skipped: realization run already active",
				"client_id", req.ClientID, "external_user_id", req.ExternalUserID,
				"customer_id", req.CustomerID)
			return HandlerCollectRequest, nil
		}
		metrics.InvoiceStateTransitions.WithLabelValues(HandlerCollectRequest, "error").Inc()
		return HandlerCollectRequest, err
	}

	for _, result := range results {
		invoiceID := strings.TrimSpace(result.ID)
		if invoiceID == "" {
			continue
		}
		s.pushPastCollectionWindow(ctx, invoiceID, req.Force)
	}

	metrics.InvoiceStateTransitions.WithLabelValues(HandlerCollectRequest, "ok").Inc()
	s.log.Info("invoiced pending lines",
		"client_id", req.ClientID, "external_user_id", req.ExternalUserID,
		"customer_id", req.CustomerID, "invoices_raised", len(results))
	return HandlerCollectRequest, nil
}

// pushPastCollectionWindow mirrors what pymthouse used to do the moment it
// raised an invoice, moved here with the raise itself. With auto_advance and
// a zero collection period Advance is frequently a no-op — the invoice may
// already be past the state it would move it from — so failures here are
// routine and only logged, never surfaced to the caller: the worst case is
// settlement's normal draft-sync webhook handling picks up the advance a
// little later instead of immediately.
func (s *Settler) pushPastCollectionWindow(ctx context.Context, invoiceID string, force bool) {
	if force {
		// Native way to skip the collection period for an invoice parked in
		// draft.waiting_for_collection.
		if err := s.om.SnapshotQuantities(ctx, invoiceID); err != nil {
			s.log.Info("collect request: snapshot skipped", "invoice_id", invoiceID, "error", err)
		}
	}
	if err := s.om.Advance(ctx, invoiceID); err != nil {
		s.log.Info("collect request: advance skipped", "invoice_id", invoiceID, "error", err)
	}
	if force {
		// Skip draft.waiting_auto_approval so settlement can issue immediately.
		if err := s.om.Approve(ctx, invoiceID); err != nil {
			s.log.Info("collect request: approve skipped", "invoice_id", invoiceID, "error", err)
		}
	}
}
