package lifecycle

import (
	"context"
	"encoding/json"
	"errors"

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

	metrics.InvoiceStateTransitions.WithLabelValues(HandlerCollectRequest, "ok").Inc()
	s.log.Info("invoiced pending lines",
		"client_id", req.ClientID, "external_user_id", req.ExternalUserID,
		"customer_id", req.CustomerID, "invoices_raised", len(results))
	return HandlerCollectRequest, nil
}
