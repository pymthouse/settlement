package lifecycle

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/openmeter"
)

func collectRequestBody(t *testing.T, req CollectRequest) []byte {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestHandleCollectRequestRaisesPendingLines(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setInvoicePendingLinesResult([]openmeter.InvoicePendingLinesResult{{ID: "inv_om_1"}})
	settler := newTestSettler(t, om, sc, nil)

	outcome, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		ClientID:       "client_1",
		ExternalUserID: "eu_1",
		CustomerID:     "cus_om_1",
		RequestID:      "req_1",
	}))
	if err != nil {
		t.Fatalf("HandleCollectRequest: %v", err)
	}
	if outcome != HandlerCollectRequest {
		t.Errorf("outcome = %q, want %q", outcome, HandlerCollectRequest)
	}

	calls := om.invoicePendingLinesCallsSeen()
	if len(calls) != 1 || calls[0] != "cus_om_1" {
		t.Errorf("invoice_pending_lines calls = %v, want a single call for cus_om_1", calls)
	}
}

// The collision this feature exists to absorb: Konnect reports the customer
// already has an unresolved raise in flight. This must come back as a clean
// success, not an error — there is nothing to retry and nothing to page
// anyone about, the prior raise will finalize on its own.
func TestHandleCollectRequestTreatsActiveRealizationRunAsSuccess(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setRealizationRunActive(true)
	settler := newTestSettler(t, om, sc, nil)

	outcome, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		CustomerID: "cus_om_1",
		RequestID:  "req_1",
	}))
	if err != nil {
		t.Fatalf("an active realization run must not surface as an error: %v", err)
	}
	if outcome != HandlerCollectRequest {
		t.Errorf("outcome = %q, want %q", outcome, HandlerCollectRequest)
	}
}

func TestHandleCollectRequestMissingCustomerIDIsPermanent(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	_, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		ClientID:       "client_1",
		ExternalUserID: "eu_1",
		RequestID:      "req_1",
	}))
	if !faults.IsPermanent(err) {
		t.Errorf("missing customerId should be a permanent failure, got %v", err)
	}
	if len(om.invoicePendingLinesCallsSeen()) != 0 {
		t.Error("must not call OpenMeter when customerId is missing")
	}
}

func TestHandleCollectRequestUnparseableBodyIsPermanent(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	if _, err := settler.HandleCollectRequest(context.Background(), []byte(`{`)); !faults.IsPermanent(err) {
		t.Errorf("malformed body should be a permanent failure, got %v", err)
	}
}
