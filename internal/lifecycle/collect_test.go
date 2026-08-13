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

// A forced ("collect now") request must aggressively push the freshly raised
// invoice past both the collection period and OpenMeter's approval delay, the
// same as pymthouse's own /billing/collect endpoint did before this raise
// moved into settlement.
func TestHandleCollectRequestForcePushesPastCollectionAndApproval(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setInvoicePendingLinesResult([]openmeter.InvoicePendingLinesResult{{ID: "inv_om_1"}})
	settler := newTestSettler(t, om, sc, nil)

	_, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		CustomerID: "cus_om_1",
		RequestID:  "req_1",
		Force:      true,
	}))
	if err != nil {
		t.Fatalf("HandleCollectRequest: %v", err)
	}

	if calls := om.snapshotCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("snapshot calls = %v, want one call for inv_om_1", calls)
	}
	if calls := om.advanceCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("advance calls = %v, want one call for inv_om_1", calls)
	}
	if calls := om.approveCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("approve calls = %v, want one call for inv_om_1", calls)
	}
}

// The automatic mid-cycle trigger (force=false) only nudges with Advance —
// snapshot and approve are reserved for an explicit collect-now, same
// distinction pymthouse's old advanceInvoice made.
func TestHandleCollectRequestWithoutForceOnlyAdvances(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setInvoicePendingLinesResult([]openmeter.InvoicePendingLinesResult{{ID: "inv_om_1"}})
	settler := newTestSettler(t, om, sc, nil)

	_, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		CustomerID: "cus_om_1",
		RequestID:  "req_1",
		Force:      false,
	}))
	if err != nil {
		t.Fatalf("HandleCollectRequest: %v", err)
	}

	if calls := om.advanceCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("advance calls = %v, want one call for inv_om_1", calls)
	}
	if len(om.snapshotCallsSeen()) != 0 {
		t.Error("a non-forced request must not snapshot")
	}
	if len(om.approveCallsSeen()) != 0 {
		t.Error("a non-forced request must not approve")
	}
}

// A snapshot failure (invoice already past the snapshot-able window) must
// not block Advance from still being attempted, and must not fail the whole
// request — this is routine, not exceptional.
func TestHandleCollectRequestSnapshotFailureDoesNotBlockAdvance(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setInvoicePendingLinesResult([]openmeter.InvoicePendingLinesResult{{ID: "inv_om_1"}})
	om.setSnapshotFails(true)
	settler := newTestSettler(t, om, sc, nil)

	_, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		CustomerID: "cus_om_1",
		RequestID:  "req_1",
		Force:      true,
	}))
	if err != nil {
		t.Fatalf("a snapshot failure must not surface as an error: %v", err)
	}
	if calls := om.advanceCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("advance calls = %v, want one call despite the snapshot failure", calls)
	}
	if calls := om.approveCallsSeen(); len(calls) != 1 || calls[0] != "inv_om_1" {
		t.Errorf("approve calls = %v, want one call despite the snapshot failure", calls)
	}
}

// Multiple invoices from one raise (a currency split, for example) must each
// get the same post-raise nudging.
func TestHandleCollectRequestNudgesEveryRaisedInvoice(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	om.setInvoicePendingLinesResult([]openmeter.InvoicePendingLinesResult{{ID: "inv_om_1"}, {ID: "inv_om_2"}})
	settler := newTestSettler(t, om, sc, nil)

	_, err := settler.HandleCollectRequest(context.Background(), collectRequestBody(t, CollectRequest{
		CustomerID: "cus_om_1",
		RequestID:  "req_1",
		Force:      true,
	}))
	if err != nil {
		t.Fatalf("HandleCollectRequest: %v", err)
	}

	if calls := om.advanceCallsSeen(); len(calls) != 2 {
		t.Errorf("advance calls = %v, want one per raised invoice", calls)
	}
}
