package lifecycle

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/openmeter"
)

// draftInvoice builds an OpenMeter invoice parked at the draft sync hook.
func draftInvoice() openmeter.Invoice {
	return openmeter.Invoice{
		ID:       "inv_om_1",
		Currency: "USD",
		Status:   openmeter.StatusDraft,
		StatusDetails: openmeter.StatusDetails{
			ExtendedStatus:   "draft.sync",
			AvailableActions: openmeter.AvailableActions{Approve: &openmeter.ActionDetails{ResultingState: "payment_processing.pending"}},
		},
		Customer: openmeter.Customer{ID: "cus_om_1", Key: "owner-42", Name: "Acme Robotics"},
		Totals:   openmeter.Totals{Total: "42.50"},
		Lines: []openmeter.Line{
			{
				ID:       "line_1",
				Type:     "usage_based",
				Name:     "Network fees",
				Currency: "USD",
				Quantity: "1204",
				Totals:   openmeter.Totals{Total: "30.00"},
				Period:   &openmeter.Period{Start: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)},
				Children: []openmeter.Line{{ID: "line_1_child", Name: "Tier 1", Totals: openmeter.Totals{Total: "30.00"}}},
				Discounts: &openmeter.LineDiscounts{
					Amount: []openmeter.Discount{{ID: "disc_1", Amount: "5.00", Description: "Included allowance"}},
				},
			},
			{
				ID:       "line_2",
				Type:     "flat_fee",
				Name:     "Platform fee",
				Currency: "USD",
				Totals:   openmeter.Totals{Total: "12.50"},
			},
		},
		UpdatedAt: time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC),
	}
}

func notificationFor(t *testing.T, invoiceID, eventType string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id":        "01J2KNP1YTXQRXHTDJ4KPR7PZ0",
		"type":      eventType,
		"timestamp": "2026-08-04T12:00:00Z",
		"data":      map[string]any{"id": invoiceID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDraftSyncBuildsStripeMirrorAndReportsIDs(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	settler := newTestSettler(t, om, sc, nil)

	handler, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))
	if err != nil {
		t.Fatalf("draft sync failed: %v", err)
	}
	if handler != HandlerDraftSync {
		t.Fatalf("handler = %q, want %q", handler, HandlerDraftSync)
	}

	stripeInvoice := sc.onlyInvoice(t)
	if stripeInvoice.Metadata[MetaInvoiceID] != invoice.ID {
		t.Errorf("stripe invoice is not tagged with the OpenMeter invoice id: %v", stripeInvoice.Metadata)
	}
	if stripeInvoice.Status != "draft" {
		t.Errorf("stripe invoice status = %q, want draft — OpenMeter still owns the lifecycle", stripeInvoice.Status)
	}

	// Two OpenMeter lines become two Stripe items, priced at the line totals.
	items := sc.itemsFor(stripeInvoice.ID)
	if len(items) != 2 {
		t.Fatalf("expected 2 invoice items, got %d", len(items))
	}
	byLine := map[string]int64{}
	for _, item := range items {
		byLine[item.Metadata[MetaLineID]] = item.Amount
	}
	if byLine["line_1"] != 3000 || byLine["line_2"] != 1250 {
		t.Errorf("item amounts = %v, want line_1=3000 line_2=1250 minor units", byLine)
	}

	body := om.draftFor(t, invoice.ID)
	if body.Invoicing == nil {
		t.Fatal("draft synchronized body carried no invoicing block")
	}
	if body.Invoicing.ExternalID != stripeInvoice.ID {
		t.Errorf("external id = %q, want the Stripe invoice %q", body.Invoicing.ExternalID, stripeInvoice.ID)
	}

	// Parent lines, their children, and the discount must all be mapped: this
	// is the only call that accepts them.
	mapped := map[string]string{}
	for _, m := range body.Invoicing.LineExternalIDs {
		mapped[m.LineID] = m.ExternalID
	}
	for _, want := range []string{"line_1", "line_1_child", "line_2"} {
		if mapped[want] == "" {
			t.Errorf("line %s was not mapped to a Stripe id", want)
		}
	}
	if mapped["line_1_child"] != mapped["line_1"] {
		t.Error("a child line should map to the item that contains it")
	}
	if len(body.Invoicing.LineDiscountExternalIDs) != 1 || body.Invoicing.LineDiscountExternalIDs[0].LineDiscountID != "disc_1" {
		t.Errorf("discount mapping = %+v, want disc_1", body.Invoicing.LineDiscountExternalIDs)
	}
}

// A redelivered draft event must not add a second copy of every line.
func TestDraftSyncIsIdempotent(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	settler := newTestSettler(t, om, sc, nil)
	notification := notificationFor(t, invoice.ID, "invoice.updated")

	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("first draft sync: %v", err)
	}

	// The second delivery arrives after OpenMeter recorded the external id.
	stripeInvoice := sc.onlyInvoice(t)
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("second draft sync: %v", err)
	}

	if got := len(sc.itemsFor(stripeInvoice.ID)); got != 2 {
		t.Fatalf("after a redelivery the invoice has %d items, want 2", got)
	}
	if created := sc.callCount("POST", "/v1/invoices"); created != 1 {
		t.Fatalf("created %d Stripe invoices, want 1", created)
	}
}

// Per-line rounding can leave the sum a unit away from the invoice total; the
// gap is posted explicitly rather than hidden.
func TestDraftSyncPostsRoundingAdjustment(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	// Three lines of 0.005 round to 0.01 each (0.03), but the exact total is
	// 0.015, which rounds to 0.02. The one-cent gap must be posted.
	invoice.Lines = []openmeter.Line{
		{ID: "l1", Name: "A", Totals: openmeter.Totals{Total: "0.005"}},
		{ID: "l2", Name: "B", Totals: openmeter.Totals{Total: "0.005"}},
		{ID: "l3", Name: "C", Totals: openmeter.Totals{Total: "0.005"}},
	}
	invoice.Totals.Total = "0.015"
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	settler := newTestSettler(t, om, sc, nil)
	if _, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated")); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	stripeInvoice := sc.onlyInvoice(t)
	var total int64
	var adjustment int64
	for _, item := range sc.itemsFor(stripeInvoice.ID) {
		total += item.Amount
		if item.Metadata[MetaLineID] == roundingLineID {
			adjustment = item.Amount
		}
	}

	if adjustment != -1 {
		t.Errorf("rounding adjustment = %d, want -1 minor unit", adjustment)
	}
	if total != 2 {
		t.Errorf("stripe items total %d minor units, want 2 to match the OpenMeter total", total)
	}
}

// A gap too large to be rounding is a mapping bug. It must stop rather than
// bill the customer the wrong amount.
func TestDraftSyncRefusesWhenTotalsDisagree(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	invoice.Totals.Total = "999.00" // lines only add up to 42.50
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	settler := newTestSettler(t, om, sc, nil)
	_, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))

	if err == nil {
		t.Fatal("a total mismatch was accepted")
	}
	if !faults.IsPermanent(err) {
		t.Fatalf("total mismatch should be permanent, got a retryable error: %v", err)
	}
	if reason := faults.Reason(err); reason != "total_mismatch" {
		t.Errorf("reason = %q, want total_mismatch", reason)
	}
	if _, called := om.draftSynchronized[invoice.ID]; called {
		t.Error("draft synchronized was called despite the mismatch")
	}
}

func TestIssuingSyncFinalizesAndReportsNumberAndPayment(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})
	settler := newTestSettler(t, om, sc, nil)

	// Draft first, so a Stripe invoice exists to finalize.
	if _, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated")); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	stripeInvoice := sc.onlyInvoice(t)
	invoice.Status = openmeter.StatusIssuing
	invoice.StatusDetails.ExtendedStatus = "issuing.sync"
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	handler, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))
	if err != nil {
		t.Fatalf("issuing sync: %v", err)
	}
	if handler != HandlerIssuingSync {
		t.Fatalf("handler = %q, want %q", handler, HandlerIssuingSync)
	}

	finalized := sc.onlyInvoice(t)
	if finalized.Status != "open" {
		t.Errorf("stripe invoice status = %q, want open after finalization", finalized.Status)
	}

	body := om.issuingFor(t, invoice.ID)
	if body.Invoicing == nil || body.Invoicing.InvoiceNumber != finalized.Number {
		t.Errorf("invoice number not carried across: %+v (stripe %q)", body.Invoicing, finalized.Number)
	}
	if body.Invoicing.SentToCustomerA == nil {
		t.Error("sentToCustomerAt was not set")
	}
	if body.Payment == nil {
		t.Fatal("payment external id missing")
	}
	pi, err := finalized.PrimaryPaymentIntent()
	if err != nil {
		t.Fatalf("finalized invoice payment intent: %v", err)
	}
	if body.Payment.ExternalID != pi {
		t.Errorf("payment external id = %+v, want the PaymentIntent %q", body.Payment, pi)
	}
	if got := sc.callCount("POST", "/v1/payment_intents/"+pi+"/confirm"); got != 1 {
		t.Errorf("confirm payment intent calls = %d, want 1", got)
	}
}

func TestIssuingSyncAppliesApplicationFeeForConnectedAccounts(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{
		"stripe_connect_account_id": "acct_dev_1",
		"stripe_charge_model":       string(config.ChargeModelDestination),
	})

	settler := newTestSettler(t, om, sc, func(c *config.Stripe) {
		c.ApplicationFeeBps = 250 // 2.5%
		c.ApplicationFeeFlatMinor = 30
	})

	notification := notificationFor(t, invoice.ID, "invoice.updated")
	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	stripeInvoice := sc.onlyInvoice(t)
	invoice.Status = openmeter.StatusIssuing
	invoice.StatusDetails.ExtendedStatus = "issuing.sync"
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("issuing sync: %v", err)
	}

	// 4250 minor units * 2.5% = 106 (truncated), plus 30 flat.
	if got := sc.onlyInvoice(t).ApplicationFee; got != 136 {
		t.Errorf("application fee = %d, want 136 minor units", got)
	}
}

// Platform mode is the no-Connect default: no fee, no connected account.
func TestPlatformModeTakesNoApplicationFee(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	settler := newTestSettler(t, om, sc, func(c *config.Stripe) {
		c.ApplicationFeeBps = 250
		c.ApplicationFeeFlatMinor = 30
	})

	notification := notificationFor(t, invoice.ID, "invoice.updated")
	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	stripeInvoice := sc.onlyInvoice(t)
	invoice.Status = openmeter.StatusIssuing
	invoice.StatusDetails.ExtendedStatus = "issuing.sync"
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("issuing sync: %v", err)
	}

	if got := sc.onlyInvoice(t).ApplicationFee; got != 0 {
		t.Errorf("platform-mode invoice took a %d fee; there is no connected account to split with", got)
	}
	if sc.accountsSeen()["acct_dev_1"] {
		t.Error("platform mode should never send a Stripe-Account header")
	}
}

// Direct charges live on the connected account, so every call must carry the
// Stripe-Account header — otherwise the invoice lands on the platform's books.
func TestDirectChargesTargetTheConnectedAccount(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{
		"stripe_connect_account_id": "acct_dev_1",
		"stripe_charge_model":       string(config.ChargeModelDirect),
	})

	settler := newTestSettler(t, om, sc, nil)
	if _, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated")); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	accounts := sc.accountsSeen()
	if !accounts["acct_dev_1"] {
		t.Fatal("no request carried the Stripe-Account header")
	}
	if accounts[""] {
		t.Error("some requests went to the platform account during a direct charge")
	}
}

// Silently falling back to the platform account would put the money in the
// wrong place, so a missing account is a hard stop.
func TestConnectModelWithoutAnAccountIsPermanentlyRejected(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{
		"stripe_charge_model": string(config.ChargeModelDirect),
	})

	settler := newTestSettler(t, om, sc, nil)
	_, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))

	if err == nil {
		t.Fatal("a direct charge without a Connect account was accepted")
	}
	if !faults.IsPermanent(err) || faults.Reason(err) != "missing_connect_account" {
		t.Fatalf("error = %v (reason %q), want a permanent missing_connect_account", err, faults.Reason(err))
	}
	if got := sc.callCount("POST", "/v1/invoices"); got != 0 {
		t.Errorf("created %d Stripe invoices despite the misconfiguration", got)
	}
}

func TestInvoiceMetadataOverridesCustomerRouting(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	// The customer has since moved to Connect, but this invoice was raised
	// under the platform model and must settle the way it was frozen.
	invoice.Metadata = openmeter.Metadata{"stripe_charge_model": string(config.ChargeModelPlatform)}
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{
		"stripe_connect_account_id": "acct_dev_1",
		"stripe_charge_model":       string(config.ChargeModelDirect),
	})

	settler := newTestSettler(t, om, sc, nil)
	if _, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated")); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	if sc.accountsSeen()["acct_dev_1"] {
		t.Error("invoice-level platform override was ignored in favour of the customer's Connect account")
	}
}

func TestStripeEventsMapToPaymentTriggers(t *testing.T) {
	cases := map[string]string{
		"invoice.payment_succeeded":       openmeter.TriggerPaid,
		"invoice.paid":                    openmeter.TriggerPaid,
		"invoice.payment_failed":          openmeter.TriggerPaymentFailed,
		"invoice.marked_uncollectible":    openmeter.TriggerUncollectible,
		"invoice.voided":                  openmeter.TriggerVoid,
		"invoice.payment_action_required": openmeter.TriggerActionRequired,
		"invoice.overdue":                 openmeter.TriggerOverdue,
	}

	for eventType, wantTrigger := range cases {
		t.Run(eventType, func(t *testing.T) {
			om, sc := newFakeOpenMeter(t), newFakeStripe(t)
			settler := newTestSettler(t, om, sc, nil)

			body := []byte(`{"id":"evt_1","type":"` + eventType + `","account":"acct_dev_1",
				"data":{"object":{"id":"in_stripe_1","object":"invoice",
				"metadata":{"` + MetaInvoiceID + `":"inv_om_1"}}}}`)

			handler, err := settler.HandleStripeEvent(context.Background(), body)
			if err != nil {
				t.Fatalf("HandleStripeEvent: %v", err)
			}
			if handler != HandlerPaymentStatus {
				t.Fatalf("handler = %q, want %q", handler, HandlerPaymentStatus)
			}

			triggers := om.triggersFor("inv_om_1")
			if len(triggers) != 1 || triggers[0] != wantTrigger {
				t.Errorf("triggers = %v, want [%s]", triggers, wantTrigger)
			}
		})
	}
}

// Stripe narrates a lot of bookkeeping that must not move an OpenMeter invoice.
func TestStripeEventsWeDoNotActOn(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	for _, eventType := range []string{"invoice.created", "invoice.updated", "customer.created", "charge.succeeded"} {
		body := []byte(`{"id":"evt_1","type":"` + eventType + `","data":{"object":{"id":"in_1",
			"metadata":{"` + MetaInvoiceID + `":"inv_om_1"}}}}`)

		handler, err := settler.HandleStripeEvent(context.Background(), body)
		if err != nil {
			t.Fatalf("%s: %v", eventType, err)
		}
		if handler != HandlerNoop {
			t.Errorf("%s produced handler %q, want a no-op", eventType, handler)
		}
	}
	if got := om.triggersFor("inv_om_1"); len(got) != 0 {
		t.Errorf("informational events advanced the invoice: %v", got)
	}
}

// Objects from another integration carry no OpenMeter id; touching them would
// be acting on someone else's money.
func TestStripeEventWithoutOurMetadataIsIgnored(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	body := []byte(`{"id":"evt_1","type":"invoice.payment_succeeded","data":{"object":{"id":"in_other"}}}`)
	handler, err := settler.HandleStripeEvent(context.Background(), body)
	if err != nil {
		t.Fatalf("HandleStripeEvent: %v", err)
	}
	if handler != HandlerNoop {
		t.Errorf("handler = %q, want a no-op for a foreign invoice", handler)
	}
}

// A 409 means the desired state already holds. Treating it as an error would
// retry forever and eventually dead-letter a perfectly settled invoice.
func TestConflictOnCompletionIsTreatedAsSuccess(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)
	om.conflictOn["payment"] = true

	body := []byte(`{"id":"evt_1","type":"invoice.payment_succeeded","data":{"object":{"id":"in_1",
		"metadata":{"` + MetaInvoiceID + `":"inv_om_1"}}}}`)

	if _, err := settler.HandleStripeEvent(context.Background(), body); err != nil {
		t.Fatalf("a 409 should be a success, got: %v", err)
	}
}

// OpenMeter often returns a non-409 4xx for already-paid trigger_paid; that
// must also ack so webhook redeliveries do not fill the DLQ.
func TestAlreadyPaidTriggerIsTreatedAsSuccess(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)
	om.alreadyPaidOn = true

	body := []byte(`{"id":"evt_1","type":"invoice.paid","data":{"object":{"id":"in_1",
		"metadata":{"` + MetaInvoiceID + `":"inv_om_1"}}}}`)

	if _, err := settler.HandleStripeEvent(context.Background(), body); err != nil {
		t.Fatalf("already-paid 4xx should be a success, got: %v", err)
	}
}

func TestDraftConflictIsTreatedAsSuccess(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})
	om.conflictOn["draft"] = true

	settler := newTestSettler(t, om, sc, nil)
	if _, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated")); err != nil {
		t.Fatalf("a 409 on draft synchronized should be a success, got: %v", err)
	}
}

// An invoice not sitting at a pause point is none of our business.
func TestInvoiceAwayFromAPauseHookIsANoop(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	invoice.Status = openmeter.StatusGathering
	invoice.StatusDetails.ExtendedStatus = "gathering"
	om.putInvoice(invoice)

	settler := newTestSettler(t, om, sc, nil)
	handler, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler != HandlerNoop {
		t.Errorf("handler = %q, want a no-op", handler)
	}
	if got := sc.callCount("POST", "/v1/invoices"); got != 0 {
		t.Errorf("a gathering invoice created %d Stripe invoices", got)
	}
}

func TestNonInvoiceNotificationsAreIgnored(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	body := []byte(`{"id":"n1","type":"entitlements.balance.threshold","data":{"id":"ent_1"}}`)
	handler, err := settler.HandleOpenMeterNotification(context.Background(), body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler != HandlerNoop {
		t.Errorf("handler = %q, want a no-op", handler)
	}
}

func TestUnparseableBodiesArePermanentFailures(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)
	settler := newTestSettler(t, om, sc, nil)

	if _, err := settler.HandleOpenMeterNotification(context.Background(), []byte(`{`)); !faults.IsPermanent(err) {
		t.Errorf("malformed notification should be permanent, got %v", err)
	}
	if _, err := settler.HandleStripeEvent(context.Background(), []byte(`{`)); !faults.IsPermanent(err) {
		t.Errorf("malformed stripe event should be permanent, got %v", err)
	}
}

// Transient upstream failures must stay retryable so the worker's backoff can
// ride them out instead of dead-lettering an invoice.
func TestTransientStripeFailureStaysRetryable(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})

	// Exhaust the client's own short retry budget so the error surfaces.
	sc.FailNext("/v1/customers", 10)

	settler := newTestSettler(t, om, sc, nil)
	_, err := settler.HandleOpenMeterNotification(context.Background(),
		notificationFor(t, invoice.ID, "invoice.updated"))

	if err == nil {
		t.Fatal("expected the simulated Stripe outage to surface")
	}
	if faults.IsPermanent(err) {
		t.Fatalf("a 500 from Stripe was classified as permanent: %v", err)
	}
}

// Payment reconciliation reads Stripe directly when the webhook never arrived.
func TestPaymentPendingReconcilesFromStripeState(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})
	settler := newTestSettler(t, om, sc, nil)

	notification := notificationFor(t, invoice.ID, "invoice.updated")
	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	// Stripe collected the money but its webhook was lost.
	stripeInvoice := sc.onlyInvoice(t)
	sc.SetInvoiceStatus(stripeInvoice.ID, "paid")

	invoice.Status = openmeter.StatusPaymentProcessing
	invoice.StatusDetails.ExtendedStatus = "payment_processing.pending"
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	handler, err := settler.HandleOpenMeterNotification(context.Background(), notification)
	if err != nil {
		t.Fatalf("payment reconciliation: %v", err)
	}
	if handler != HandlerPaymentStatus {
		t.Fatalf("handler = %q, want %q", handler, HandlerPaymentStatus)
	}
	if triggers := om.triggersFor(invoice.ID); len(triggers) != 1 || triggers[0] != openmeter.TriggerPaid {
		t.Errorf("triggers = %v, want [paid]", triggers)
	}
}

// While Stripe is still collecting, doing nothing is the right answer.
func TestPaymentPendingWaitsWhileStripeIsStillCollecting(t *testing.T) {
	om, sc := newFakeOpenMeter(t), newFakeStripe(t)

	invoice := draftInvoice()
	om.putInvoice(invoice)
	om.putCustomerMetadata("cus_om_1", openmeter.Metadata{})
	settler := newTestSettler(t, om, sc, nil)

	notification := notificationFor(t, invoice.ID, "invoice.updated")
	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("draft sync: %v", err)
	}

	stripeInvoice := sc.onlyInvoice(t)
	invoice.Status = openmeter.StatusPaymentProcessing
	invoice.StatusDetails.ExtendedStatus = "payment_processing.pending"
	invoice.ExternalIDs.Invoicing = stripeInvoice.ID
	om.putInvoice(invoice)

	if _, err := settler.HandleOpenMeterNotification(context.Background(), notification); err != nil {
		t.Fatalf("payment reconciliation: %v", err)
	}
	if triggers := om.triggersFor(invoice.ID); len(triggers) != 0 {
		t.Errorf("advanced the invoice while payment was still open: %v", triggers)
	}
}

func TestLineDescriptionIncludesQuantity(t *testing.T) {
	got := lineDescription(openmeter.Line{Name: "Network fees", Quantity: "1204"})
	if !strings.Contains(got, "Network fees") || !strings.Contains(got, "1204") {
		t.Errorf("description = %q, want the name and quantity", got)
	}

	if got := lineDescription(openmeter.Line{Description: "Fallback"}); got != "Fallback" {
		t.Errorf("description = %q, want the fallback description", got)
	}
	if got := lineDescription(openmeter.Line{}); got != "Usage" {
		t.Errorf("description = %q, want the Usage placeholder", got)
	}
	if got := lineDescription(openmeter.Line{Name: strings.Repeat("x", 600)}); len(got) > 500 {
		t.Errorf("description is %d characters; Stripe caps it at 500", len(got))
	}
}

func TestEscapeSearchQuotesValues(t *testing.T) {
	if got := escapeSearch("it's"); got != `it\'s` {
		t.Errorf("escapeSearch = %q, want the quote escaped", got)
	}
}
