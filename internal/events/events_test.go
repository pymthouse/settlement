package events

import (
	"errors"
	"testing"
)

func TestDescribeStripePrefersConnectAccountAsKey(t *testing.T) {
	body := []byte(`{
		"id":"evt_123","type":"invoice.payment_succeeded","account":"acct_dev1","livemode":true,
		"data":{"object":{"id":"in_9","customer":"cus_9"}}
	}`)

	desc, err := DescribeStripe(body)
	if err != nil {
		t.Fatalf("DescribeStripe: %v", err)
	}
	if desc.PartitionKey != "acct_dev1" {
		t.Errorf("PartitionKey = %q, want the Connect account", desc.PartitionKey)
	}
	if desc.EventID != "evt_123" || desc.EventType != "invoice.payment_succeeded" {
		t.Errorf("unexpected descriptor: %+v", desc)
	}
	if !desc.Livemode {
		t.Error("livemode not carried through")
	}
	if desc.DedupeKey() != "stripe:evt_123" {
		t.Errorf("DedupeKey = %q", desc.DedupeKey())
	}
}

func TestDescribeStripeKeyFallbacks(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "falls back to the customer when there is no account",
			body: `{"id":"evt_1","type":"invoice.paid","data":{"object":{"id":"in_1","customer":"cus_7"}}}`,
			want: "cus_7",
		},
		{
			name: "reads an expanded customer object",
			body: `{"id":"evt_1","type":"invoice.paid","data":{"object":{"id":"in_1","customer":{"id":"cus_8"}}}}`,
			want: "cus_8",
		},
		{
			name: "falls back to the object id",
			body: `{"id":"evt_1","type":"customer.created","data":{"object":{"id":"cus_9"}}}`,
			want: "cus_9",
		},
		{
			name: "falls back to the event id",
			body: `{"id":"evt_1","type":"ping","data":{"object":{}}}`,
			want: "evt_1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc, err := DescribeStripe([]byte(tc.body))
			if err != nil {
				t.Fatalf("DescribeStripe: %v", err)
			}
			if desc.PartitionKey != tc.want {
				t.Errorf("PartitionKey = %q, want %q", desc.PartitionKey, tc.want)
			}
		})
	}
}

func TestDescribeStripeRejectsUnusableBodies(t *testing.T) {
	for _, body := range []string{`not json`, `{}`, `{"id":"evt_1"}`, `{"type":"invoice.paid"}`} {
		if _, err := DescribeStripe([]byte(body)); !errors.Is(err, ErrUnparseable) {
			t.Errorf("DescribeStripe(%q) error = %v, want ErrUnparseable", body, err)
		}
	}
}

func TestDescribeOpenMeterKeysOnCustomer(t *testing.T) {
	body := []byte(`{
		"id":"01J2KNP","type":"invoice.updated","timestamp":"2026-08-04T12:00:00Z",
		"data":{"id":"inv_1","customer":{"id":"cus_om_1","key":"owner-42"}}
	}`)

	desc, err := DescribeOpenMeter(body)
	if err != nil {
		t.Fatalf("DescribeOpenMeter: %v", err)
	}
	if desc.PartitionKey != "cus_om_1" {
		t.Errorf("PartitionKey = %q, want the OpenMeter customer id", desc.PartitionKey)
	}
	if desc.Source != SourceOpenMeter || desc.EventID != "01J2KNP" {
		t.Errorf("unexpected descriptor: %+v", desc)
	}
}

func TestDescribeOpenMeterKeyFallbacks(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"customer key", `{"id":"n1","type":"invoice.updated","data":{"id":"inv_1","customer":{"key":"owner-42"}}}`, "owner-42"},
		{"invoice id", `{"id":"n1","type":"invoice.updated","data":{"id":"inv_1"}}`, "inv_1"},
		{"event id", `{"id":"n1","type":"entitlements.reset"}`, "n1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc, err := DescribeOpenMeter([]byte(tc.body))
			if err != nil {
				t.Fatalf("DescribeOpenMeter: %v", err)
			}
			if desc.PartitionKey != tc.want {
				t.Errorf("PartitionKey = %q, want %q", desc.PartitionKey, tc.want)
			}
		})
	}
}

// Every event for one payer must land on one partition, or the lifecycle can
// be processed out of order.
func TestPartitionKeyIsStableAcrossEventsForOnePayer(t *testing.T) {
	first, err := DescribeOpenMeter([]byte(`{"id":"n1","type":"invoice.created","data":{"id":"inv_1","customer":{"id":"cus_1"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := DescribeOpenMeter([]byte(`{"id":"n2","type":"invoice.updated","data":{"id":"inv_1","customer":{"id":"cus_1"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.PartitionKey != second.PartitionKey {
		t.Fatalf("keys diverged: %q vs %q", first.PartitionKey, second.PartitionKey)
	}
}

func TestDescribeUnknownSource(t *testing.T) {
	if _, err := Describe("paypal", []byte(`{}`)); !errors.Is(err, ErrUnparseable) {
		t.Fatalf("expected ErrUnparseable for an unknown source, got %v", err)
	}
}

func TestIsOpenMeterInvoiceEvent(t *testing.T) {
	yes := []string{"invoice.created", "invoice.updated", "invoicing.invoice.updated"}
	no := []string{"entitlements.reset", "entitlements.balance.threshold", "invoice.deleted", ""}

	for _, eventType := range yes {
		if !IsOpenMeterInvoiceEvent(eventType) {
			t.Errorf("IsOpenMeterInvoiceEvent(%q) = false, want true", eventType)
		}
	}
	for _, eventType := range no {
		if IsOpenMeterInvoiceEvent(eventType) {
			t.Errorf("IsOpenMeterInvoiceEvent(%q) = true, want false", eventType)
		}
	}
}
