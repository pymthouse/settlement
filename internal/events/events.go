// Package events describes the messages that travel between the doorman and
// the settlement worker.
//
// The Kafka message value is always the *raw, unmodified* webhook body: it is
// the audit record, and re-serialising it would destroy the ability to
// re-verify a signature months later. Everything the worker needs for routing
// and partitioning is carried in Kafka headers instead, so the worker can make
// dispatch decisions without trusting a re-parse of the body.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Source names the system a message came from.
const (
	SourceStripe    = "stripe"
	SourceOpenMeter = "openmeter"
	// SourceCollectRequest marks a pymthouse-originated request to raise a
	// customer's pending gathering lines now. Unlike the other two sources
	// this is not a third-party webhook — the producer verifies it with a
	// shared secret, not a signature, but it flows through the identical
	// verify-then-publish path so it gets the same audit trail and the same
	// per-customer lane ordering as everything else.
	SourceCollectRequest = "pymthouse"
)

// Kafka header keys. They are prefixed to avoid colliding with headers added
// by brokers, mirrors or connectors.
const (
	HeaderSource     = "settlement-source"
	HeaderEventID    = "settlement-event-id"
	HeaderEventType  = "settlement-event-type"
	HeaderKey        = "settlement-key"
	HeaderAccount    = "settlement-account"
	HeaderLivemode   = "settlement-livemode"
	HeaderReceivedAt = "settlement-received-at"
	HeaderProducer   = "settlement-producer"

	// Dead-letter provenance, written when a message is parked.
	HeaderDLQReason    = "settlement-dlq-reason"
	HeaderDLQError     = "settlement-dlq-error"
	HeaderDLQTopic     = "settlement-dlq-source-topic"
	HeaderDLQPartition = "settlement-dlq-source-partition"
	HeaderDLQOffset    = "settlement-dlq-source-offset"
	HeaderDLQAttempts  = "settlement-dlq-attempts"
	HeaderDLQAt        = "settlement-dlq-at"

	// Replay provenance, written by `settlementctl dlq-redrive`.
	HeaderReplayOf = "settlement-replay-of"
)

// ErrUnparseable marks a body that is not the JSON shape we expect. The
// doorman rejects these with 400 rather than polluting the billing log.
var ErrUnparseable = errors.New("event body is not a recognisable event")

// Descriptor is the routing metadata extracted from a raw webhook body.
//
// Extracting it is the one piece of parsing the doorman does. It reads the
// body, it never rewrites it: PartitionKey has to be decided before the write
// to Kafka, and getting the key wrong silently breaks per-customer ordering.
type Descriptor struct {
	Source string
	// EventID is the provider's event id — the deduplication key.
	EventID string
	// EventType is the provider's event type, e.g. "invoice.payment_succeeded".
	EventType string
	// PartitionKey keeps all events for one payer on one partition, and so in
	// order relative to each other.
	PartitionKey string
	// Account is the Stripe Connect account id when the event was delivered on
	// behalf of a connected account.
	Account string
	// Livemode is false for Stripe test-mode events.
	Livemode bool
}

// stripeEvent is the subset of a Stripe event the doorman needs.
type stripeEvent struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Account  string `json:"account"`
	Livemode bool   `json:"livemode"`
	Object   string `json:"object"`
	Data     struct {
		Object struct {
			ID       string `json:"id"`
			Customer any    `json:"customer"`
		} `json:"object"`
	} `json:"data"`
}

// DescribeStripe extracts routing metadata from a raw Stripe webhook body.
//
// The partition key prefers the Connect account, because ordering matters most
// between events belonging to the same merchant; within a platform-mode event
// it falls back to the customer, then the object, then the event id. Every
// fallback still yields a stable key for the same logical payer.
func DescribeStripe(raw []byte) (Descriptor, error) {
	var ev stripeEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrUnparseable, err)
	}
	if ev.ID == "" || ev.Type == "" {
		return Descriptor{}, fmt.Errorf("%w: missing id or type", ErrUnparseable)
	}

	key := ev.Account
	if key == "" {
		key = customerID(ev.Data.Object.Customer)
	}
	if key == "" {
		key = ev.Data.Object.ID
	}
	if key == "" {
		key = ev.ID
	}

	return Descriptor{
		Source:       SourceStripe,
		EventID:      ev.ID,
		EventType:    ev.Type,
		PartitionKey: key,
		Account:      ev.Account,
		Livemode:     ev.Livemode,
	}, nil
}

// customerID reads Stripe's `customer` field, which is either an id string or
// an expanded object.
func customerID(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]any:
		if id, ok := t["id"].(string); ok {
			return id
		}
	}
	return ""
}

// openMeterNotification is the subset of an OpenMeter notification payload the
// doorman needs. `data` is the fully expanded invoice.
type openMeterNotification struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		ID       string `json:"id"`
		Customer struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		} `json:"customer"`
	} `json:"data"`
}

// DescribeOpenMeter extracts routing metadata from a raw OpenMeter
// notification body.
//
// The key is the OpenMeter customer id so that every invoice event for one
// customer is ordered — a draft.sync must never be processed after the
// issuing.sync that follows it.
func DescribeOpenMeter(raw []byte) (Descriptor, error) {
	var n openMeterNotification
	if err := json.Unmarshal(raw, &n); err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrUnparseable, err)
	}
	if n.ID == "" || n.Type == "" {
		return Descriptor{}, fmt.Errorf("%w: missing id or type", ErrUnparseable)
	}

	key := n.Data.Customer.ID
	if key == "" {
		key = n.Data.Customer.Key
	}
	if key == "" {
		key = n.Data.ID
	}
	if key == "" {
		key = n.ID
	}

	return Descriptor{
		Source:       SourceOpenMeter,
		EventID:      n.ID,
		EventType:    n.Type,
		PartitionKey: key,
	}, nil
}

// collectRequest is the subset of a pymthouse collect-request body the
// doorman needs. It mirrors lifecycle.CollectRequest's JSON shape but stays a
// separate type here — the events package must not import lifecycle.
type collectRequest struct {
	ClientID       string `json:"clientId"`
	ExternalUserID string `json:"externalUserId"`
	CustomerID     string `json:"customerId"`
	// RequestID is minted once by pymthouse per raise *decision*, not per
	// HTTP attempt: reused verbatim if pymthouse retries the same decision,
	// fresh for the next one. It is the dedupe key. A content hash of the
	// body was considered instead and rejected — two genuinely separate raise
	// requests for the same customer (the normal case across a billing cycle)
	// would carry identical {clientId, externalUserId, customerId} and hash
	// the same, silently dropping the second as if it were a retry of the
	// first.
	RequestID string `json:"requestId"`
}

// DescribeCollectRequest extracts routing metadata from a raw pymthouse
// collect-request body.
//
// The key is the OpenMeter customer id, same as DescribeOpenMeter — a raise
// request for a customer must land in the identical lane as that customer's
// draft/issuing/payment events, or it could run concurrently with one and
// race it on Konnect's side, which is the exact problem this event exists to
// avoid.
func DescribeCollectRequest(raw []byte) (Descriptor, error) {
	var req collectRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrUnparseable, err)
	}
	if req.CustomerID == "" {
		return Descriptor{}, fmt.Errorf("%w: missing customerId", ErrUnparseable)
	}
	if req.RequestID == "" {
		return Descriptor{}, fmt.Errorf("%w: missing requestId", ErrUnparseable)
	}
	return Descriptor{
		Source:       SourceCollectRequest,
		EventID:      req.RequestID,
		EventType:    "collect.requested",
		PartitionKey: req.CustomerID,
	}, nil
}

// Describe dispatches to the per-source describer.
func Describe(source string, raw []byte) (Descriptor, error) {
	switch source {
	case SourceStripe:
		return DescribeStripe(raw)
	case SourceOpenMeter:
		return DescribeOpenMeter(raw)
	case SourceCollectRequest:
		return DescribeCollectRequest(raw)
	default:
		return Descriptor{}, fmt.Errorf("%w: unknown source %q", ErrUnparseable, source)
	}
}

// DedupeKey is the idempotency key for an event. Sources are namespaced
// because provider event ids are only unique within their own system.
func (d Descriptor) DedupeKey() string {
	return d.Source + ":" + d.EventID
}

// IsOpenMeterInvoiceEvent reports whether the notification carries an invoice.
//
// OpenMeter emits `invoice.created` and `invoice.updated`; the leading
// `invoicing.` form appears in some documentation, so both are accepted.
func IsOpenMeterInvoiceEvent(eventType string) bool {
	t := strings.TrimPrefix(eventType, "invoicing.")
	return t == "invoice.created" || t == "invoice.updated"
}
