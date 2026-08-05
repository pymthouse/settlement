// Package lifecycle drives an OpenMeter invoice through its three pause
// points using Stripe.
//
// OpenMeter's Custom Invoicing app stops and waits for us at draft sync,
// issuing sync and payment. Each stop is a question — "does this draft make
// sense?", "is it issued?", "was it paid?" — that only the integration can
// answer, because OpenMeter never talks to Stripe itself. Everything in this
// package exists to answer one of those three questions and then call the
// matching completion endpoint so the invoice can move again.
package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
)

// Metadata keys written onto Stripe objects. They are the join between the two
// systems: without them a Stripe webhook cannot be traced back to the OpenMeter
// invoice it settles, and settlement would need its own database to remember.
const (
	MetaInvoiceID   = "openmeter_invoice_id"
	MetaCustomerID  = "openmeter_customer_id"
	MetaCustomerKey = "openmeter_customer_key"
	MetaLineID      = "openmeter_line_id"
	MetaSource      = "pymthouse_source"

	// sourceTag marks every object settlement creates, so an operator can tell
	// our invoices apart from ones written by another integration.
	sourceTag = "pymthouse-settlement"

	// roundingLineID labels the adjustment item that absorbs the difference
	// between per-line rounding and the invoice total.
	roundingLineID = "rounding-adjustment"
)

// target is where an invoice's money should land.
type target struct {
	// Model is the charge model in force for this customer.
	Model config.ChargeModel
	// Account is the Connect account id, empty in platform mode.
	Account string
}

// requestOptions turns a target into the Stripe request headers.
//
// Direct charges are made *on* the connected account, so the API call carries
// Stripe-Account and the objects live in the developer's own Stripe dashboard.
// Destination and platform charges are made on the platform account, and the
// routing is expressed in the invoice body instead.
func (t target) requestOptions(idempotencyKey string) stripe.RequestOptions {
	opts := stripe.RequestOptions{IdempotencyKey: idempotencyKey}
	if t.Model == config.ChargeModelDirect {
		opts.Account = t.Account
	}
	return opts
}

// resolveTarget decides the charge model and Connect account for an invoice.
//
// Invoice metadata wins over customer metadata, which wins over the configured
// default. The invoice is checked first because OpenMeter freezes it at
// creation: an invoice raised while a developer was on direct charges must keep
// settling that way even if they switch afterwards.
func (s *Settler) resolveTarget(ctx context.Context, inv *openmeter.Invoice) (target, error) {
	model := s.cfg.DefaultChargeModel
	account := ""

	metadata, err := s.customerMetadata(ctx, inv)
	if err != nil {
		return target{}, err
	}

	if v := metadata.Get(s.cfg.ConnectAccountMetadataKey); v != "" {
		account = v
	}
	if v := inv.Metadata.Get(s.cfg.ConnectAccountMetadataKey); v != "" {
		account = v
	}
	if v := metadata.Get(s.cfg.ChargeModelMetadataKey); v != "" {
		model = config.ChargeModel(strings.ToLower(v))
	}
	if v := inv.Metadata.Get(s.cfg.ChargeModelMetadataKey); v != "" {
		model = config.ChargeModel(strings.ToLower(v))
	}

	if !config.ValidChargeModel(model) {
		return target{}, faults.Permanentf("invalid_charge_model",
			"invoice %s: charge model %q is not one of direct, destination, platform", inv.ID, model)
	}
	if model != config.ChargeModelPlatform && account == "" {
		// Failing here is better than silently invoicing on the platform
		// account: the money would land in the wrong place and the developer
		// would never see the invoice.
		return target{}, faults.Permanentf("missing_connect_account",
			"invoice %s: charge model %s requires a Connect account in customer metadata %q",
			inv.ID, model, s.cfg.ConnectAccountMetadataKey)
	}
	if account != "" && !strings.HasPrefix(account, "acct_") {
		return target{}, faults.Permanentf("invalid_connect_account",
			"invoice %s: %q is not a Stripe Connect account id", inv.ID, account)
	}

	return target{Model: model, Account: account}, nil
}

// customerMetadata reads the OpenMeter customer's metadata, cached briefly.
//
// The same customer appears on every invoice in a billing run, and the routing
// answer does not change within a run; caching keeps a burst of a thousand
// invoices from becoming a thousand identical customer lookups.
func (s *Settler) customerMetadata(ctx context.Context, inv *openmeter.Invoice) (openmeter.Metadata, error) {
	if inv.Customer.ID == "" {
		return openmeter.Metadata{}, nil
	}

	if cached, ok := s.customerCache.get(inv.Customer.ID, s.now()); ok {
		return cached, nil
	}

	metadata, err := s.om.CustomerMetadata(ctx, inv.Customer.ID)
	if err != nil {
		if openmeter.IsNotFound(err) {
			// A deleted customer still has live invoices; fall back to whatever
			// the invoice itself froze rather than stalling the lifecycle.
			s.customerCache.put(inv.Customer.ID, openmeter.Metadata{}, s.now())
			return openmeter.Metadata{}, nil
		}
		return nil, fmt.Errorf("read customer %s metadata: %w", inv.Customer.ID, err)
	}

	s.customerCache.put(inv.Customer.ID, metadata, s.now())
	return metadata, nil
}

// metadataCache is a small TTL cache for customer metadata.
type metadataCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]metadataEntry
}

type metadataEntry struct {
	value   openmeter.Metadata
	expires time.Time
}

func newMetadataCache(ttl time.Duration) *metadataCache {
	return &metadataCache{ttl: ttl, entries: make(map[string]metadataEntry)}
}

func (c *metadataCache) get(key string, now time.Time) (openmeter.Metadata, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || !entry.expires.After(now) {
		return nil, false
	}
	return entry.value, true
}

func (c *metadataCache) put(key string, value openmeter.Metadata, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Bound the map so a long-lived worker in a large tenant cannot grow it
	// without limit; a cold miss costs one cheap GET.
	if len(c.entries) > 10_000 {
		for k, entry := range c.entries {
			if !entry.expires.After(now) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) > 10_000 {
			c.entries = make(map[string]metadataEntry)
		}
	}
	c.entries[key] = metadataEntry{value: value, expires: now.Add(c.ttl)}
}
