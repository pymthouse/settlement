package lifecycle

import (
	"context"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/money"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
)

// draftSync answers OpenMeter's first question: does this draft make sense,
// and what are its ids in the external system?
//
// It builds the mirror of the OpenMeter invoice in Stripe — customer, draft
// invoice, one item per line — and reports every id back. This is the only
// point in the lifecycle where line and discount mappings can be supplied, so
// anything the later stages need to reference has to exist by the time this
// returns.
func (s *Settler) draftSync(ctx context.Context, inv *openmeter.Invoice) error {
	if inv.Currency == "" {
		return faults.Permanentf("missing_currency", "invoice %s has no currency", inv.ID)
	}

	tgt, err := s.resolveTarget(ctx, inv)
	if err != nil {
		return err
	}

	customerID, err := s.ensureStripeCustomer(ctx, inv, tgt)
	if err != nil {
		return classify("stripe_customer_rejected", err)
	}

	stripeInvoice, err := s.ensureStripeInvoice(ctx, inv, tgt, customerID)
	if err != nil {
		return classify("stripe_invoice_rejected", err)
	}

	mapping, err := s.syncInvoiceItems(ctx, inv, tgt, customerID, stripeInvoice.ID)
	if err != nil {
		return classify("stripe_items_rejected", err)
	}

	body := openmeter.DraftSynchronizedRequest{
		Invoicing: &openmeter.SyncResult{
			ExternalID:              stripeInvoice.ID,
			LineExternalIDs:         mapping.lines,
			LineDiscountExternalIDs: mapping.discounts,
		},
	}

	if err := s.om.DraftSynchronized(ctx, inv.ID, body); err != nil {
		if openmeter.IsConflict(err) {
			// Already advanced past draft — a duplicate delivery. The Stripe
			// side is built and idempotent, so this is a success.
			metrics.InvoiceStateTransitions.WithLabelValues(HandlerDraftSync, "already_applied").Inc()
			s.log.Info("draft already synchronized", "invoice_id", inv.ID)
			return nil
		}
		metrics.InvoiceStateTransitions.WithLabelValues(HandlerDraftSync, "error").Inc()
		return fmt.Errorf("draft synchronized %s: %w", inv.ID, err)
	}

	metrics.InvoiceStateTransitions.WithLabelValues(HandlerDraftSync, "ok").Inc()
	s.log.Info("draft synchronized",
		"invoice_id", inv.ID,
		"stripe_invoice", stripeInvoice.ID,
		"stripe_customer", customerID,
		"charge_model", tgt.Model,
		"connect_account", tgt.Account,
		"lines", len(mapping.lines),
		"discounts", len(mapping.discounts),
	)
	return nil
}

// ensureStripeCustomer resolves, or creates, the Stripe customer to bill.
//
// Three lookups in decreasing order of certainty: an id OpenMeter already
// knows, a search by our own metadata, then a create keyed on the OpenMeter
// customer id. The idempotency key covers the window where Stripe's search
// index has not caught up with a customer we created seconds ago.
func (s *Settler) ensureStripeCustomer(ctx context.Context, inv *openmeter.Invoice, tgt target) (string, error) {
	metadata, err := s.customerMetadata(ctx, inv)
	if err != nil {
		return "", err
	}

	if known := metadata.Get(s.cfg.CustomerMetadataKey); known != "" {
		customer, err := s.stripe.GetCustomer(ctx, known, tgt.requestOptions(""))
		switch {
		case err == nil && !customer.Deleted:
			return customer.ID, nil
		case err != nil && !stripe.IsNotFound(err):
			return "", fmt.Errorf("read stripe customer %s: %w", known, err)
		}
		// Recorded but gone (deleted, or belongs to a different account after
		// a Connect migration). Fall through and resolve it again.
		s.log.Warn("recorded stripe customer is unusable, re-resolving",
			"invoice_id", inv.ID, "stripe_customer", known)
	}

	if inv.Customer.ID != "" {
		query := fmt.Sprintf("metadata['%s']:'%s'", MetaCustomerID, escapeSearch(inv.Customer.ID))
		found, err := s.stripe.SearchCustomers(ctx, query, tgt.requestOptions(""))
		if err != nil {
			return "", fmt.Errorf("search stripe customers: %w", err)
		}
		for _, c := range found {
			if !c.Deleted {
				return c.ID, nil
			}
		}
	}

	params := url.Values{}
	if inv.Customer.Name != "" {
		params.Set("name", inv.Customer.Name)
	}
	setStripeMetadata(params, MetaCustomerID, inv.Customer.ID)
	setStripeMetadata(params, MetaCustomerKey, inv.Customer.Key)
	setStripeMetadata(params, MetaSource, sourceTag)

	created, err := s.stripe.CreateCustomer(ctx, params,
		tgt.requestOptions("settlement-customer-"+inv.Customer.ID))
	if err != nil {
		return "", fmt.Errorf("create stripe customer for %s: %w", inv.Customer.ID, err)
	}

	s.log.Info("created stripe customer",
		"invoice_id", inv.ID, "openmeter_customer", inv.Customer.ID, "stripe_customer", created.ID)
	return created.ID, nil
}

// ensureStripeInvoice resolves, or creates, the Stripe draft invoice.
func (s *Settler) ensureStripeInvoice(ctx context.Context, inv *openmeter.Invoice, tgt target, customerID string) (*stripe.Invoice, error) {
	if existing := inv.ExternalIDs.Invoicing; existing != "" {
		found, err := s.stripe.GetInvoice(ctx, existing, tgt.requestOptions(""))
		switch {
		case err == nil && found.Status != "void":
			return found, nil
		case err != nil && !stripe.IsNotFound(err):
			return nil, fmt.Errorf("read stripe invoice %s: %w", existing, err)
		}
		s.log.Warn("recorded stripe invoice is unusable, re-resolving",
			"invoice_id", inv.ID, "stripe_invoice", existing)
	}

	query := fmt.Sprintf("metadata['%s']:'%s'", MetaInvoiceID, escapeSearch(inv.ID))
	found, err := s.stripe.SearchInvoices(ctx, query, tgt.requestOptions(""))
	if err != nil {
		return nil, fmt.Errorf("search stripe invoices: %w", err)
	}
	for i := range found {
		if found[i].Status != "void" {
			return &found[i], nil
		}
	}

	params := url.Values{}
	params.Set("customer", customerID)
	params.Set("currency", strings.ToLower(inv.Currency))
	params.Set("collection_method", s.cfg.CollectionMethod)
	// Stripe must not advance the invoice on its own: OpenMeter owns the
	// lifecycle until the issuing hook says otherwise.
	params.Set("auto_advance", "false")
	setStripeMetadata(params, MetaInvoiceID, inv.ID)
	setStripeMetadata(params, MetaCustomerID, inv.Customer.ID)
	setStripeMetadata(params, MetaSource, sourceTag)
	if inv.Description != "" {
		params.Set("description", inv.Description)
	}
	if s.cfg.CollectionMethod == "send_invoice" {
		params.Set("days_until_due", strconv.FormatInt(s.cfg.DaysUntilDue, 10))
	}
	if s.cfg.StatementDescriptorSuffix != "" {
		params.Set("statement_descriptor", s.cfg.StatementDescriptorSuffix)
	}

	// Destination charges keep the platform as merchant of record and transfer
	// the net to the developer; on_behalf_of puts the developer's details on
	// the invoice so the customer sees who they are buying from.
	if tgt.Model == config.ChargeModelDestination {
		params.Set("transfer_data[destination]", tgt.Account)
		params.Set("on_behalf_of", tgt.Account)
	}

	created, err := s.stripe.CreateInvoice(ctx, params, tgt.requestOptions("settlement-invoice-"+inv.ID))
	if err != nil {
		return nil, fmt.Errorf("create stripe invoice for %s: %w", inv.ID, err)
	}

	s.log.Info("created stripe draft invoice",
		"invoice_id", inv.ID, "stripe_invoice", created.ID, "charge_model", tgt.Model)
	return created, nil
}

// lineMapping is the set of external-id mappings reported back to OpenMeter.
type lineMapping struct {
	lines     []openmeter.LineExternalIDMapping
	discounts []openmeter.LineDiscountIDMapping
}

// syncInvoiceItems makes the Stripe invoice's items match the OpenMeter lines.
//
// Each top-level OpenMeter line becomes one Stripe invoice item priced at the
// line's settled total. A usage-based line's children are OpenMeter's internal
// breakdown of that same charge — billing them as well would double the
// invoice — so they are mapped to the item that contains them instead.
func (s *Settler) syncInvoiceItems(ctx context.Context, inv *openmeter.Invoice, tgt target, customerID, stripeInvoiceID string) (lineMapping, error) {
	existing, err := s.stripe.ListInvoiceItems(ctx, stripeInvoiceID, tgt.requestOptions(""))
	if err != nil {
		return lineMapping{}, fmt.Errorf("list stripe invoice items: %w", err)
	}

	byLine := indexInvoiceItemsByLine(existing)
	mapping, itemsTotal, matched, err := s.syncBillableLines(ctx, inv, tgt, customerID, stripeInvoiceID, byLine)
	if err != nil {
		return lineMapping{}, err
	}

	if err := s.deleteOrphanedInvoiceItems(ctx, inv, tgt, byLine, matched); err != nil {
		return lineMapping{}, err
	}
	if err := s.reconcileTotal(ctx, inv, tgt, customerID, stripeInvoiceID, byLine, itemsTotal); err != nil {
		return lineMapping{}, err
	}
	return mapping, nil
}

func indexInvoiceItemsByLine(existing []stripe.InvoiceItem) map[string]stripe.InvoiceItem {
	byLine := make(map[string]stripe.InvoiceItem, len(existing))
	for _, item := range existing {
		if lineID := item.Metadata[MetaLineID]; lineID != "" {
			byLine[lineID] = item
		}
	}
	return byLine
}

func (s *Settler) syncBillableLines(
	ctx context.Context,
	inv *openmeter.Invoice,
	tgt target,
	customerID, stripeInvoiceID string,
	byLine map[string]stripe.InvoiceItem,
) (lineMapping, int64, map[string]struct{}, error) {
	var mapping lineMapping
	var itemsTotal int64
	matched := make(map[string]struct{}, len(inv.BillableLines()))

	for _, line := range inv.BillableLines() {
		amount, err := money.ToMinorUnits(line.Totals.Total, inv.Currency)
		if err != nil {
			return lineMapping{}, 0, nil, faults.Permanentf("bad_line_amount",
				"invoice %s line %s: %v", inv.ID, line.ID, err)
		}

		matched[line.ID] = struct{}{}
		item, seen := byLine[line.ID]
		if !seen {
			item, err = s.createLineItem(ctx, inv, tgt, customerID, stripeInvoiceID, line, amount)
			if err != nil {
				return lineMapping{}, 0, nil, err
			}
		} else if item.Amount != amount {
			if err := s.stripe.DeleteInvoiceItem(ctx, item.ID, tgt.requestOptions("")); err != nil {
				return lineMapping{}, 0, nil, fmt.Errorf("replace stale invoice item %s: %w", item.ID, err)
			}
			item, err = s.createLineItem(ctx, inv, tgt, customerID, stripeInvoiceID, line, amount)
			if err != nil {
				return lineMapping{}, 0, nil, err
			}
		}

		itemsTotal += item.Amount
		mapping.lines = append(mapping.lines, openmeter.LineExternalIDMapping{
			LineID: line.ID, ExternalID: item.ID,
		})
		for _, childID := range line.ChildIDs() {
			mapping.lines = append(mapping.lines, openmeter.LineExternalIDMapping{
				LineID: childID, ExternalID: item.ID,
			})
		}
		for _, discountID := range line.LineDiscountIDs() {
			mapping.discounts = append(mapping.discounts, openmeter.LineDiscountIDMapping{
				LineDiscountID: discountID, ExternalID: item.ID,
			})
		}
	}
	return mapping, itemsTotal, matched, nil
}

func (s *Settler) deleteOrphanedInvoiceItems(
	ctx context.Context,
	inv *openmeter.Invoice,
	tgt target,
	byLine map[string]stripe.InvoiceItem,
	matched map[string]struct{},
) error {
	for lineID, item := range byLine {
		if lineID == roundingLineID {
			continue
		}
		if _, ok := matched[lineID]; ok {
			continue
		}
		if err := s.stripe.DeleteInvoiceItem(ctx, item.ID, tgt.requestOptions("")); err != nil {
			return fmt.Errorf("delete orphaned invoice item %s (line %s): %w", item.ID, lineID, err)
		}
		delete(byLine, lineID)
	}
	return nil
}

func (s *Settler) createLineItem(ctx context.Context, inv *openmeter.Invoice, tgt target, customerID, stripeInvoiceID string, line openmeter.Line, amount int64) (stripe.InvoiceItem, error) {
	params := url.Values{}
	params.Set("customer", customerID)
	params.Set("invoice", stripeInvoiceID)
	params.Set("currency", strings.ToLower(inv.Currency))
	params.Set("amount", strconv.FormatInt(amount, 10))
	params.Set("description", lineDescription(line))
	setStripeMetadata(params, MetaLineID, line.ID)
	setStripeMetadata(params, MetaInvoiceID, inv.ID)
	setStripeMetadata(params, MetaSource, sourceTag)
	if line.Period != nil && !line.Period.Start.IsZero() && !line.Period.End.IsZero() {
		params.Set("period[start]", strconv.FormatInt(line.Period.Start.Unix(), 10))
		params.Set("period[end]", strconv.FormatInt(line.Period.End.Unix(), 10))
	}

	// The idempotency key includes the amount and the invoice's UpdatedAt so a
	// delete-then-create replacement cannot replay a deleted Stripe item when
	// the amount returns to a previous value (Stripe replays keys for 24h even
	// after the created object is deleted).
	key := fmt.Sprintf("settlement-item-%s-%s-%d-%d", inv.ID, line.ID, amount, inv.UpdatedAt.UnixNano())
	item, err := s.stripe.CreateInvoiceItem(ctx, params, tgt.requestOptions(key))
	if err != nil {
		return stripe.InvoiceItem{}, fmt.Errorf("create invoice item for line %s: %w", line.ID, err)
	}
	return *item, nil
}

// reconcileTotal makes the Stripe invoice add up to the OpenMeter total.
//
// Rounding each line to minor units independently can leave the sum a unit or
// two away from the invoice total OpenMeter computed at full precision. That
// difference is real money and it will be noticed, so it is posted explicitly
// as an adjustment rather than being absorbed into an arbitrary line.
func (s *Settler) reconcileTotal(ctx context.Context, inv *openmeter.Invoice, tgt target, customerID, stripeInvoiceID string, existing map[string]stripe.InvoiceItem, itemsTotal int64) error {
	invoiceTotal, err := money.ToMinorUnits(inv.Totals.Total, inv.Currency)
	if err != nil {
		return faults.Permanentf("bad_invoice_total", "invoice %s: %v", inv.ID, err)
	}

	delta := invoiceTotal - itemsTotal
	current, hasAdjustment := existing[roundingLineID]

	if delta == 0 {
		if hasAdjustment {
			if err := s.stripe.DeleteInvoiceItem(ctx, current.ID, tgt.requestOptions("")); err != nil {
				return fmt.Errorf("remove obsolete rounding adjustment: %w", err)
			}
		}
		return nil
	}

	// A delta larger than one minor unit per line is not rounding — it is a
	// mapping bug, and quietly papering over it would ship a wrong invoice.
	if maxDrift := int64(len(inv.BillableLines())) + 1; delta > maxDrift || delta < -maxDrift {
		return faults.Permanentf("total_mismatch",
			"invoice %s: stripe items total %s but openmeter total is %s (delta %d minor units)",
			inv.ID, money.FromMinorUnits(itemsTotal, inv.Currency),
			money.FromMinorUnits(invoiceTotal, inv.Currency), delta)
	}

	if hasAdjustment {
		if current.Amount == delta {
			return nil
		}
		if err := s.stripe.DeleteInvoiceItem(ctx, current.ID, tgt.requestOptions("")); err != nil {
			return fmt.Errorf("replace rounding adjustment: %w", err)
		}
	}

	params := url.Values{}
	params.Set("customer", customerID)
	params.Set("invoice", stripeInvoiceID)
	params.Set("currency", strings.ToLower(inv.Currency))
	params.Set("amount", strconv.FormatInt(delta, 10))
	params.Set("description", "Rounding adjustment")
	setStripeMetadata(params, MetaLineID, roundingLineID)
	setStripeMetadata(params, MetaInvoiceID, inv.ID)
	setStripeMetadata(params, MetaSource, sourceTag)

	key := fmt.Sprintf("settlement-rounding-%s-%d-%d", inv.ID, delta, inv.UpdatedAt.UnixNano())
	if _, err := s.stripe.CreateInvoiceItem(ctx, params, tgt.requestOptions(key)); err != nil {
		return fmt.Errorf("create rounding adjustment: %w", err)
	}

	s.log.Info("posted rounding adjustment",
		"invoice_id", inv.ID, "delta_minor", delta, "currency", inv.Currency)
	return nil
}

// lineDescription builds the text the customer reads on the Stripe invoice.
func lineDescription(line openmeter.Line) string {
	name := strings.TrimSpace(line.Name)
	if name == "" {
		name = strings.TrimSpace(line.Description)
	}
	if name == "" {
		name = "Usage"
	}

	// Show the metered quantity when there is one: "API requests (1,204)"
	// reads far better on a statement than a bare line name.
	if qty := strings.TrimSpace(line.Quantity); qty != "" && qty != "0" {
		if parsed, err := money.Parse(qty); err == nil {
			name = fmt.Sprintf("%s (%s)", name, trimTrailingZeros(parsed))
		}
	}

	// Stripe caps invoice item descriptions at 500 characters (Unicode runes).
	if utf8.RuneCountInString(name) > 500 {
		runes := []rune(name)
		name = string(runes[:497]) + "..."
	}
	return name
}

func trimTrailingZeros(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	s := strings.TrimRight(r.FloatString(6), "0")
	return strings.TrimSuffix(s, ".")
}

// escapeSearch quotes a value for a Stripe search query string.
func escapeSearch(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`)
}
