package lifecycle

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/money"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/stripe"
)

// issuingSync answers OpenMeter's second question: is this invoice issued?
//
// It sets the application fee against the now-final total, finalizes the
// Stripe invoice — which assigns its number and, for automatic collection,
// creates the PaymentIntent that will be charged — and reports the number and
// payment reference back. The payment external id is stamped here rather than
// at payment time, because the payment endpoint accepts only a trigger.
func (s *Settler) issuingSync(ctx context.Context, inv *openmeter.Invoice) error {
	tgt, err := s.resolveTarget(ctx, inv)
	if err != nil {
		return err
	}

	stripeInvoiceID := inv.ExternalIDs.Invoicing
	if stripeInvoiceID == "" {
		// Draft sync either never ran or its response was lost. Recover by
		// searching rather than creating: a second invoice here would be a
		// duplicate charge, not a missing one.
		query := fmt.Sprintf("metadata['%s']:'%s'", MetaInvoiceID, escapeSearch(inv.ID))
		found, searchErr := s.stripe.SearchInvoices(ctx, query, tgt.requestOptions(""))
		if searchErr != nil {
			return fmt.Errorf("search stripe invoices for %s: %w", inv.ID, searchErr)
		}
		for i := range found {
			if found[i].Status != "void" {
				stripeInvoiceID = found[i].ID
				break
			}
		}
	}
	if stripeInvoiceID == "" {
		return faults.Permanentf("missing_stripe_invoice",
			"invoice %s reached issuing with no Stripe invoice; re-drive the draft sync hook", inv.ID)
	}

	stripeInvoice, err := s.stripe.GetInvoice(ctx, stripeInvoiceID, tgt.requestOptions(""))
	if err != nil {
		return classify("stripe_invoice_unreadable", fmt.Errorf("read stripe invoice %s: %w", stripeInvoiceID, err))
	}

	if stripeInvoice.Status == "draft" {
		if err := s.applyApplicationFee(ctx, inv, tgt, stripeInvoice); err != nil {
			return classify("application_fee_rejected", err)
		}

		// Finalization is the irreversible step: it assigns the number and
		// starts collection. The idempotency key makes a redelivery a no-op
		// rather than a second attempt at an already-open invoice.
		finalized, err := s.stripe.FinalizeInvoice(ctx, stripeInvoiceID, s.cfg.AutoAdvance,
			tgt.requestOptions("settlement-finalize-"+inv.ID))
		if err != nil {
			return classify("stripe_finalize_rejected",
				fmt.Errorf("finalize stripe invoice %s: %w", stripeInvoiceID, err))
		}
		stripeInvoice = finalized
		s.log.Info("finalized stripe invoice",
			"invoice_id", inv.ID,
			"stripe_invoice", stripeInvoice.ID,
			"stripe_number", stripeInvoice.Number,
			"total_minor", stripeInvoice.Total,
			"application_fee_minor", stripeInvoice.ApplicationFee,
		)
	}

	if stripeInvoice.Status == "void" {
		return faults.Permanentf("stripe_invoice_void",
			"invoice %s: stripe invoice %s was voided; it cannot be issued", inv.ID, stripeInvoice.ID)
	}

	issuedAt := s.now()
	body := openmeter.FinalizedRequest{
		Invoicing: &openmeter.FinalizedInvoicingRequest{
			// Carry Stripe's number across so the two systems agree on what
			// the customer will quote back to support. When Stripe has not
			// assigned one, OpenMeter generates an INV- number instead.
			InvoiceNumber:   stripeInvoice.Number,
			SentToCustomerA: &issuedAt,
		},
	}
	ref, err := paymentReference(stripeInvoice)
	if err != nil {
		return faults.Permanentf("missing_payment_reference",
			"invoice %s: finalized stripe invoice %s has no payment reference: %v",
			inv.ID, stripeInvoice.ID, err)
	}
	body.Payment = &openmeter.FinalizedPaymentRequest{ExternalID: ref}

	if err := s.om.IssuingSynchronized(ctx, inv.ID, body); err != nil {
		if openmeter.IsConflict(err) {
			metrics.InvoiceStateTransitions.WithLabelValues(HandlerIssuingSync, "already_applied").Inc()
			s.log.Info("issuing already synchronized", "invoice_id", inv.ID)
			return nil
		}
		metrics.InvoiceStateTransitions.WithLabelValues(HandlerIssuingSync, "error").Inc()
		return fmt.Errorf("issuing synchronized %s: %w", inv.ID, err)
	}

	metrics.InvoiceStateTransitions.WithLabelValues(HandlerIssuingSync, "ok").Inc()
	s.log.Info("issuing synchronized",
		"invoice_id", inv.ID,
		"stripe_invoice", stripeInvoice.ID,
		"invoice_number", stripeInvoice.Number,
		"payment_reference", ref,
	)
	return nil
}

// applyApplicationFee sets the platform's cut before finalization.
//
// It has to happen while the invoice is still a draft — Stripe will not accept
// a fee change afterwards — and it is computed from the invoice's own total so
// a quantity snapshot between draft and issuing is reflected in the fee.
//
// Platform-mode invoices have no fee: there is no connected account to split
// with, the platform already keeps everything.
func (s *Settler) applyApplicationFee(ctx context.Context, inv *openmeter.Invoice, tgt target, stripeInvoice *stripe.Invoice) error {
	if tgt.Model == config.ChargeModelPlatform {
		return nil
	}
	if s.cfg.ApplicationFeeBps == 0 && s.cfg.ApplicationFeeFlatMinor == 0 {
		return nil
	}

	total := stripeInvoice.Total
	if total <= 0 {
		// Nothing to take a percentage of. Setting a flat fee on a zero
		// invoice would make Stripe reject the finalization outright.
		return nil
	}

	fee := money.FeeMinorUnits(total, s.cfg.ApplicationFeeBps, s.cfg.ApplicationFeeFlatMinor)
	if fee == stripeInvoice.ApplicationFee {
		return nil
	}

	params := url.Values{}
	params.Set("application_fee_amount", strconv.FormatInt(fee, 10))

	updated, err := s.stripe.UpdateInvoice(ctx, stripeInvoice.ID, params, tgt.requestOptions(""))
	if err != nil {
		return fmt.Errorf("set application fee on %s: %w", stripeInvoice.ID, err)
	}
	*stripeInvoice = *updated

	s.log.Info("applied application fee",
		"invoice_id", inv.ID,
		"stripe_invoice", stripeInvoice.ID,
		"charge_model", tgt.Model,
		"connect_account", tgt.Account,
		"total_minor", total,
		"fee_minor", fee,
	)
	return nil
}

// paymentReference is the Stripe object OpenMeter should record as the payment.
//
// Under the pinned Stripe API the PaymentIntent lives on the payments list (or
// confirmation_secret), not on removed payment_intent / charge fields.
func paymentReference(inv *stripe.Invoice) (string, error) {
	return inv.PrimaryPaymentIntent()
}
