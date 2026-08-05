package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/lifecycle"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/openmeter"
)

// Reconciler re-drives invoices that stopped moving.
//
// Every event-driven pipeline eventually drops something: a notification
// channel is misconfigured for an afternoon, a webhook endpoint 500s past its
// retry budget, a topic is recreated. Without a sweep those invoices sit at a
// pause point indefinitely and nobody finds out until a customer asks why they
// were never billed. The sweep asks OpenMeter directly which invoices are
// still waiting, and pushes them through the same handlers a live event would
// have used.
type Reconciler struct {
	cfg     config.Worker
	log     *slog.Logger
	om      *openmeter.Client
	settler *lifecycle.Settler
	now     func() time.Time
}

// NewReconciler builds the sweeper.
func NewReconciler(cfg config.Worker, log *slog.Logger, om *openmeter.Client, settler *lifecycle.Settler) *Reconciler {
	return &Reconciler{
		cfg:     cfg,
		log:     log,
		om:      om,
		settler: settler,
		now:     func() time.Time { return time.Now().UTC() },
	}
}

// Run sweeps on an interval until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	if r.cfg.ReconcileInterval <= 0 {
		r.log.Info("reconciliation sweep disabled")
		return
	}

	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()

	r.log.Info("reconciliation sweep enabled",
		"interval", r.cfg.ReconcileInterval.String(), "min_age", r.cfg.ReconcileMinAge.String())

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Sweep(ctx); err != nil {
				metrics.ReconcileSweeps.WithLabelValues("error").Inc()
				r.log.Error("reconciliation sweep failed", "error", err)
				continue
			}
			metrics.ReconcileSweeps.WithLabelValues("ok").Inc()
		}
	}
}

// Sweep runs one pass over the invoices parked at a sync hook.
func (r *Reconciler) Sweep(ctx context.Context) error {
	statuses := []string{
		openmeter.StatusDraft,
		openmeter.StatusIssuing,
		openmeter.StatusPaymentProcessing,
	}

	page := 1
	redriven := 0

	for {
		list, err := r.om.ListInvoices(ctx, openmeter.ListInvoicesInput{
			Statuses: statuses,
			PageSize: r.cfg.ReconcilePageSize,
			Page:     page,
		})
		if err != nil {
			return err
		}
		if len(list.Items) == 0 {
			break
		}

		for i := range list.Items {
			invoice := &list.Items[i]

			// Skip anything recent enough that its event may still be in
			// flight. Racing a live notification would mean two workers
			// driving the same invoice at once.
			if r.now().Sub(invoice.UpdatedAt) < r.cfg.ReconcileMinAge {
				continue
			}

			// List responses are not guaranteed to carry the lines, totals,
			// external ids and metadata draftSync/DriveInvoice need — re-fetch
			// the full invoice before driving it.
			full, err := r.om.GetInvoice(ctx, invoice.ID)
			if err != nil {
				r.log.Error("reconcile could not reload invoice",
					"invoice_id", invoice.ID, "error", err)
				continue
			}

			handler, err := r.settler.DriveInvoice(ctx, full)
			if err != nil {
				// Log and continue: one stuck invoice must not stop the sweep
				// from rescuing the rest. The live pipeline will retry it too.
				r.log.Error("reconcile could not advance invoice",
					"invoice_id", full.ID,
					"status", full.Status,
					"extended_status", full.StatusDetails.ExtendedStatus,
					"handler", handler,
					"error", err)
				continue
			}
			if handler == lifecycle.HandlerNoop {
				continue
			}

			redriven++
			metrics.ReconcileRedriven.WithLabelValues(handler).Inc()
			r.log.Info("reconcile re-drove stuck invoice",
				"invoice_id", full.ID,
				"handler", handler,
				"status", full.Status,
				"extended_status", full.StatusDetails.ExtendedStatus,
				"stalled_for", r.now().Sub(full.UpdatedAt).Round(time.Second).String())
		}

		if len(list.Items) < r.cfg.ReconcilePageSize {
			break
		}
		page++

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	r.log.Info("reconciliation sweep complete", "redriven", redriven)
	return nil
}
