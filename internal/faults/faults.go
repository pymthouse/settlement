// Package faults classifies processing failures.
//
// The distinction that matters in a billing pipeline is not what went wrong
// but whether trying again could ever work. A 502 from Stripe deserves eight
// increasingly patient attempts; an invoice line denominated in a currency the
// connected account cannot accept will fail identically forever, and retrying
// it only delays every other event queued behind it on that lane.
package faults

import (
	"errors"
	"fmt"
)

// Permanent wraps an error that will not succeed on retry.
type Permanent struct {
	// Reason is a short, stable slug used for DLQ headers and metrics.
	Reason string
	Err    error
}

func (p *Permanent) Error() string {
	return fmt.Sprintf("permanent (%s): %v", p.Reason, p.Err)
}

func (p *Permanent) Unwrap() error { return p.Err }

// Permanentf marks a failure as non-retryable.
func Permanentf(reason string, format string, args ...any) error {
	return &Permanent{Reason: reason, Err: fmt.Errorf(format, args...)}
}

// Wrap marks an existing error as non-retryable.
func Wrap(reason string, err error) error {
	return &Permanent{Reason: reason, Err: err}
}

// IsPermanent reports whether err should skip retries and go straight to the
// dead-letter topic.
func IsPermanent(err error) bool {
	var p *Permanent
	return errors.As(err, &p)
}

// Reason returns the slug of a permanent error, or "retry_exhausted" for a
// transient error that ran out of attempts.
func Reason(err error) string {
	var p *Permanent
	if errors.As(err, &p) {
		return p.Reason
	}
	return "retry_exhausted"
}
