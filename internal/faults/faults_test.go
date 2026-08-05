package faults

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanentIsRecognisedThroughWrapping(t *testing.T) {
	err := Permanentf("missing_connect_account", "invoice %s has no account", "inv_1")

	if !IsPermanent(err) {
		t.Fatal("a permanent fault was not recognised")
	}
	// Handlers routinely add context on the way up; the classification must
	// survive it, or a permanent failure would be retried for hours.
	wrapped := fmt.Errorf("draft sync: %w", err)
	if !IsPermanent(wrapped) {
		t.Fatal("wrapping lost the permanent classification")
	}
	if Reason(wrapped) != "missing_connect_account" {
		t.Errorf("Reason = %q", Reason(wrapped))
	}
}

func TestTransientErrorsAreNotPermanent(t *testing.T) {
	err := errors.New("connection reset by peer")

	if IsPermanent(err) {
		t.Fatal("an ordinary error was classified as permanent")
	}
	if got := Reason(err); got != "retry_exhausted" {
		t.Errorf("Reason = %q, want retry_exhausted", got)
	}
}

func TestWrapPreservesTheCause(t *testing.T) {
	cause := errors.New("stripe: no such customer")
	err := Wrap("stripe_customer_rejected", cause)

	if !errors.Is(err, cause) {
		t.Fatal("the underlying cause was lost")
	}
	if Reason(err) != "stripe_customer_rejected" {
		t.Errorf("Reason = %q", Reason(err))
	}
	if got := err.Error(); got == "" || !contains(got, "no such customer") {
		t.Errorf("Error() = %q, want it to include the cause", got)
	}
}

func TestNilIsNotPermanent(t *testing.T) {
	if IsPermanent(nil) {
		t.Fatal("nil classified as permanent")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || len(needle) == 0 ||
		indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
