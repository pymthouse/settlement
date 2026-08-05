// Package money converts OpenMeter's decimal amounts into the integer minor
// units Stripe charges in.
//
// Nothing here uses float64. OpenMeter emits amounts as decimal strings
// precisely so they survive transport exactly, and binary floating point
// cannot represent 0.10 — accumulating a few thousand such lines is how
// invoices end up a cent off. Every value is parsed into a big.Rat and rounded
// exactly once, at the boundary where Stripe demands an integer.
package money

import (
	"fmt"
	"math/big"
	"strings"
)

// zeroDecimal are the Stripe currencies whose amounts are whole units: there
// is no "cent" to divide by, so the minor-unit amount is the amount itself.
var zeroDecimal = map[string]bool{
	"BIF": true, "CLP": true, "DJF": true, "GNF": true, "JPY": true,
	"KMF": true, "KRW": true, "MGA": true, "PYG": true, "RWF": true,
	"UGX": true, "VND": true, "VUV": true, "XAF": true, "XOF": true,
	"XPF": true,
}

// threeDecimal currencies have 1000 minor units per major unit. Stripe requires
// amounts in these to be evenly divisible by 10 (it charges in hundredths and
// pads the last digit), which Round enforces.
var threeDecimal = map[string]bool{
	"BHD": true, "JOD": true, "KWD": true, "OMR": true, "TND": true,
}

// Exponent returns the number of decimal places a currency's minor unit has.
func Exponent(currency string) int {
	switch c := strings.ToUpper(strings.TrimSpace(currency)); {
	case zeroDecimal[c]:
		return 0
	case threeDecimal[c]:
		return 3
	default:
		return 2
	}
}

// Parse reads a decimal amount ("12.34", "-0.005", "1e3") exactly.
func Parse(amount string) (*big.Rat, error) {
	trimmed := strings.TrimSpace(amount)
	if trimmed == "" {
		return nil, fmt.Errorf("empty amount")
	}
	// big.Rat.SetString accepts rational forms like "1/3"; those are not
	// decimal amounts OpenMeter emits and must be rejected.
	if strings.Contains(trimmed, "/") {
		return nil, fmt.Errorf("not a decimal amount: %q", amount)
	}
	r, ok := new(big.Rat).SetString(trimmed)
	if !ok {
		return nil, fmt.Errorf("not a decimal amount: %q", amount)
	}
	return r, nil
}

// ToMinorUnits converts a decimal amount string to integer minor units for the
// currency, rounding half away from zero.
//
// Half-away-from-zero is the rounding a customer expects to see and the one
// that keeps a credit note the exact mirror of the invoice it reverses. It is
// applied once per amount; callers reconcile any residual against the invoice
// total rather than re-rounding intermediate sums.
func ToMinorUnits(amount, currency string) (int64, error) {
	r, err := Parse(amount)
	if err != nil {
		return 0, err
	}
	return RatToMinorUnits(r, currency)
}

// RatToMinorUnits converts an exact rational to minor units.
func RatToMinorUnits(r *big.Rat, currency string) (int64, error) {
	scale := pow10(Exponent(currency))
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(scale))

	rounded := roundHalfAwayFromZero(scaled)
	if !rounded.IsInt64() {
		return 0, fmt.Errorf("amount %s %s does not fit in int64 minor units", r.FloatString(6), currency)
	}
	value := rounded.Int64()

	// Stripe rejects three-decimal amounts whose last digit is not zero.
	if Exponent(currency) == 3 && value%10 != 0 {
		value = (value / 10) * 10
	}
	return value, nil
}

// FromMinorUnits renders minor units back as a decimal string, for logging and
// for the totals assertions in reconciliation.
func FromMinorUnits(value int64, currency string) string {
	exp := Exponent(currency)
	r := new(big.Rat).SetFrac(big.NewInt(value), pow10(exp))
	return r.FloatString(exp)
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// roundHalfAwayFromZero rounds a rational to the nearest integer, breaking ties
// away from zero.
func roundHalfAwayFromZero(r *big.Rat) *big.Int {
	num, denom := r.Num(), r.Denom()

	quotient, remainder := new(big.Int).QuoRem(num, denom, new(big.Int))
	if remainder.Sign() == 0 {
		return quotient
	}

	// Compare |remainder| * 2 against the denominator to find the halfway mark.
	twiceRemainder := new(big.Int).Abs(remainder)
	twiceRemainder.Lsh(twiceRemainder, 1)

	if twiceRemainder.Cmp(denom) >= 0 {
		if r.Sign() < 0 {
			return quotient.Sub(quotient, big.NewInt(1))
		}
		return quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

// FeeMinorUnits computes an application fee from a total in minor units:
// bps basis points of the total, plus a flat amount, truncated downward.
//
// Truncation is deliberate — rounding a fee up would take a cent the platform
// did not earn, and the connected account is the one who notices.
func FeeMinorUnits(totalMinor, bps, flatMinor int64) int64 {
	if totalMinor <= 0 {
		return 0
	}
	fee := (totalMinor*bps)/10_000 + flatMinor
	if fee < 0 {
		return 0
	}
	if fee > totalMinor {
		// Never take more than the invoice is worth; Stripe would reject the
		// charge and the invoice would stall in payment_processing.
		return totalMinor
	}
	return fee
}
