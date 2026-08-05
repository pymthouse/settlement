package money

import (
	"math/big"
	"testing"
)

func TestExponent(t *testing.T) {
	cases := map[string]int{
		"USD": 2, "usd": 2, "EUR": 2, "GBP": 2,
		"JPY": 0, "KRW": 0, "VND": 0,
		"KWD": 3, "BHD": 3,
		"":    2, // unknown currencies get the common default
		"XYZ": 2,
	}
	for currency, want := range cases {
		if got := Exponent(currency); got != want {
			t.Errorf("Exponent(%q) = %d, want %d", currency, got, want)
		}
	}
}

func TestToMinorUnits(t *testing.T) {
	cases := []struct {
		amount   string
		currency string
		want     int64
	}{
		{"0", "USD", 0},
		{"1", "USD", 100},
		{"12.34", "USD", 1234},
		{"0.005", "USD", 1},   // half rounds away from zero
		{"0.004", "USD", 0},   // below half rounds down
		{"-0.005", "USD", -1}, // symmetric for credits
		{"-12.34", "USD", -1234},
		{"1234", "JPY", 1234},   // zero-decimal currency
		{"12.5", "JPY", 13},     // still rounds
		{"1.234", "KWD", 1230},  // three-decimal, divisible by 10
		{"1.2345", "KWD", 1230}, // 1234.5 -> 1235 -> truncated to 1230
		{"0.1", "USD", 10},      // the classic float trap
		{"19.99", "USD", 1999},
		{"1e3", "USD", 100000},
	}

	for _, tc := range cases {
		got, err := ToMinorUnits(tc.amount, tc.currency)
		if err != nil {
			t.Fatalf("ToMinorUnits(%q, %q): %v", tc.amount, tc.currency, err)
		}
		if got != tc.want {
			t.Errorf("ToMinorUnits(%q, %q) = %d, want %d", tc.amount, tc.currency, got, tc.want)
		}
	}
}

// Summing many decimal amounts must not drift. float64 accumulates error here;
// big.Rat does not.
func TestToMinorUnitsDoesNotDriftOverManyLines(t *testing.T) {
	total := int64(0)
	for i := 0; i < 10_000; i++ {
		minor, err := ToMinorUnits("0.10", "USD")
		if err != nil {
			t.Fatal(err)
		}
		total += minor
	}
	if want := int64(100_000); total != want {
		t.Fatalf("10000 x $0.10 = %d minor units, want %d", total, want)
	}
}

func TestToMinorUnitsRejectsGarbage(t *testing.T) {
	for _, amount := range []string{"", "  ", "abc", "1.2.3", "$5", "1/3"} {
		if _, err := ToMinorUnits(amount, "USD"); err == nil {
			t.Errorf("ToMinorUnits(%q) accepted an invalid amount", amount)
		}
	}
}

func TestFromMinorUnits(t *testing.T) {
	cases := []struct {
		value    int64
		currency string
		want     string
	}{
		{1234, "USD", "12.34"},
		{0, "USD", "0.00"},
		{-1234, "USD", "-12.34"},
		{1234, "JPY", "1234"},
		{1230, "KWD", "1.230"},
	}
	for _, tc := range cases {
		if got := FromMinorUnits(tc.value, tc.currency); got != tc.want {
			t.Errorf("FromMinorUnits(%d, %q) = %q, want %q", tc.value, tc.currency, got, tc.want)
		}
	}
}

func TestRatToMinorUnitsRejectsOverflow(t *testing.T) {
	huge := new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 70))
	if _, err := RatToMinorUnits(huge, "USD"); err == nil {
		t.Fatal("expected an overflow error for an amount beyond int64 minor units")
	}
}

func TestFeeMinorUnits(t *testing.T) {
	cases := []struct {
		name  string
		total int64
		bps   int64
		flat  int64
		want  int64
	}{
		{"no fee configured", 10_000, 0, 0, 0},
		{"one percent", 10_000, 100, 0, 100},
		{"two and a half percent", 10_000, 250, 0, 250},
		{"percent plus flat", 10_000, 100, 30, 130},
		{"truncates rather than rounding up", 999, 100, 0, 9}, // 9.99 -> 9
		{"zero invoice", 0, 250, 30, 0},
		{"negative invoice", -500, 250, 30, 0},
		{"fee capped at the invoice total", 100, 10_000, 500, 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FeeMinorUnits(tc.total, tc.bps, tc.flat); got != tc.want {
				t.Errorf("FeeMinorUnits(%d, %d, %d) = %d, want %d", tc.total, tc.bps, tc.flat, got, tc.want)
			}
		})
	}
}

func TestRoundHalfAwayFromZero(t *testing.T) {
	cases := []struct {
		num, denom int64
		want       int64
	}{
		{1, 2, 1},   // 0.5 -> 1
		{-1, 2, -1}, // -0.5 -> -1
		{3, 2, 2},   // 1.5 -> 2
		{1, 3, 0},   // 0.333 -> 0
		{2, 3, 1},   // 0.667 -> 1
		{4, 2, 2},   // exact
	}
	for _, tc := range cases {
		got := roundHalfAwayFromZero(new(big.Rat).SetFrac(big.NewInt(tc.num), big.NewInt(tc.denom)))
		if got.Int64() != tc.want {
			t.Errorf("round(%d/%d) = %s, want %d", tc.num, tc.denom, got, tc.want)
		}
	}
}
