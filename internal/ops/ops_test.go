package ops

import "testing"

func TestClampInspectCount(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero defaults to 10", 0, 10},
		{"negative defaults to 10", -5, 10},
		{"within bound passes through", 25, 25},
		{"at ceiling passes through", MaxInspectCount, MaxInspectCount},
		{"over ceiling clamps", MaxInspectCount + 1, MaxInspectCount},
		{"large user-supplied value clamps", 1 << 30, MaxInspectCount},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampInspectCount(tc.in); got != tc.want {
				t.Fatalf("clampInspectCount(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
