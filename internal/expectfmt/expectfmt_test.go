package expectfmt

import (
	"strconv"
	"strings"
	"testing"
)

type mismatch int

func (m mismatch) String() string { return "m" + strconv.Itoa(int(m)) }

func render(matched bool, n int) string {
	list := make([]mismatch, n)
	for i := range list {
		list[i] = mismatch(i + 1)
	}
	var b strings.Builder
	WriteMismatches(&b, matched, list)
	return b.String()
}

func TestWriteMismatches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		matched bool
		n       int
		want    string
	}{
		{"matched", true, 0, "match"},
		{"one", false, 1, "m1"},
		{"eight", false, 8, "m1; m2; m3; m4; m5; m6; m7; m8"},
		{"nine", false, 9, "m1; m2; m3; m4; m5; m6; m7; m8; 1 more"},
		{"eleven", false, 11, "m1; m2; m3; m4; m5; m6; m7; m8; 3 more"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(tc.matched, tc.n); got != tc.want {
				t.Fatalf("WriteMismatches = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQuoteEmpty(t *testing.T) {
	if got := QuoteEmpty(""); got != `""` {
		t.Fatalf("QuoteEmpty(\"\") = %q", got)
	}
	if got := QuoteEmpty("x"); got != "x" {
		t.Fatalf("QuoteEmpty(\"x\") = %q", got)
	}
}
