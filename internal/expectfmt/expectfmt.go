// Package expectfmt renders the parts that every expect evaluation's String
// shares: the bounded mismatch list and the spelling of an empty value.
package expectfmt

import (
	"fmt"
	"strings"
)

// Shown bounds the mismatches that an evaluation's String lists.
const Shown = 8

// WriteMismatches writes "match" when matched is true. Otherwise it writes
// the first Shown mismatches joined by "; " and, when more remain, "; N more"
// with N the number left out.
func WriteMismatches[M fmt.Stringer](b *strings.Builder, matched bool, mismatches []M) {
	if matched {
		b.WriteString("match")
		return
	}
	for i, m := range mismatches {
		if i == Shown {
			fmt.Fprintf(b, "; %d more", len(mismatches)-i)
			return
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
}

// QuoteEmpty spells an empty value as "" so that a mismatch text never shows
// a bare gap.
func QuoteEmpty(s string) string {
	if s == "" {
		return `""`
	}
	return s
}
