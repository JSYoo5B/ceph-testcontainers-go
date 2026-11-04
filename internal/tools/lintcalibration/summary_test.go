package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestMarkdownFocusLimitAndProvenance(t *testing.T) {
	src := snap(t, map[string]string{"a.go": hotSrc})
	base := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)}, fd{"dupl", "a.go", 3, 0, "<script>|[link](bad)"})
	head := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 50)}, fd{"dupl", "a.go", 3, 0, "<script>|[link](bad)"})
	d := mustPair(t, base, head, src, src)
	var b bytes.Buffer
	if err := writeDeltaMarkdown(&b, d, 1); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Showing 1 of 3", "2 candidates beyond row limit", "cognitive complexity 40", "cognitive complexity 50", d.Base.RawSHA256, d.Head.RawSHA256, "not a severity", "test=false"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q: %s", want, b.String())
		}
	}
	b.Reset()
	if err := writeDeltaMarkdown(&b, d, 20); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "<script>") || strings.Contains(b.String(), "[link]") {
		t.Fatal("unescaped source data", b.String())
	}
	if !strings.Contains(b.String(), "&#124;") {
		t.Fatal("unescaped table delimiter", b.String())
	}
}

func TestMarkdownOmitsUnchangedMessagesButKeepsCounts(t *testing.T) {
	src := snap(t, map[string]string{"a.go": hotSrc})
	r := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)})
	d := mustPair(t, r, r, src, src)
	var b bytes.Buffer
	if err := writeDeltaMarkdown(&b, d, 2); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"reported_both: 1", "Showing 0 of 0", "1 unchanged messages omitted"} {
		if !strings.Contains(b.String(), want) {
			t.Fatal(b.String())
		}
	}
	if strings.Contains(b.String(), "cognitive complexity 40") {
		t.Fatal("unchanged row included")
	}
}

type failingSummaryWriter struct{}

func (failingSummaryWriter) Write([]byte) (int, error) { return 0, errors.New("write failure") }
func TestMarkdownWriteFailure(t *testing.T) {
	if err := writeDeltaMarkdown(failingSummaryWriter{}, &delta{}, 1); err == nil {
		t.Fatal("write failure lost")
	}
}

func TestMarkdownInspectionOrderAndAbsence(t *testing.T) {
	d := &delta{Entries: []deltaEntry{
		{State: stateNotReported, Linter: "gocognit", Function: "Below", Base: &site{Message: "value 40"}, Caveats: []string{caveatBuildConstr}},
		{State: stateUnmatched, Linter: "dupl", Reason: reasonNoIdentity, Head: &site{Message: "duplicate"}},
		{State: stateBoth, Linter: "gocognit", Function: "Changed", Base: &site{Message: "value 40"}, Head: &site{Message: "value 35"}},
		{State: stateNew, Linter: "gocognit", Function: "New", Head: &site{Message: "value 45"}},
	}}
	var b bytes.Buffer
	if err := writeDeltaMarkdown(&b, d, 20); err != nil {
		t.Fatal(err)
	}
	s := b.String()
	last := -1
	for _, term := range []string{"///New", "///Changed", "duplicate", "///Below"} {
		pos := strings.Index(s, term)
		if pos <= last {
			t.Fatalf("wrong order for %s: %s", term, s)
		}
		last = pos
	}
	for _, term := range []string{"file&#95;has&#95;build&#95;constraint", "no paired diagnostic; see state/reason", "value 35"} {
		if !strings.Contains(s, term) {
			t.Fatalf("missing %s: %s", term, s)
		}
	}
}
