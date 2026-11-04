package main

import (
	"bytes"
	"errors"
	"os"
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

func TestMarkdownFocusKeepsBothSidesAndReportsExclusions(t *testing.T) {
	d := &delta{Counts: map[string]int{stateUnmatched: 3}, Entries: []deltaEntry{
		{State: stateUnmatched, Reason: reasonAbsentInHead, Base: &site{File: "old/a.go", Message: "old"}},
		{State: stateUnmatched, Reason: reasonAbsentInBase, Head: &site{File: "new/a.go", Message: "new"}},
		{State: stateUnmatched, Base: &site{File: "other/a.go", Message: "unrelated"}},
	}}
	var out bytes.Buffer
	if err := writeFocusedDeltaMarkdown(&out, d, 20, []string{"old/a.go", "new/a.go"}); err != nil {
		t.Fatal(err)
	}
	for _, term := range []string{"1 entries outside focus omitted", "moved_or_unmatched: 3", "old/a.go", "new/a.go", "State counts below describe the full comparison"} {
		if !strings.Contains(out.String(), term) {
			t.Fatalf("missing %s: %s", term, out.String())
		}
	}
	if strings.Contains(out.String(), "unrelated") {
		t.Fatal("unfocused entry retained")
	}
	for _, file := range []string{"../a.go", "/a.go", "a\\b.go", "a\nb.go"} {
		if err := writeFocusedDeltaMarkdown(&bytes.Buffer{}, d, 20, []string{file}); err == nil {
			t.Errorf("accepted %q", file)
		}
	}
	if err := writeFocusedDeltaMarkdown(failingSummaryWriter{}, d, 20, []string{"old/a.go"}); err == nil {
		t.Fatal("writer failure lost")
	}
}

func TestMarkdownFocusCommandValidation(t *testing.T) {
	dir := t.TempDir()
	rawPath := dir + "/raw.json"
	if err := os.WriteFile(rawPath, raw(t, fd{"dupl", "a.go", 3, 0, "duplicate"}), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"pair", "-base", rawPath, "-head", rawPath, "-base-ref", "b", "-head-ref", "h", "-format", "markdown", "-changed-files", dir + "/focus.json"}
	for _, content := range []string{`null`, `{}`, `[3]`, `["../a.go"]`} {
		if err := os.WriteFile(dir+"/focus.json", []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Errorf("accepted %s", content)
		}
	}
	if err := os.WriteFile(dir+"/focus.json", []byte(`["a.go"]`), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Findings focus") {
		t.Fatal(out.String())
	}
	args[len(args)-3] = "json"
	if err := run(args, &bytes.Buffer{}); err == nil {
		t.Fatal("focus silently accepted for JSON")
	}
}
