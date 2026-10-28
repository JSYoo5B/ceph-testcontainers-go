package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Fixtures here are fake raw collections made through the real collector with a
// fake golangci-lint, next to tiny synthetic Go modules for the snapshots. No
// test needs Docker, the network or a real golangci-lint.

// fd is one fake diagnostic.
type fd struct {
	linter, file string
	line, col    int
	msg          string
}

func cyc(name string, v int) string {
	return fmt.Sprintf("calculated cyclomatic complexity for function %s is %d, max is 10", name, v)
}

func cog(name string, v int) string {
	return fmt.Sprintf("cognitive complexity %d of func `%s` is high (> 30)", v, name)
}

func flen(name string, v int) string {
	return fmt.Sprintf("Function '%s' is too long (%d > 60)", name, v)
}

// rawFor makes a raw collection the way `report` does, via a fake binary.
func rawFor(t *testing.T, version string, patterns []string, diags ...fd) []byte {
	t.Helper()
	var items []string
	for _, d := range diags {
		items = append(items, fmt.Sprintf(`{"FromLinter":%q,"Text":%q,"Pos":{"Filename":%q,"Line":%d,"Column":%d}}`, d.linter, d.msg, d.file, d.line, d.col))
	}
	js := `{"Issues":[` + strings.Join(items, ",") + `],` + linterList(allMetrics...) + `}`
	rep, err := collect(t.TempDir(), fakeBinary(t, version, js, 0), patterns)
	if err != nil {
		t.Fatal(err)
	}
	return marshalReport(t, rep)
}

func marshalReport(t *testing.T, rep *report) []byte {
	t.Helper()
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func raw(t *testing.T, diags ...fd) []byte {
	return rawFor(t, "2.14.0", []string{"./..."}, diags...)
}

func snap(t *testing.T, files map[string]string) string { return writeModule(t, files) }

func pair(t *testing.T, base, head []byte, baseDir, headDir string) (*delta, error) {
	t.Helper()
	return makeDelta(pairInput{BaseRaw: base, HeadRaw: head, BaseRef: "base-id", HeadRef: "head-id", BaseDir: baseDir, HeadDir: headDir})
}

func mustPair(t *testing.T, base, head []byte, baseDir, headDir string) *delta {
	t.Helper()
	d, err := pair(t, base, head, baseDir, headDir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func only(t *testing.T, d *delta) deltaEntry {
	t.Helper()
	if len(d.Entries) != 1 {
		t.Fatalf("want one entry, got %+v", d.Entries)
	}
	return d.Entries[0]
}

const hotSrc = "package m\n\nfunc Hot(x int) int { return x }\n"

// Provenance: every field that makes two collections incomparable is refused,
// and the error names it.
func TestPairRefusesMismatchedProvenance(t *testing.T) {
	base := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)})
	cases := map[string]func(*report){
		"golangci_lint_version": func(r *report) { r.Tool.Version = "2.15.0"; r.Tool.MatchesCIPin = false },
		"ci_pinned_version":     func(r *report) { r.Tool.CIPinned = "2.15.0"; r.Tool.MatchesCIPin = r.Tool.Version == "2.15.0" },
		"config.sha256":         func(r *report) { r.Config.SHA256 = strings.Repeat("0", 64) },
		"patterns":              func(r *report) { r.Patterns = []string{"./ceph/..."} },
		"scope":                 func(r *report) { r.Scope = append(slices.Clone(r.Scope), "different scope line") },
	}
	for field, mutate := range cases {
		t.Run(field, func(t *testing.T) {
			var rep report
			if err := json.Unmarshal(base, &rep); err != nil {
				t.Fatal(err)
			}
			mutate(&rep)
			_, err := pair(t, base, marshalReport(t, &rep), "", "")
			if err == nil || !strings.Contains(err.Error(), "mismatching field(s): "+field+":") {
				t.Fatalf("want a refusal naming %s, got %v", field, err)
			}
		})
	}
	if _, err := pair(t, base, base, "", ""); err != nil {
		t.Errorf("identical provenance must pair: %v", err)
	}
}

func TestPairRefusesUnusableRawCollections(t *testing.T) {
	good := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)})
	edit := func(f func(map[string]any)) []byte {
		var m map[string]any
		if err := json.Unmarshal(good, &m); err != nil {
			t.Fatal(err)
		}
		f(m)
		b, _ := json.Marshal(m)
		return b
	}
	locs := func(m map[string]any) map[string]any { return m["locations"].([]any)[0].(map[string]any) }
	cases := map[string][]byte{
		"missing locations":      edit(func(m map[string]any) { delete(m, "locations") }),
		"missing patterns":       edit(func(m map[string]any) { delete(m, "patterns") }),
		"null patterns":          edit(func(m map[string]any) { m["patterns"] = nil }),
		"wrong schema":           edit(func(m map[string]any) { m["schema"] = "other/v1" }),
		"missing tool":           edit(func(m map[string]any) { delete(m, "tool") }),
		"bad digest":             edit(func(m map[string]any) { m["config"] = map[string]any{"sha256": "abc"} }),
		"enabled set differs":    edit(func(m map[string]any) { m["enabled_linters"] = []any{"cyclop"} }),
		"total disagrees":        edit(func(m map[string]any) { m["diagnostics_total"] = 7 }),
		"count disagrees":        edit(func(m map[string]any) { m["diagnostics_per_linter"].(map[string]any)["cyclop"] = 1 }),
		"null count":             edit(func(m map[string]any) { m["diagnostics_per_linter"].(map[string]any)["cyclop"] = nil }),
		"unknown field":          edit(func(m map[string]any) { m["extra"] = 1 }),
		"missing message":        edit(func(m map[string]any) { delete(locs(m)["diagnostics"].([]any)[0].(map[string]any), "message") }),
		"missing column":         edit(func(m map[string]any) { delete(locs(m)["diagnostics"].([]any)[0].(map[string]any), "column") }),
		"wrong overlap":          edit(func(m map[string]any) { locs(m)["overlap"] = true }),
		"wrong test flag":        edit(func(m map[string]any) { locs(m)["test_file"] = true }),
		"empty diagnostics":      edit(func(m map[string]any) { locs(m)["diagnostics"] = []any{} }),
		"line as string":         edit(func(m map[string]any) { locs(m)["line"] = "3" }),
		"not json":               []byte("{"),
		"trailing data":          append(slices.Clone(good), []byte(" {}")...),
		"matches_ci_pin lies":    edit(func(m map[string]any) { m["tool"].(map[string]any)["matches_ci_pin"] = false }),
		"unexpected linter name": edit(func(m map[string]any) { locs(m)["diagnostics"].([]any)[0].(map[string]any)["linter"] = "govet" }),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := pair(t, b, good, "", ""); err == nil || !strings.HasPrefix(err.Error(), "base:") {
				t.Errorf("as base: want a refusal, got %v", err)
			}
			if _, err := pair(t, good, b, "", ""); err == nil || !strings.HasPrefix(err.Error(), "head:") {
				t.Errorf("as head: want a refusal, got %v", err)
			}
		})
	}
	// Controls: an empty collection is accepted whether Locations is null or [].
	for _, empty := range []string{"null", "[]"} {
		b := edit(func(m map[string]any) {
			m["locations"] = nil
			m["diagnostics_total"] = 0
			m["overlapping_locations"] = 0
			m["diagnostics_per_linter"] = map[string]any{"cyclop": 0, "dupl": 0, "funlen": 0, "gocognit": 0}
		})
		if empty == "[]" {
			b = bytes.Replace(b, []byte(`"locations":null`), []byte(`"locations":[]`), 1)
		}
		if _, err := pair(t, b, b, "", ""); err != nil {
			t.Errorf("empty collection with locations %s: %v", empty, err)
		}
	}
}

func TestPairRefusesUnidentifiedSnapshots(t *testing.T) {
	r := raw(t)
	for _, refs := range [][2]string{{"", "h"}, {"b", ""}, {"b c", "h"}, {"b", "h\t"}} {
		_, err := makeDelta(pairInput{BaseRaw: r, HeadRaw: r, BaseRef: refs[0], HeadRef: refs[1]})
		if err == nil {
			t.Errorf("refs %q must be refused", refs)
		}
	}
	if _, err := pair(t, r, r, filepath.Join(t.TempDir(), "absent"), ""); err == nil {
		t.Error("a missing snapshot directory must be refused")
	}
}

// Same function and metric on both sides: reported_both with both messages,
// including when the function moved to another line or another file of the
// same package.
func TestPairReportedBothKeepsBothValues(t *testing.T) {
	baseDir := snap(t, map[string]string{"a.go": hotSrc})
	headDir := snap(t, map[string]string{"b.go": "package m\n\n// pad\n\nfunc Hot(x int) int { return x }\n"})
	d := mustPair(t,
		raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)}),
		raw(t, fd{"gocognit", "b.go", 5, 1, cog("Hot", 55)}), baseDir, headDir)
	e := only(t, d)
	if e.State != stateBoth || e.Base == nil || e.Head == nil || !strings.Contains(e.Base.Message, "complexity 40") || !strings.Contains(e.Head.Message, "complexity 55") {
		t.Fatalf("%+v", e)
	}
	if e.Base.File != "a.go" || e.Head.File != "b.go" || e.Function != "Hot" || e.PkgName != "m" || e.PkgDir != "." {
		t.Errorf("identity or locations: %+v", e)
	}
	if d.Counts[stateBoth] != 1 || d.Counts[stateNew] != 0 || d.Counts[stateNotReported] != 0 || d.Counts[stateUnmatched] != 0 {
		t.Errorf("counts %v", d.Counts)
	}
}

// A different metric of one linter is a different row: funlen reports lines and
// statements apart, so losing one while the other stays is not "both".
func TestPairSeparatesMetricsOfOneLinter(t *testing.T) {
	dir := snap(t, map[string]string{"a.go": hotSrc})
	d := mustPair(t,
		raw(t, fd{"funlen", "a.go", 3, 1, flen("Hot", 80)}, fd{"funlen", "a.go", 3, 1, "Function 'Hot' has too many statements (50 > 40)"}),
		raw(t, fd{"funlen", "a.go", 3, 1, flen("Hot", 70)}), dir, dir)
	got := map[string]string{}
	for _, e := range d.Entries {
		got[e.Metric] = e.State
	}
	if len(d.Entries) != 2 || got["Function 'Hot' is too long (# > #)"] != stateBoth || got["Function 'Hot' has too many statements (# > #)"] != stateNotReported {
		t.Fatalf("%+v", d.Entries)
	}
}

// Absent-versus-zero control: a diagnostic that disappears while the function
// is still declared is "not reported above threshold". It carries no value, no
// zero and no improvement wording. Without the head source the same input
// stays unmatched, so the state is never reached by guessing.
func TestPairAbsentIsNotZeroAndNotImprovement(t *testing.T) {
	src := "package m\n\nfunc Hot(x int) int { return x }\n"
	baseDir := snap(t, map[string]string{"a.go": src})
	headDir := snap(t, map[string]string{"a.go": src})
	base := raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)})
	head := raw(t)
	e := only(t, mustPair(t, base, head, baseDir, headDir))
	if e.State != stateNotReported || e.Head != nil || e.Base == nil || e.HeadDecl == nil || e.HeadDecl.File != "a.go" || e.HeadDecl.Line != 3 {
		t.Fatalf("%+v", e)
	}
	enc, _ := json.Marshal(e)
	for _, banned := range []string{"improv", "zero", `"head_value"`, `"delta"`} {
		if strings.Contains(string(enc), banned) {
			t.Errorf("entry mentions %q: %s", banned, enc)
		}
	}
	for _, c := range []struct {
		name         string
		baseD, headD string
	}{{"no head source", baseDir, ""}, {"no base source", "", headDir}, {"no source", "", ""}} {
		e := only(t, mustPair(t, base, head, c.baseD, c.headD))
		if e.State != stateUnmatched || e.Reason != reasonNoSnapshot {
			t.Errorf("%s: want unmatched/%s, got %+v", c.name, reasonNoSnapshot, e)
		}
	}
}

func TestPairNewAboveThreshold(t *testing.T) {
	baseDir := snap(t, map[string]string{"a.go": hotSrc})
	headDir := snap(t, map[string]string{"a.go": hotSrc})
	e := only(t, mustPair(t, raw(t), raw(t, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}), baseDir, headDir))
	if e.State != stateNew || e.BaseDecl == nil || e.BaseDecl.Line != 3 || e.Head == nil || e.Base != nil {
		t.Fatalf("%+v", e)
	}
	// Control: a declaration that did not exist at base is not asserted to be an
	// existing function that crossed the threshold.
	emptyBase := snap(t, map[string]string{"a.go": "package m\n"})
	e = only(t, mustPair(t, raw(t), raw(t, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}), emptyBase, headDir))
	if e.State != stateUnmatched || e.Reason != reasonAbsentInBase {
		t.Fatalf("%+v", e)
	}
}

// Moved-function control: the function leaves one package for another, and a
// rename leaves the name. Neither is an improvement or an introduction.
func TestPairMovedOrRenamedFunctionIsUnmatchedOnBothSides(t *testing.T) {
	baseDir := snap(t, map[string]string{"a/a.go": "package a\n\nfunc Hot(x int) int { return x }\n"})
	cases := map[string]struct {
		files map[string]string
		pos   fd
	}{
		"other package":  {map[string]string{"b/b.go": "package b\n\nfunc Hot(x int) int { return x }\n"}, fd{"gocognit", "b/b.go", 3, 1, cog("Hot", 40)}},
		"other name":     {map[string]string{"a/a.go": "package a\n\nfunc Warm(x int) int { return x }\n"}, fd{"gocognit", "a/a.go", 3, 1, cog("Warm", 40)}},
		"other receiver": {map[string]string{"a/a.go": "package a\n\ntype T struct{}\n\nfunc (T) Hot(x int) int { return x }\n"}, fd{"gocognit", "a/a.go", 5, 1, cog("Hot", 40)}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			headDir := snap(t, c.files)
			d := mustPair(t, raw(t, fd{"gocognit", "a/a.go", 3, 1, cog("Hot", 40)}), raw(t, c.pos), baseDir, headDir)
			if len(d.Entries) != 2 || d.Counts[stateUnmatched] != 2 || d.Counts[stateNotReported] != 0 || d.Counts[stateNew] != 0 || d.Counts[stateBoth] != 0 {
				t.Fatalf("%+v counts %v", d.Entries, d.Counts)
			}
			reasons := []string{d.Entries[0].Reason, d.Entries[1].Reason}
			slices.Sort(reasons)
			if !slices.Equal(reasons, []string{reasonAbsentInBase, reasonAbsentInHead}) {
				t.Errorf("reasons %v", reasons)
			}
		})
	}
	// Control: when only the base diagnostic exists and the function moved away,
	// the disappearance is unmatched, not "not reported".
	headDir := snap(t, map[string]string{"b/b.go": "package b\n\nfunc Hot(x int) int { return x }\n"})
	e := only(t, mustPair(t, raw(t, fd{"gocognit", "a/a.go", 3, 1, cog("Hot", 40)}), raw(t), baseDir, headDir))
	if e.State != stateUnmatched || e.Reason != reasonAbsentInHead {
		t.Fatalf("%+v", e)
	}
}

// Receiver identity: pointer, value and generic receivers of one type are the
// same function; another type's method of the same name is not.
func TestPairReceiverIdentity(t *testing.T) {
	baseDir := snap(t, map[string]string{"a.go": "package m\n\ntype S[T any] struct{}\ntype U struct{}\n\nfunc (s *S[T]) Do() {}\n\nfunc (U) Do() {}\n"})
	headDir := snap(t, map[string]string{"a.go": "package m\n\ntype S[T any] struct{}\ntype U struct{}\n\nfunc (s S[T]) Do() {}\n\nfunc (U) Do() {}\n"})
	d := mustPair(t,
		raw(t, fd{"cyclop", "a.go", 6, 1, cyc("Do", 12)}, fd{"cyclop", "a.go", 8, 1, cyc("Do", 12)}),
		raw(t, fd{"cyclop", "a.go", 6, 1, cyc("Do", 14)}), baseDir, headDir)
	if len(d.Entries) != 2 {
		t.Fatalf("%+v", d.Entries)
	}
	byRecv := map[string]string{}
	for _, e := range d.Entries {
		byRecv[e.Receiver] = e.State
	}
	if byRecv["S"] != stateBoth || byRecv["U"] != stateNotReported {
		t.Errorf("%v", byRecv)
	}
}

// When the snapshot cannot give one unambiguous identity the diagnostic stays
// unmatched instead of being guessed.
func TestPairStaysUnmatchedWhenIdentityIsNotReliable(t *testing.T) {
	cases := map[string]struct {
		files  map[string]string
		diag   fd
		reason string
	}{
		"no declaration at the reported line":  {map[string]string{"a.go": hotSrc}, fd{"cyclop", "a.go", 4, 1, cyc("Hot", 12)}, reasonNoDecl},
		"snapshot does not match the message":  {map[string]string{"a.go": hotSrc}, fd{"cyclop", "a.go", 3, 1, cyc("Cold", 12)}, reasonMessage},
		"name is only a prefix of the message": {map[string]string{"a.go": hotSrc}, fd{"cyclop", "a.go", 3, 1, cyc("Hotter", 12)}, reasonMessage},
		"file is absent":                       {map[string]string{"b.go": hotSrc}, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}, reasonNoDecl},
		"file does not parse":                  {map[string]string{"a.go": "package m\n\nfunc Hot( {\n"}, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}, reasonUnparsable},
		"dupl names no function":               {map[string]string{"a.go": hotSrc}, fd{"dupl", "a.go", 3, 1, "3-9 lines are duplicate of `a.go:20-26`"}, reasonNoIdentity},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			dir := snap(t, c.files)
			d := mustPair(t, raw(t, c.diag), raw(t, c.diag), dir, dir)
			if len(d.Entries) != 2 {
				t.Fatalf("%+v", d.Entries)
			}
			for _, e := range d.Entries {
				if e.State != stateUnmatched || e.Reason != c.reason || e.Function != "" {
					t.Errorf("%+v", e)
				}
			}
		})
	}
	// Two declarations of one identity (build-tagged variants) are ambiguous.
	dir := snap(t, map[string]string{"a.go": hotSrc, "b.go": hotSrc})
	d := mustPair(t, raw(t, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}, fd{"cyclop", "b.go", 3, 1, cyc("Hot", 12)}), raw(t), dir, dir)
	if len(d.Entries) != 2 || d.Counts[stateUnmatched] != 2 || d.Entries[0].Reason != reasonAmbiguous {
		t.Errorf("%+v", d.Entries)
	}
	// And one that is reported once but declared twice at head cannot be called "not reported".
	e := only(t, mustPair(t, raw(t, fd{"cyclop", "a.go", 3, 1, cyc("Hot", 12)}), raw(t), snap(t, map[string]string{"a.go": hotSrc}), dir))
	if e.State != stateUnmatched || e.Reason != reasonManyInHead {
		t.Errorf("%+v", e)
	}
}

// Declarations with //nolint or a build constraint carry a caveat on the state
// that relies on their absence from the head collection.
func TestPairCarriesCaveatsOnAbsence(t *testing.T) {
	baseDir := snap(t, map[string]string{"a.go": hotSrc})
	headDir := snap(t, map[string]string{"a.go": "//go:build linux\n\npackage m\n\nfunc Hot(x int) int { //nolint:gocognit\n\treturn x\n}\n"})
	e := only(t, mustPair(t, raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)}), raw(t), baseDir, headDir))
	if e.State != stateNotReported || !slices.Equal(e.Caveats, []string{caveatBuildConstr, caveatNolint}) {
		t.Errorf("%+v", e)
	}
	plain := only(t, mustPair(t, raw(t, fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)}), raw(t), baseDir, baseDir))
	if len(plain.Caveats) != 0 {
		t.Errorf("control: %+v", plain)
	}
}

func scenario(t *testing.T) (base, head []fd, baseDir, headDir string) {
	t.Helper()
	baseDir = snap(t, map[string]string{
		"a.go":   "package m\n\nfunc Hot(x int) int { return x }\n\nfunc Gone(x int) int { return x }\n\nfunc Same(x int) int { return x }\n",
		"x/x.go": "package x\n\nfunc Mv(x int) int { return x }\n",
	})
	headDir = snap(t, map[string]string{
		"a.go":   "package m\n\nfunc Hot(x int) int { return x }\n\nfunc Gone(x int) int { return x }\n\nfunc Same(x int) int { return x }\n",
		"y/y.go": "package y\n\nfunc Mv(x int) int { return x }\n",
	})
	base = []fd{
		{"cyclop", "a.go", 3, 1, cyc("Hot", 12)},
		{"gocognit", "a.go", 3, 1, cog("Hot", 40)},
		{"funlen", "a.go", 5, 1, flen("Gone", 80)},
		{"gocognit", "a.go", 7, 1, cog("Same", 33)},
		{"gocognit", "x/x.go", 3, 1, cog("Mv", 33)},
		{"dupl", "a.go", 3, 1, "3-9 lines are duplicate of `a.go:20-26`"},
	}
	head = []fd{
		{"gocognit", "a.go", 3, 1, cog("Hot", 45)},
		{"cyclop", "a.go", 5, 1, cyc("Gone", 11)},
		{"gocognit", "a.go", 7, 1, cog("Same", 34)},
		{"gocognit", "y/y.go", 3, 1, cog("Mv", 33)},
		{"dupl", "a.go", 3, 1, "3-9 lines are duplicate of `a.go:30-36`"},
	}
	return
}

func reverse[T any](s []T) []T {
	out := slices.Clone(s)
	slices.Reverse(out)
	return out
}

func render(t *testing.T, d *delta, format string) string {
	t.Helper()
	var b bytes.Buffer
	var err error
	if format == "tsv" {
		err = writeDeltaTSV(&b, d)
	} else {
		enc := json.NewEncoder(&b)
		enc.SetIndent("", "  ")
		err = enc.Encode(d)
	}
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// The delta does not depend on the order of the diagnostics in either input.
func TestPairOutputIsDeterministicInBothInputOrders(t *testing.T) {
	base, head, bd, hd := scenario(t)
	var outs []string
	for _, flip := range []bool{false, true} {
		b, h := base, head
		if flip {
			b, h = reverse(base), reverse(head)
		}
		d := mustPair(t, raw(t, b...), raw(t, h...), bd, hd)
		outs = append(outs, render(t, d, "json"), render(t, d, "tsv"))
	}
	if outs[0] != outs[2] || outs[1] != outs[3] {
		t.Errorf("output depends on input order:\n%s\n---\n%s", outs[1], outs[3])
	}
	d := mustPair(t, raw(t, base...), raw(t, head...), bd, hd)
	// Hot cyclop and Gone funlen become not_reported; Hot gocognit and Same are
	// both; Gone cyclop is new; the moved Mv and the two dupl are unmatched.
	want := map[string]int{stateBoth: 2, stateNew: 1, stateNotReported: 2, stateUnmatched: 4}
	for s, n := range want {
		if d.Counts[s] != n {
			t.Errorf("%s: want %d, got %d (%v)", s, n, d.Counts[s], d.Counts)
		}
	}
	// Swapping base and head mirrors the one-sided states.
	sw := mustPair(t, raw(t, head...), raw(t, base...), hd, bd)
	if sw.Counts[stateNew] != 2 || sw.Counts[stateNotReported] != 1 || sw.Counts[stateBoth] != 2 {
		t.Errorf("swapped counts %v", sw.Counts)
	}
}

// Both output formats carry the evidence references and provenance, and they
// name neither a local path nor the snapshot directories.
func TestPairOutputCarriesProvenanceAndEvidence(t *testing.T) {
	base, head, bd, hd := scenario(t)
	rb, rh := rawFor(t, "2.15.0", []string{"./a/...", "./b/..."}, base...), rawFor(t, "2.15.0", []string{"./a/...", "./b/..."}, head...)
	d := mustPair(t, rb, rh, bd, hd)
	sum := sha256.Sum256(rb)
	if d.Base.RawSHA256 != hex.EncodeToString(sum[:]) || d.Base.Ref != "base-id" || d.Head.Ref != "head-id" || !d.Base.SnapshotIndexed || d.Tool.Version != "2.15.0" || d.Tool.MatchesCIPin {
		t.Errorf("%+v %+v", d.Base, d.Tool)
	}
	if d.Base.Diagnostics != len(base) || d.Head.Diagnostics != len(head) {
		t.Errorf("evidence counts %+v %+v", d.Base, d.Head)
	}
	tsv := render(t, d, "tsv")
	for _, want := range []string{
		"# schema: " + deltaSchema,
		"# base.ref: base-id", "# head.ref: head-id", "# base.raw_sha256: " + d.Base.RawSHA256, "# head.raw_sha256: " + d.Head.RawSHA256,
		"# golangci_lint_version: 2.15.0", "# ci_pinned_version: 2.14.0", "# matches_ci_pin: false", "# config_sha256: " + d.Config.SHA256,
		"# patterns: ./a/...,./b/...", "# enabled_linters: cyclop,dupl,funlen,gocognit",
		"# entries.reported_both: 2", "# entries.new_above_threshold: 1", "# entries.not_reported_above_threshold: 2", "# entries.moved_or_unmatched: 4",
		"# scope: ", "# raw_scope: ", "state\treason\tlinter\t",
	} {
		if !strings.Contains(tsv, want) {
			t.Errorf("TSV lacks %q", want)
		}
	}
	js := render(t, d, "json")
	for _, want := range []string{`"schema": "` + deltaSchema + `"`, `"raw_sha256": "` + d.Head.RawSHA256 + `"`, `"golangci_lint_version": "2.15.0"`, `"entries_per_state"`, `"raw_scope"`} {
		if !strings.Contains(js, want) {
			t.Errorf("JSON lacks %q", want)
		}
	}
	for _, out := range []string{tsv, js} {
		if strings.Contains(out, bd) || strings.Contains(out, hd) {
			t.Error("output names a snapshot directory")
		}
	}
	// Every TSV data row has the same number of cells as the header.
	var cols int
	for _, line := range strings.Split(strings.TrimRight(tsv, "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		n := strings.Count(line, "\t")
		if cols == 0 {
			cols = n
		}
		if n != cols {
			t.Errorf("row has %d tabs, header has %d: %q", n, cols, line)
		}
	}
}

func TestPairCommandReadsFilesAndRefusesBadUsage(t *testing.T) {
	base, head, bd, hd := scenario(t)
	dir := t.TempDir()
	bp, hp := filepath.Join(dir, "base.json"), filepath.Join(dir, "head.json")
	if err := os.WriteFile(bp, raw(t, base...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hp, raw(t, head...), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	args := []string{"pair", "-base", bp, "-head", hp, "-base-ref", "b1", "-head-ref", "h1", "-base-dir", bd, "-head-dir", hd, "-format", "tsv"}
	if err := run(args, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "# base.ref: b1") || strings.Contains(out.String(), dir) {
		t.Errorf("unexpected output %q", out.String())
	}
	for _, bad := range [][]string{
		{"pair"},
		{"pair", "-base", bp, "-head", hp},
		{"pair", "-base", bp, "-head", hp, "-base-ref", "b", "-head-ref", "h", "-format", "xml"},
		{"pair", "-base", bp, "-head", filepath.Join(dir, "none.json"), "-base-ref", "b", "-head-ref", "h"},
		{"pair", "-base", bp, "-head", hp, "-base-ref", "b", "-head-ref", "h", "extra"},
	} {
		if err := run(bad, &bytes.Buffer{}); err == nil {
			t.Errorf("%v must fail", bad)
		}
	}
}

// With a real golangci-lint the whole chain (collect, pair) runs on two small
// synthetic snapshots. Skipped without a binary; a skip is not a pass.
func TestPairOnRealBinary(t *testing.T) {
	bin := realBinary(t)
	baseRoot := writeModule(t, map[string]string{"a.go": "package m\n\n" + hotFunc("Hot") + "\n" + cognitiveOnly("Cog")})
	headRoot := writeModule(t, map[string]string{"a.go": "package m\n\nfunc Hot(x int) int { return x }\n\n" + cognitiveOnly("Cog")})
	collectJSON := func(root string) []byte {
		rep, err := collect(root, bin, []string{"./..."})
		if err != nil {
			t.Fatal(err)
		}
		return marshalReport(t, rep)
	}
	d := mustPair(t, collectJSON(baseRoot), collectJSON(headRoot), baseRoot, headRoot)
	states := map[string]string{}
	for _, e := range d.Entries {
		states[e.Function+"/"+e.Linter] = e.State
	}
	for _, linter := range []string{"cyclop", "funlen", "gocognit"} {
		if states["Hot/"+linter] != stateNotReported {
			t.Errorf("Hot/%s: %v", linter, states)
		}
	}
	if d.Counts[stateUnmatched] != 0 {
		t.Errorf("%+v", d.Entries)
	}
}

// Declaration multiplicity is counted per package, receiver and function in the
// whole snapshot, whether or not the extra declaration has a diagnostic.
func TestPairAmbiguousDeclarationStaysUnmatched(t *testing.T) {
	two := snap(t, map[string]string{"a.go": hotSrc, "b.go": "//go:build linux\n\npackage m\n\nfunc Hot(x int) int { return x }\n"})
	one := snap(t, map[string]string{"a.go": hotSrc})
	d1 := fd{"gocognit", "a.go", 3, 1, cog("Hot", 40)}
	d2 := fd{"gocognit", "a.go", 3, 1, cog("Hot", 41)}
	// Paired: one diagnostic per side, two declarations per snapshot.
	d := mustPair(t, raw(t, d1), raw(t, d2), two, two)
	if d.Counts[stateBoth] != 0 || d.Counts[stateUnmatched] != 2 || d.Entries[0].Reason != reasonAmbiguous {
		t.Errorf("paired: %+v", d.Entries)
	}
	// One-sided: ambiguous originating side, unique opposite side without a diagnostic.
	e := only(t, mustPair(t, raw(t, d1), raw(t), two, one))
	if e.State != stateUnmatched || e.Reason != reasonAmbiguous {
		t.Errorf("base ambiguous: %+v", e)
	}
	e = only(t, mustPair(t, raw(t), raw(t, d2), one, two))
	if e.State != stateUnmatched || e.Reason != reasonAmbiguous {
		t.Errorf("head ambiguous: %+v", e)
	}
	// Positive controls: unique declarations pair and disappear as before.
	if e := only(t, mustPair(t, raw(t, d1), raw(t, d2), one, one)); e.State != stateBoth {
		t.Errorf("unique paired: %+v", e)
	}
	if e := only(t, mustPair(t, raw(t, d1), raw(t), one, one)); e.State != stateNotReported {
		t.Errorf("unique one-sided: %+v", e)
	}
}
