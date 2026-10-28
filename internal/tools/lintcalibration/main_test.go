package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The fixtures are synthetic Go modules in a temporary directory. Nothing here
// reads this repository's own packages, so a diagnostic in the repository
// cannot change a test result, and no test needs Docker or the network.

// hotFunc is a function that exceeds the default thresholds of cyclop,
// funlen and gocognit at once: 11 nested conditions give cyclomatic
// complexity 12 and cognitive complexity 66, and 60 extra statements push the
// statement count over 40 and the line count over 60. All three diagnostics are
// reported on the line of the func keyword.
func hotFunc(name string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "func %s(x int) int {\n", name)
	for i := 0; i < 11; i++ {
		fmt.Fprintf(&b, "%sif x > %d {\n", strings.Repeat("\t", i+1), i)
	}
	fmt.Fprintf(&b, "%sx++\n", strings.Repeat("\t", 12))
	for i := 10; i >= 0; i-- {
		fmt.Fprintf(&b, "%s}\n", strings.Repeat("\t", i+1))
	}
	b.WriteString(strings.Repeat("\tx++\n", 60))
	b.WriteString("\treturn x\n}\n")
	return b.String()
}

// cognitiveOnly is long enough for neither funlen nor cyclop (cyclomatic 9) but
// cognitively complex through nesting, so only gocognit reports it.
func cognitiveOnly(name string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "func %s(x int) int {\n", name)
	for i := 0; i < 8; i++ {
		fmt.Fprintf(&b, "%sif x > %d {\n", strings.Repeat("\t", i+1), i)
	}
	fmt.Fprintf(&b, "%sx++\n", strings.Repeat("\t", 9))
	for i := 7; i >= 0; i-- {
		fmt.Fprintf(&b, "%s}\n", strings.Repeat("\t", i+1))
	}
	b.WriteString("\treturn x\n}\n")
	return b.String()
}

func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files["go.mod"] = "module example.test/m\n\ngo 1.22\n"
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// realBinary returns the golangci-lint to test against. Without one the test
// is skipped, which is not a pass: the fake-binary tests below still run, but
// only a real binary exercises the actual line filter.
func realBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("GOLANGCI_LINT")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("golangci-lint"); err != nil {
			t.Skip("golangci-lint is not installed; the real line-filter controls did not run")
		}
	}
	return bin
}

// runWithConfig runs the binary with an explicit configuration text, as collect does
// but without the calibration config, and returns the linters seen per line.
func runWithConfig(t *testing.T, bin, root, config string) map[string][]string {
	t.Helper()
	cfg := filepath.Join(t.TempDir(), "c.yml")
	if err := os.WriteFile(cfg, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	jsonPath := filepath.Join(t.TempDir(), "issues.json")
	cmd := exec.Command(bin, "run", "--config", cfg, "--issues-exit-code", "0", "--output.json.path", jsonPath, "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%v: %s", err, stderr.String())
	}
	var res struct {
		Issues []struct {
			FromLinter string
			Pos        struct {
				Filename string
				Line     int
			}
		}
	}
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, is := range res.Issues {
		key := fmt.Sprintf("%s:%d", is.Pos.Filename, is.Pos.Line)
		got[key] = append(got[key], is.FromLinter)
	}
	for _, v := range got {
		slices.Sort(v)
	}
	return got
}

func pairConfig(linters string, uniq bool) string {
	return fmt.Sprintf(`version: "2"
run:
  relative-path-mode: gomod
linters:
  default: none
  enable: [%s]
issues:
  uniq-by-line: %t
`, linters, uniq)
}

// Control: the golangci-lint defaults hide a second metric at the same
// location, whichever pair of metrics is enabled. This reproduces the owner's
// finding on the installed binary and is the failing-first baseline for the
// calibration configuration.
func TestDefaultLineFilterHidesSecondMetric(t *testing.T) {
	bin := realBinary(t)
	root := writeModule(t, map[string]string{"a.go": "package m\n\n" + hotFunc("Hot")})
	for _, pair := range []string{"cyclop, gocognit", "cyclop, funlen", "funlen, gocognit"} {
		defaults := runWithConfig(t, bin, root, pairConfig(pair, true))
		if got := defaults["a.go:3"]; len(got) != 1 {
			t.Errorf("pair [%s] with uniq-by-line on: want exactly one survivor at a.go:3, got %v", pair, got)
		}
		kept := runWithConfig(t, bin, root, pairConfig(pair, false))
		if got := kept["a.go:3"]; len(got) != 2 {
			t.Errorf("pair [%s] with uniq-by-line off: want both at a.go:3, got %v", pair, got)
		}
	}
}

// The survivor under the default filter depends on which metrics are enabled
// together, so a combined run can hide a different linter from a different
// pairing. Pinned as observed behaviour of the installed binary: if it stops
// holding the filter changed and the calibration reasoning needs a re-check.
func TestDefaultLineFilterSurvivorDependsOnPairing(t *testing.T) {
	bin := realBinary(t)
	root := writeModule(t, map[string]string{"a.go": "package m\n\n" + hotFunc("Hot")})
	a := runWithConfig(t, bin, root, pairConfig("cyclop, gocognit", true))["a.go:3"]
	b := runWithConfig(t, bin, root, pairConfig("cyclop, funlen", true))["a.go:3"]
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("each default run must keep exactly one diagnostic, got %v and %v", a, b)
	}
	if !slices.Contains(a, "gocognit") || !slices.Contains(b, "cyclop") {
		t.Errorf("survivors changed: cyclop+gocognit kept %v, cyclop+funlen kept %v", a, b)
	}
}

// The default caps hide diagnostics beyond three identical texts and fifty per
// linter; the calibration configuration keeps them all.
func TestCalibrationConfigDisablesCaps(t *testing.T) {
	bin := realBinary(t)
	files := map[string]string{}
	for i := 0; i < 55; i++ {
		files[fmt.Sprintf("p%02d/a.go", i)] = fmt.Sprintf("package p%02d\n\n%s", i, cognitiveOnly("F"))
	}
	root := writeModule(t, files)
	base := `version: "2"
run:
  relative-path-mode: gomod
linters:
  default: none
  enable: [gocognit]
issues:
  uniq-by-line: false
`
	if got := len(runWithConfig(t, bin, root, base)); got != 3 {
		// 55 same-text diagnostics collapse to the default three.
		t.Errorf("default caps: want 3 locations kept, got %d", got)
	}
	rep, err := collect(root, bin, []string{"./..."})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Counts["gocognit"] != 55 {
		t.Errorf("calibration kept %d of 55 gocognit diagnostics", rep.Counts["gocognit"])
	}
}

func TestReportKeepsOverlapOnRealBinary(t *testing.T) {
	bin := realBinary(t)
	root := writeModule(t, map[string]string{
		"a.go":      "package m\n\n" + hotFunc("Hot"),
		"b.go":      "package m\n\n" + cognitiveOnly("OnlyCognitive"),
		"c_test.go": "package m\n\n" + cognitiveOnly("OnlyCognitiveInTest"),
	})
	rep, err := collect(root, bin, []string{"./..."})
	if err != nil {
		t.Fatal(err)
	}
	locs := map[string]location{}
	for _, l := range rep.Locations {
		locs[fmt.Sprintf("%s:%d", l.File, l.Line)] = l
	}
	hot := locs["a.go:3"]
	var linters []string
	for _, d := range hot.Diagnostics {
		linters = append(linters, d.Linter)
	}
	if !hot.Overlap || !slices.Equal(linters, []string{"cyclop", "funlen", "gocognit"}) {
		t.Errorf("a.go:3 should keep cyclop, funlen and gocognit, got %v overlap=%t", linters, hot.Overlap)
	}
	if b := locs["b.go:3"]; b.Overlap || len(b.Diagnostics) != 1 || b.Diagnostics[0].Linter != "gocognit" {
		t.Errorf("b.go:3 is a single-metric control, got %+v", b)
	}
	if c := locs["c_test.go:3"]; !c.Test || c.Overlap {
		t.Errorf("c_test.go:3 should be flagged as a test file, got %+v", c)
	}
	if rep.OverlapLocation != 1 || rep.Total != 5 {
		t.Errorf("overlapping=%d total=%d, want 1 and 5", rep.OverlapLocation, rep.Total)
	}
	// Order independence: the grouping does not depend on the order in which
	// the linters were reported.
	again, err := collect(root, bin, []string{"./..."})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(rep)
	b, _ := json.Marshal(again)
	if !bytes.Equal(a, b) {
		t.Error("two runs on the same input differ")
	}
}

// The regular repository configuration is not the calibration configuration.
func TestEmbeddedConfigPinsFilters(t *testing.T) {
	cfg := string(calibrationConfig)
	for _, line := range []string{"uniq-by-line: false", "max-issues-per-linter: 0", "max-same-issues: 0", "default: none", "relative-path-mode: gomod"} {
		if !strings.Contains(cfg, line) {
			t.Errorf("embedded configuration lacks %q", line)
		}
	}
	for _, l := range metricLinters {
		if !strings.Contains(cfg, "    - "+l+"\n") {
			t.Errorf("embedded configuration does not enable %s", l)
		}
	}
}

// fakeBinary writes a shell script that answers --version and prints canned
// JSON for run. It needs a POSIX shell.
func fakeBinary(t *testing.T, version, json string, exit int) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake binary")
	}
	dir := t.TempDir()
	payload := filepath.Join(dir, "out.json")
	if err := os.WriteFile(payload, []byte(json), 0o644); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'golangci-lint has version %s built with go1.27 from abc on 2026-01-01'; exit 0; fi\nwhile [ $# -gt 0 ]; do if [ \"$1\" = \"--output.json.path\" ]; then cp '%s' \"$2\"; fi; shift; done\nexit %d\n", version, payload, exit)
	path := filepath.Join(dir, "golangci-lint")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func linterList(enabled ...string) string {
	var parts []string
	for _, n := range []string{"asciicheck", "cyclop", "dupl", "funlen", "gocognit", "govet"} {
		parts = append(parts, fmt.Sprintf(`{"Name":%q,"Enabled":%t}`, n, slices.Contains(enabled, n)))
	}
	return `"Report":{"Linters":[` + strings.Join(parts, ",") + `]}`
}

func issue(linter, file string, line, col int) string {
	return fmt.Sprintf(`{"FromLinter":%q,"Text":"m","Pos":{"Filename":%q,"Line":%d,"Column":%d}}`, linter, file, line, col)
}

var allMetrics = []string{"cyclop", "dupl", "funlen", "gocognit"}

func TestReportGroupsAndOrdersEitherInputOrder(t *testing.T) {
	for _, order := range [][]string{{"cyclop", "gocognit"}, {"gocognit", "cyclop"}} {
		js := `{"Issues":[` + issue(order[0], "x.go", 3, 1) + `,` + issue(order[1], "x.go", 3, 6) + `,` + issue("funlen", "y.go", 9, 6) + `],` + linterList(allMetrics...) + `}`
		rep, err := collect(t.TempDir(), fakeBinary(t, "2.14.0", js, 0), []string{"./..."})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Locations) != 2 || !rep.Locations[0].Overlap || rep.Locations[1].Overlap {
			t.Fatalf("order %v: %+v", order, rep.Locations)
		}
		got := []string{rep.Locations[0].Diagnostics[0].Linter, rep.Locations[0].Diagnostics[1].Linter}
		if !slices.Equal(got, []string{"cyclop", "gocognit"}) {
			t.Errorf("order %v: diagnostics not sorted: %v", order, got)
		}
		if rep.Total != 3 || rep.OverlapLocation != 1 || rep.Counts["funlen"] != 1 || rep.Counts["dupl"] != 0 {
			t.Errorf("order %v: counts %+v total %d", order, rep.Counts, rep.Total)
		}
	}
}

func TestReportRecordsVersionAgainstCIPin(t *testing.T) {
	js := `{"Issues":null,` + linterList(allMetrics...) + `}`
	for version, want := range map[string]bool{"2.14.0": true, "2.15.0": false} {
		rep, err := collect(t.TempDir(), fakeBinary(t, version, js, 0), []string{"./..."})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Tool.Version != version || rep.Tool.MatchesCIPin != want || rep.Tool.CIPinned != "2.14.0" {
			t.Errorf("version %s: %+v", version, rep.Tool)
		}
		if rep.Total != 0 || len(rep.Locations) != 0 {
			t.Errorf("no issues must give an empty report: %+v", rep)
		}
	}
}

// Missing, null or wrong-typed evidence is an error, never an empty finding.
func TestReportRejectsInconclusiveOutput(t *testing.T) {
	good := issue("cyclop", "x.go", 3, 1)
	cases := map[string]string{
		"empty object":            `{}`,
		"not json":                `golangci-lint: boom`,
		"no report linters":       `{"Issues":[` + good + `],"Report":{}}`,
		"null linters":            `{"Issues":[` + good + `],"Report":{"Linters":null}}`,
		"calibration not applied": `{"Issues":[],` + linterList("govet", "asciicheck") + `}`,
		"missing a metric":        `{"Issues":[],` + linterList("cyclop", "dupl", "funlen") + `}`,
		"extra linter enabled":    `{"Issues":[],` + linterList("cyclop", "dupl", "funlen", "gocognit", "govet") + `}`,
		"issue without pos":       `{"Issues":[{"FromLinter":"cyclop","Text":"m"}],` + linterList(allMetrics...) + `}`,
		"issue without line":      `{"Issues":[{"FromLinter":"cyclop","Text":"m","Pos":{"Filename":"x.go","Column":1}}],` + linterList(allMetrics...) + `}`,
		"issue without linter":    `{"Issues":[{"Text":"m","Pos":{"Filename":"x.go","Line":1,"Column":1}}],` + linterList(allMetrics...) + `}`,
		"string line":             `{"Issues":[{"FromLinter":"cyclop","Text":"m","Pos":{"Filename":"x.go","Line":"3","Column":1}}],` + linterList(allMetrics...) + `}`,
		"unexpected linter":       `{"Issues":[` + issue("govet", "x.go", 3, 1) + `],` + linterList(allMetrics...) + `}`,
	}
	for name, js := range cases {
		if _, err := collect(t.TempDir(), fakeBinary(t, "2.14.0", js, 0), []string{"./..."}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestReportSurfacesToolFailure(t *testing.T) {
	js := `{"Issues":[],` + linterList(allMetrics...) + `}`
	if _, err := collect(t.TempDir(), fakeBinary(t, "2.14.0", js, 3), []string{"./..."}); err == nil {
		t.Error("a non-zero exit must be an error")
	}
	if _, err := collect(t.TempDir(), filepath.Join(t.TempDir(), "absent"), nil); err == nil {
		t.Error("a missing binary must be an error")
	}
}

func TestTSVCarriesProvenanceAndOverlap(t *testing.T) {
	js := `{"Issues":[` + issue("cyclop", "x.go", 3, 1) + `,` + issue("gocognit", "x.go", 3, 1) + `],` + linterList(allMetrics...) + `}`
	bin := fakeBinary(t, "2.14.0", js, 0)
	var out bytes.Buffer
	if err := run([]string{"report", "-dir", t.TempDir(), "-golangci-lint", bin, "-format", "tsv"}, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"# schema: " + schema, "# golangci_lint_version: 2.14.0", "# matches_ci_pin: true", "# config_sha256: ", "# overlapping_locations: 1",
		"x.go\t3\t1\tcyclop\ttrue\tfalse\tm", "x.go\t3\t1\tgocognit\ttrue\tfalse\tm"} {
		if !strings.Contains(text, want) {
			t.Errorf("TSV lacks %q:\n%s", want, text)
		}
	}
	if err := run([]string{"report", "-format", "xml"}, &out); err == nil {
		t.Error("unknown format must fail")
	}
	if err := run([]string{"delta"}, &out); err == nil {
		t.Error("unknown subcommand must fail")
	}
}
