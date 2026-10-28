// Command lintcalibration runs the metric linters of issue #226 (cyclop, dupl,
// funlen, gocognit) through golangci-lint with a report-only configuration that
// keeps every diagnostic, and prints them grouped by source location.
//
//	lintcalibration report [-dir MODULE_ROOT] [-golangci-lint PATH] [-format json|tsv] [PACKAGE_PATTERN...]
//
// Why a separate configuration: golangci-lint's uniq-by-line processor keys
// only by file and line, not by linter, and its default caps drop issues beyond
// 50 per linter and three identical texts. A combined comparison made through
// those defaults can hide a second metric reported at the same location. The
// embedded configuration turns all three off. It does not touch the
// repository's own .golangci.yml and nothing gates on its output.
//
// The report records the golangci-lint version that ran, whether it equals the
// version pinned by Code CI, and a digest of the configuration, and lists the
// linters the binary reported as enabled. Nothing is committed or written to
// the module.
//
// See docs/contributing/lint-calibration.md.
package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	schema = "ceph-lint-calibration/v1"
	// ciPinnedVersion is the golangci-lint version of .github/workflows/code.yml.
	ciPinnedVersion = "2.14.0"
)

//go:embed golangci.yml
var calibrationConfig []byte

// metricLinters are the linters the embedded configuration enables. The report
// refuses output whose enabled set differs, so a drifting configuration or a
// binary that ignores it cannot pass as a calibration run.
var metricLinters = []string{"cyclop", "dupl", "funlen", "gocognit"}

// diagnostic is one issue as golangci-lint emitted it.
type diagnostic struct {
	Linter  string `json:"linter"`
	Message string `json:"message"`
	Column  int    `json:"column"`
}

// location groups the diagnostics reported at one file and line.
type location struct {
	File        string       `json:"file"`
	Line        int          `json:"line"`
	Test        bool         `json:"test_file"`
	Overlap     bool         `json:"overlap"`
	Diagnostics []diagnostic `json:"diagnostics"`
}

type report struct {
	Schema          string         `json:"schema"`
	Tool            toolInfo       `json:"tool"`
	Config          configInfo     `json:"config"`
	Patterns        []string       `json:"patterns"`
	EnabledLinters  []string       `json:"enabled_linters"`
	Counts          map[string]int `json:"diagnostics_per_linter"`
	Total           int            `json:"diagnostics_total"`
	OverlapLocation int            `json:"overlapping_locations"`
	Locations       []location     `json:"locations"`
	Scope           []string       `json:"scope"`
}

type toolInfo struct {
	Version      string `json:"golangci_lint_version"`
	CIPinned     string `json:"ci_pinned_version"`
	MatchesCIPin bool   `json:"matches_ci_pin"`
}

type configInfo struct {
	SHA256 string `json:"sha256"`
}

var reportScope = []string{
	"every diagnostic of cyclop, dupl, funlen and gocognit with the thresholds of the embedded configuration, which are the linters' own defaults and not a project policy",
	"uniq-by-line is off and both output caps are off, so diagnostics of different linters at one file and line are all kept",
	"test files are included and flagged test_file; golangci-lint skips generated files by its own default",
	"//nolint directives and the build-tag set of .golangci.yml still apply",
	"a raw collection for calibration, not a base-versus-head delta, a ranking, a threshold or a gate",
	"a diagnostic is a signal to read the code, not proof of a poor design",
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "lintcalibration:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "report" {
		return errors.New("usage: lintcalibration report [-dir MODULE_ROOT] [-golangci-lint PATH] [-format json|tsv] [PACKAGE_PATTERN...]")
	}
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fs.String("dir", ".", "module root to analyse")
	bin := fs.String("golangci-lint", "golangci-lint", "golangci-lint executable")
	format := fs.String("format", "json", "json or tsv")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *format != "json" && *format != "tsv" {
		return fmt.Errorf("unknown format %q", *format)
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	rep, err := collect(*dir, *bin, patterns)
	if err != nil {
		return err
	}
	if *format == "tsv" {
		return writeTSV(out, rep)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

var versionRE = regexp.MustCompile(`has version v?(\d+\.\d+\.\d+)`)

func collect(dir, bin string, patterns []string) (*report, error) {
	version, err := binaryVersion(bin)
	if err != nil {
		return nil, err
	}
	cfgDir, err := os.MkdirTemp("", "lintcalibration-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(cfgDir) }()
	cfgPath := filepath.Join(cfgDir, "golangci.yml")
	if err := os.WriteFile(cfgPath, calibrationConfig, 0o600); err != nil {
		return nil, err
	}
	jsonPath := filepath.Join(cfgDir, "issues.json")
	args := []string{"run", "--config", cfgPath, "--issues-exit-code", "0",
		"--output.json.path", jsonPath, "--timeout=15m"}
	args = append(args, patterns...)
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("golangci-lint failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("golangci-lint wrote no JSON output: %w", err)
	}
	rep, err := parse(raw)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(calibrationConfig)
	rep.Tool = toolInfo{Version: version, CIPinned: ciPinnedVersion, MatchesCIPin: version == ciPinnedVersion}
	rep.Config = configInfo{SHA256: hex.EncodeToString(sum[:])}
	rep.Patterns = patterns
	rep.Scope = reportScope
	return rep, nil
}

func binaryVersion(bin string) (string, error) {
	out, err := exec.Command(bin, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("golangci-lint --version: %w", err)
	}
	m := versionRE.FindSubmatch(out)
	if m == nil {
		return "", fmt.Errorf("cannot read a version from %q", strings.TrimSpace(string(out)))
	}
	return string(m[1]), nil
}

// parse decodes golangci-lint JSON output. A missing or wrong-typed field is an
// error: an absent issue list must not read as "no findings".
func parse(raw []byte) (*report, error) {
	var top struct {
		Issues json.RawMessage `json:"Issues"`
		Report *struct {
			Linters *[]struct {
				Name    string `json:"Name"`
				Enabled bool   `json:"Enabled"`
			} `json:"Linters"`
		} `json:"Report"`
	}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("golangci-lint JSON: %w", err)
	}
	// The member must be present: golangci-lint always emits it, as an array or
	// null. RawMessage stays nil only when the key is absent (an explicit null
	// is the four bytes "null"), so absence is refused here.
	if top.Issues == nil {
		return nil, errors.New(`golangci-lint JSON has no top-level "Issues" member, so an empty result cannot be told from incomplete output`)
	}
	if top.Report == nil || top.Report.Linters == nil {
		return nil, errors.New("golangci-lint JSON has no Report.Linters, so the enabled linters cannot be checked")
	}
	var enabled []string
	for _, l := range *top.Report.Linters {
		// typecheck is golangci-lint's built-in loader error reporter; it is
		// always reported as enabled and is not a metric.
		if l.Enabled && l.Name != "typecheck" {
			enabled = append(enabled, l.Name)
		}
	}
	sort.Strings(enabled)
	if !slices.Equal(enabled, metricLinters) {
		return nil, fmt.Errorf("enabled linters are %v, want %v: the calibration configuration was not applied", enabled, metricLinters)
	}
	rep := &report{Schema: schema, EnabledLinters: enabled, Counts: map[string]int{}}
	for _, l := range metricLinters {
		rep.Counts[l] = 0
	}
	byLoc := map[string]*location{}
	var issues []struct {
		FromLinter *string `json:"FromLinter"`
		Text       *string `json:"Text"`
		Pos        *struct {
			Filename *string `json:"Filename"`
			Line     *int    `json:"Line"`
			Column   *int    `json:"Column"`
		} `json:"Pos"`
	}
	if err := json.Unmarshal(top.Issues, &issues); err != nil {
		return nil, fmt.Errorf(`golangci-lint JSON "Issues": %w`, err)
	}
	for i, is := range issues {
		if is.FromLinter == nil || is.Text == nil || is.Pos == nil || is.Pos.Filename == nil || is.Pos.Line == nil || is.Pos.Column == nil {
			return nil, fmt.Errorf("issue %d lacks FromLinter, Text or Pos.Filename/Line/Column", i)
		}
		if _, ok := rep.Counts[*is.FromLinter]; !ok {
			return nil, fmt.Errorf("issue %d is from unexpected linter %q", i, *is.FromLinter)
		}
		file := filepath.ToSlash(*is.Pos.Filename)
		key := fmt.Sprintf("%s:%d", file, *is.Pos.Line)
		loc := byLoc[key]
		if loc == nil {
			loc = &location{File: file, Line: *is.Pos.Line, Test: strings.HasSuffix(file, "_test.go")}
			byLoc[key] = loc
		}
		loc.Diagnostics = append(loc.Diagnostics, diagnostic{Linter: *is.FromLinter, Message: *is.Text, Column: *is.Pos.Column})
		rep.Counts[*is.FromLinter]++
		rep.Total++
	}
	for _, loc := range byLoc {
		sort.Slice(loc.Diagnostics, func(a, b int) bool {
			x, y := loc.Diagnostics[a], loc.Diagnostics[b]
			if x.Linter != y.Linter {
				return x.Linter < y.Linter
			}
			if x.Column != y.Column {
				return x.Column < y.Column
			}
			return x.Message < y.Message
		})
		seen := map[string]bool{}
		for _, d := range loc.Diagnostics {
			seen[d.Linter] = true
		}
		loc.Overlap = len(seen) > 1
		if loc.Overlap {
			rep.OverlapLocation++
		}
		rep.Locations = append(rep.Locations, *loc)
	}
	sort.Slice(rep.Locations, func(a, b int) bool {
		x, y := rep.Locations[a], rep.Locations[b]
		if x.File != y.File {
			return x.File < y.File
		}
		return x.Line < y.Line
	})
	return rep, nil
}

func writeTSV(out io.Writer, rep *report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# schema: %s\n", rep.Schema)
	fmt.Fprintf(&b, "# golangci_lint_version: %s\n# ci_pinned_version: %s\n# matches_ci_pin: %t\n",
		rep.Tool.Version, rep.Tool.CIPinned, rep.Tool.MatchesCIPin)
	fmt.Fprintf(&b, "# config_sha256: %s\n# patterns: %s\n# enabled_linters: %s\n",
		rep.Config.SHA256, strings.Join(rep.Patterns, ","), strings.Join(rep.EnabledLinters, ","))
	for _, l := range metricLinters {
		fmt.Fprintf(&b, "# diagnostics.%s: %d\n", l, rep.Counts[l])
	}
	fmt.Fprintf(&b, "# diagnostics_total: %d\n# overlapping_locations: %d\n", rep.Total, rep.OverlapLocation)
	for _, s := range rep.Scope {
		fmt.Fprintf(&b, "# scope: %s\n", s)
	}
	b.WriteString("file\tline\tcolumn\tlinter\toverlap\ttest_file\tmessage\n")
	for _, loc := range rep.Locations {
		for _, d := range loc.Diagnostics {
			fmt.Fprintf(&b, "%s\t%d\t%d\t%s\t%t\t%t\t%s\n", loc.File, loc.Line, d.Column, d.Linter, loc.Overlap, loc.Test,
				strings.NewReplacer("\t", " ", "\n", " ").Replace(d.Message))
		}
	}
	_, err := io.WriteString(out, b.String())
	return err
}
