package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// The pair subcommand joins two raw collections made by the report subcommand,
// one for an identified base snapshot and one for an identified head snapshot,
// into a focused delta. The raw collections stay the evidence: the delta only
// references them by digest and never replaces them.
//
// The collector reports a diagnostic only above a linter threshold. A
// diagnostic that is absent on one side is therefore not a measured value of
// zero, and a file and line group does not identify a function that moved. The
// delta keeps four states apart:
//
//	reported_both                 the same function and metric is reported on both sides
//	new_above_threshold           reported at head only, and the function was declared at base
//	not_reported_above_threshold  reported at base only, and the function is declared at head
//	moved_or_unmatched            identity cannot be established, so nothing is concluded
//
// Identity is the declaring package (module-relative directory plus package
// clause), the receiver type name and the function name, read with go/parser
// from the snapshot source at the reported position. Without that source, or
// when the position, the message or the declaration set is not unambiguous,
// the diagnostic stays moved_or_unmatched.
const deltaSchema = "ceph-lint-calibration-delta/v1"

const (
	stateBoth        = "reported_both"
	stateNew         = "new_above_threshold"
	stateNotReported = "not_reported_above_threshold"
	stateUnmatched   = "moved_or_unmatched"
)

var deltaStates = []string{stateBoth, stateNew, stateNotReported, stateUnmatched}

// Reasons attached to moved_or_unmatched entries.
const (
	reasonNoSnapshot   = "no_snapshot_source"
	reasonNoDecl       = "no_declaration_at_position"
	reasonNoIdentity   = "linter_has_no_function_identity"
	reasonMessage      = "message_does_not_name_declaration"
	reasonUnparsable   = "snapshot_file_unparsable"
	reasonAmbiguous    = "ambiguous_identity"
	reasonAbsentInHead = "declaration_absent_in_head"
	reasonAbsentInBase = "declaration_absent_in_base"
	reasonManyInHead   = "declaration_ambiguous_in_head"
	reasonManyInBase   = "declaration_ambiguous_in_base"
)

// Caveats attached to states that rely on a declaration's absence from a collection.
const (
	caveatBuildConstr = "file_has_build_constraint"
	caveatNolint      = "nolint_in_declaration"
)

const (
	deltaUsage          = "usage: lintcalibration pair -base RAW.json -head RAW.json -base-ref ID -head-ref ID [-base-dir SNAPSHOT] [-head-dir SNAPSHOT] [-format json|tsv]"
	snapshotCaveatLimit = "a snapshot directory must be the module root the matching collection ran in"
)

var deltaScope = []string{
	"a focused pairing of two raw collections, not a ranking, a threshold, a gate or a verdict",
	"the collector reports only above its own linter thresholds, so an absent diagnostic is not a measured value of zero and not_reported_above_threshold is never an improvement",
	"identity is package directory and name, receiver type name and function name read from the snapshot source; a diagnostic without it stays moved_or_unmatched",
	"dupl reports a code block, not a declaration, so every dupl diagnostic is moved_or_unmatched",
	"//nolint directives and file build constraints are not evaluated: a declaration with either carries a caveat on the states that rely on its absence being a measurement",
	"a function moved to another package or renamed is moved_or_unmatched on both sides, never an improvement and never an introduction",
	"the raw collections remain the evidence; this output references them by digest",
}

// site is one reported diagnostic.
type site struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
	Test    bool   `json:"test_file"`
	Message string `json:"message"`
}

// declSite is where an identified function is declared in a snapshot.
type declSite struct {
	File string `json:"file"`
	Line int    `json:"line"`
}

type deltaEntry struct {
	State    string    `json:"state"`
	Reason   string    `json:"reason,omitempty"`
	Linter   string    `json:"linter"`
	Metric   string    `json:"metric,omitempty"`
	PkgDir   string    `json:"package_dir,omitempty"`
	PkgName  string    `json:"package_name,omitempty"`
	Receiver string    `json:"receiver,omitempty"`
	Function string    `json:"function,omitempty"`
	Base     *site     `json:"base,omitempty"`
	Head     *site     `json:"head,omitempty"`
	BaseDecl *declSite `json:"base_declaration,omitempty"`
	HeadDecl *declSite `json:"head_declaration,omitempty"`
	Caveats  []string  `json:"caveats,omitempty"`
}

type evidenceRef struct {
	Ref             string `json:"ref"`
	RawSHA256       string `json:"raw_sha256"`
	Diagnostics     int    `json:"diagnostics_total"`
	Locations       int    `json:"locations"`
	SnapshotIndexed bool   `json:"snapshot_source_indexed"`
	UnparsableFiles int    `json:"snapshot_unparsable_files"`
}

type delta struct {
	Schema         string         `json:"schema"`
	Base           evidenceRef    `json:"base_evidence"`
	Head           evidenceRef    `json:"head_evidence"`
	Tool           toolInfo       `json:"tool"`
	Config         configInfo     `json:"config"`
	Patterns       []string       `json:"patterns"`
	EnabledLinters []string       `json:"enabled_linters"`
	RawScope       []string       `json:"raw_scope"`
	Counts         map[string]int `json:"entries_per_state"`
	Scope          []string       `json:"scope"`
	Entries        []deltaEntry   `json:"entries"`
}

// rawReport mirrors report with pointer and RawMessage members so a missing or
// wrong-typed member is refused instead of decoding to a zero value.
type rawReport struct {
	Schema *string `json:"schema"`
	Tool   *struct {
		Version  *string `json:"golangci_lint_version"`
		CIPinned *string `json:"ci_pinned_version"`
		Matches  *bool   `json:"matches_ci_pin"`
	} `json:"tool"`
	Config *struct {
		SHA256 *string `json:"sha256"`
	} `json:"config"`
	Patterns  json.RawMessage `json:"patterns"`
	Enabled   json.RawMessage `json:"enabled_linters"`
	Counts    json.RawMessage `json:"diagnostics_per_linter"`
	Total     *int            `json:"diagnostics_total"`
	Overlaps  *int            `json:"overlapping_locations"`
	Locations json.RawMessage `json:"locations"`
	Scope     json.RawMessage `json:"scope"`
}

type rawLocation struct {
	File        *string `json:"file"`
	Line        *int    `json:"line"`
	Test        *bool   `json:"test_file"`
	Overlap     *bool   `json:"overlap"`
	Diagnostics *[]struct {
		Linter  *string `json:"linter"`
		Message *string `json:"message"`
		Column  *int    `json:"column"`
	} `json:"diagnostics"`
}

// loadRaw decodes and validates one raw collection. Every member the collector
// always writes must be present with its type; counts, overlap flags and test
// flags must agree with the locations, so a hand-edited file is refused.
func loadRaw(data []byte) (*report, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw rawReport
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("raw collection JSON: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("raw collection JSON has trailing data")
	}
	if raw.Schema == nil || *raw.Schema != schema {
		return nil, fmt.Errorf("raw collection schema is not %q", schema)
	}
	if raw.Tool == nil || raw.Tool.Version == nil || *raw.Tool.Version == "" || raw.Tool.CIPinned == nil || raw.Tool.Matches == nil {
		return nil, errors.New("raw collection lacks tool.golangci_lint_version, ci_pinned_version or matches_ci_pin")
	}
	if *raw.Tool.Matches != (*raw.Tool.Version == *raw.Tool.CIPinned) {
		return nil, errors.New("raw collection tool.matches_ci_pin disagrees with its versions")
	}
	if raw.Config == nil || raw.Config.SHA256 == nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(*raw.Config.SHA256) {
		return nil, errors.New("raw collection lacks a config.sha256 digest")
	}
	rep := &report{Schema: schema}
	rep.Tool = toolInfo{Version: *raw.Tool.Version, CIPinned: *raw.Tool.CIPinned, MatchesCIPin: *raw.Tool.Matches}
	rep.Config = configInfo{SHA256: *raw.Config.SHA256}
	if err := decodeList(raw.Patterns, "patterns", &rep.Patterns); err != nil {
		return nil, err
	}
	if len(rep.Patterns) == 0 {
		return nil, errors.New("raw collection has no patterns")
	}
	if err := decodeList(raw.Enabled, "enabled_linters", &rep.EnabledLinters); err != nil {
		return nil, err
	}
	if !slices.Equal(rep.EnabledLinters, metricLinters) {
		return nil, fmt.Errorf("raw collection enabled_linters are %v, want %v", rep.EnabledLinters, metricLinters)
	}
	if err := decodeList(raw.Scope, "scope", &rep.Scope); err != nil {
		return nil, err
	}
	if raw.Counts == nil || raw.Total == nil || raw.Overlaps == nil || raw.Locations == nil {
		return nil, errors.New("raw collection lacks diagnostics_per_linter, diagnostics_total, overlapping_locations or locations")
	}
	var counts map[string]int
	if err := json.Unmarshal(raw.Counts, &counts); err != nil || counts == nil {
		return nil, errors.New("raw collection diagnostics_per_linter is not an object of counts")
	}
	var locs []rawLocation
	if err := json.Unmarshal(raw.Locations, &locs); err != nil {
		return nil, fmt.Errorf("raw collection locations: %w", err)
	}
	want := map[string]int{}
	for _, l := range metricLinters {
		want[l] = 0
	}
	seen := map[string]bool{}
	overlaps := 0
	for i, rl := range locs {
		if rl.File == nil || *rl.File == "" || rl.Line == nil || *rl.Line < 1 || rl.Test == nil || rl.Overlap == nil || rl.Diagnostics == nil || len(*rl.Diagnostics) == 0 {
			return nil, fmt.Errorf("raw collection location %d lacks file, line, test_file, overlap or a non-empty diagnostics list", i)
		}
		key := fmt.Sprintf("%s:%d", *rl.File, *rl.Line)
		if seen[key] {
			return nil, fmt.Errorf("raw collection lists location %s twice", key)
		}
		seen[key] = true
		if *rl.Test != strings.HasSuffix(*rl.File, "_test.go") {
			return nil, fmt.Errorf("raw collection location %s has an inconsistent test_file flag", key)
		}
		loc := location{File: *rl.File, Line: *rl.Line, Test: *rl.Test, Overlap: *rl.Overlap}
		linters := map[string]bool{}
		for j, rd := range *rl.Diagnostics {
			if rd.Linter == nil || rd.Message == nil || rd.Column == nil || *rd.Column < 1 {
				return nil, fmt.Errorf("raw collection location %s diagnostic %d lacks linter, message or column", key, j)
			}
			if _, ok := want[*rd.Linter]; !ok {
				return nil, fmt.Errorf("raw collection location %s diagnostic %d is from unexpected linter %q", key, j, *rd.Linter)
			}
			want[*rd.Linter]++
			linters[*rd.Linter] = true
			loc.Diagnostics = append(loc.Diagnostics, diagnostic{Linter: *rd.Linter, Message: *rd.Message, Column: *rd.Column})
		}
		if *rl.Overlap != (len(linters) > 1) {
			return nil, fmt.Errorf("raw collection location %s has an inconsistent overlap flag", key)
		}
		if *rl.Overlap {
			overlaps++
		}
		rep.Total += len(loc.Diagnostics)
		rep.Locations = append(rep.Locations, loc)
	}
	if !equalCounts(counts, want) || *raw.Total != rep.Total || *raw.Overlaps != overlaps {
		return nil, errors.New("raw collection counts disagree with its locations")
	}
	rep.Counts = want
	rep.OverlapLocation = overlaps
	return rep, nil
}

func decodeList(raw json.RawMessage, name string, dst *[]string) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("raw collection lacks %s", name)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("raw collection %s: %w", name, err)
	}
	return nil
}

func equalCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// provenanceMismatches names every field that differs between two collections.
// A delta of collections made under different executables, configurations,
// patterns or scopes would compare unlike measurements.
func provenanceMismatches(base, head *report) []string {
	var out []string
	add := func(name string, equal bool, b, h any) {
		if !equal {
			out = append(out, fmt.Sprintf("%s: base %v, head %v", name, b, h))
		}
	}
	add("schema", base.Schema == head.Schema, base.Schema, head.Schema)
	add("golangci_lint_version", base.Tool.Version == head.Tool.Version, base.Tool.Version, head.Tool.Version)
	add("ci_pinned_version", base.Tool.CIPinned == head.Tool.CIPinned, base.Tool.CIPinned, head.Tool.CIPinned)
	add("config.sha256", base.Config.SHA256 == head.Config.SHA256, base.Config.SHA256, head.Config.SHA256)
	add("patterns", slices.Equal(base.Patterns, head.Patterns), base.Patterns, head.Patterns)
	add("enabled_linters", slices.Equal(base.EnabledLinters, head.EnabledLinters), base.EnabledLinters, head.EnabledLinters)
	add("scope", slices.Equal(base.Scope, head.Scope), "(scope lines)", "(scope lines differ)")
	return out
}

// decl is one function or method declaration found in a snapshot.
type decl struct {
	File    string
	Line    int
	Dir     string
	Pkg     string
	Recv    string
	Name    string
	Caveats []string
}

func (d decl) key() string { return strings.Join([]string{d.Dir, d.Pkg, d.Recv, d.Name}, "\x00") }

// snapshot indexes the Go source of one snapshot directory.
type snapshot struct {
	indexed    bool
	byPos      map[string][]decl // "file:line" of the func keyword
	byKey      map[string][]decl
	unparsable map[string]bool
}

func receiverName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverName(t.X)
	case *ast.ParenExpr:
		return receiverName(t.X)
	case *ast.IndexExpr:
		return receiverName(t.X)
	case *ast.IndexListExpr:
		return receiverName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// indexSnapshot parses every non-generated Go file below dir. Directories the
// go tool ignores (vendor, testdata, names starting with . or _) are skipped.
func indexSnapshot(dir string) (*snapshot, error) {
	s := &snapshot{indexed: true, byPos: map[string][]decl{}, byKey: map[string][]decl{}, unparsable: map[string]bool{}}
	if dir == "" {
		s.indexed = false
		return s, nil
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("snapshot directory is not a readable directory")
	}
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != dir && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		src, err := os.ReadFile(path)
		if err != nil {
			s.unparsable[rel] = true
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, rel, src, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			s.unparsable[rel] = true
			return nil
		}
		if ast.IsGenerated(f) {
			return nil
		}
		pkgDir := filepath.ToSlash(filepath.Dir(rel))
		constrained := false
		for _, cg := range f.Comments {
			if cg.End() > f.Package {
				break
			}
			for _, c := range cg.List {
				if strings.HasPrefix(c.Text, "//go:build") || strings.HasPrefix(c.Text, "// +build") {
					constrained = true
				}
			}
		}
		for _, node := range f.Decls {
			fn, ok := node.(*ast.FuncDecl)
			if !ok {
				continue
			}
			d := decl{File: rel, Line: fset.Position(fn.Pos()).Line, Dir: pkgDir, Pkg: f.Name.Name, Name: fn.Name.Name}
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				d.Recv = receiverName(fn.Recv.List[0].Type)
			}
			first, last := fset.Position(fn.Pos()).Line, fset.Position(fn.End()).Line
			if fn.Doc != nil {
				first = fset.Position(fn.Doc.Pos()).Line
			}
			if constrained {
				d.Caveats = append(d.Caveats, caveatBuildConstr)
			}
			for _, cg := range f.Comments {
				if l := fset.Position(cg.Pos()).Line; l >= first && l <= last && hasNolint(cg) {
					d.Caveats = append(d.Caveats, caveatNolint)
					break
				}
			}
			s.byPos[fmt.Sprintf("%s:%d", rel, d.Line)] = append(s.byPos[fmt.Sprintf("%s:%d", rel, d.Line)], d)
			s.byKey[d.key()] = append(s.byKey[d.key()], d)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("indexing snapshot: %w", err)
	}
	return s, nil
}

// hasNolint looks at the raw comment text: CommentGroup.Text drops directive
// comments such as //nolint:x.
func hasNolint(cg *ast.CommentGroup) bool {
	for _, c := range cg.List {
		if strings.Contains(c.Text, "nolint") {
			return true
		}
	}
	return false
}

var digitRun = regexp.MustCompile(`[0-9]+`)

// metricOf is the message with its numbers masked, so the same metric of the
// same function keeps one identity when its value changes while a different
// metric of the same linter (funlen reports lines and statements apart) does not.
func metricOf(msg string) string { return digitRun.ReplaceAllString(msg, "#") }

func namesIdent(msg, name string) bool {
	for i := 0; ; {
		j := strings.Index(msg[i:], name)
		if j < 0 {
			return false
		}
		start, end := i+j, i+j+len(name)
		before := start == 0 || !isIdentByte(msg[start-1])
		after := end == len(msg) || !isIdentByte(msg[end])
		if before && after {
			return true
		}
		i = start + 1
	}
}

func isIdentByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

// side is one diagnostic with the identity resolved against its snapshot.
type sideDiag struct {
	site   site
	linter string
	metric string
	decl   decl
	ok     bool
	reason string
}

func resolve(s *snapshot, loc location, d diagnostic) sideDiag {
	sd := sideDiag{linter: d.Linter, metric: metricOf(d.Message),
		site: site{File: loc.File, Line: loc.Line, Column: d.Column, Test: loc.Test, Message: d.Message}}
	switch {
	case d.Linter == "dupl":
		sd.reason = reasonNoIdentity
	case !s.indexed:
		sd.reason = reasonNoSnapshot
	case s.unparsable[loc.File]:
		sd.reason = reasonUnparsable
	default:
		cands := s.byPos[fmt.Sprintf("%s:%d", loc.File, loc.Line)]
		switch {
		case len(cands) != 1:
			sd.reason = reasonNoDecl
		case !namesIdent(d.Message, cands[0].Name):
			sd.reason = reasonMessage
		default:
			sd.decl, sd.ok = cands[0], true
		}
	}
	return sd
}

func resolveAll(s *snapshot, rep *report) []sideDiag {
	var out []sideDiag
	for _, loc := range rep.Locations {
		for _, d := range loc.Diagnostics {
			out = append(out, resolve(s, loc, d))
		}
	}
	return out
}

// groupKey identifies a metric of a function.
func groupKey(sd sideDiag) string {
	return sd.decl.key() + "\x00" + sd.linter + "\x00" + sd.metric
}

func (sd sideDiag) fill(e *deltaEntry) {
	e.Linter, e.Metric = sd.linter, sd.metric
	e.PkgDir, e.PkgName, e.Receiver, e.Function = sd.decl.Dir, sd.decl.Pkg, sd.decl.Recv, sd.decl.Name
}

// pairing builds the delta entries from the two resolved sides.
func pairing(baseSnap, headSnap *snapshot, base, head *report) []deltaEntry {
	var entries []deltaEntry
	group := func(all []sideDiag) (map[string][]sideDiag, []sideDiag) {
		by := map[string][]sideDiag{}
		var loose []sideDiag
		for _, sd := range all {
			if sd.ok {
				by[groupKey(sd)] = append(by[groupKey(sd)], sd)
			} else {
				loose = append(loose, sd)
			}
		}
		return by, loose
	}
	baseBy, baseLoose := group(resolveAll(baseSnap, base))
	headBy, headLoose := group(resolveAll(headSnap, head))
	for _, sd := range baseLoose {
		s := sd.site
		e := deltaEntry{State: stateUnmatched, Reason: sd.reason, Linter: sd.linter, Metric: sd.metric, Base: &s}
		entries = append(entries, e)
	}
	for _, sd := range headLoose {
		s := sd.site
		entries = append(entries, deltaEntry{State: stateUnmatched, Reason: sd.reason, Linter: sd.linter, Metric: sd.metric, Head: &s})
	}
	keys := map[string]bool{}
	for k := range baseBy {
		keys[k] = true
	}
	for k := range headBy {
		keys[k] = true
	}
	for k := range keys {
		b, h := baseBy[k], headBy[k]
		switch {
		case len(b) > 1 || len(h) > 1:
			for _, sd := range b {
				s := sd.site
				e := deltaEntry{State: stateUnmatched, Reason: reasonAmbiguous, Base: &s}
				sd.fill(&e)
				entries = append(entries, e)
			}
			for _, sd := range h {
				s := sd.site
				e := deltaEntry{State: stateUnmatched, Reason: reasonAmbiguous, Head: &s}
				sd.fill(&e)
				entries = append(entries, e)
			}
		case len(b) == 1 && len(h) == 1:
			bs, hs := b[0].site, h[0].site
			e := deltaEntry{State: stateBoth, Base: &bs, Head: &hs}
			b[0].fill(&e)
			entries = append(entries, e)
		case len(b) == 1:
			bs := b[0].site
			e := deltaEntry{Base: &bs}
			b[0].fill(&e)
			where := headSnap.byKey[b[0].decl.key()]
			switch {
			case !headSnap.indexed:
				e.State, e.Reason = stateUnmatched, reasonNoSnapshot
				entries = append(entries, e)
				continue
			}
			switch len(where) {
			case 1:
				e.State = stateNotReported
				e.HeadDecl = &declSite{File: where[0].File, Line: where[0].Line}
				e.Caveats = where[0].Caveats
			case 0:
				e.State, e.Reason = stateUnmatched, reasonAbsentInHead
			default:
				e.State, e.Reason = stateUnmatched, reasonManyInHead
			}
			entries = append(entries, e)
		default:
			hs := h[0].site
			e := deltaEntry{Head: &hs}
			h[0].fill(&e)
			where := baseSnap.byKey[h[0].decl.key()]
			switch {
			case !baseSnap.indexed:
				e.State, e.Reason = stateUnmatched, reasonNoSnapshot
				entries = append(entries, e)
				continue
			}
			switch len(where) {
			case 1:
				e.State = stateNew
				e.BaseDecl = &declSite{File: where[0].File, Line: where[0].Line}
				e.Caveats = where[0].Caveats
			case 0:
				e.State, e.Reason = stateUnmatched, reasonAbsentInBase
			default:
				e.State, e.Reason = stateUnmatched, reasonManyInBase
			}
			entries = append(entries, e)
		}
	}
	for i := range entries {
		slices.Sort(entries[i].Caveats)
	}
	sort.Slice(entries, func(i, j int) bool { return entrySortKey(entries[i]) < entrySortKey(entries[j]) })
	return entries
}

func siteKey(s *site) string {
	if s == nil {
		return "-"
	}
	return fmt.Sprintf("%s\x00%09d\x00%09d\x00%s", s.File, s.Line, s.Column, s.Message)
}

// entrySortKey is a total order over every field that distinguishes entries,
// so the output does not depend on the order of the inputs.
func entrySortKey(e deltaEntry) string {
	return strings.Join([]string{e.PkgDir, e.PkgName, e.Receiver, e.Function, e.Linter, e.Metric, e.State, e.Reason,
		siteKey(e.Base), siteKey(e.Head)}, "\x01")
}

func evidence(ref string, raw []byte, rep *report, s *snapshot) evidenceRef {
	sum := sha256.Sum256(raw)
	return evidenceRef{Ref: ref, RawSHA256: hex.EncodeToString(sum[:]), Diagnostics: rep.Total, Locations: len(rep.Locations),
		SnapshotIndexed: s.indexed, UnparsableFiles: len(s.unparsable)}
}

type pairInput struct {
	BaseRaw, HeadRaw []byte
	BaseRef, HeadRef string
	BaseDir, HeadDir string
}

func makeDelta(in pairInput) (*delta, error) {
	for name, ref := range map[string]string{"-base-ref": in.BaseRef, "-head-ref": in.HeadRef} {
		if ref == "" || strings.ContainsAny(ref, " \t\r\n") {
			return nil, fmt.Errorf("%s must identify the snapshot with a non-empty value without whitespace", name)
		}
	}
	base, err := loadRaw(in.BaseRaw)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	head, err := loadRaw(in.HeadRaw)
	if err != nil {
		return nil, fmt.Errorf("head: %w", err)
	}
	if bad := provenanceMismatches(base, head); len(bad) > 0 {
		return nil, fmt.Errorf("base and head collections were not made under the same provenance, so they are not paired; mismatching field(s): %s", strings.Join(bad, "; "))
	}
	baseSnap, err := indexSnapshot(in.BaseDir)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	headSnap, err := indexSnapshot(in.HeadDir)
	if err != nil {
		return nil, fmt.Errorf("head: %w", err)
	}
	d := &delta{Schema: deltaSchema,
		Base: evidence(in.BaseRef, in.BaseRaw, base, baseSnap), Head: evidence(in.HeadRef, in.HeadRaw, head, headSnap),
		Tool: base.Tool, Config: base.Config, Patterns: base.Patterns, EnabledLinters: base.EnabledLinters, RawScope: base.Scope,
		Counts: map[string]int{}, Scope: deltaScope}
	d.Entries = pairing(baseSnap, headSnap, base, head)
	for _, s := range deltaStates {
		d.Counts[s] = 0
	}
	for _, e := range d.Entries {
		d.Counts[e.State]++
	}
	return d, nil
}

func runPair(args []string, out io.Writer) error {
	fl := flag.NewFlagSet("pair", flag.ContinueOnError)
	basePath := fl.String("base", "", "raw collection (report JSON) of the base snapshot")
	headPath := fl.String("head", "", "raw collection (report JSON) of the head snapshot")
	baseRef := fl.String("base-ref", "", "identifier of the base snapshot, for example a commit")
	headRef := fl.String("head-ref", "", "identifier of the head snapshot, for example a commit")
	baseDir := fl.String("base-dir", "", "module root of the base snapshot; "+snapshotCaveatLimit)
	headDir := fl.String("head-dir", "", "module root of the head snapshot; "+snapshotCaveatLimit)
	format := fl.String("format", "json", "json or tsv")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *format != "json" && *format != "tsv" {
		return fmt.Errorf("unknown format %q", *format)
	}
	if *basePath == "" || *headPath == "" || fl.NArg() != 0 {
		return errors.New(deltaUsage)
	}
	in := pairInput{BaseRef: *baseRef, HeadRef: *headRef, BaseDir: *baseDir, HeadDir: *headDir}
	var err error
	if in.BaseRaw, err = os.ReadFile(*basePath); err != nil {
		return fmt.Errorf("reading base collection: %w", err)
	}
	if in.HeadRaw, err = os.ReadFile(*headPath); err != nil {
		return fmt.Errorf("reading head collection: %w", err)
	}
	d, err := makeDelta(in)
	if err != nil {
		return err
	}
	if *format == "tsv" {
		return writeDeltaTSV(out, d)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(d)
}

func writeDeltaTSV(out io.Writer, d *delta) error {
	var b strings.Builder
	clean := strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace
	fmt.Fprintf(&b, "# schema: %s\n", d.Schema)
	for _, ev := range []struct {
		name string
		e    evidenceRef
	}{{"base", d.Base}, {"head", d.Head}} {
		fmt.Fprintf(&b, "# %s.ref: %s\n# %s.raw_sha256: %s\n# %s.diagnostics_total: %d\n# %s.locations: %d\n# %s.snapshot_source_indexed: %t\n# %s.snapshot_unparsable_files: %d\n",
			ev.name, ev.e.Ref, ev.name, ev.e.RawSHA256, ev.name, ev.e.Diagnostics, ev.name, ev.e.Locations, ev.name, ev.e.SnapshotIndexed, ev.name, ev.e.UnparsableFiles)
	}
	fmt.Fprintf(&b, "# golangci_lint_version: %s\n# ci_pinned_version: %s\n# matches_ci_pin: %t\n# config_sha256: %s\n# patterns: %s\n# enabled_linters: %s\n",
		d.Tool.Version, d.Tool.CIPinned, d.Tool.MatchesCIPin, d.Config.SHA256, strings.Join(d.Patterns, ","), strings.Join(d.EnabledLinters, ","))
	for _, s := range deltaStates {
		fmt.Fprintf(&b, "# entries.%s: %d\n", s, d.Counts[s])
	}
	for _, s := range d.RawScope {
		fmt.Fprintf(&b, "# raw_scope: %s\n", s)
	}
	for _, s := range d.Scope {
		fmt.Fprintf(&b, "# scope: %s\n", s)
	}
	b.WriteString("state\treason\tlinter\tpackage_dir\tpackage_name\treceiver\tfunction\tmetric\tbase_file\tbase_line\tbase_column\tbase_test_file\tbase_message\thead_file\thead_line\thead_column\thead_test_file\thead_message\tbase_declaration\thead_declaration\tcaveats\n")
	cells := func(s *site) string {
		if s == nil {
			return "\t\t\t\t"
		}
		return fmt.Sprintf("%s\t%d\t%d\t%t\t%s", s.File, s.Line, s.Column, s.Test, clean(s.Message))
	}
	dcell := func(s *declSite) string {
		if s == nil {
			return ""
		}
		return fmt.Sprintf("%s:%d", s.File, s.Line)
	}
	for _, e := range d.Entries {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", e.State, e.Reason, e.Linter, e.PkgDir, e.PkgName, e.Receiver, e.Function,
			clean(e.Metric), cells(e.Base), cells(e.Head), dcell(e.BaseDecl), dcell(e.HeadDecl), strings.Join(e.Caveats, ","))
	}
	_, err := io.WriteString(out, b.String())
	return err
}
