// Command structmetrics reports a fixed structural measurement of named struct
// types: declared methods (split by exported-name and private) and direct
// fields per type, for chosen package directories of one source snapshot, and
// the per-type difference between a base and a head snapshot.
//
// It is a reference measurement for triage, not an acceptance threshold. It
// parses source lexically with the standard library, never type-checks, builds,
// imports or executes the code it inspects, and needs no Docker. The output is
// deterministic: it holds no timestamps, host details or local paths.
//
//	structmetrics measure -packages ceph,override [-ref REF | -dir DIR]
//	structmetrics delta   -packages ceph,override -base-ref REF [-head-ref REF | -head-dir DIR]
//
// See docs/contributing/structural-metrics.md for what is and is not measured.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const (
	measureSchema = "ceph-structure-metrics/v1"
	deltaSchema   = "ceph-structure-metrics-delta/v1"
	defaultMoves  = "docs/design/api-naming/moves"
)

// scope is printed in every report so that a reader sees what was and was not
// looked at without opening the documentation.
type scope struct {
	Measure    []string `json:"measure"`
	Exclusions []string `json:"exclusions"`
	Caveats    []string `json:"caveats"`
}

var reportScope = scope{
	Measure: []string{
		"owner: a top-level named struct type declared in a listed package directory",
		"exported_methods: methods declared on the owner whose name is exported (a private owner can have them)",
		"private_methods: methods declared on the owner whose name is not exported",
		"fields: direct struct members; an embedded member counts once, a multi-name member counts each name",
	},
	Exclusions: []string{
		"files ending in _test.go",
		"generated files (ast.IsGenerated)",
		"files whose name starts with . or _",
		"subdirectories of a listed package directory",
		"type aliases, non-struct named types and struct types declared inside functions",
		"promoted methods and promoted fields",
		"top-level functions, function complexity, length and duplication",
	},
	Caveats: []string{
		"lexical and untyped: every file is read regardless of build constraints and merged, so a type declared in several build profiles reports declarations above 1",
		"methods on a receiver that is not a measured struct owner are not counted anywhere",
		"a moved or renamed owner appears as one removed and one introduced row; counts alone do not show that behavior is equivalent",
		"a reference metric for triage; no threshold, target or gate is defined by it",
	},
}

type sourceInfo struct {
	Kind   string `json:"kind"`
	Ref    string `json:"ref,omitempty"`
	Commit string `json:"commit,omitempty"`
}

// counts are the measured values of one owner.
type counts struct {
	Declarations    int `json:"declarations"`
	ExportedMethods int `json:"exported_methods"`
	PrivateMethods  int `json:"private_methods"`
	Fields          int `json:"fields"`
}

type ownerRow struct {
	Owner     string   `json:"owner"`
	Package   string   `json:"package"`
	Type      string   `json:"type"`
	Locations []string `json:"locations"`
	counts
}

type packageRow struct {
	Package         string `json:"package"`
	Files           int    `json:"files"`
	Owners          int    `json:"owners"`
	ExportedMethods int    `json:"exported_methods"`
	PrivateMethods  int    `json:"private_methods"`
	Fields          int    `json:"fields"`
}

type snapshot struct {
	Source     sourceInfo
	Unmeasured []string // listed packages without production Go files
	Owners     []ownerRow
	Packages   []packageRow
}

// source reads one snapshot of a module tree.
type source interface {
	info() sourceInfo
	// list returns the regular file names directly inside dir, sorted. A
	// missing directory lists nothing.
	list(dir string) ([]string, error)
	read(file string) ([]byte, error)
}

type dirSource struct{ root string }

func (dirSource) info() sourceInfo { return sourceInfo{Kind: "dir"} }

func (d dirSource) list(dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(d.root, filepath.FromSlash(dir)))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

func (d dirSource) read(file string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.root, filepath.FromSlash(file)))
}

type gitSource struct {
	repo, ref, commit string
}

func newGitSource(repo, ref string) (*gitSource, error) {
	out, err := git(repo, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", ref, err)
	}
	return &gitSource{repo: repo, ref: ref, commit: strings.TrimSpace(string(out))}, nil
}

func git(repo string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %v: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func (g *gitSource) info() sourceInfo {
	return sourceInfo{Kind: "git", Ref: g.ref, Commit: g.commit}
}

func (g *gitSource) list(dir string) ([]string, error) {
	out, err := git(g.repo, "ls-tree", "-z", g.commit, dir+"/")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, record := range strings.Split(string(out), "\x00") {
		meta, name, ok := strings.Cut(record, "\t")
		if !ok {
			continue
		}
		if fields := strings.Fields(meta); len(fields) == 3 && fields[1] == "blob" && fields[0] != "120000" {
			names = append(names, path.Base(name))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (g *gitSource) read(file string) ([]byte, error) {
	return git(g.repo, "cat-file", "blob", g.commit+":"+file)
}

func productionFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") &&
		!strings.HasPrefix(name, ".") && !strings.HasPrefix(name, "_")
}

// receiverName normalizes a receiver type expression to its named type, the
// same rule as internal/apiinventory/catalogue: pointer, value and generic
// receivers all reduce to the type name. A parenthesized receiver is also
// reduced. Anything else yields "" and is not counted.
func receiverName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.StarExpr:
		return receiverName(expr.X)
	case *ast.ParenExpr:
		return receiverName(expr.X)
	case *ast.IndexExpr:
		return receiverName(expr.X)
	case *ast.IndexListExpr:
		return receiverName(expr.X)
	case *ast.Ident:
		return expr.Name
	}
	return ""
}

func fieldCount(fields *ast.FieldList) int {
	total := 0
	for _, field := range fields.List {
		total += max(1, len(field.Names))
	}
	return total
}

type owner struct {
	counts
	locations []string
}

// measurePackage counts the owners of one package directory.
func measurePackage(src source, pkg string) ([]ownerRow, packageRow, error) {
	names, err := src.list(pkg)
	if err != nil {
		return nil, packageRow{}, err
	}
	owners := map[string]*owner{}
	methods := map[string]*counts{}
	files := 0
	for _, name := range names {
		if !productionFile(name) {
			continue
		}
		rel := pkg + "/" + name
		data, err := src.read(rel)
		if err != nil {
			return nil, packageRow{}, fmt.Errorf("read %s: %w", rel, err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, rel, data, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, packageRow{}, fmt.Errorf("parse %s: %w", rel, err)
		}
		if ast.IsGenerated(file) {
			continue
		}
		files++
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				if decl.Tok != token.TYPE {
					continue
				}
				for _, spec := range decl.Specs {
					spec := spec.(*ast.TypeSpec)
					st, ok := spec.Type.(*ast.StructType)
					if !ok || spec.Assign.IsValid() {
						continue
					}
					o := owners[spec.Name.Name]
					if o == nil {
						o = &owner{}
						owners[spec.Name.Name] = o
					}
					o.Declarations++
					o.Fields += fieldCount(st.Fields)
					pos := fset.Position(spec.Name.Pos())
					o.locations = append(o.locations, fmt.Sprintf("%s:%d", rel, pos.Line))
				}
			case *ast.FuncDecl:
				if decl.Recv == nil || len(decl.Recv.List) != 1 {
					continue
				}
				recv := receiverName(decl.Recv.List[0].Type)
				if recv == "" {
					continue
				}
				m := methods[recv]
				if m == nil {
					m = &counts{}
					methods[recv] = m
				}
				if decl.Name.IsExported() {
					m.ExportedMethods++
				} else {
					m.PrivateMethods++
				}
			}
		}
	}
	var rows []ownerRow
	total := packageRow{Package: pkg, Files: files}
	for name, o := range owners {
		row := ownerRow{Owner: pkg + "." + name, Package: pkg, Type: name, Locations: o.locations, counts: o.counts}
		if m := methods[name]; m != nil {
			row.ExportedMethods, row.PrivateMethods = m.ExportedMethods, m.PrivateMethods
		}
		slices.Sort(row.Locations)
		rows = append(rows, row)
		total.Owners++
		total.ExportedMethods += row.ExportedMethods
		total.PrivateMethods += row.PrivateMethods
		total.Fields += row.Fields
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Owner < rows[j].Owner })
	return rows, total, nil
}

func measure(src source, pkgs []string) (*snapshot, error) {
	snap := &snapshot{Source: src.info(), Owners: []ownerRow{}, Packages: []packageRow{}, Unmeasured: []string{}}
	for _, pkg := range pkgs {
		rows, total, err := measurePackage(src, pkg)
		if err != nil {
			return nil, err
		}
		if total.Files == 0 {
			snap.Unmeasured = append(snap.Unmeasured, pkg)
			continue
		}
		snap.Owners = append(snap.Owners, rows...)
		snap.Packages = append(snap.Packages, total)
	}
	return snap, nil
}

// parsePackages validates a comma-separated list of module-relative package
// directories. The list is sorted and deduplicated so that its order cannot
// change the output.
func parsePackages(list string) ([]string, error) {
	var pkgs []string
	for _, pkg := range strings.Split(list, ",") {
		pkg = strings.TrimSpace(pkg)
		if pkg == "" || pkg == "." || !fs.ValidPath(pkg) || strings.HasPrefix(pkg, "/") {
			return nil, fmt.Errorf("package %q must be a module-relative directory such as ceph or rgw/multisite", pkg)
		}
		pkgs = append(pkgs, pkg)
	}
	slices.Sort(pkgs)
	return slices.Compact(pkgs), nil
}

type measureReport struct {
	Schema     string       `json:"schema"`
	Scope      scope        `json:"scope"`
	Source     sourceInfo   `json:"source"`
	Unmeasured []string     `json:"unmeasured_packages"`
	Packages   []packageRow `json:"packages"`
	Owners     []ownerRow   `json:"owners"`
}

// metrics are the three measured values of an owner side.
type side struct {
	counts
	Locations []string `json:"locations"`
}

type delta struct {
	Declarations    int `json:"declarations"`
	ExportedMethods int `json:"exported_methods"`
	PrivateMethods  int `json:"private_methods"`
	Fields          int `json:"fields"`
}

func subtract(head, base counts) delta {
	return delta{
		Declarations:    head.Declarations - base.Declarations,
		ExportedMethods: head.ExportedMethods - base.ExportedMethods,
		PrivateMethods:  head.PrivateMethods - base.PrivateMethods,
		Fields:          head.Fields - base.Fields,
	}
}

type moveRef struct {
	Change string `json:"change"`
	Role   string `json:"role"`
}

type ownerDelta struct {
	Owner   string    `json:"owner"`
	Package string    `json:"package"`
	Type    string    `json:"type"`
	Status  string    `json:"status"` // introduced, removed or changed
	Base    *side     `json:"base"`
	Head    *side     `json:"head"`
	Delta   delta     `json:"delta"`
	Moves   []moveRef `json:"move_records"`
}

type packageDelta struct {
	Package string     `json:"package"`
	Base    packageRow `json:"base"`
	Head    packageRow `json:"head"`
	Delta   delta      `json:"delta"`
}

type deltaReport struct {
	Schema     string         `json:"schema"`
	Scope      scope          `json:"scope"`
	Base       sourceInfo     `json:"base"`
	Head       sourceInfo     `json:"head"`
	MoveRecord string         `json:"move_records_read"`
	Unmeasured []unmeasured   `json:"unmeasured_packages"`
	Summary    string         `json:"summary"`
	Packages   []packageDelta `json:"changed_packages"`
	Owners     []ownerDelta   `json:"changed_owners"`
}

type unmeasured struct {
	Package string `json:"package"`
	Side    string `json:"side"` // base or head
}

func (r ownerRow) side() *side { return &side{counts: r.counts, Locations: r.Locations} }

func (p packageRow) counts() counts {
	return counts{Declarations: p.Owners, ExportedMethods: p.ExportedMethods, PrivateMethods: p.PrivateMethods, Fields: p.Fields}
}

// diff compares two snapshots. Every owner whose measured values differ is
// kept on both sides of the comparison: an owner present on one side only is
// reported as removed or introduced, never dropped. An owner whose values are
// equal is omitted, so a pure relocation inside a package yields no row.
func diff(base, head *snapshot, moves map[string][]moveRef) *deltaReport {
	report := &deltaReport{
		Schema: deltaSchema, Scope: reportScope, Base: base.Source, Head: head.Source,
		Unmeasured: []unmeasured{}, Packages: []packageDelta{}, Owners: []ownerDelta{},
	}
	for _, pkg := range base.Unmeasured {
		report.Unmeasured = append(report.Unmeasured, unmeasured{pkg, "base"})
	}
	for _, pkg := range head.Unmeasured {
		report.Unmeasured = append(report.Unmeasured, unmeasured{pkg, "head"})
	}
	baseOwners := map[string]ownerRow{}
	for _, row := range base.Owners {
		baseOwners[row.Owner] = row
	}
	headOwners := map[string]ownerRow{}
	for _, row := range head.Owners {
		headOwners[row.Owner] = row
	}
	names := map[string]bool{}
	for name := range baseOwners {
		names[name] = true
	}
	for name := range headOwners {
		names[name] = true
	}
	for name := range names {
		b, inBase := baseOwners[name]
		h, inHead := headOwners[name]
		row := ownerDelta{Owner: name, Moves: moves[name]}
		if row.Moves == nil {
			row.Moves = []moveRef{}
		}
		switch {
		case inBase && inHead:
			if b.counts == h.counts {
				continue
			}
			row.Package, row.Type, row.Status = h.Package, h.Type, "changed"
			row.Base, row.Head, row.Delta = b.side(), h.side(), subtract(h.counts, b.counts)
		case inHead:
			row.Package, row.Type, row.Status = h.Package, h.Type, "introduced"
			row.Head, row.Delta = h.side(), subtract(h.counts, counts{})
		default:
			row.Package, row.Type, row.Status = b.Package, b.Type, "removed"
			row.Base, row.Delta = b.side(), subtract(counts{}, b.counts)
		}
		report.Owners = append(report.Owners, row)
	}
	sort.Slice(report.Owners, func(i, j int) bool { return report.Owners[i].Owner < report.Owners[j].Owner })

	basePkgs := map[string]packageRow{}
	for _, row := range base.Packages {
		basePkgs[row.Package] = row
	}
	for _, h := range head.Packages {
		b, ok := basePkgs[h.Package]
		if !ok || b.counts() == h.counts() {
			continue
		}
		report.Packages = append(report.Packages, packageDelta{Package: h.Package, Base: b, Head: h, Delta: subtract(h.counts(), b.counts())})
	}
	report.Summary = "no measured structural changes"
	if len(report.Owners) > 0 {
		report.Summary = fmt.Sprintf("%d owners changed", len(report.Owners))
	}
	if len(report.Unmeasured) > 0 {
		report.Summary += "; some listed packages are unmeasured on one side"
	}
	return report
}

// moveRecords reads the per-change API move records of a snapshot, if the
// directory exists, and maps declaration names ("pkg.Type") to the records
// that name them. Annotations only explain a disappearance; they are not
// evidence that behavior is equivalent.
func moveRecords(src source, dir string) (map[string][]moveRef, string, error) {
	out := map[string][]moveRef{}
	if dir == "" {
		return out, "disabled", nil
	}
	names, err := src.list(dir)
	if err != nil {
		return nil, "", err
	}
	read := 0
	for _, name := range names {
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := src.read(dir + "/" + name)
		if err != nil {
			return nil, "", err
		}
		var record struct {
			Change string `json:"change"`
			Moves  []struct{ From, To string }
			Remove []string `json:"removed"`
			Add    []string `json:"added"`
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&record); err != nil {
			return nil, "", fmt.Errorf("move record %s: %w", name, err)
		}
		if record.Change == "" {
			return nil, "", fmt.Errorf("move record %s: missing change", name)
		}
		read++
		for _, m := range record.Moves {
			out[m.From] = append(out[m.From], moveRef{record.Change, "from"})
			out[m.To] = append(out[m.To], moveRef{record.Change, "to"})
		}
		for _, n := range record.Remove {
			out[n] = append(out[n], moveRef{record.Change, "removed"})
		}
		for _, n := range record.Add {
			out[n] = append(out[n], moveRef{record.Change, "added"})
		}
	}
	for _, refs := range out {
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].Change != refs[j].Change {
				return refs[i].Change < refs[j].Change
			}
			return refs[i].Role < refs[j].Role
		})
	}
	return out, fmt.Sprintf("%d", read), nil
}

func writeJSON(w io.Writer, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func writeMeasureTSV(out io.Writer, r *measureReport) error {
	var w bytes.Buffer
	fmt.Fprintln(&w, "owner\texported_methods\tprivate_methods\tfields\tdeclarations\tlocations")
	for _, o := range r.Owners {
		fmt.Fprintf(&w, "%s\t%d\t%d\t%d\t%d\t%s\n", o.Owner, o.ExportedMethods, o.PrivateMethods, o.Fields, o.Declarations, strings.Join(o.Locations, ","))
	}
	_, err := out.Write(w.Bytes())
	return err
}

func sideCells(s *side) string {
	if s == nil {
		return "-\t-\t-\t-\t-"
	}
	return fmt.Sprintf("%d\t%d\t%d\t%d\t%s", s.ExportedMethods, s.PrivateMethods, s.Fields, s.Declarations, strings.Join(s.Locations, ","))
}

func writeDeltaTSV(out io.Writer, r *deltaReport) error {
	var w bytes.Buffer
	fmt.Fprintln(&w, "status\towner\tbase_exported\tbase_private\tbase_fields\tbase_declarations\tbase_locations\t"+
		"head_exported\thead_private\thead_fields\thead_declarations\thead_locations\t"+
		"delta_exported\tdelta_private\tdelta_fields\tdelta_declarations\tmove_records")
	for _, o := range r.Owners {
		var moves []string
		for _, m := range o.Moves {
			moves = append(moves, m.Change+":"+m.Role)
		}
		fmt.Fprintf(&w, "%s\t%s\t%s\t%s\t%+d\t%+d\t%+d\t%+d\t%s\n", o.Status, o.Owner, sideCells(o.Base), sideCells(o.Head),
			o.Delta.ExportedMethods, o.Delta.PrivateMethods, o.Delta.Fields, o.Delta.Declarations, strings.Join(moves, ","))
	}
	for _, u := range r.Unmeasured {
		// Padded to the header width so every row has the same columns.
		fmt.Fprintf(&w, "unmeasured\t%s\t%s%s\n", u.Package, u.Side, strings.Repeat("\t", 14))
	}
	_, err := out.Write(w.Bytes())
	return err
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 || (args[0] != "measure" && args[0] != "delta") {
		return fmt.Errorf("usage: structmetrics measure|delta -packages a,b ... (see the command documentation)")
	}
	cmd := args[0]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	packages := fs.String("packages", "", "comma-separated module-relative package directories (required)")
	format := fs.String("format", "json", "json or tsv")
	repo := fs.String("repo", ".", "git repository used to resolve refs")
	ref := fs.String("ref", "", "measure: git ref to read (default: the working tree in -dir)")
	dir := fs.String("dir", ".", "measure: module root to read when -ref is not given")
	baseRef := fs.String("base-ref", "", "delta: base git ref")
	baseDir := fs.String("base-dir", "", "delta: base module root")
	headRef := fs.String("head-ref", "", "delta: head git ref")
	headDir := fs.String("head-dir", "", "delta: head module root (default \".\" when -head-ref is not given)")
	moves := fs.String("moves-dir", defaultMoves, "delta: move-record directory read from the head snapshot (empty disables)")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *format != "json" && *format != "tsv" {
		return fmt.Errorf("unknown format %q", *format)
	}
	pkgs, err := parsePackages(*packages)
	if err != nil {
		return err
	}
	open := func(ref, dir string) (source, error) {
		if ref != "" {
			return newGitSource(*repo, ref)
		}
		return dirSource{dir}, nil
	}

	if cmd == "measure" {
		src, err := open(*ref, *dir)
		if err != nil {
			return err
		}
		snap, err := measure(src, pkgs)
		if err != nil {
			return err
		}
		if len(snap.Packages) == 0 {
			return fmt.Errorf("no production Go files in any of %v", pkgs)
		}
		report := &measureReport{Schema: measureSchema, Scope: reportScope, Source: snap.Source,
			Unmeasured: snap.Unmeasured, Packages: snap.Packages, Owners: snap.Owners}
		if *format == "tsv" {
			return writeMeasureTSV(stdout, report)
		}
		return writeJSON(stdout, report)
	}

	if (*baseRef == "") == (*baseDir == "") {
		return fmt.Errorf("give exactly one of -base-ref and -base-dir")
	}
	if *headRef != "" && *headDir != "" {
		return fmt.Errorf("give at most one of -head-ref and -head-dir")
	}
	if *headRef == "" && *headDir == "" {
		*headDir = "."
	}
	baseSrc, err := open(*baseRef, *baseDir)
	if err != nil {
		return err
	}
	headSrc, err := open(*headRef, *headDir)
	if err != nil {
		return err
	}
	base, err := measure(baseSrc, pkgs)
	if err != nil {
		return fmt.Errorf("base: %w", err)
	}
	head, err := measure(headSrc, pkgs)
	if err != nil {
		return fmt.Errorf("head: %w", err)
	}
	for _, pkg := range pkgs {
		if slices.Contains(base.Unmeasured, pkg) && slices.Contains(head.Unmeasured, pkg) {
			return fmt.Errorf("package %q has no production Go files in base or head", pkg)
		}
	}
	records, read, err := moveRecords(headSrc, *moves)
	if err != nil {
		return err
	}
	report := diff(base, head, records)
	report.MoveRecord = read
	if *format == "tsv" {
		return writeDeltaTSV(stdout, report)
	}
	return writeJSON(stdout, report)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "structmetrics:", err)
		os.Exit(1)
	}
}
