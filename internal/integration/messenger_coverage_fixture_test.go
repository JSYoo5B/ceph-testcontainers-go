//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The Messenger coverage manifest maps each environment requirement of the
// ceph-msgr-go SPEC to the native recipes in this package that prove it, and
// to the Ceph declarations (OSD operations, message types, feature bits) it
// concerns. The validator below runs without Docker: it checks the manifest
// against the test sources and the extracted reference inventory, so a
// renamed or removed test or subtest, a receipt marker that the cited test
// no longer emits or an unknown symbol fails here instead of leaving a stale
// claim. It does not run the recipes; whether they passed is the business of
// the native CI runs.
const (
	coverageManifestPath  = "testdata/messenger_coverage.json"
	coverageReferencePath = "testdata/messenger_reference.json"
	coverageGuidePath     = "../../docs/guides/messenger-client-fixtures.md"
)

type coverageManifest struct {
	Spec       coverageSpec  `json:"spec"`
	CephCommit string        `json:"ceph_commit"`
	Rows       []coverageRow `json:"rows"`
}

type coverageSpec struct {
	Repository string `json:"repository"`
	Commit     string `json:"commit"`
}

// coverageRow is one requirement. Status is the availability of a native
// recipe on this branch: covered when its cases assert every condition the
// requirement names, partial when they assert some (gap says which are
// missing), uncovered when no recipe asserts it. Evidence is the strongest
// kind the cases give: native-state when daemons or native clients reported
// the condition itself, outcome when only a result was checked.
type coverageRow struct {
	ID          string         `json:"id"`
	Area        string         `json:"area"`
	Requirement string         `json:"requirement"`
	Symbols     []string       `json:"symbols"`
	Status      string         `json:"status"`
	Evidence    string         `json:"evidence"`
	Cases       []coverageCase `json:"cases"`
	Gap         string         `json:"gap"`
	Owner       string         `json:"owner"`
}

// coverageCase names one test (TestName or TestName/subtest) and the
// assertion it makes for the row, with the log marker of its receipt when it
// prints one.
type coverageCase struct {
	Test    string `json:"test"`
	Receipt string `json:"receipt"`
	Asserts string `json:"asserts"`
}

type coverageReference struct {
	CephCommit string           `json:"ceph_commit"`
	Counts     map[string]int   `json:"counts"`
	Entries    []referenceEntry `json:"entries"`
}

type referenceEntry struct {
	Kind   string `json:"kind"`
	Symbol string `json:"symbol"`
}

// coverageSources is what the validator knows about the package's test
// sources: what each top-level test function runs and reaches.
type coverageSources struct {
	tests map[string]*coverageTest
}

// coverageTest is one top-level test: the subtest paths it runs, relative
// to it (such as "host" or "host/v2"), and the string literals of the code
// it reaches, where its receipt markers are.
type coverageTest struct {
	subtests map[string]bool
	literals map[string]bool
}

var (
	coverageAreas    = []string{"messenger", "client-mon-mgr", "client-osd", "client-mds", "rbd", "targeted-command", "daemon-path", "service-path"}
	coverageEvidence = []string{"native-state", "outcome", "none"}
	coverageIDs      = regexp.MustCompile(`^[a-z0-9]+(?:[-./][a-z0-9]+)*$`)
	coverageOwners   = regexp.MustCompile(`^(?:#[1-9][0-9]*|JSYoo5B/ceph-testcontainers-images#[1-9][0-9]*)$`)
	coverageMarkers  = regexp.MustCompile(`^[A-Z][A-Z0-9_]*[A-Z0-9]$`)
	coverageTestName = regexp.MustCompile(`^Test[A-Z][A-Za-z0-9]*(?:/[^/\s]+)*$`)
	hexCommit        = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// readCoverageJSON decodes path into value. The manifest is strict, so a
// misspelt field fails; the generated reference carries more fields per
// entry than the validator reads.
func readCoverageJSON(t *testing.T, path string, value any, strict bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(value); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// scanCoverageSources parses every _test.go file of the package directory,
// whatever its build tags, and analyses its test functions.
func scanCoverageSources(t *testing.T, dir string) coverageSources {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("test sources in %s: %v %v", dir, paths, err)
	}
	files := map[string]string{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = string(data)
	}
	sources, err := parseCoverageSources(files)
	if err != nil {
		t.Fatal(err)
	}
	return sources
}

// parseCoverageSources analyses the given files of one package, keyed by
// name. For each top-level test function it records the subtest paths the
// test runs and the string literals of the code it reaches.
//
// The analysis is static and best effort. A test reaches the functions and
// methods of the package it calls or passes as values, transitively. A
// subtest is a Run call on a value that is no imported package, with a
// function literal or a function as its second argument; the call adds its
// names, and the paths its function runs below them, to the scope it appears
// in, whether that is the test, a helper the test calls or another subtest.
// A name is followed when it is a string literal, or a variable or field
// whose values are string literals in the same file: a table in a composite
// literal ranged over directly or through a variable, keyed fields of its
// elements, and the arguments of the networkModes constructor. A Run whose
// name is not followed adds no path, so a citation below it fails instead of
// matching a literal that some other test uses.
func parseCoverageSources(files map[string]string) (coverageSources, error) {
	a := &coverageAnalyzer{
		funcs:    map[string][]*ast.FuncDecl{},
		methods:  map[string][]*ast.FuncDecl{},
		globals:  map[string][]*ast.ValueSpec{},
		subtests: map[*ast.FuncDecl]map[string]bool{},
		visiting: map[*ast.FuncDecl]bool{},
	}
	fset := token.NewFileSet()
	var tests []*ast.FuncDecl
	for _, name := range slices.Sorted(maps.Keys(files)) {
		// Object resolution binds each local identifier to its declaration
		// within the file, which is what following a subtest name needs.
		file, err := parser.ParseFile(fset, name, files[name], 0)
		if err != nil {
			return coverageSources{}, err
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				if decl.Recv != nil {
					a.methods[decl.Name.Name] = append(a.methods[decl.Name.Name], decl)
					continue
				}
				a.funcs[decl.Name.Name] = append(a.funcs[decl.Name.Name], decl)
				if strings.HasPrefix(decl.Name.Name, "Test") {
					tests = append(tests, decl)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if spec, ok := spec.(*ast.ValueSpec); ok {
						for _, ident := range spec.Names {
							a.globals[ident.Name] = append(a.globals[ident.Name], spec)
						}
					}
				}
			}
		}
	}
	sources := coverageSources{tests: map[string]*coverageTest{}}
	for _, decl := range tests {
		sources.tests[decl.Name.Name] = &coverageTest{subtests: a.funcSubtests(decl), literals: a.literals(decl)}
	}
	return sources, nil
}

// coverageAnalyzer holds the package-level declarations of the analysed
// sources.
type coverageAnalyzer struct {
	funcs    map[string][]*ast.FuncDecl
	methods  map[string][]*ast.FuncDecl
	globals  map[string][]*ast.ValueSpec
	subtests map[*ast.FuncDecl]map[string]bool
	visiting map[*ast.FuncDecl]bool
}

// subtestNameConstructors are package helpers whose string arguments name
// the values they return, so ranging over a call to one names the subtests
// by its literal arguments.
var subtestNameConstructors = map[string]bool{"networkModes": true}

// isImport reports whether ident is an unresolved name that no package-level
// declaration owns, which in these sources is an imported package.
func (a *coverageAnalyzer) isImport(ident *ast.Ident) bool {
	return ident.Obj == nil && a.funcs[ident.Name] == nil && a.globals[ident.Name] == nil
}

// referenced returns the package-level functions and values that node
// names. A selector names every method of that name, whatever its receiver.
func (a *coverageAnalyzer) referenced(node ast.Node) ([]*ast.FuncDecl, []*ast.ValueSpec) {
	switch n := node.(type) {
	case *ast.Ident:
		if n.Obj == nil {
			return a.funcs[n.Name], a.globals[n.Name]
		}
		switch decl := n.Obj.Decl.(type) {
		case *ast.FuncDecl:
			if decl.Recv == nil {
				return []*ast.FuncDecl{decl}, nil
			}
		case *ast.ValueSpec:
			if slices.Contains(a.globals[n.Name], decl) {
				return nil, []*ast.ValueSpec{decl}
			}
		}
	case *ast.SelectorExpr:
		if x, ok := n.X.(*ast.Ident); ok && a.isImport(x) {
			return nil, nil
		}
		return a.methods[n.Sel.Name], nil
	}
	return nil, nil
}

// walk visits node like ast.Inspect but skips the identifiers that only
// name a field or a selected member, so they are not taken for references to
// package-level declarations of the same name.
func walk(node ast.Node, visit func(ast.Node) bool) {
	ast.Inspect(node, func(n ast.Node) bool {
		if n == nil || !visit(n) {
			return false
		}
		switch n := n.(type) {
		case *ast.SelectorExpr:
			walk(n.X, visit)
			return false
		case *ast.KeyValueExpr:
			if _, ok := n.Key.(*ast.Ident); !ok {
				walk(n.Key, visit)
			}
			walk(n.Value, visit)
			return false
		}
		return true
	})
}

// literals returns the string literals of decl and of every function,
// method and package-level value it reaches.
func (a *coverageAnalyzer) literals(decl *ast.FuncDecl) map[string]bool {
	literals := map[string]bool{}
	seen := map[ast.Node]bool{}
	var visit func(ast.Node)
	visit = func(root ast.Node) {
		if seen[root] {
			return
		}
		seen[root] = true
		walk(root, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if value, err := strconv.Unquote(lit.Value); err == nil {
					literals[value] = true
				}
			}
			funcs, specs := a.referenced(n)
			for _, fn := range funcs {
				visit(fn)
			}
			for _, spec := range specs {
				visit(spec)
			}
			return true
		})
	}
	visit(decl)
	return literals
}

// funcSubtests returns the subtest paths that decl runs, relative to the
// scope it is called in.
func (a *coverageAnalyzer) funcSubtests(decl *ast.FuncDecl) map[string]bool {
	if paths, ok := a.subtests[decl]; ok {
		return paths
	}
	if a.visiting[decl] {
		return nil
	}
	a.visiting[decl] = true
	paths := a.nodeSubtests(decl.Body)
	delete(a.visiting, decl)
	a.subtests[decl] = paths
	return paths
}

// nodeSubtests returns the subtest paths that the code of node runs.
func (a *coverageAnalyzer) nodeSubtests(node ast.Node) map[string]bool {
	paths := map[string]bool{}
	walk(node, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if names, bodies, ok := a.subtestCall(call); ok {
				inner := map[string]bool{}
				for _, body := range bodies {
					switch body := body.(type) {
					case *ast.FuncDecl:
						maps.Copy(inner, a.funcSubtests(body))
					default:
						maps.Copy(inner, a.nodeSubtests(body))
					}
				}
				for _, name := range names {
					paths[name] = true
					for path := range inner {
						paths[name+"/"+path] = true
					}
				}
				return false
			}
		}
		funcs, _ := a.referenced(n)
		for _, fn := range funcs {
			maps.Copy(paths, a.funcSubtests(fn))
		}
		return true
	})
	return paths
}

// subtestCall reports whether call runs a subtest, and returns the names it
// can be followed to, as go test spells them, and the functions it runs.
func (a *coverageAnalyzer) subtestCall(call *ast.CallExpr) ([]string, []ast.Node, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Run" || len(call.Args) != 2 {
		return nil, nil, false
	}
	if x, ok := sel.X.(*ast.Ident); ok && a.isImport(x) {
		return nil, nil, false
	}
	var bodies []ast.Node
	if ident, ok := call.Args[1].(*ast.Ident); ok && (ident.Obj == nil || ident.Obj.Kind == ast.Fun) {
		for _, fn := range a.funcs[ident.Name] {
			bodies = append(bodies, fn)
		}
	} else if values, ok := a.values(call.Args[1], 0); ok {
		for _, value := range values {
			if lit, ok := value.(*ast.FuncLit); ok {
				bodies = append(bodies, lit.Body)
			}
		}
	}
	if len(bodies) == 0 {
		return nil, nil, false
	}
	values, ok := a.values(call.Args[0], 0)
	if !ok {
		return nil, bodies, true
	}
	var names []string
	for _, value := range values {
		lit, ok := value.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return nil, bodies, true
		}
		name, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, bodies, true
		}
		names = append(names, strings.ReplaceAll(name, " ", "_"))
	}
	return names, bodies, true
}

// values returns the expressions that expr can be followed to: literals as
// they are, local variables to what they are assigned or ranged over, and
// fields to the keyed values of the composite literals that own them.
func (a *coverageAnalyzer) values(expr ast.Expr, depth int) ([]ast.Expr, bool) {
	if depth > 8 {
		return nil, false
	}
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return a.values(e.X, depth+1)
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return a.values(e.X, depth+1)
		}
	case *ast.BasicLit, *ast.CompositeLit, *ast.FuncLit:
		return []ast.Expr{e}, true
	case *ast.Ident:
		if e.Obj == nil || e.Obj.Kind != ast.Var {
			return nil, false
		}
		switch decl := e.Obj.Decl.(type) {
		case *ast.AssignStmt:
			index := slices.IndexFunc(decl.Lhs, func(lhs ast.Expr) bool {
				ident, ok := lhs.(*ast.Ident)
				return ok && ident.Obj == e.Obj
			})
			if index < 0 {
				return nil, false
			}
			if r, ok := decl.Rhs[0].(*ast.UnaryExpr); ok && len(decl.Rhs) == 1 && r.Op == token.RANGE {
				return a.rangeValues(r.X, index == 0, depth+1)
			}
			if len(decl.Rhs) == len(decl.Lhs) {
				return a.values(decl.Rhs[index], depth+1)
			}
		case *ast.ValueSpec:
			index := slices.IndexFunc(decl.Names, func(name *ast.Ident) bool { return name.Obj == e.Obj })
			if index >= 0 && len(decl.Values) == len(decl.Names) {
				return a.values(decl.Values[index], depth+1)
			}
		}
	case *ast.SelectorExpr:
		owners, ok := a.values(e.X, depth+1)
		if !ok {
			return nil, false
		}
		var result []ast.Expr
		for _, owner := range owners {
			switch owner := owner.(type) {
			case *ast.BasicLit:
				// An element of a subtestNameConstructors call: every
				// field of it is named by its literal argument.
				result = append(result, owner)
			case *ast.CompositeLit:
				index := slices.IndexFunc(owner.Elts, func(elt ast.Expr) bool {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						return false
					}
					key, ok := kv.Key.(*ast.Ident)
					return ok && key.Name == e.Sel.Name
				})
				if index < 0 {
					return nil, false
				}
				values, ok := a.values(owner.Elts[index].(*ast.KeyValueExpr).Value, depth+1)
				if !ok {
					return nil, false
				}
				result = append(result, values...)
			default:
				return nil, false
			}
		}
		return result, true
	}
	return nil, false
}

// rangeValues returns the keys or the elements that ranging over x yields.
func (a *coverageAnalyzer) rangeValues(x ast.Expr, keys bool, depth int) ([]ast.Expr, bool) {
	if call, ok := x.(*ast.CallExpr); ok {
		ident, ok := call.Fun.(*ast.Ident)
		if !ok || keys || !subtestNameConstructors[ident.Name] {
			return nil, false
		}
		var result []ast.Expr
		for _, arg := range call.Args {
			if lit, ok := arg.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				result = append(result, lit)
			}
		}
		return result, len(result) > 0
	}
	collections, ok := a.values(x, depth+1)
	if !ok {
		return nil, false
	}
	var result []ast.Expr
	for _, collection := range collections {
		lit, ok := collection.(*ast.CompositeLit)
		if !ok {
			return nil, false
		}
		_, isMap := lit.Type.(*ast.MapType)
		if keys && !isMap {
			return nil, false
		}
		for _, elt := range lit.Elts {
			kv, isKV := elt.(*ast.KeyValueExpr)
			switch {
			case keys && !isKV:
				return nil, false
			case keys:
				result = append(result, kv.Key)
			case isKV:
				result = append(result, kv.Value)
			default:
				result = append(result, elt)
			}
		}
	}
	return result, true
}

// hasMarker reports whether some reachable string literal contains marker
// as a whole word, such as "HELD_REPLICATION_RECEIPT %s".
func (c *coverageTest) hasMarker(marker string) bool {
	word := func(b byte) bool { return b == '_' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' }
	for literal := range c.literals {
		for offset := 0; ; {
			index := strings.Index(literal[offset:], marker)
			if index < 0 {
				break
			}
			start, end := offset+index, offset+index+len(marker)
			if (start == 0 || !word(literal[start-1])) && (end == len(literal) || !word(literal[end])) {
				return true
			}
			offset = start + 1
		}
	}
	return false
}

// validateCoverage returns one problem per violated rule.
func validateCoverage(manifest coverageManifest, reference coverageReference, sources coverageSources) []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if manifest.Spec.Repository == "" || !hexCommit.MatchString(manifest.Spec.Commit) {
		add("spec must name the repository and a full commit: %+v", manifest.Spec)
	}
	if !hexCommit.MatchString(manifest.CephCommit) || manifest.CephCommit != reference.CephCommit {
		add("ceph_commit %q is not the reference inventory's %q", manifest.CephCommit, reference.CephCommit)
	}
	symbols := map[string]bool{}
	for _, entry := range reference.Entries {
		symbols[entry.Symbol] = true
	}
	ids := map[string]bool{}
	for _, row := range manifest.Rows {
		where := "row " + strconv.Quote(row.ID)
		if !coverageIDs.MatchString(row.ID) {
			add("%s: malformed id", where)
		}
		if ids[row.ID] {
			add("%s: duplicate id", where)
		}
		ids[row.ID] = true
		if !slices.Contains(coverageAreas, row.Area) {
			add("%s: unknown area %q", where, row.Area)
		}
		if strings.TrimSpace(row.Requirement) == "" {
			add("%s: no requirement", where)
		}
		seen := map[string]bool{}
		for _, symbol := range row.Symbols {
			if !symbols[symbol] {
				add("%s: symbol %s is not in the reference inventory of %s", where, symbol, reference.CephCommit)
			}
			if seen[symbol] {
				add("%s: symbol %s listed twice", where, symbol)
			}
			seen[symbol] = true
		}
		switch row.Status {
		case "covered":
			if len(row.Cases) == 0 {
				add("%s: covered without a case", where)
			}
			if row.Evidence == "none" {
				add("%s: covered with evidence none", where)
			}
		case "partial":
			if len(row.Cases) == 0 {
				add("%s: partial without a case", where)
			}
			if row.Evidence == "none" {
				add("%s: partial with evidence none", where)
			}
		case "uncovered":
			if len(row.Cases) != 0 {
				add("%s: uncovered but lists cases; use partial", where)
			}
			if row.Evidence != "none" {
				add("%s: uncovered with evidence %q", where, row.Evidence)
			}
		default:
			add("%s: unknown status %q", where, row.Status)
		}
		if !slices.Contains(coverageEvidence, row.Evidence) {
			add("%s: unknown evidence %q", where, row.Evidence)
		}
		if row.Status != "covered" && strings.TrimSpace(row.Gap) == "" {
			add("%s: %s without a gap", where, row.Status)
		}
		// An owner is optional: a row without one is better than a row
		// that names an issue which does not track it.
		if row.Owner != "" && !coverageOwners.MatchString(row.Owner) {
			add("%s: malformed owner %q", where, row.Owner)
		}
		for _, c := range row.Cases {
			if !coverageTestName.MatchString(c.Test) {
				add("%s: malformed test %q", where, c.Test)
				continue
			}
			if strings.TrimSpace(c.Asserts) == "" {
				add("%s: case %s does not say what it asserts", where, c.Test)
			}
			if c.Receipt != "" && !coverageMarkers.MatchString(c.Receipt) {
				add("%s: malformed receipt marker %q", where, c.Receipt)
			}
			parent, path, _ := strings.Cut(c.Test, "/")
			test := sources.tests[parent]
			if test == nil {
				add("%s: test %s does not exist", where, parent)
				continue
			}
			if path != "" && !test.subtests[path] {
				add("%s: subtest %q is not one that %s runs as far as its sources can be followed; cite %s", where, path, parent, parent)
			}
			if c.Receipt != "" && coverageMarkers.MatchString(c.Receipt) && !test.hasMarker(c.Receipt) {
				add("%s: receipt marker %s is not emitted by code that %s reaches", where, c.Receipt, parent)
			}
		}
	}
	return problems
}

// guideDaemonPathTests returns the tests that the guide's daemon-to-daemon
// path table names as proof.
func guideDaemonPathTests(guide string) ([]string, error) {
	start := strings.Index(guide, "\n## Daemon-to-daemon paths\n")
	if start < 0 {
		return nil, fmt.Errorf("the guide has no daemon-to-daemon path section")
	}
	section := guide[start+1:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	names := map[string]bool{}
	rows := 0
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "| Path ") || strings.HasPrefix(line, "| ---") {
			continue
		}
		cells := strings.Split(strings.TrimSuffix(line, "|"), "|")
		rows++
		for _, name := range regexp.MustCompile("`(Test[A-Za-z0-9]+)").FindAllStringSubmatch(cells[len(cells)-1], -1) {
			names[name[1]] = true
		}
	}
	if rows == 0 || len(names) == 0 {
		return nil, fmt.Errorf("the daemon-to-daemon path table names no tests")
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func TestMessengerCoverageManifest(t *testing.T) {
	var manifest coverageManifest
	var reference coverageReference
	readCoverageJSON(t, coverageManifestPath, &manifest, true)
	readCoverageJSON(t, coverageReferencePath, &reference, false)
	sources := scanCoverageSources(t, ".")
	for _, problem := range validateCoverage(manifest, reference, sources) {
		t.Error(problem)
	}

	// Every test the guide's daemon-path table cites is mapped, so the
	// guide and the manifest cannot drift apart silently.
	guide, err := os.ReadFile(coverageGuidePath)
	if err != nil {
		t.Fatal(err)
	}
	cited, err := guideDaemonPathTests(string(guide))
	if err != nil {
		t.Fatal(err)
	}
	mapped := map[string]bool{}
	for _, row := range manifest.Rows {
		if row.Area != "daemon-path" && row.Area != "service-path" {
			continue
		}
		for _, c := range row.Cases {
			mapped[strings.SplitN(c.Test, "/", 2)[0]] = true
		}
	}
	for _, name := range cited {
		if !mapped[name] {
			t.Errorf("the guide's daemon-to-daemon paths cite %s, which no daemon-path or service-path row maps", name)
		}
	}

	// Report what is not covered, so a reader of the test log sees the gaps
	// rather than a percentage.
	byStatus := map[string][]string{}
	used := map[string]bool{}
	for _, row := range manifest.Rows {
		byStatus[row.Status] = append(byStatus[row.Status], row.ID+" ("+row.Owner+")")
		for _, symbol := range row.Symbols {
			used[symbol] = true
		}
	}
	t.Logf("MESSENGER_COVERAGE rows=%d covered=%d partial=%d uncovered=%d", len(manifest.Rows),
		len(byStatus["covered"]), len(byStatus["partial"]), len(byStatus["uncovered"]))
	for _, status := range []string{"partial", "uncovered"} {
		for _, row := range byStatus[status] {
			t.Logf("MESSENGER_COVERAGE %s %s", status, row)
		}
	}
	unmapped := map[string][]string{}
	for _, entry := range reference.Entries {
		if !used[entry.Symbol] {
			unmapped[entry.Kind] = append(unmapped[entry.Kind], entry.Symbol)
		}
	}
	for _, kind := range slices.Sorted(func(yield func(string) bool) {
		for kind := range reference.Counts {
			if !yield(kind) {
				return
			}
		}
	}) {
		t.Logf("MESSENGER_COVERAGE unmapped %s %d of %d: %s", kind, len(unmapped[kind]), reference.Counts[kind],
			strings.Join(unmapped[kind], " "))
	}
}

// TestMessengerCoverageValidator proves that each rule rejects what it is
// for, on a manifest that is valid apart from the one change.
func TestMessengerCoverageValidator(t *testing.T) {
	const commit = "7f793731f1b39eb4f465e960113d2363c311b964"
	reference := coverageReference{CephCommit: commit, Entries: []referenceEntry{
		{Kind: "osd_op", Symbol: "CEPH_OSD_OP_WRITE"}, {Kind: "message", Symbol: "CEPH_MSG_OSD_OP"},
	}}
	sources, err := parseCoverageSources(map[string]string{"writes_test.go": coverageValidatorSource})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]string{
		"TestWrites":     {"bridge", "host", "nested", "nested/crc", "nested/v1", "nested/v2"},
		"TestNoSubtests": nil,
		"TestOther":      {"foreign"},
		"TestComputed":   nil,
	} {
		if got := slices.Sorted(maps.Keys(sources.tests[name].subtests)); !slices.Equal(got, want) {
			t.Errorf("%s runs subtests %q, want %q", name, got, want)
		}
	}
	valid := func() coverageManifest {
		return coverageManifest{
			Spec:       coverageSpec{Repository: "JSYoo5B/ceph-msgr-go", Commit: strings.Repeat("a", 40)},
			CephCommit: commit,
			Rows: []coverageRow{
				{ID: "client-osd/write", Area: "client-osd", Requirement: "write", Symbols: []string{"CEPH_OSD_OP_WRITE"},
					Status: "covered", Evidence: "outcome",
					Cases: []coverageCase{{Test: "TestWrites/bridge", Receipt: "WRITE_RECEIPT", Asserts: "readback"}}},
				{ID: "client-osd/backoff", Area: "client-osd", Requirement: "backoff", Status: "uncovered",
					Evidence: "none", Gap: "no recipe", Owner: "#50"},
			},
		}
	}
	if problems := validateCoverage(valid(), reference, sources); len(problems) != 0 {
		t.Fatalf("valid manifest: %v", problems)
	}
	unowned := valid()
	unowned.Rows[1].Owner = ""
	if problems := validateCoverage(unowned, reference, sources); len(problems) != 0 {
		t.Fatalf("an uncovered row without an owner: %v", problems)
	}
	for _, c := range []coverageCase{
		{Test: "TestWrites", Receipt: "WRITE_RECEIPT"}, {Test: "TestWrites/host", Receipt: "WRITE_RECEIPT"},
		{Test: "TestWrites/nested/v2"}, {Test: "TestWrites/nested/crc"}, {Test: "TestNoSubtests"},
		{Test: "TestOther/foreign", Receipt: "OTHER_RECEIPT"}, {Test: "TestComputed"},
	} {
		manifest := valid()
		c.Asserts = "readback"
		manifest.Rows[0].Cases = []coverageCase{c}
		if problems := validateCoverage(manifest, reference, sources); len(problems) != 0 {
			t.Errorf("citing %+v: %v", c, problems)
		}
	}
	for name, test := range map[string]struct {
		change func(*coverageManifest)
		want   string
	}{
		"missing test":          {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestGone" }, "test TestGone does not exist"},
		"missing subtest":       {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestWrites/nested/v3" }, `subtest "nested/v3"`},
		"subtest of other test": {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestNoSubtests/foreign" }, `subtest "foreign"`},
		"sibling's subtest":     {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestWrites/foreign" }, `subtest "foreign"`},
		"unfollowed name":       {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestComputed/n1/inner" }, `subtest "n1/inner"`},
		"other test's receipt": {func(m *coverageManifest) { m.Rows[0].Cases[0].Test = "TestOther/foreign" },
			"receipt marker WRITE_RECEIPT is not emitted by code that TestOther reaches"},
		"receipt of no test": {func(m *coverageManifest) {
			m.Rows[0].Cases[0].Test, m.Rows[0].Cases[0].Receipt = "TestNoSubtests", "OTHER_RECEIPT"
		},
			"receipt marker OTHER_RECEIPT is not emitted by code that TestNoSubtests reaches"},
		"missing receipt":       {func(m *coverageManifest) { m.Rows[0].Cases[0].Receipt = "READ_RECEIPT" }, "receipt marker READ_RECEIPT"},
		"receipt prefix only":   {func(m *coverageManifest) { m.Rows[0].Cases[0].Receipt = "WRITE_RECEI" }, "receipt marker WRITE_RECEI"},
		"unknown symbol":        {func(m *coverageManifest) { m.Rows[0].Symbols = []string{"CEPH_OSD_OP_WRITE2"} }, "not in the reference"},
		"covered without case":  {func(m *coverageManifest) { m.Rows[0].Cases = nil }, "covered without a case"},
		"uncovered with case":   {func(m *coverageManifest) { m.Rows[1].Cases = m.Rows[0].Cases }, "uncovered but lists cases"},
		"malformed owner":       {func(m *coverageManifest) { m.Rows[1].Owner = "50" }, `malformed owner "50"`},
		"partial no gap":        {func(m *coverageManifest) { m.Rows[0].Status, m.Rows[0].Gap, m.Rows[0].Owner = "partial", "", "#50" }, "partial without a gap"},
		"duplicate id":          {func(m *coverageManifest) { m.Rows[1].ID = m.Rows[0].ID }, "duplicate id"},
		"other ceph commit":     {func(m *coverageManifest) { m.CephCommit = strings.Repeat("b", 40) }, "is not the reference"},
		"unknown status":        {func(m *coverageManifest) { m.Rows[0].Status = "done" }, `unknown status "done"`},
		"evidence on uncovered": {func(m *coverageManifest) { m.Rows[1].Evidence = "outcome" }, "uncovered with evidence"},
		"case without asserts":  {func(m *coverageManifest) { m.Rows[0].Cases[0].Asserts = "" }, "does not say what it asserts"},
	} {
		manifest := valid()
		test.change(&manifest)
		problems := validateCoverage(manifest, reference, sources)
		if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, test.want) }) {
			t.Errorf("%s: problems %q, want one containing %q", name, problems, test.want)
		}
	}

	guide := "# G\n\n## Daemon-to-daemon paths\n\n| Path | What | Evidence | Proven by |\n| --- | --- | --- | --- |\n" +
		"| A | x | y | `TestOne/a` and `/b`, `TestTwo` (bridge) |\n\n## Next\n\n| Path | `TestThree` |\n"
	if cited, err := guideDaemonPathTests(guide); err != nil || !slices.Equal(cited, []string{"TestOne", "TestTwo"}) {
		t.Fatalf("guide tests %v, %v", cited, err)
	}
	if _, err := guideDaemonPathTests("# G\n"); err == nil {
		t.Fatal("a guide without the section was accepted")
	}
}

// coverageValidatorSource is the test package TestMessengerCoverageValidator
// checks citations against. TestOther runs a subtest whose name no other
// test runs, and the subtests of TestComputed are below a computed name.
const coverageValidatorSource = `package fake

import (
	"fmt"
	"testing"
)

func TestWrites(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { writeAndReport(t, host) })
	}
	t.Run("nested", nestedCases)
}

func writeAndReport(t *testing.T, host bool) { t.Logf("WRITE_RECEIPT host=%t", host) }

func nestedCases(t *testing.T) {
	cases := []struct{ name string }{{name: "v1"}, {name: "v2"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {})
	}
	for name := range map[string]bool{"crc": true} {
		check := func(t *testing.T) {}
		t.Run(name, check)
	}
}

func TestNoSubtests(t *testing.T) {}

func TestOther(t *testing.T) {
	t.Run("foreign", func(t *testing.T) { t.Log("OTHER_RECEIPT") })
}

func TestComputed(t *testing.T) {
	t.Run(fmt.Sprintf("n%d", 1), func(t *testing.T) { t.Run("inner", func(t *testing.T) {}) })
}
`
