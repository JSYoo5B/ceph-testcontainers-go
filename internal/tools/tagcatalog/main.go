// Command tagcatalog reads test declarations and build expressions without
// importing or executing packages from the repository it inspects.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type expression struct {
	Op       string        `json:"op"`
	Tag      string        `json:"tag,omitempty"`
	Children []*expression `json:"children,omitempty"`
}

type file struct {
	Path       string      `json:"path"`
	Package    string      `json:"package"`
	Constraint *expression `json:"constraint"`
	Tags       []string    `json:"tags"`
	Tests      []string    `json:"tests"`
	Metadata   []string    `json:"metadata"`
	// Networks are the literal arguments of the file's harness.NetworkModes calls,
	// the Docker network modes its tests run in, default first; NetworkTests
	// are the tests that call it.
	Networks     []string `json:"networks"`
	NetworkTests []string `json:"network_tests"`
	// SuiteCases are the testify suite methods the file declares, as
	// Type.Method: methods named like a test that take and return nothing.
	// suite.Run finds them by reflection, so the planner requires each one's
	// leaf in the native log. SuiteRuns are, per test of the file, the types
	// it passes to suite.Run; together they bind a method to the tests that
	// run it, wherever in the package the method is declared.
	SuiteCases []string            `json:"suite_cases"`
	SuiteRuns  map[string][]string `json:"suite_runs"`
	// Reproductions are, per package-level declaration of the file, what it
	// contributes to the reproduction units a test starts (readReproductions).
	Reproductions map[string]reproductionReach `json:"reproductions"`
	SHA256        string                       `json:"sha256"`
}

// reproductionReach is one package-level function, method (Type.Method) or
// variable: the units its code starts with harness.StartReproduction, the
// identifiers it uses that are not its own parameters or locals, which may
// name another package-level function or variable, and the names it selects
// (x.Name), which may name a method. Only code counts: comments and string
// contents are not identifiers.
type reproductionReach struct {
	Starts    []string `json:"starts"`
	Names     []string `json:"names"`
	Selectors []string `json:"selectors"`
}

func convert(e constraint.Expr, tags map[string]bool) *expression {
	switch v := e.(type) {
	case *constraint.TagExpr:
		tags[v.Tag] = true
		return &expression{Op: "tag", Tag: v.Tag}
	case *constraint.NotExpr:
		return &expression{Op: "not", Children: []*expression{convert(v.X, tags)}}
	case *constraint.AndExpr:
		return &expression{Op: "and", Children: []*expression{convert(v.X, tags), convert(v.Y, tags)}}
	case *constraint.OrExpr:
		return &expression{Op: "or", Children: []*expression{convert(v.X, tags), convert(v.Y, tags)}}
	default:
		panic("unexpected Go build expression")
	}
}

func testName(name string) bool {
	if !strings.HasPrefix(name, "Test") || name == "TestMain" {
		return false
	}
	for _, r := range name[len("Test"):] {
		return !unicode.IsLower(r)
	}
	return true
}

func inspect(root, path string) (file, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return file{}, err
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ParseComments)
	if err != nil {
		return file{}, err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return file{}, err
	}
	hash := sha256.Sum256(data)
	out := file{Path: filepath.ToSlash(rel), Package: f.Name.Name, Tests: []string{}, Tags: []string{}, Metadata: []string{}, Networks: []string{}, NetworkTests: []string{}, SuiteCases: []string{}, SuiteRuns: map[string][]string{}, Reproductions: map[string]reproductionReach{}, SHA256: hex.EncodeToString(hash[:])}
	for _, group := range f.Comments {
		if group.Pos() > f.Package {
			continue
		}
		for _, comment := range group.List {
			if constraint.IsGoBuild(comment.Text) {
				if out.Constraint != nil {
					return file{}, fmt.Errorf("%s: multiple go:build lines", rel)
				}
				expr, err := constraint.Parse(comment.Text)
				if err != nil {
					return file{}, err
				}
				tags := map[string]bool{}
				out.Constraint = convert(expr, tags)
				for tag := range tags {
					out.Tags = append(out.Tags, tag)
				}
				sort.Strings(out.Tags)
			}
			if strings.HasPrefix(comment.Text, "//ci:") {
				out.Metadata = append(out.Metadata, strings.TrimSpace(strings.TrimPrefix(comment.Text, "//ci:")))
			}
		}
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv != nil && testName(fn.Name.Name) && fn.Type.TypeParams == nil &&
			(fn.Type.Params == nil || len(fn.Type.Params.List) == 0) && fn.Type.Results == nil && len(fn.Recv.List) == 1 {
			if recv := receiverType(fn.Recv.List[0].Type); recv != "" {
				out.SuiteCases = append(out.SuiteCases, recv+"."+fn.Name.Name)
			}
			continue
		}
		if !ok || fn.Recv != nil || !testName(fn.Name.Name) {
			continue
		}
		if fn.Type.TypeParams != nil || fn.Type.Results != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
			return file{}, fmt.Errorf("%s: invalid test signature for %s", rel, fn.Name.Name)
		}
		field := fn.Type.Params.List[0]
		if len(field.Names) > 1 {
			return file{}, fmt.Errorf("%s: invalid test parameter for %s", rel, fn.Name.Name)
		}
		pointer, ok := field.Type.(*ast.StarExpr)
		if !ok {
			return file{}, fmt.Errorf("%s: invalid test parameter for %s", rel, fn.Name.Name)
		}
		selector, ok := pointer.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "T" {
			return file{}, fmt.Errorf("%s: invalid testing.T parameter for %s", rel, fn.Name.Name)
		}
		alias, ok := selector.X.(*ast.Ident)
		if !ok {
			return file{}, fmt.Errorf("%s: invalid testing import for %s", rel, fn.Name.Name)
		}
		valid := false
		for _, imported := range f.Imports {
			if imported.Path.Value == `"testing"` && ((imported.Name == nil && alias.Name == "testing") || (imported.Name != nil && imported.Name.Name == alias.Name)) {
				valid = true
			}
		}
		if !valid {
			return file{}, fmt.Errorf("%s: test parameter does not refer to testing.T", rel)
		}
		out.Tests = append(out.Tests, fn.Name.Name)
		if types := suiteRuns(fn.Body); len(types) > 0 {
			out.SuiteRuns[fn.Name.Name] = types
		}
	}
	sort.Strings(out.Tests)
	sort.Strings(out.SuiteCases)
	networks, tests, err := readNetworkModes(f)
	if err != nil {
		return file{}, fmt.Errorf("%s: %w", rel, err)
	}
	out.Networks, out.NetworkTests = networks, tests
	if out.Reproductions, err = readReproductions(f); err != nil {
		return file{}, fmt.Errorf("%s: %w", rel, err)
	}
	return out, nil
}

// readReproductions records, per package-level declaration of a file, its
// reproductionReach. The tag runner follows the names and selectors from the
// tests that a profile runs to the units they start, so it expects verdicts
// of those units only. The unit of each harness.StartReproduction call is a
// string literal, the name the catalogue lists.
func readReproductions(f *ast.File) (map[string]reproductionReach, error) {
	out := map[string]reproductionReach{}
	var failure error
	add := func(key string, nodes ...ast.Node) {
		starts, names, selectors := map[string]bool{}, map[string]bool{}, map[string]bool{}
		var visit func(ast.Node) bool
		visit = func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.CallExpr:
				if !startReproductionCall(v.Fun) {
					return true
				}
				if len(v.Args) < 2 {
					failure = fmt.Errorf("%s: harness.StartReproduction without a unit", key)
					return false
				}
				lit, ok := v.Args[1].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					failure = fmt.Errorf("%s: the unit of harness.StartReproduction is not a string literal", key)
					return false
				}
				unit, err := strconv.Unquote(lit.Value)
				if err != nil {
					failure = fmt.Errorf("%s: %w", key, err)
					return false
				}
				starts[unit] = true
			case *ast.SelectorExpr:
				selectors[v.Sel.Name] = true
				ast.Inspect(v.X, visit)
				return false
			case *ast.Ident:
				// A parameter or local of the same name shadows a
				// package-level declaration; the parser resolves those.
				// A name declared in another file of the package stays
				// unresolved (nil Obj), like an import.
				if v.Name != "_" && (v.Obj == nil || f.Scope.Objects[v.Name] == v.Obj) {
					names[v.Name] = true
				}
			}
			return true
		}
		for _, node := range nodes {
			if node != nil {
				ast.Inspect(node, visit)
			}
		}
		reach := out[key]
		reach.Starts = merged(reach.Starts, starts)
		reach.Names = merged(reach.Names, names)
		reach.Selectors = merged(reach.Selectors, selectors)
		out[key] = reach
	}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			key := d.Name.Name
			if d.Recv != nil {
				key = receiverType(d.Recv.List[0].Type) + "." + key
			}
			add(key, d.Body)
		case *ast.GenDecl:
			if d.Tok != token.VAR && d.Tok != token.CONST {
				continue
			}
			for _, spec := range d.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if name.Name == "_" {
						continue
					}
					// var a, b = x, y gives each name its own value; var
					// a, b = f() gives both the one call.
					if len(value.Values) == len(value.Names) {
						add(name.Name, value.Values[i])
						continue
					}
					nodes := make([]ast.Node, 0, len(value.Values))
					for _, expr := range value.Values {
						nodes = append(nodes, expr)
					}
					add(name.Name, nodes...)
				}
			}
		}
	}
	return out, failure
}

func startReproductionCall(fun ast.Expr) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "StartReproduction" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "harness"
}

// merged is the sorted union of names and the keys of set.
func merged(names []string, set map[string]bool) []string {
	for _, name := range names {
		set[name] = true
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// receiverType is the type name of a method receiver: T or *T.
func receiverType(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

// suiteRuns returns the types that body passes to suite.Run as &T{...} or
// new(T), sorted and without duplicates.
func suiteRuns(body *ast.BlockStmt) []string {
	seen := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Run" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "suite" {
			return true
		}
		switch arg := call.Args[1].(type) {
		case *ast.UnaryExpr:
			if lit, ok := arg.X.(*ast.CompositeLit); ok {
				if ident, ok := lit.Type.(*ast.Ident); ok {
					seen[ident.Name] = true
				}
			}
		case *ast.CallExpr:
			if fun, ok := arg.Fun.(*ast.Ident); ok && fun.Name == "new" && len(arg.Args) == 1 {
				if ident, ok := arg.Args[0].(*ast.Ident); ok {
					seen[ident.Name] = true
				}
			}
		}
		return true
	})
	types := make([]string, 0, len(seen))
	for name := range seen {
		types = append(types, name)
	}
	sort.Strings(types)
	return types
}

// readNetworkModes reads the modes of every harness.NetworkModes(t, "mode", ...) call
// in a file and the tests that make one. The planner builds the network axis
// from them, so the modes are string literals, the same in every call of the
// file, and each call is in a test function itself, which runs only the
// declared modes; a test without one runs once, in a batch's default mode.
func readNetworkModes(f *ast.File) ([]string, []string, error) {
	modes, tests := []string{}, []string{}
	seen := false
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		calls := 0
		var failure error
		ast.Inspect(decl, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || failure != nil {
				return failure == nil
			}
			if !networkModesCall(call.Fun) {
				return true
			}
			calls++
			var args []string
			for _, arg := range call.Args[min(1, len(call.Args)):] {
				literal, ok := arg.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					failure = fmt.Errorf("harness.NetworkModes takes string literal modes")
					return false
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					failure = err
					return false
				}
				args = append(args, value)
			}
			if len(args) == 0 {
				failure = fmt.Errorf("harness.NetworkModes needs at least one mode")
			} else if seen && !slices.Equal(args, modes) {
				failure = fmt.Errorf("harness.NetworkModes calls of one file declare different modes")
			}
			modes, seen = args, true
			return true
		})
		if failure != nil {
			return nil, nil, failure
		}
		if calls == 0 {
			continue
		}
		if !ok || fn.Recv != nil || !testName(fn.Name.Name) {
			return nil, nil, fmt.Errorf("harness.NetworkModes is called outside a test function")
		}
		tests = append(tests, fn.Name.Name)
	}
	sort.Strings(tests)
	return modes, tests, nil
}

// networkModesCall reports a call of harness.NetworkModes, the helper of
// internal/integration/harness that declares a test's network modes.
func networkModesCall(fun ast.Expr) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "NetworkModes" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "harness"
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: tagcatalog ROOT")
		os.Exit(2)
	}
	root, err := filepath.Abs(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	files := []file{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			// The go command ignores testdata too: its test files are
			// fixtures of other tests, never built as packages.
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "artifacts" || entry.Name() == "vendor" ||
				entry.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("test source is a symlink: %s", path)
		}
		f, err := inspect(root, path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if err := json.NewEncoder(os.Stdout).Encode(files); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
