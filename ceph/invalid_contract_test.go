package ceph

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// This file guards the shared construction of the invalid-request error
// used by the rbd, cephfs,
// rgw and the two mirror packages each used to keep a copy of
//
//	func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }
//
// and now call internal/servicehook.Invalid, which reaches the one construction
// in package ceph. The tests run without Docker.

// invalidCallSites is the number of non-test call sites of the former
// per-package invalid function, measured by AST at the commit before the
// replacement. The replacement keeps every call site.
//
// Counts cover the retained standalone service callers.
var invalidCallSites = map[string]int{
	"rbd": 28, "cephfs": 65, "rgw": 60,
	"rbd/mirror": 28, "cephfs/mirror": 15,
}

// countInvalidCalls counts, in the non-test files of dir, the calls of a bare
// invalid(...) and of servicehook.Invalid(...).
func countInvalidCalls(t *testing.T, dir string) (bare, shared int) {
	t.Helper()
	for name, src := range nonTestSources(t, dir) {
		file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				if fn.Name == "invalid" {
					bare++
				}
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && id.Name == "servicehook" && fn.Sel.Name == "Invalid" {
					shared++
				}
			}
			return true
		})
	}
	return bare, shared
}

func TestInvalidCallSitesAllUseTheSharedConstruction(t *testing.T) {
	for dir, want := range invalidCallSites {
		bare, shared := countInvalidCalls(t, filepath.Join("..", filepath.FromSlash(dir)))
		if bare != 0 || shared != want {
			t.Errorf("%s: bare invalid calls = %d, servicehook.Invalid calls = %d, want 0 and %d", dir, bare, shared, want)
		}
	}
	// Negative controls: the counter sees both shapes.
	bare, shared := countCallsIn(t, "package p\nfunc f() { _ = invalid(nil); _ = servicehook.Invalid(nil) }")
	if bare != 1 || shared != 1 {
		t.Fatalf("the counter missed a call shape: %d %d", bare, shared)
	}
}

func countCallsIn(t *testing.T, src string) (bare, shared int) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return countInvalidCalls(t, dir)
}

// bodyOf returns the printed body of the function or method named name in src.
func bodyOf(t *testing.T, src, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			var b strings.Builder
			if err := printer.Fprint(&b, fset, fn.Body); err != nil {
				t.Fatal(err)
			}
			return strings.Join(strings.Fields(b.String()), " ")
		}
	}
	t.Fatalf("function %s not found", name)
	return ""
}

// TestInvalidConstructionBodiesAreTheFormerCopy compares function bodies: the
// one construction in ceph is the body that the five copies had, the
// registered hook only forwards to it, the typed accessor only forwards to the
// hook, and ceph's own invalidRequest builds through it.
func TestInvalidConstructionBodiesAreTheFormerCopy(t *testing.T) {
	read := func(dir, file string) string { return nonTestSources(t, dir)[file] }
	if got, want := bodyOf(t, read(".", "errors.go"), "newInvalid"), "{ return &OperationError{Kind: ErrInvalidRequest, Err: err} }"; got != want {
		t.Errorf("newInvalid body = %q, want the former per-package construction %q", got, want)
	}
	if got, want := bodyOf(t, read(".", "errors.go"), "invalidRequest"), "{ return newInvalid(fmt.Errorf(format, args...)) }"; got != want {
		t.Errorf("invalidRequest body = %q, want %q", got, want)
	}
	if got, want := bodyOf(t, read(".", "service_hooks.go"), "Invalid"), "{ return newInvalid(err) }"; got != want {
		t.Errorf("serviceHooks.Invalid body = %q, want %q", got, want)
	}
	if got, want := bodyOf(t, read(filepath.Join("..", "internal", "servicehook"), "servicehook.go"), "Invalid"), "{ return get().Invalid(err) }"; got != want {
		t.Errorf("servicehook.Invalid body = %q, want %q", got, want)
	}
	// Negative control: the comparison tells a classify-based body (which
	// returns nil for a nil error) from the construction.
	if bodyOf(t, "package p\nfunc newInvalid(err error) error { return classify(ErrInvalidRequest, err) }", "newInvalid") == "{ return &OperationError{Kind: ErrInvalidRequest, Err: err} }" {
		t.Fatal("the body comparison does not distinguish a classify-based construction")
	}
}

func TestCoreDoesNotImportTheServiceContract(t *testing.T) {
	if slices.Contains(moduleImports(t, "."), "internal/servicehook") {
		t.Fatal("package ceph imports internal/servicehook; the contract is registered through internal/hookreg")
	}
}
