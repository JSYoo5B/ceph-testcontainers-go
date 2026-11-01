package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func productionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.File{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out[file] = parsed
	}
	return out
}

func callee(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

// TestSessionAdmissionHasOneOwnerAndKeepsItsPredicateLocal pins for the
// session flow what the cephfs guards
// TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal,
// TestFilesystemIdentityIsReadOnlyFromImmutableName and
// TestNativeIdentityIsCapturedNotReplacedInProduction pinned for it while it
// was in package cephfs: only begin calls Admit, with exactly one own Check
// and no lock of its own; only begin reads the exported descriptor through
// DescriptorIntact; nothing installs or captures a native identity; only the
// override's Restore reads under the setup gate without an admission; and the
// package adds no init and no mutex, so the override state is guarded by the
// setup gate alone.
func TestSessionAdmissionHasOneOwnerAndKeepsItsPredicateLocal(t *testing.T) {
	sites := map[string][]string{}
	var beginFn *ast.FuncDecl
	for file, parsed := range productionFiles(t) {
		for _, imp := range parsed.Imports {
			if strings.Trim(imp.Path.Value, `"`) == "sync" {
				t.Errorf("%s imports sync: the override state is guarded by the setup gate", file)
			}
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil {
				if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					name = star.X.(*ast.Ident).Name + "." + name
				}
			} else if name == "init" {
				t.Errorf("%s declares init", file)
			}
			if name == "begin" {
				beginFn = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch c := callee(call); c {
					case "Admit", "DescriptorIntact", "ReplaceNative", "CaptureNativePools", "WithSetupGate":
						sites[c] = append(sites[c], name)
					}
				}
				return true
			})
		}
	}
	want := map[string][]string{
		"Admit":            {"begin"},
		"DescriptorIntact": {"begin"},
		"WithSetupGate":    {"TimeoutsOverrideHandle.Restore"},
	}
	if !reflect.DeepEqual(sites, want) {
		t.Errorf("session admission sites changed: %v, want %v", sites, want)
	}
	if beginFn == nil {
		t.Fatal("begin is missing")
	}
	checks := 0
	ast.Inspect(beginFn.Body, func(n ast.Node) bool {
		if kv, ok := n.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Check" {
				if _, lit := kv.Value.(*ast.FuncLit); lit {
					checks++
				}
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "RecheckCanceled" {
				t.Error("the session admission does not recheck cancellation after the setup gate")
			}
		}
		if call, ok := n.(*ast.CallExpr); ok {
			switch callee(call) {
			case "LockMutex", "LockTopology", "Lock", "Unlock":
				t.Errorf("begin calls %s; the lifetime belongs to Admit", callee(call))
			}
		}
		return true
	})
	if checks != 1 {
		t.Errorf("begin must pass exactly one own check function, has %d", checks)
	}
}
