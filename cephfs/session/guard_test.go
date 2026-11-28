package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
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
// session flow what the cephfs guards pin for package cephfs: only begin
// admits, through fsbase.Core.AdmitOwned, which owns the predicate; only the
// overrides' Restore methods run the shared restore protocol
// (RestoreOverride); nothing else admits, reads the exported descriptor,
// reads under the setup gate without an admission, or installs or captures a
// native identity; and the package adds no init and no mutex, so the
// override state is guarded by the setup gate alone.
func TestSessionAdmissionHasOneOwnerAndKeepsItsPredicateLocal(t *testing.T) {
	sites := map[string][]string{}
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
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch c := callee(call); c {
					case "Admit", "AdmitOwned", "AdmitActive", "RestoreOverride", "DescriptorIntact", "ReplaceNative", "CaptureNativePools", "WithSetupGate":
						sites[c] = append(sites[c], name)
					}
				}
				return true
			})
		}
	}
	for _, list := range sites {
		slices.Sort(list)
	}
	want := map[string][]string{
		"AdmitOwned":      {"begin"},
		"RestoreOverride": {"RefusalOverrideHandle.Restore", "RequiredFeaturesOverrideHandle.Restore", "TimeoutsOverrideHandle.Restore"},
	}
	if !reflect.DeepEqual(sites, want) {
		t.Errorf("session admission sites changed: %v, want %v", sites, want)
	}
}
