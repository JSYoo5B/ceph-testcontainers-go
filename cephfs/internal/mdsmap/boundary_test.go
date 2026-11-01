package mdsmap

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

func parseFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]*ast.File{}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = parsed
	}
	return files
}

// TestDecodersArePureFunctionsOfTheirArguments pins what keeps the decoders
// pure: the package imports nothing that can reach a lock, a command, Docker
// or package ceph, and declares no method, so a decode depends only on the
// document and the plain expectation it is given.
func TestDecodersArePureFunctionsOfTheirArguments(t *testing.T) {
	allowed := []string{"encoding/json", "errors", "slices", "strconv", "strings",
		"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming", "github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"}
	for name, file := range parseFiles(t) {
		for _, imp := range file.Imports {
			if path := strings.Trim(imp.Path.Value, `"`); !slices.Contains(allowed, path) {
				t.Errorf("%s imports %s", name, path)
			}
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil {
				t.Errorf("%s declares the method %s", name, fn.Name.Name)
			}
		}
	}
}

// TestRowStateListHasOneCopy guards the shared allowlist: the state names
// are written only in state.go, and each decoder reaches them through
// StateAccepted, once.
func TestRowStateListHasOneCopy(t *testing.T) {
	callers := map[string][]string{}
	for name, file := range parseFiles(t) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "StateAccepted" {
						callers[fn.Name.Name] = append(callers[fn.Name.Name], name)
					}
				case *ast.BasicLit:
					if name != "state.go" && strings.Contains(node.Value, `"up:creating"`) {
						t.Errorf("%s: %s writes a second copy of the daemon state list", name, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	want := map[string][]string{"DecodeCold": {"cold.go"}, "admitStoppedMDSRow": {"stopped.go"}, "admitReplacementMDSRow": {"replacement.go"}}
	if !reflect.DeepEqual(callers, want) {
		t.Errorf("state-list callers changed: %v, pinned %v", callers, want)
	}
}
