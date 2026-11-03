package monstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// parseProductionFiles parses the package's non-test files by name.
func parseProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	return parseProductionFilesIn(t, ".")
}

// parseProductionFilesIn parses the non-test files of dir by name.
func parseProductionFilesIn(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
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
		out[filepath.Base(file)] = parsed
	}
	return out
}

// receiverTypeName returns the type name of a method receiver, or "".
func receiverTypeName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// TestPackageAddsNoMutex pins that no record, adapter or inspector of the
// package has a lock of its own: a State is used under the cluster's topology
// lock, so a mutex here would add a lock domain to the cluster's lock order.
func TestPackageAddsNoMutex(t *testing.T) {
	for name, parsed := range parseProductionFiles(t) {
		for _, imp := range parsed.Imports {
			if strings.HasPrefix(imp.Path.Value, `"sync`) {
				t.Errorf("%s imports %s", name, imp.Path.Value)
			}
		}
	}
}
