package evidencehook

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// registeredAtInit is read while this package initializes, before any init
// function of it runs. It holds because this package imports package ceph,
// whose initialization, the registration included, completes first.
var registeredAtInit = hookreg.Evidence

func TestRegisteredHooksSatisfyTheContract(t *testing.T) {
	if _, ok := hookreg.Evidence.(hooks); !ok {
		t.Error("package ceph registered evidence hooks that do not satisfy the evidencehook contract")
	}
}

// The registration is visible to package-level initializers of every
// importer, and the registered value is package ceph's own.
func TestRegistrationPrecedesEveryImporter(t *testing.T) {
	if _, ok := registeredAtInit.(hooks); !ok {
		t.Fatal("the hooks were not registered when an importer initialized")
	}
	if path := reflect.TypeOf(registeredAtInit).PkgPath(); path != "github.com/JSYoo5B/ceph-testcontainers-go/ceph" {
		t.Fatalf("the registered hooks come from %s", path)
	}
}

// The only assignment to the slot in the module's production code is the
// one init function of package ceph, so no other package can replace the
// provider and no refactoring can move it into a package this one does not
// import without failing here.
func TestOnlyPackageCephWritesTheSlot(t *testing.T) {
	var writers []string
	root := filepath.Join("..", "..", "..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if assign, ok := n.(*ast.AssignStmt); ok {
				for _, lhs := range assign.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Evidence" {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "hookreg" {
							rel, _ := filepath.Rel(root, path)
							writers = append(writers, filepath.ToSlash(rel))
						}
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(writers) != 1 || writers[0] != "ceph/evidence_hooks.go" {
		t.Fatalf("hookreg.Evidence is written by %v", writers)
	}
}
