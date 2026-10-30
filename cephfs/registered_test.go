package cephfs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// registered is the one place that tests the filesystem's own registry entry;
// every admission predicate and ownership proof calls it and adds its own
// conjuncts. A new open-coded byName[fs.name] lookup fails this test.
func TestRegistryOwnershipIsReadOnlyThroughRegistered(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	var sites []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(set, file, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				index, ok := n.(*ast.IndexExpr)
				if !ok {
					return true
				}
				if sel, ok := index.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "byName" {
					if key, ok := index.Index.(*ast.SelectorExpr); ok && key.Sel.Name == "name" {
						sites = append(sites, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if len(sites) != 1 || sites[0] != "registered" {
		t.Fatalf("byName[fs.name] is read in %v, want only registered", sites)
	}
}

func TestRegisteredRefusesClosedAndReplacedFilesystems(t *testing.T) {
	fs, _ := subvolumeFixture()
	if !fs.id.registered() {
		t.Fatal("a live filesystem is not registered")
	}
	replacement := &Filesystem{id: filesystemIdentity{cluster: fs.id.cluster, name: fs.id.name}}
	filesystems(fs.id.cluster).byName[fs.id.name] = replacement
	if fs.id.registered() || !replacement.id.registered() {
		t.Fatal("a replaced registry entry still admits the original handle")
	}
	filesystems(fs.id.cluster).byName[fs.id.name] = fs
	testhelper.TestSetClosed(fs.id.cluster, true)
	if fs.id.registered() {
		t.Fatal("a closed cluster still admits its filesystem")
	}
}
