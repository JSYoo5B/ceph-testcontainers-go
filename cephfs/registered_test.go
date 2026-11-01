package cephfs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// ownsRegistryEntry, the liveness predicate that newFilesystem hands to every
// core, is the one place that tests the filesystem's own registry entry; the
// core's Registered calls it and every admission predicate and ownership proof
// calls Registered and adds its own conjuncts. A new open-coded
// byName[core.Name()] lookup fails this test. fsbase's own guard pins that
// Registered is the only caller of the predicate.
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
					if key, ok := index.Index.(*ast.CallExpr); ok {
						if name, ok := key.Fun.(*ast.SelectorExpr); ok && name.Sel.Name == "Name" {
							sites = append(sites, fn.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
	if len(sites) != 1 || sites[0] != "ownsRegistryEntry" {
		t.Fatalf("byName[core.Name()] is read in %v, want only ownsRegistryEntry", sites)
	}
}

func TestRegisteredRefusesClosedAndReplacedFilesystems(t *testing.T) {
	fs, _ := filesystemFixture()
	if !fs.base().Registered() {
		t.Fatal("a live filesystem is not registered")
	}
	replacement := newFilesystem(fs.base().Cluster(), Config{Name: fs.base().Name()}, nil)
	filesystems(fs.base().Cluster()).byName[fs.base().Name()] = replacement
	if fs.base().Registered() || !replacement.base().Registered() {
		t.Fatal("a replaced registry entry still admits the original handle")
	}
	filesystems(fs.base().Cluster()).byName[fs.base().Name()] = fs
	testhelper.TestSetClosed(fs.base().Cluster(), true)
	if fs.base().Registered() {
		t.Fatal("a closed cluster still admits its filesystem")
	}
}

// TestNativeIdentityIsCapturedNotReplacedInProduction pins that production
// code records the native identity only through the core's creation capture:
// ReplaceNative is a test mutator, and createNative is the only capture.
func TestNativeIdentityIsCapturedNotReplacedInProduction(t *testing.T) {
	sites := map[string][]string{}
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch name := admissionCallee(call); name {
					case "ReplaceNative", "CaptureNativePools":
						sites[name] = append(sites[name], fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if len(sites["ReplaceNative"]) != 0 || !slices.Equal(sites["CaptureNativePools"], []string{"createNative"}) {
		t.Fatalf("native identity writers changed: %v", sites)
	}
}
