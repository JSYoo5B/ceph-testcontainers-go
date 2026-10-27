package override

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/JSYoo5B/ceph-testcontainers-go"

// moduleImports returns the module packages that the non-test source of the
// package in dir imports directly, as paths relative to the module root.
func moduleImports(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var imports []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if rel, ok := strings.CutPrefix(path, modulePath+"/"); ok {
				imports = append(imports, rel)
			}
		}
	}
	return imports
}

// reachable returns every module package that the non-test source of the
// package at root reaches through imports of the module.
func reachable(t *testing.T, root string) []string {
	t.Helper()
	seen := map[string]bool{}
	queue := []string{root}
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		for _, imported := range moduleImports(t, filepath.Join("..", filepath.FromSlash(pkg))) {
			if !seen[imported] {
				seen[imported] = true
				queue = append(queue, imported)
			}
		}
	}
	return slices.Sorted(func(yield func(string) bool) {
		for pkg := range seen {
			if !yield(pkg) {
				return
			}
		}
	})
}

// TestCephNeverImportsOverride fails when package ceph reaches override or its
// hook package, directly or through another package of the module.
func TestCephNeverImportsOverride(t *testing.T) {
	deps := reachable(t, "ceph")
	if len(deps) == 0 {
		t.Fatal("the walk found no import of ceph")
	}
	for _, forbidden := range []string{"override", "internal/overridehook"} {
		if slices.Contains(deps, forbidden) {
			t.Errorf("package ceph reaches %s: %v", forbidden, deps)
		}
	}
}

// TestOverrideCannotReachRawHooks fails when the public package reaches the
// raw CLI, control and cluster-lock hooks, which would hand it an unrestricted
// administrative executor.
func TestOverrideCannotReachRawHooks(t *testing.T) {
	deps := reachable(t, "override")
	if !slices.Contains(deps, "ceph") || !slices.Contains(deps, "internal/overridehook") {
		t.Fatalf("the walk missed the expected imports: %v", deps)
	}
	for _, forbidden := range []string{"internal/faulthook", "internal/servicehook"} {
		if slices.Contains(deps, forbidden) {
			t.Errorf("package override reaches %s: %v", forbidden, deps)
		}
	}
}
