package ceph

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/JSYoo5B/ceph-testcontainers-go"

func moduleImports(t *testing.T, dir string) []string {
	t.Helper()
	set := map[string]bool{}
	for name, src := range nonTestSources(t, dir) {
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if rest, ok := strings.CutPrefix(p, modulePath+"/"); ok {
				set[rest] = true
			}
		}
	}
	return sortedKeys(set)
}

// nonTestSources returns the non-test Go files of dir by base name.
func nonTestSources(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := map[string]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		src[filepath.Base(file)] = string(b)
	}
	return src
}
