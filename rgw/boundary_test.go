package rgw

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestPackageDoesNotImportMultisite keeps the dependency one way: package
// rgw/multisite imports rgw, never the reverse. The check covers production
// and test sources of this package.
func TestPackageDoesNotImportMultisite(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("list sources: %v", err)
	}
	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(imported, "/rgw/multisite") {
				t.Errorf("%s imports %s; rgw must not import its child package", path, imported)
			}
		}
	}
}
