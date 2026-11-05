package consumerevidence

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/JSYoo5B/ceph-testcontainers-go"

// examplePath is this example's own import path, which its command imports.
const examplePath = modulePath + "/internal/consumerevidence"

// internalImport reports whether path is a package of this module that a
// consumer outside the module cannot import: one with an internal element,
// such as internal/integration/harness or ceph/internal/evidence. The example
// itself is exempt for its own command and tests.
func internalImport(path string) bool {
	if path == examplePath || strings.HasPrefix(path, examplePath+"/") {
		return false
	}
	if path != modulePath && !strings.HasPrefix(path, modulePath+"/") {
		return false
	}
	return slices.Contains(strings.Split(strings.TrimPrefix(path, modulePath), "/"), "internal")
}

// importProblems parses the Go file src and returns the internal packages it
// imports.
func importProblems(name string, src any) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	var problems []string
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		if internalImport(path) {
			problems = append(problems, name+" imports "+path)
		}
	}
	return problems, nil
}

// Every Go file of the example, its command and tests included, imports only
// packages that a consumer outside this module can import as well.
func TestImportsOnlyPublicPackages(t *testing.T) {
	files := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		files++
		problems, err := importProblems(path, nil)
		for _, problem := range problems {
			t.Error(problem)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 8 {
		t.Fatalf("checked %d Go files; the walk missed the example", files)
	}
}

// The check refuses the harness and other internal packages of the module and
// keeps public ones.
func TestImportCheckRefusesInternalPackages(t *testing.T) {
	for path, internal := range map[string]bool{
		modulePath + "/internal/integration/harness":  true,
		modulePath + "/internal/clusteraccess":        true,
		modulePath + "/ceph/internal/evidence":        true,
		modulePath + "/ceph":                          false,
		modulePath + "/ceph/diagnostics":              false,
		modulePath + "/ceph/finalize":                 false,
		examplePath:                                   false,
		"github.com/testcontainers/testcontainers-go": false,
		"context": false,
	} {
		src := "package p\n\nimport _ " + strconv.Quote(path) + "\n"
		problems, err := importProblems("p.go", src)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(problems) > 0; got != internal {
			t.Errorf("%s: refused %t, want %t", path, got, internal)
		}
	}
}
