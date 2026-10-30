package apiinventory_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// clientModules are module path prefixes of Ceph and object-storage client
// libraries. The fixture controls Ceph through the CLIs in its containers; a
// client library is what the SUT, or a test acting as the SUT, uses. Such a
// test lives in its own module, like internal/integration/goceph/probe.
var clientModules = []string{
	"github.com/ceph/go-ceph",
	"github.com/aws/aws-sdk-go",
	"github.com/aws/aws-sdk-go-v2",
	"github.com/aws/smithy-go",
	"github.com/minio/minio-go",
	"github.com/ncw/swift",
}

// clientModule reports the clientModules entry that path is, or is inside.
func clientModule(path string) (string, bool) {
	for _, prefix := range clientModules {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return prefix, true
		}
	}
	return "", false
}

// The root module's go.mod names no client library, directly or indirectly.
// go.mod lists the dependencies of the module's tests too, so a test of this
// module that imported one would add it to every consumer's module graph.
func TestModuleRequiresNoClientLibrary(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range goModClientProblems(string(data)) {
		t.Error(problem)
	}
}

// goModClientProblems reports every module path of a go.mod that is a client
// library, wherever it appears (require, replace, exclude, tool).
func goModClientProblems(goMod string) []string {
	var problems []string
	for n, line := range strings.Split(goMod, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		for _, field := range strings.Fields(line) {
			if prefix, ok := clientModule(field); ok {
				problems = append(problems, "go.mod:"+strconv.Itoa(n+1)+": "+field+" is a client library ("+prefix+")")
			}
		}
	}
	return problems
}

// No Go file of the root module, test files included, imports a client
// library or uses cgo: the module, and its tests, build with CGO_ENABLED=0.
// Directories that hold their own go.mod are other modules and are skipped,
// as are the directories the go command ignores.
func TestModuleImportsNoClientLibraryOrCgo(t *testing.T) {
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			name := entry.Name()
			if name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		problems, err := importClientProblems(path, src)
		if err != nil {
			return err
		}
		for _, problem := range problems {
			t.Error(problem)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// importClientProblems reports every import of one file that is cgo or a
// client library.
func importClientProblems(path string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	var problems []string
	for _, spec := range file.Imports {
		imported, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		where := fset.Position(spec.Pos()).String()
		if imported == "C" {
			problems = append(problems, where+": imports \"C\"; the module builds with CGO_ENABLED=0")
		} else if prefix, ok := clientModule(imported); ok {
			problems = append(problems, where+": imports "+imported+", a client library ("+prefix+"); put the test in its own module")
		}
	}
	return problems, nil
}

func TestClientDependencyProblemsRefuseViolations(t *testing.T) {
	goMods := []struct {
		name  string
		goMod string
		want  int
	}{
		{"clean", "module m\n\nrequire github.com/testcontainers/testcontainers-go v0.44.0\n", 0},
		{"direct go-ceph", "module m\n\nrequire github.com/ceph/go-ceph v0.36.0\n", 1},
		{"indirect sdk in block", "module m\n\nrequire (\n\tgithub.com/aws/aws-sdk-go-v2/service/s3 v1.0.0 // indirect\n\tgithub.com/aws/smithy-go v1.0.0 // indirect\n)\n", 2},
		{"replace", "module m\n\nreplace github.com/minio/minio-go/v7 => ../minio\n", 1},
		{"similar name", "module m\n\nrequire github.com/ceph/go-cephx v1.0.0\n", 0},
		{"comment only", "module m\n\n// github.com/ceph/go-ceph is used by the probe module\n", 0},
	}
	for _, c := range goMods {
		if got := goModClientProblems(c.goMod); len(got) != c.want {
			t.Errorf("go.mod %s: %d problems %q, want %d", c.name, len(got), got, c.want)
		}
	}

	files := []struct {
		name string
		src  string
		want int
	}{
		{"clean", "package p\n\nimport \"strings\"\n", 0},
		{"cgo", "package p\n\n// #include <stdlib.h>\nimport \"C\"\n", 1},
		{"go-ceph rados", "package p\n\nimport \"github.com/ceph/go-ceph/rados\"\n", 1},
		{"aliased sdk", "package p_test\n\nimport (\n\ts3 \"github.com/aws/aws-sdk-go-v2/service/s3\"\n\t_ \"github.com/ncw/swift/v2\"\n)\n", 2},
		{"similar name", "package p\n\nimport \"github.com/ceph/go-cephx\"\n", 0},
	}
	for _, c := range files {
		got, err := importClientProblems(c.name+".go", []byte(c.src))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(got) != c.want {
			t.Errorf("file %s: %d problems %q, want %d", c.name, len(got), got, c.want)
		}
	}
}
