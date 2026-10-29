package harness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/sharedinvariants"
)

// The shared pool name has one definition, sharedinvariants.SharedPool. The
// public SharedPool of ceph/harness is declared from it (issue 155); core
// checks take the value from the internal definition or as an argument, and a
// second literal in non-test code would drift from it unnoticed. These controls run without
// Docker and read the source files only.

// sharedPoolDefinition is the one file allowed to spell the pool name.
const sharedPoolDefinition = "internal/sharedinvariants/sharedinvariants.go"

// poolNameLiterals returns the string literals of src that spell the shared
// pool name: the bare name or a Ceph capability or command fragment that
// names it ("pool=<name>" and "pool <name>").
func poolNameLiterals(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		if value == sharedinvariants.SharedPool ||
			strings.Contains(value, "pool="+sharedinvariants.SharedPool) ||
			strings.Contains(value, "pool "+sharedinvariants.SharedPool) {
			found = append(found, value)
		}
		return true
	})
	return found
}

// sharedPoolDeclaredFrom reports how the file declares the package-level
// constant SharedPool: the source expression of its value, or "" when the
// file does not declare it.
func sharedPoolDeclaredFrom(t *testing.T, name, src string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, n := range vs.Names {
				if n.Name != "SharedPool" || i >= len(vs.Values) {
					continue
				}
				switch v := vs.Values[i].(type) {
				case *ast.SelectorExpr:
					if id, ok := v.X.(*ast.Ident); ok {
						return id.Name + "." + v.Sel.Name
					}
				case *ast.BasicLit:
					return v.Value
				}
				return "<expression>"
			}
		}
	}
	return ""
}

func TestSharedPoolValueIsTheExistingPoolIdentity(t *testing.T) {
	// The move is a placement change: existing clusters, namespaces and the
	// capability strings that name the pool keep working.
	if sharedinvariants.SharedPool != "tenants" {
		t.Fatalf("internal definition = %q, want the existing pool name", sharedinvariants.SharedPool)
	}
	if SharedPool != sharedinvariants.SharedPool {
		t.Fatalf("SharedPool = %q, internal definition = %q", SharedPool, sharedinvariants.SharedPool)
	}
}

func TestSharedPoolHasOneDefinitionInNonTestCode(t *testing.T) {
	root := filepath.Join("..", "..")
	var spelled, decl []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && rel != "." {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(b)
		if len(poolNameLiterals(t, rel, src)) > 0 {
			spelled = append(spelled, rel)
		}
		if from := sharedPoolDeclaredFrom(t, rel, src); from != "" {
			decl = append(decl, rel+": "+from)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{sharedPoolDefinition}; !slices.Equal(spelled, want) {
		t.Fatalf("non-test files that spell the shared pool name: %q, want only %q; take the value from sharedinvariants.SharedPool or as an argument", spelled, want)
	}
	slices.Sort(decl)
	want := []string{"ceph/harness/shared.go: sharedinvariants.SharedPool", sharedPoolDefinition + ": \"tenants\""}
	if !slices.Equal(decl, want) {
		t.Fatalf("SharedPool is declared by %q, want %q", decl, want)
	}
}

// Negative controls: the readers see a second literal, a capability fragment
// and a redeclared constant, so a drifted copy would fail the test above.
func TestSharedPoolDefinitionCheckersSeeADrift(t *testing.T) {
	bad := "package x\n" +
		"const other = \"tenants\"\n" +
		"var caps = \"allow rw pool=tenants namespace=t\"\n" +
		"var cmd = \"osd pool ls\"\n" +
		"var prose = \"tenants of a shared cluster\"\n"
	got := poolNameLiterals(t, "bad.go", bad)
	want := []string{"tenants", "allow rw pool=tenants namespace=t"}
	if !slices.Equal(got, want) {
		t.Fatalf("literals found %q, want %q", got, want)
	}
	if got := sharedPoolDeclaredFrom(t, "bad.go", "package x\nconst SharedPool = \"tenants\"\n"); got != "\"tenants\"" {
		t.Fatalf("redeclared literal reported as %q", got)
	}
	if got := sharedPoolDeclaredFrom(t, "ok.go", "package x\nconst SharedPool = sharedinvariants.SharedPool\n"); got != "sharedinvariants.SharedPool" {
		t.Fatalf("derived declaration reported as %q", got)
	}
	if got := sharedPoolDeclaredFrom(t, "none.go", "package x\nconst Other = 1\n"); got != "" {
		t.Fatalf("unrelated file reported as %q", got)
	}
}
