// Package apiinventory checks that each public package's page in docs names
// every identifier the package exports: functions, types, constants,
// variables and the methods of its types.
package apiinventory_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const root = "../.."

// Public packages and the page that documents each.
var packages = map[string]string{
	"ceph":   "docs/ceph.md",
	"fault":  "docs/fault.md",
	"rbd":    "docs/rbd.md",
	"cephfs": "docs/cephfs.md",
	"rgw":    "docs/rgw.md",
}

func parseDir(t *testing.T, dir string, visit func(file *ast.File)) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list %s sources: %v", dir, err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		visit(file)
	}
}

func receiverName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.StarExpr:
		return receiverName(expr.X)
	case *ast.IndexExpr:
		return receiverName(expr.X)
	case *ast.IndexListExpr:
		return receiverName(expr.X)
	case *ast.Ident:
		return expr.Name
	}
	return ""
}

// methods maps an exported receiver type to its exported method names.
func methods(t *testing.T, dir string) map[string][]string {
	result := map[string][]string{}
	parseDir(t, dir, func(file *ast.File) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() {
				continue
			}
			receiver := receiverName(fn.Recv.List[0].Type)
			if ast.IsExported(receiver) {
				result[receiver] = append(result[receiver], fn.Name.Name)
			}
		}
	})
	return result
}

// exported returns the names a package exports, with methods as Type.Method.
func exported(t *testing.T, pkg string) []string {
	t.Helper()
	local := methods(t, pkg)
	var names []string
	parseDir(t, pkg, func(file *ast.File) {
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.IsExported() {
					names = append(names, decl.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if !spec.Name.IsExported() {
							continue
						}
						names = append(names, spec.Name.Name)
						for _, method := range local[spec.Name.Name] {
							names = append(names, spec.Name.Name+"."+method)
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								names = append(names, name.Name)
							}
						}
					}
				}
			}
		}
	})
	slices.Sort(names)
	return slices.Compact(names)
}

// A method counts as documented when its name appears on the page, usually
// as its own heading under the type's section.
func TestPackagePagesNameEveryExport(t *testing.T) {
	for pkg, page := range packages {
		data, err := os.ReadFile(filepath.Join(root, page))
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, name := range exported(t, pkg) {
			word := name[strings.LastIndex(name, ".")+1:]
			if !regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`).MatchString(text) {
				t.Errorf("%s.%s is not documented in %s", pkg, name, page)
			}
		}
	}
}
