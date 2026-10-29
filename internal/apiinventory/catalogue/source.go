// Package catalogue derives, stores and checks the package-scoped public API
// catalogues under internal/apiinventory/catalogues.
//
// Every public package owns three files in internal/apiinventory/catalogues/<package>:
//
//   - declarations.json and members.json are source-derived output. Only
//     Regenerate writes them.
//   - review.json is the reviewed receipt. Only Approve writes it, and only
//     for a catalogue that matches the source.
//
// No global index, count or hash file exists. The package list is discovered
// from the source tree, and the totals are computed on demand by Summarize.
package catalogue

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
)

// Dir is the catalogue directory, relative to the module root.
const Dir = "internal/apiinventory/catalogues"

// The three files of a package directory.
const (
	DeclarationsFile = "declarations.json"
	MembersFile      = "members.json"
	ReceiptFile      = "review.json"
)

// Discover returns the public packages below root: every directory outside
// internal, testdata, vendor and hidden or underscore directories that has a
// non-test Go source. Names are slash-separated paths relative to root.
func Discover(root string) ([]string, error) {
	var packages []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := entry.Name()
		if rel != "." && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
			name == "internal" || name == "testdata" || name == "vendor") {
			return filepath.SkipDir
		}
		sources, err := sourceFiles(path)
		if err != nil {
			return err
		}
		if len(sources) == 0 {
			return nil
		}
		if rel == "." {
			return fmt.Errorf("the module root has Go sources; a public package needs its own directory")
		}
		packages = append(packages, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(packages)
	return packages, nil
}

func sourceFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(files, func(path string) bool { return strings.HasSuffix(path, "_test.go") }), nil
}

func parseDir(root, dir string, visit func(file *ast.File) error) error {
	files, err := sourceFiles(filepath.Join(root, filepath.FromSlash(dir)))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("list %s sources: %v", dir, err)
	}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		if err := visit(file); err != nil {
			return err
		}
	}
	return nil
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

// ExportedNames returns the names a package exports, with methods as
// Type.Method. The API reference page check uses it.
func ExportedNames(root, pkg string) ([]string, error) {
	local := map[string][]string{}
	err := parseDir(root, pkg, func(file *ast.File) error {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() {
				continue
			}
			if receiver := receiverName(fn.Recv.List[0].Type); ast.IsExported(receiver) {
				local[receiver] = append(local[receiver], fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var names []string
	err = parseDir(root, pkg, func(file *ast.File) error {
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
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}

func syntax(node ast.Node) (string, error) {
	var buf bytes.Buffer
	if err := format.Node(&buf, token.NewFileSet(), node); err != nil {
		return "", err
	}
	return buf.String(), nil
}
