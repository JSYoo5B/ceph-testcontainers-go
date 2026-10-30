package servicehook

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// The hook surface is shared by every service package, so a new hook goes in
// name order in each of its three lists: the hooks interface, the wrappers of
// this package and the serviceHooks methods of package ceph. Two pull requests
// that add different hooks then edit different places instead of the same
// list end.
func TestHookListsAreInNameOrder(t *testing.T) {
	own := parseFile(t, "servicehook.go")
	var methods, wrappers []string
	for _, decl := range own.Decls {
		switch decl := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				if spec, ok := spec.(*ast.TypeSpec); ok && spec.Name.Name == "hooks" {
					for _, field := range spec.Type.(*ast.InterfaceType).Methods.List {
						for _, name := range field.Names {
							methods = append(methods, name.Name)
						}
					}
				}
			}
		case *ast.FuncDecl:
			if decl.Recv == nil && decl.Name.IsExported() {
				wrappers = append(wrappers, decl.Name.Name)
			}
		}
	}
	var implemented []string
	for _, decl := range parseFile(t, "../../ceph/service_hooks.go").Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil {
			if recv, ok := fn.Recv.List[0].Type.(*ast.Ident); ok && recv.Name == "serviceHooks" {
				implemented = append(implemented, fn.Name.Name)
			}
		}
	}
	for _, list := range []struct {
		where string
		names []string
	}{
		{"internal/servicehook hooks interface", methods},
		{"internal/servicehook wrappers", wrappers},
		{"ceph serviceHooks methods", implemented},
	} {
		if len(list.names) == 0 {
			t.Errorf("%s: none found", list.where)
		}
		if !slices.IsSorted(list.names) {
			t.Errorf("%s are not in name order: %v", list.where, list.names)
		}
	}
}

func parseFile(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}
