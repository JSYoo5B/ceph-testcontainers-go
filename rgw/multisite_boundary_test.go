package rgw

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// boundaryFile is one parsed source file of this package.
type boundaryFile struct {
	name     string
	file     *ast.File
	moving   bool
	declared []string
}

func loadBoundaryFiles(t *testing.T) []boundaryFile {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []boundaryFile
	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, boundaryFile{
			name:     path,
			file:     file,
			moving:   strings.HasPrefix(path, "multisite"),
			declared: packageLevelNames(file),
		})
	}
	return files
}

// packageLevelNames lists the package-level functions (not methods), types,
// variables and constants a file declares.
func packageLevelNames(file *ast.File) []string {
	var names []string
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && d.Name.Name != "init" && d.Name.Name != "_" {
				names = append(names, d.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					names = append(names, s.Name.Name)
				case *ast.ValueSpec:
					for _, n := range s.Names {
						if n.Name != "_" {
							names = append(names, n.Name)
						}
					}
				}
			}
		}
	}
	return names
}

// structTypeNames lists the names of struct types declared in the files.
func structTypeNames(files ...*ast.File) map[string]bool {
	names := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok {
					if _, ok := ts.Type.(*ast.StructType); ok {
						names[ts.Name.Name] = true
					}
				}
			}
		}
	}
	return names
}

// packageReferences lists the identifiers a file uses without declaring them
// itself, so each is a package-level declaration of another file, a builtin
// or an import. An identifier key is dropped only when the enclosing literal
// is a struct literal: its type is a struct type literal or names a struct
// type in structs, or, for an elided literal type, the enclosing literal is
// one of those or an array or slice of them. Map keys, and keys of literals
// whose type is not known to be a struct, stay references because they can
// name a package-level declaration.
func packageReferences(file *ast.File, structs map[string]bool) []string {
	isStruct := func(typ ast.Expr) bool {
		switch x := typ.(type) {
		case *ast.StructType:
			return true
		case *ast.Ident:
			return structs[x.Name]
		}
		return false
	}
	// element reports whether a composite type's elements are structs.
	element := func(typ ast.Expr) bool {
		switch x := typ.(type) {
		case *ast.ArrayType:
			return isStruct(x.Elt)
		case *ast.MapType:
			return isStruct(x.Value)
		}
		return false
	}
	keys := map[*ast.Ident]bool{}
	// mark records the struct field labels of lit when structLit is true.
	var mark func(lit *ast.CompositeLit, structLit bool)
	mark = func(lit *ast.CompositeLit, structLit bool) {
		inner := lit.Type != nil && element(lit.Type)
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if id, ok := kv.Key.(*ast.Ident); ok && structLit {
					keys[id] = true
				}
				elt = kv.Value
			}
			if sub, ok := elt.(*ast.CompositeLit); ok && sub.Type == nil {
				mark(sub, inner)
			}
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		if lit, ok := n.(*ast.CompositeLit); ok && lit.Type != nil {
			mark(lit, isStruct(lit.Type))
		}
		return true
	})
	var refs []string
	// The parser leaves composite-literal keys out of file.Unresolved because
	// a key may be a field label, so a map key naming a package-level
	// declaration of another file is collected here: any key that is not a
	// known struct label and resolved to nothing in this file.
	ast.Inspect(file, func(n ast.Node) bool {
		if lit, ok := n.(*ast.CompositeLit); ok {
			for _, elt := range lit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if id, ok := kv.Key.(*ast.Ident); ok && id.Obj == nil && !keys[id] {
						refs = append(refs, id.Name)
					}
				}
			}
		}
		return true
	})
	for _, id := range file.Unresolved {
		if !keys[id] {
			refs = append(refs, id.Name)
		}
	}
	return refs
}

// TestMultisiteFamilyBoundary pins the boundary the multisite package
// decision in docs/design/rgw-multisite-boundary.md relies on: the multisite
// family (every file whose name starts with "multisite") reaches the
// single-gateway files only through exported declarations, and no
// single-gateway file refers to a multisite declaration. It is a static
// check on package-level identifiers; it does not see struct fields or
// methods, and it does not prove runtime behavior.
func TestMultisiteFamilyBoundary(t *testing.T) {
	files := loadBoundaryFiles(t)
	var parsed []*ast.File
	for _, f := range files {
		parsed = append(parsed, f.file)
	}
	structs := structTypeNames(parsed...)
	staying := map[string]string{}
	moving := map[string]string{}
	for _, f := range files {
		for _, name := range f.declared {
			if f.moving {
				moving[name] = f.name
			} else {
				staying[name] = f.name
			}
		}
	}
	if len(moving) == 0 || len(staying) == 0 {
		t.Fatalf("boundary check found no declarations: moving=%d staying=%d", len(moving), len(staying))
	}
	if _, ok := staying["invalid"]; !ok {
		t.Fatal("boundary check no longer sees the single-gateway invalid helper; the unexported control is vacuous")
	}
	for _, f := range files {
		for _, ref := range packageReferences(f.file, structs) {
			if f.moving {
				if where, ok := staying[ref]; ok && !ast.IsExported(ref) {
					t.Errorf("%s uses unexported single-gateway declaration %s from %s", f.name, ref, where)
				}
			} else if where, ok := moving[ref]; ok {
				t.Errorf("%s uses multisite declaration %s from %s", f.name, ref, where)
			}
		}
	}
	// Both directions only count when the same name is not declared on both
	// sides.
	for name, where := range moving {
		if other, ok := staying[name]; ok {
			t.Errorf("%s is declared in both %s and %s", name, where, other)
		}
	}
}

func TestMultisiteFamilyBoundaryDetectsViolations(t *testing.T) {
	parse := func(src string) *ast.File {
		file, err := parser.ParseFile(token.NewFileSet(), "x.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"call of another file's helper", "package rgw\nfunc f() error { return invalid(nil) }", []string{"error", "invalid", "nil"}},
		{"struct key is not a reference", "package rgw\nvar v = Config{Zone: 1}", []string{"Config"}},
		{"map key is a reference", "package rgw\nvar v = map[any]bool{SyncAllowed: true}", []string{"SyncAllowed", "any", "bool", "true"}},
		{"map key of a private declaration", "package rgw\nvar v = map[string]bool{rgwBootstrapKind: true}", []string{"bool", "rgwBootstrapKind", "string", "true"}},
		{"nested slice of struct keys are labels", "package rgw\nvar v = []Config{{Zone: 1}}", []string{"Config"}},
		{"map of struct values keeps labels only", "package rgw\nvar v = map[string]Config{\"a\": {Zone: 1}}", []string{"Config", "string"}},
		{"unknown type keys stay references", "package rgw\nvar v = Named{Zone: 1}", []string{"Named", "Zone"}},
		{"direct assignment", "package rgw\nvar v = SyncAllowed", []string{"SyncAllowed"}},
		{"local shadow is not a reference", "package rgw\nfunc f() { invalid := 1; _ = invalid }", nil},
		{"selector name is not a reference", "package rgw\nfunc f(g G) { g.invalid() }", []string{"G"}},
	}
	for _, test := range tests {
		got := packageReferences(parse(test.src), map[string]bool{"Config": true})
		slices.Sort(got)
		if !slices.Equal(got, test.want) {
			t.Errorf("%s: references = %v, want %v", test.name, got, test.want)
		}
	}
}
