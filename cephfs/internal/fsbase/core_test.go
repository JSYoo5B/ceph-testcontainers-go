package fsbase

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// parseProductionFiles parses the non-test files of package fsbase.
func parseProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.File{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out[file] = parsed
	}
	return out
}

func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func TestOfReturnsTheEmbeddedCoreAndRegisteredFollowsLiveness(t *testing.T) {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{})
	name := "fixture"
	live := true
	core := NewCore(Params{Cluster: c, Name: "fixture", DescriptorName: &name, Live: func(*Core) bool { return live }})
	owner := &struct{ Handle }{NewHandle(core)}
	if Of(owner) != core {
		t.Fatal("Of does not return the embedded core")
	}
	if !core.Registered() || !core.DescriptorIntact() {
		t.Fatal("a live core with an intact descriptor is refused")
	}
	name = "edited"
	if core.DescriptorIntact() {
		t.Fatal("an edited descriptor name is reported intact")
	}
	live = false
	if core.Registered() {
		t.Fatal("a core the registry no longer holds is registered")
	}
	live = true
	testhelper.TestSetClosed(c, true)
	if core.Registered() {
		t.Fatal("a closed cluster still admits its filesystem")
	}
	var missing *Core
	if missing.Cluster() != nil || missing.Name() != "" {
		t.Fatal("a nil core reports a cluster or a name")
	}
}

// TestCoreReadsInjectedRegistryStateOnlyAtItsOwnSites pins where the core
// reaches the state cephfs injected: the liveness predicate is called only by
// Registered, after the Closed test; the descriptor address is read only by
// DescriptorIntact; the native identity is assigned only by
// CaptureNativePools and the test mutator.
func TestCoreReadsInjectedRegistryStateOnlyAtItsOwnSites(t *testing.T) {
	sites := map[string][]string{}
	var registered *ast.FuncDecl
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "Registered" {
				registered = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "live" {
						sites["live"] = append(sites["live"], fn.Name.Name)
					}
				case *ast.SelectorExpr:
					if node.Sel.Name == "descriptorName" {
						sites["descriptorName"] = append(sites["descriptorName"], fn.Name.Name)
					}
				case *ast.AssignStmt:
					for _, lhs := range node.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "native" {
							sites["native"] = append(sites["native"], fn.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
	for key := range sites {
		slices.Sort(sites[key])
	}
	want := map[string][]string{
		"live":           {"Registered"},
		"descriptorName": {"DescriptorIntact"},
		"native":         {"CaptureNativePools", "ReplaceNative"},
	}
	for key, list := range want {
		if !slices.Equal(sites[key], list) {
			t.Errorf("%s is reached in %v, want %v", key, sites[key], list)
		}
	}
	if registered == nil {
		t.Fatal("Registered is missing")
	}
	closedAt, liveAt := token.NoPos, token.NoPos
	ast.Inspect(registered.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			switch calleeName(call) {
			case "Closed":
				closedAt = call.Pos()
			case "live":
				liveAt = call.Pos()
			}
		}
		return true
	})
	if closedAt == token.NoPos || liveAt == token.NoPos || closedAt > liveAt {
		t.Error("Registered must test Closed before it asks the registry")
	}
}

// TestFoundationKeepsNoStateOfItsOwn pins the ownership rules of the package
// doc: no package-level variable and no init (no registration), no mutex value
// (the setup gate is the registry's, held by address), Handle with only
// unexported members, and no import of package cephfs or a cephfs feature
// package.
func TestFoundationKeepsNoStateOfItsOwn(t *testing.T) {
	for file, parsed := range parseProductionFiles(t) {
		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if strings.HasSuffix(path, "/cephfs") || strings.Contains(path, "/cephfs/") && !strings.Contains(path, "/cephfs/internal/") {
				t.Errorf("%s imports %s: the foundation must not depend on cephfs public types", file, path)
			}
		}
		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				if decl.Tok == token.VAR {
					t.Errorf("%s declares a package-level variable", file)
				}
				for _, spec := range decl.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range st.Fields.List {
						if sel, ok := field.Type.(*ast.SelectorExpr); ok {
							if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" {
								t.Errorf("%s: %s holds a sync value; the foundation adds no lock", file, ts.Name.Name)
							}
						}
						if ts.Name.Name == "Handle" {
							for _, name := range field.Names {
								if name.IsExported() {
									t.Errorf("Handle has the exported field %s", name.Name)
								}
							}
							if len(field.Names) == 0 {
								t.Error("Handle embeds a field, which would promote its members")
							}
						}
					}
				}
			case *ast.FuncDecl:
				if decl.Name.Name == "init" && decl.Recv == nil {
					t.Errorf("%s declares init", file)
				}
				if receiverName(decl) == "Handle" && decl.Name.IsExported() {
					t.Errorf("Handle has the exported method %s", decl.Name.Name)
				}
			}
		}
	}
}
