package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// membershipControlFields are the Container fields of the membership and
// control group that no hook may publish a pointer to. The locks are not in the
// list: UnitState still publishes the three lock domains, which the lock
// queueing tests need as lock identities (see the design note).
var membershipControlFields = map[string]bool{
	"closed": true, "controlPlane": true, "settings": true, "monitorTerminated": true,
	"manager": true, "monitors": true, "managers": true, "services": true,
	"extensions": true, "bootstrap": true,
}

// testStateFields is the exact field set of the unit-test state view in internal/testhelper. A new pointer needs a review of what it exposes.
var testStateFields = []string{"ConfigMutex", "ControlMutex", "Mutex", "NetworkRemoved", "PortLeases"}

func structFieldNames(t *testing.T, file, typeName string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	found := false
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != typeName {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("%s.%s is not a struct", file, typeName)
			}
			found = true
			for _, f := range st.Fields.List {
				for _, n := range f.Names {
					names = append(names, n.Name)
				}
			}
		}
	}
	if !found {
		t.Fatalf("%s does not declare %s", file, typeName)
	}
	slices.Sort(names)
	return names
}

// TestTestStateExposesNoMembershipControlPointer pins the field set of the
// unit-test state view in internal/testhelper and the result list of the one
// seam method behind it. The closed flag, the control plane and the options are
// not in it: they are written by the UnitSet* methods. Package ceph itself has
// no state-view struct.
func TestTestStateExposesNoMembershipControlPointer(t *testing.T) {
	if got := structFieldNames(t, filepath.Join("..", "internal", "testhelper", "testhelper.go"), "TestStateView"); !slices.Equal(got, testStateFields) {
		t.Fatalf("TestStateView fields = %v, pinned %v", got, testStateFields)
	}
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok {
				for _, spec := range gen.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == "testState" {
						t.Fatal("package ceph declares a test-state struct")
					}
				}
			}
		}
	}
	results := unitStateResults(t)
	if want := []string{"mu", "control", "config", "portLeases", "networkRemoved"}; !slices.Equal(results, want) {
		t.Fatalf("UnitState results = %v, pinned %v", results, want)
	}
}

// unitStateResults returns the result names of unitAccess.UnitState.
func unitStateResults(t *testing.T) []string {
	t.Helper()
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "UnitState" {
				continue
			}
			var names []string
			for _, field := range fn.Type.Results.List {
				if strings.Contains(types.ExprString(field.Type), "OSDContainer") {
					t.Fatal("UnitState returns OSD descriptors")
				}
				for _, name := range field.Names {
					names = append(names, name.Name)
				}
			}
			return names
		}
	}
	t.Fatal("UnitState not found: the guard is vacuous")
	return nil
}

// TestMembershipControlStateIsNeverAddressed guards that no production code in
// the package takes the address of a membership or control field, or of
// anything under one (c.settings.osds, c.services["x"]). An address is how the
// old UnitState let another package write the closed flag and the control plane
// without a lock.
func TestMembershipControlStateIsNeverAddressed(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				u, ok := n.(*ast.UnaryExpr)
				if !ok || u.Op != token.AND {
					return true
				}
				if name := membershipFieldUnder(u.X); name != "" {
					problems = append(problems, file+": "+fn.Name.Name+" takes the address of "+name)
				}
				return true
			})
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("membership/control state is addressed: %v", problems)
	}
}

// membershipFieldUnder returns the membership/control field that e selects
// from a cluster receiver, directly or through nested selectors and indexes.
func membershipFieldUnder(e ast.Expr) string {
	for {
		switch v := e.(type) {
		case *ast.ParenExpr:
			e = v.X
		case *ast.IndexExpr:
			e = v.X
		case *ast.StarExpr:
			e = v.X
		case *ast.SelectorExpr:
			if membershipControlFields[v.Sel.Name] && isContainerReceiver(v.X) {
				return v.Sel.Name
			}
			e = v.X
		default:
			return ""
		}
	}
}

// TestClosedWritersAreAudited lists every function that writes c.closed
// (assignment, or a key of a Container literal). The modes are "terminate"
// (terminate holds Container.mu), "build" (a cluster no other goroutine uses
// yet) and "hook" (a TestSet* hook whose doc names the lock the caller holds).
// A new writer, or a stale row, fails. The control-plane handle left this table
// when it moved into controlPlaneState; its writers are pinned by
// TestControlPlaneHandleHasAuditedWriters in control_plane_state_test.go.
func TestClosedWritersAreAudited(t *testing.T) {
	type key struct{ fn, field string }
	want := map[key]string{
		{"terminate", "closed"}:     "terminate",
		{"UnitCluster", "closed"}:   "build",
		{"UnitSetClosed", "closed"}: "hook",
	}
	docs := osdRegistryDocs(t)
	got := map[key]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			note := func(field string) {
				k := key{fn.Name.Name, field}
				got[k] = true
				mode, known := want[k]
				if !known {
					problems = append(problems, fn.Name.Name+" writes "+field+" but is not in the writer table")
					return
				}
				if mode == "hook" && !(strings.Contains(docs[fn.Name.Name], "holds") && strings.Contains(docs[fn.Name.Name], "not shared")) {
					problems = append(problems, fn.Name.Name+" is a hook row but its doc comment does not name the lock contract")
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "closed" && isContainerReceiver(sel.X) {
							note("closed")
						}
					}
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, elt := range x.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "closed" {
									note(key.Name)
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	for k := range want {
		if !got[k] {
			problems = append(problems, k.fn+" is listed for "+k.field+" but no longer writes it")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("closed writers changed: %v", problems)
	}
}

// callsMethod reports whether fn calls <recv>.<method>() on a selector named
// recv, such as s.mu.Lock().
func callsMethod(fn *ast.FuncDecl, recv, method string) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == method {
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == recv {
					found = true
				}
			}
		}
		return true
	})
	return found
}

// testSetHooks pins the TestSet* methods of serviceHooks and the one field set
// each assigns. A hook calls nothing: it is a plain write for a caller that
// holds the lock or owns the cluster.
var testSetHooks = map[string][]string{
	"UnitSetClosed":         {"closed"},
	"UnitSetControlPlane":   {"handle"},
	"UnitSetConfig":         {"config"},
	"UnitSetKeyring":        {"keyring"},
	"UnitSetCephVersion":    {"cephVersion"},
	"UnitSetInitialOSDs":    {"osds"},
	"UnitSetPoolDefaults":   {"poolMinSize", "poolReplicas"},
	"UnitSetStartupTimeout": {"startupTimeout"},
	"UnitSetCephFSImage":    {"cephfsOverride"},
}

// TestTestSetHooksAreSingleWritesWithoutCalls pins the method set of the
// TestSet* hooks, the fields each assigns, that none makes a call or takes a
// lock, and that each documents the lock contract.
func TestTestSetHooksAreSingleWritesWithoutCalls(t *testing.T) {
	docs := osdRegistryDocs(t)
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "unitAccess" || !strings.HasPrefix(fn.Name.Name, "UnitSet") {
				continue
			}
			name := fn.Name.Name
			want, known := testSetHooks[name]
			if !known {
				problems = append(problems, name+" is not pinned; review what it writes and update testSetHooks")
				continue
			}
			seen[name] = true
			var assigned []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					problems = append(problems, name+" makes a call")
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok {
							assigned = append(assigned, sel.Sel.Name)
						} else {
							problems = append(problems, name+" assigns something other than a field")
						}
					}
				}
				return true
			})
			slices.Sort(assigned)
			if !slices.Equal(assigned, want) {
				problems = append(problems, name+" assigns "+strings.Join(assigned, ",")+", pinned "+strings.Join(want, ","))
			}
			if !strings.Contains(docs[name], "holds") || !strings.Contains(docs[name], "not shared") {
				problems = append(problems, name+" does not document the lock contract")
			}
		}
	}
	for name := range testSetHooks {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("TestSet hooks changed: %v", slices.Compact(problems))
	}
}

// TestTestSetHooksAreCalledOnlyFromTests is the cross-package caller table: no
// non-test source file in the module calls a testhelper.TestSet* function or a
// UnitSet* seam method. The wrappers in internal/testhelper are the only callers; production code
// that wrote the closed flag or the control plane through them would bypass the
// terminate writer and the control-plane writers audited in control_plane_state_test.go.
func TestTestSetHooksAreCalledOnlyFromTests(t *testing.T) {
	var problems []string
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "testhelper" && strings.HasPrefix(sel.Sel.Name, "TestSet") {
				problems = append(problems, filepath.ToSlash(path))
			}
			// get().UnitSetX(...) inside the helper's wrappers is the definition.
			if strings.HasPrefix(sel.Sel.Name, "UnitSet") && !strings.HasSuffix(filepath.ToSlash(path), "internal/testhelper/testhelper.go") {
				problems = append(problems, filepath.ToSlash(path))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 {
		t.Fatalf("non-test code calls a TestSet hook: %v", problems)
	}
}
