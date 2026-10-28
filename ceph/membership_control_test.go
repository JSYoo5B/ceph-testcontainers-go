package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// membershipControlFields are the Container fields of the membership and
// control group that no hook may publish a pointer to. The locks are not in the
// list: TestState still publishes the three lock domains, which the lock
// queueing tests need as lock identities (see the design note).
var membershipControlFields = map[string]bool{
	"closed": true, "controlPlane": true, "settings": true, "monitorTerminated": true,
	"manager": true, "monitors": true, "managers": true, "services": true,
	"extensions": true,
}

// testStateFields is the exact field set of the unit-test state view in ceph and
// in internal/servicehook. A new pointer needs a review of what it exposes.
var testStateFields = []string{"CephVersion", "Config", "ConfigMutex", "ControlMutex", "Keyring", "Mutex", "NetworkRemoved", "PortLeases"}

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
// unit-test state view on both sides of the hook. The closed flag, the control
// plane and the options are not in it: they are written by the TestSet* hooks.
func TestTestStateExposesNoMembershipControlPointer(t *testing.T) {
	if got := structFieldNames(t, "service_hooks.go", "testState"); !slices.Equal(got, testStateFields) {
		t.Fatalf("testState fields = %v, pinned %v", got, testStateFields)
	}
	if got := structFieldNames(t, filepath.Join("..", "internal", "servicehook", "servicehook.go"), "TestStateView"); !slices.Equal(got, testStateFields) {
		t.Fatalf("TestStateView fields = %v, pinned %v", got, testStateFields)
	}
}

// TestMembershipControlStateIsNeverAddressed guards that no production code in
// the package takes the address of a membership or control field, or of
// anything under one (c.settings.osds, c.services["x"]). An address is how the
// old TestState let another package write the closed flag and the control plane
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

// TestClosedAndControlPlaneWritersAreAudited lists every function that writes
// c.closed or c.controlPlane (assignment, or a key of a Container literal). The
// terminate function appears under both fields; the table keys writers by name
// and field together. The modes are "terminate" (terminate holds Container.mu
// and takes controlMu around the control plane), "takes" (the function takes
// controlMu around the write), "build" (a cluster no other goroutine uses yet)
// and "hook" (a TestSet* hook whose doc names the lock the caller holds). A new
// writer, or a stale row, fails.
func TestClosedAndControlPlaneWritersAreAudited(t *testing.T) {
	type key struct{ fn, field string }
	want := map[key]string{
		{"terminate", "closed"}:                 "terminate",
		{"terminate", "controlPlane"}:           "terminate",
		{"TestCluster", "closed"}:               "build",
		{"TestSetClosed", "closed"}:             "hook",
		{"ensureControlPlane", "controlPlane"}:  "takes",
		{"TestSetControlPlane", "controlPlane"}: "hook",
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
				if mode == "takes" && !callsMethod(fn, "controlMu", "Lock") {
					problems = append(problems, fn.Name.Name+" must take controlMu around its write")
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						for _, field := range []string{"closed", "controlPlane"} {
							if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == field && isContainerReceiver(sel.X) {
								note(field)
							}
						}
					}
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, elt := range x.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && (key.Name == "closed" || key.Name == "controlPlane") {
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
		t.Fatalf("closed/controlPlane writers changed: %v", problems)
	}
}

// callsMethod reports whether fn calls <recv>.<method>() on a selector named
// recv, such as c.controlMu.Lock().
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
	"TestSetClosed":         {"closed"},
	"TestSetControlPlane":   {"controlPlane"},
	"TestSetInitialOSDs":    {"osds"},
	"TestSetPoolDefaults":   {"poolMinSize", "poolReplicas"},
	"TestSetStartupTimeout": {"startupTimeout"},
	"TestSetCephFSImage":    {"cephfsOverride"},
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
			if !ok || fn.Body == nil || receiverTypeName(fn) != "serviceHooks" || !strings.HasPrefix(fn.Name.Name, "TestSet") {
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
// non-test source file in the module calls a servicehook.TestSet* function. The
// wrappers in internal/servicehook are the only definitions; production code
// that wrote the closed flag or the control plane through them would bypass the
// terminate and ensureControlPlane writers audited above.
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
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(sel.Sel.Name, "TestSet") {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "servicehook" {
					problems = append(problems, filepath.ToSlash(path))
				}
				// get().TestSetX(...) inside the wrappers themselves is the definition.
				if _, ok := sel.X.(*ast.CallExpr); ok && !strings.HasSuffix(filepath.ToSlash(path), "internal/servicehook/servicehook.go") {
					problems = append(problems, filepath.ToSlash(path))
				}
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
