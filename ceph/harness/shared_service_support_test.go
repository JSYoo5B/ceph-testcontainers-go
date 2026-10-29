package harness

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Controls for the instance-owned service support of a SharedCluster (#155).
// The registry characterization is in shared_services_registry_test.go.

func TestSharedClustersOwnDifferentServiceSets(t *testing.T) {
	a, b := &serviceCalls{}, &serviceCalls{}
	withA := newFakeShared(&fakeFleet{}, withSharedServices(a.service("a", nil, map[string]string{"a res": "x"}, nil)))
	withB := newFakeShared(&fakeFleet{}, withSharedServices(b.service("b", nil, nil, nil)))
	bare := newFakeShared(&fakeFleet{})
	for _, shared := range []*SharedCluster{withA, withB, bare} {
		tb := newFakeTB(t)
		if shared.Lease(tb) == nil {
			t.Fatalf("lease failed: %s", tb.errorText())
		}
		tb.finish()
	}
	if !slices.Equal(a.prepared, []string{"a"}) || !slices.Equal(b.prepared, []string{"b"}) {
		t.Fatalf("prepared a=%v b=%v: each cluster prepares only its own services", a.prepared, b.prepared)
	}
	if len(a.probed) == 0 || slices.Contains(b.probed, "a") {
		t.Fatalf("probed a=%v b=%v", a.probed, b.probed)
	}
}

func TestSharedExplicitEmptySetPreparesAndProbesNothing(t *testing.T) {
	calls := &serviceCalls{}
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	if tenant == nil {
		t.Fatal(tb.errorText())
	}
	if err := tenant.requireService("rbd"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a missing support returned %v", err)
	}
	tb.finish()
	if len(calls.prepared)+len(calls.probed) != 0 {
		t.Fatal("an unconfigured cluster ran a service callback")
	}
}

func TestSharedServiceSetValidation(t *testing.T) {
	for name, services := range map[string][]sharedService{
		"duplicate kind": {{kind: "rbd"}, {kind: "rbd"}},
		"zero value":     {{}},
	} {
		t.Run(name, func(t *testing.T) {
			fleet := &fakeFleet{}
			shared := newFakeShared(fleet, withSharedServices(services...))
			if _, err := shared.lease(t.Context(), leaseSettings{holder: "t"}, t.Logf); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("lease returned %v", err)
			}
			if _, err := shared.exclusiveLease(t.Context(), "t", t.Logf); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("exclusive lease returned %v", err)
			}
			if len(fleet.clusters()) != 0 {
				t.Fatal("an invalid service set started a cluster")
			}
		})
	}
	t.Run("repeated option", func(t *testing.T) {
		shared := newFakeShared(&fakeFleet{}, withSharedServices(sharedService{kind: "rbd"}), withSharedServices(sharedService{kind: "rbd"}))
		if _, err := shared.lease(t.Context(), leaseSettings{holder: "t"}, t.Logf); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("lease returned %v", err)
		}
	})
	t.Run("order does not matter", func(t *testing.T) {
		set, err := newSharedServiceSet([]sharedService{{kind: "rgw"}, {kind: "cephfs"}, {kind: "rbd"}})
		if err != nil || !set.has("rbd") || set.has("other") {
			t.Fatalf("set %v, %v", set, err)
		}
		var kinds []string
		for _, s := range set.services {
			kinds = append(kinds, s.kind)
		}
		if !slices.Equal(kinds, []string{"cephfs", "rbd", "rgw"}) {
			t.Fatalf("kinds %v", kinds)
		}
	})
}

func TestSharedRequireServiceThroughThePort(t *testing.T) {
	shared := newFakeShared(&fakeFleet{}, withSharedServices(sharedService{kind: "rbd"}))
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	// The model has no cluster, so the port is built over the tenant alone;
	// RequireService reads only the instance's set.
	var port tenantPort = tenantLease{t: tenant}
	if err := port.RequireService("rbd"); err != nil {
		t.Fatal(err)
	}
	if err := port.RequireService("cephfs"); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "cephfs") {
		t.Fatalf("missing support returned %v", err)
	}
	tb.finish()
}

func TestSharedServicePartialFailures(t *testing.T) {
	t.Run("prepare failure fails the start and terminates the partial cluster", func(t *testing.T) {
		cause := errors.New("no zone")
		fleet := &fakeFleet{}
		after := &serviceCalls{}
		shared := newFakeShared(fleet, withSharedServices(
			(&serviceCalls{}).service("a", cause, nil, nil), after.service("b", nil, nil, nil)))
		tb := newFakeTB(t)
		if shared.Lease(tb) != nil || !strings.Contains(tb.errorText(), "prepare a for a shared cluster: no zone") {
			t.Fatalf("lease: %q", tb.errorText())
		}
		if len(after.prepared) != 0 {
			t.Fatal("a service after the failing one was prepared")
		}
		clusters := fleet.clusters()
		if len(clusters) != 1 || !clusters[0].isTerminated() {
			t.Fatal("the partial cluster was not terminated")
		}
	})
	t.Run("probe failure at release poisons", func(t *testing.T) {
		cause := errors.New("gateway gone")
		fail := false
		svc := sharedService{kind: "a", probe: func(context.Context, *ceph.Container) (map[string]string, error) {
			if fail {
				return nil, cause
			}
			return nil, nil
		}}
		shared := newFakeShared(&fakeFleet{}, withSharedServices(svc))
		tb := newFakeTB(t)
		tenant := shared.Lease(tb)
		first := backendOf(tenant)
		fail = true
		tb.finish()
		if !strings.Contains(strings.Join(tb.logs, "\n")+tb.errorText(), "list the a resources: gateway gone") || !first.isTerminated() || shared.Stats().Poisonings != 1 {
			t.Fatalf("a failed probe did not poison: %q %q", tb.logs, tb.errorText())
		}
	})
}

func TestSharedServiceSetHoldsNoLock(t *testing.T) {
	// The set is immutable after NewShared and is read without a lock.
	typ := reflect.TypeFor[sharedServiceSet]()
	for i := range typ.NumField() {
		if strings.Contains(typ.Field(i).Type.String(), "sync.") {
			t.Fatalf("sharedServiceSet field %s holds a lock", typ.Field(i).Name)
		}
	}
}

// functionsCalling returns, per file of dir, the names of the functions whose
// body calls an identifier or selector named one of names.
func functionsCalling(t *testing.T, src map[string]string, names ...string) []string {
	t.Helper()
	var got []string
	fset := token.NewFileSet()
	for file, text := range src {
		f, err := parser.ParseFile(fset, file, text, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name string
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					name = fun.Name
				case *ast.SelectorExpr:
					name = fun.Sel.Name
				}
				if slices.Contains(names, name) {
					got = append(got, file+": "+fn.Name.Name+" -> "+name)
				}
				return true
			})
		}
	}
	slices.Sort(got)
	return got
}

func nonTestSources(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := map[string]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		src[filepath.Base(file)] = string(b)
	}
	return src
}

// isSelector reports whether sel is pkg.name.
func isSelector(sel *ast.SelectorExpr, pkg, name string) bool {
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == name
}

func TestServiceAdaptersRequireTheirSupportBeforeCreating(t *testing.T) {
	// Each adapter Lease calls RequireService with its own kind constant,
	// after taking the port and before AttachScope or any command.
	for dir, kind := range map[string]string{"rbd": "RBD", "cephfs": "CephFS", "rgw": "RGW"} {
		src := nonTestSources(t, filepath.Join("..", "..", dir, "harness"))["tenant.go"]
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "tenant.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "Lease" || fn.Recv != nil {
				continue
			}
			var order []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "RequireService":
					if sel, ok := call.Args[0].(*ast.SelectorExpr); !ok || !isSelector(sel, "servicekind", kind) {
						t.Errorf("%s.Lease requires a service other than servicekind.%s", dir, kind)
					}
					order = append(order, "require")
				case "AttachScope", "Command":
					order = append(order, "create")
				}
				return true
			})
			if len(order) == 0 || order[0] != "require" {
				t.Errorf("%s.Lease order %v: RequireService must come first", dir, order)
			}
			found = true
		}
		if !found {
			t.Errorf("%s has no Lease", dir)
		}
	}
}
