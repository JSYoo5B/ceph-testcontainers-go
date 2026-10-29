package harness

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Controls for the explicit service selection (issue 155, owner decisions of
// 2026-10-28 and 2026-10-29): the process-global registry and the three init
// registrations are gone, WithServices is the only input, and a service Lease
// on a cluster that was not given its support is refused before it has an
// effect. They run without Docker.

// removedRegistryNames are the declarations of the former registry and its
// hook; no non-test file of the module may declare or call one.
var removedRegistryNames = []string{
	"RegisterSharedService", "registerSharedService", "registeredSharedServices",
	"transitionalRegisteredServices", "withSharedServices", "sharedServices",
}

// registryUses returns the identifiers of src that name a removed registry
// declaration, and, for a file of a service harness package, any init function.
func registryUses(t *testing.T, name, src string, serviceHarness bool) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if slices.Contains(removedRegistryNames, x.Name) {
				found = append(found, x.Name)
			}
		case *ast.FuncDecl:
			if serviceHarness && x.Recv == nil && x.Name.Name == "init" {
				found = append(found, "init")
			}
		}
		return true
	})
	return found
}

func TestNoServiceRegistryOrInitRegistrationRemains(t *testing.T) {
	root := filepath.Join("..", "..")
	serviceDirs := []string{"rbd/harness", "cephfs/harness", "rgw/harness"}
	var offenders []string
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
		inServiceHarness := slices.Contains(serviceDirs, filepath.ToSlash(filepath.Dir(rel)))
		if uses := registryUses(t, rel, string(b), inServiceHarness); len(uses) > 0 {
			offenders = append(offenders, rel+": "+strings.Join(uses, ","))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("a service registry or an init registration is back: %q", offenders)
	}
}

// Negative control: the scan sees the old registration shapes.
func TestRegistryScanSeesTheOldShapes(t *testing.T) {
	old := `package rbd
import "x/servicehook"
func init() { servicehook.RegisterSharedService("rbd", nil, probe) }
var sharedServices struct{}
`
	if got := registryUses(t, "old.go", old, true); len(got) != 3 {
		t.Fatalf("scan found %q", got)
	}
	if got := registryUses(t, "ok.go", "package harness\nfunc Support() int { return 1 }\n", true); len(got) != 0 {
		t.Fatalf("scan rejected a clean file: %q", got)
	}
	// An init outside a service harness is not an offence by itself.
	if got := registryUses(t, "core.go", "package harness\nfunc init() {}\n", false); len(got) != 0 {
		t.Fatalf("scan rejected an unrelated init: %q", got)
	}
}

// TestUnconfiguredClusterPreparesAndProbesNothing: a cluster built without
// WithServices has an empty set, whatever packages the binary links.
func TestUnconfiguredClusterHasNoServices(t *testing.T) {
	set := NewShared("img").settings.services
	if len(set.services) != 0 || len(set.prepares) != 0 || len(set.probes) != 0 || set.has("rbd") {
		t.Fatalf("an unconfigured cluster owns services: %+v", set)
	}
}

func TestWithServicesIsTheOnlyInputAndValidatesAtConstruction(t *testing.T) {
	hooks := tenantHooks{}
	rbd := hooks.NewServiceSupport("rbd", nil, nil)
	cephfs := hooks.NewServiceSupport("cephfs", nil, nil)
	set := NewShared("img", WithServices(rbd, cephfs)).settings
	if set.err != nil || !set.services.has("rbd") || !set.services.has("cephfs") || set.services.has("rgw") {
		t.Fatalf("settings err=%v services=%+v", set.err, set.services)
	}
	for name, supports := range map[string][]ServiceSupport{
		"zero value":    {{}},
		"given twice":   {rbd, rbd},
		"zero and real": {rbd, {}},
	} {
		shared := NewShared("img", WithServices(supports...))
		if _, err := shared.lease(t.Context(), leaseSettings{holder: "t"}, t.Logf); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: lease returned %v, want ErrInvalidRequest", name, err)
		}
	}
	// Repeated options add up; two clusters of one binary differ.
	both := NewShared("img", WithServices(rbd), WithServices(cephfs)).settings.services
	other := NewShared("img", WithServices(cephfs)).settings.services
	if !both.has("rbd") || other.has("rbd") {
		t.Fatalf("clusters do not own separate sets: %v %v", both.has("rbd"), other.has("rbd"))
	}
}

// TestServiceLeaseWithoutSupportIsRefusedBeforeAnyEffect follows the first
// steps of a service Lease on a cluster that was not given the service: the
// port refuses, and neither a command nor a scope nor a client was reached.
func TestServiceLeaseWithoutSupportIsRefusedBeforeAnyEffect(t *testing.T) {
	ops := &recordingOps{}
	shared := newOpsShared(ops)
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	port, err := (tenantHooks{}).TenantPort(tenant)
	if err != nil {
		t.Fatal(err)
	}
	before := len(ops.recorded())
	if err := port.RequireService("rbd"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("RequireService on an unconfigured cluster returned %v", err)
	}
	if got := ops.recorded(); len(got) != before {
		t.Fatalf("the refusal reached the cluster: %q", got[before:])
	}
	tenant.mu.Lock()
	scopes, clients := len(tenant.scopes), len(tenant.clients)
	tenant.mu.Unlock()
	if scopes != 0 || clients != 1 {
		t.Fatalf("scopes=%d clients=%d, want no scope and only the tenant's own client", scopes, clients)
	}
	tb.finish()

	// With the support, the same step passes.
	configured := newFakeShared(&fakeFleet{}, func(s *sharedSettings) {
		inner := s.start
		s.start = func(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error) {
			backend, err := inner(ctx, image, opts)
			if backend == nil {
				return nil, err
			}
			return opsShared{fakeShared: backend.(*fakeShared), o: ops}, err
		}
	}, WithServices((tenantHooks{}).NewServiceSupport("rbd", nil, nil)))
	tb = newFakeTB(t)
	tenant = configured.Lease(tb)
	port, err = (tenantHooks{}).TenantPort(tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := port.RequireService("rbd"); err != nil {
		t.Fatalf("RequireService with the support returned %v", err)
	}
	if err := port.RequireService("rgw"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a service that was not given returned %v", err)
	}
	tb.finish()
}

// lendCallers lists, per enclosing function, the calls of Lend and of the raw
// accessor of clusterOps in src.
func lendCallers(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "raw" || sel.Sel.Name == "Lend") {
				found = append(found, fn.Name.Name+" -> "+sel.Sel.Name)
			}
			return true
		})
	}
	slices.Sort(found)
	return found
}

// TestLendHasOnlyTheAllowedCallers pins where the harness may obtain the
// gated cluster: the exclusive lease, the tenant Observer path and the
// test-only lookup, plus the adapter that forwards to the core.
func TestLendHasOnlyTheAllowedCallers(t *testing.T) {
	want := map[string][]string{
		"core.go":             {"raw -> Lend"},
		"shared_exclusive.go": {"Exclusive -> raw"},
		"tenant.go":           {"container -> raw"},
		"tenant_port.go":      {"TenantCluster -> raw"},
	}
	for file, src := range nonTestSources(t, ".") {
		got := lendCallers(t, file, src)
		if !slices.Equal(got, want[file]) {
			t.Errorf("%s obtains the cluster as %q, want %q", file, got, want[file])
		}
	}
	// Negative control: a new caller is reported.
	bad := "package harness\nfunc (t *Tenant) Peek() { _ = t.inst.backend.ops().raw() }\n"
	if got := lendCallers(t, "bad.go", bad); !slices.Equal(got, []string{"Peek -> raw"}) {
		t.Fatalf("checker found %q", got)
	}
}
