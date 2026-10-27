package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// recordingOps is a clusterOps that records which operation a tenant reached.
// Operations a test does not override panic through the nil embedded
// interface, so a forwarder that reaches the wrong one fails loudly.
type recordingOps struct {
	clusterOps
	mu       sync.Mutex
	calls    []string
	rawValue *Container
}

func (r *recordingOps) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordingOps) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recordingOps) startupTimeout() time.Duration {
	r.record("startupTimeout")
	return 7 * time.Second
}

func (r *recordingOps) command(_ context.Context, args ...string) ([]byte, error) {
	r.record("command " + args[0])
	return []byte("out"), nil
}

func (r *recordingOps) withTopology(fn func() error) error {
	r.record("withTopology")
	return fn()
}

func (r *recordingOps) extension(kind string, create func() any) any {
	r.record("extension " + kind)
	return create()
}

func (r *recordingOps) raw() *Container { r.record("raw"); return r.rawValue }

// opsShared is a fakeShared whose cluster operations are a recordingOps, as a
// real backend's containerOps would be.
type opsShared struct {
	*fakeShared
	o clusterOps
}

func (b opsShared) ops() clusterOps { return b.o }

// newOpsShared returns a shared cluster of models that hand out ops.
func newOpsShared(ops clusterOps) *SharedCluster {
	fleet := &fakeFleet{}
	return newFakeShared(fleet, func(s *sharedSettings) {
		inner := s.start
		s.start = func(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error) {
			backend, err := inner(ctx, image, opts)
			if backend == nil {
				return nil, err
			}
			return opsShared{fakeShared: backend.(*fakeShared), o: ops}, err
		}
	})
}

func TestTenantReachesTheClusterOnlyThroughItsOps(t *testing.T) {
	ops := &recordingOps{}
	shared := newOpsShared(ops)
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	ctx := t.Context()

	// The tenant's observations go through the Observer, which takes the
	// gated cluster from the ops once for each query.
	observer, err := ObserveTenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observer.cluster(); err != nil {
		t.Fatal(err)
	}
	want := []string{"raw"}
	if got := ops.recorded(); !slices.Equal(got, want) {
		t.Fatalf("calls = %q, want %q", got, want)
	}

	port, err := (serviceHooks{}).TenantPort(tenant)
	if err != nil {
		t.Fatal(err)
	}
	if port.StartupTimeout() != 7*time.Second {
		t.Fatal("StartupTimeout did not come from the ops")
	}
	if out, err := port.Command(ctx, "rbd", "ls"); err != nil || string(out) != "out" {
		t.Fatalf("Command = %q, %v", out, err)
	}
	if err := port.WithTopology(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_ = port.Extension("kind", func() any { return 1 })
	tail := ops.recorded()[len(want):]
	if wantTail := []string{"startupTimeout", "command rbd", "withTopology", "extension kind"}; !slices.Equal(tail, wantTail) {
		t.Fatalf("port calls = %q, want %q", tail, wantTail)
	}

	// Negative control for admission: once released, no view or port taken
	// afterwards reaches the cluster.
	tb.finish()
	before := len(ops.recorded())
	if _, err := observer.cluster(); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a query after release returned %v", err)
	}
	if _, err := observer.Pools(ctx); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Pools after release returned %v", err)
	}
	if _, err := (serviceHooks{}).TenantPort(tenant); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("TenantPort after release returned %v", err)
	}
	if got := ops.recorded(); len(got) != before {
		t.Fatalf("a released tenant reached the cluster: %q", got[before:])
	}
}

func TestTenantOfAModelWithoutOpsRefusesClusterViews(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tenant := shared.Lease(newFakeTB(t))
	ctx := t.Context()
	checks := map[string]error{}
	observer, _ := ObserveTenant(tenant)
	_, checks["Status"] = observer.Status(ctx)
	_, checks["Pools"] = observer.Pools(ctx)
	_, checks["OSDFlags"] = observer.OSDFlags(ctx)
	_, checks["TenantPort"] = (serviceHooks{}).TenantPort(tenant)
	for name, err := range checks {
		if !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s without a cluster returned %v", name, err)
		}
	}
	if (serviceHooks{}).TenantCluster(tenant) != nil {
		t.Error("TenantCluster returned a cluster for a model")
	}
}

func TestExclusiveLendsTheClusterThatTheBackendNames(t *testing.T) {
	sentinel := &Container{}
	ops := &recordingOps{rawValue: sentinel}
	shared := newOpsShared(ops)
	tb := newFakeTB(t)
	if got := shared.Exclusive(tb); got != sentinel {
		t.Fatalf("Exclusive returned %p, want the backend's cluster %p", got, sentinel)
	}
	tb.finish()

	// A model without ops lends nothing, as it had no container.
	bare := newFakeShared(&fakeFleet{})
	tb = newFakeTB(t)
	if got := bare.Exclusive(tb); got != nil {
		t.Fatalf("Exclusive of a model returned %v", got)
	}
	tb.finish()
}

// containerUses lists where src names the identifier Container in code, as
// "<enclosing declaration>", comments excluded.
func containerUses(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var uses []string
	for _, decl := range file.Decls {
		label := ""
		switch d := decl.(type) {
		case *ast.FuncDecl:
			label = d.Name.Name
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok {
					label = ts.Name.Name
				}
			}
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			// testcontainers.Container is the Docker container type.
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "testcontainers" {
					return false
				}
			}
			if id, ok := n.(*ast.Ident); ok && id.Name == "Container" {
				uses = append(uses, label)
			}
			return true
		})
	}
	return uses
}

func TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList(t *testing.T) {
	// The orchestration reaches a cluster through sharedBackend and
	// clusterOps. The only places that still name *Container are the raw
	// handle that Exclusive lends on purpose and the service hooks, whose
	// registered prepare and probe functions take the cluster.
	allowed := map[string][]string{
		"shared.go":           nil,
		"shared_exclusive.go": {"Exclusive"},
		"shared_services.go":  {"sharedService"},
		"tenant.go":           nil,
	}
	for file, allow := range allowed {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, use := range containerUses(t, file, string(src)) {
			if !slices.Contains(allow, use) {
				t.Errorf("%s: %s names Container; reach the cluster through clusterOps", file, use)
			}
		}
	}
	// tenantLease keeps clusterOps, not a *Container.
	src, err := os.ReadFile("service_hooks.go")
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(containerUses(t, "service_hooks.go", string(src)), "tenantLease") {
		t.Error("tenantLease holds a Container")
	}
}

// Negative control: the checker sees a Container in the places the policy
// forbids, so a regression of the routing would fail the test above.
func TestContainerUseCheckerSeesARegression(t *testing.T) {
	bad := `package ceph
type tenantLease struct { t *Tenant; c *Container }
func (t *Tenant) Status() { _ = t.inst.backend.(*Container) }
// Container in a comment is ignored.
func (s *SharedCluster) Exclusive() *Container { return nil }`
	got := containerUses(t, "bad.go", bad)
	for _, want := range []string{"tenantLease", "Status", "Exclusive"} {
		if !slices.Contains(got, want) {
			t.Errorf("checker missed %s in %q", want, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("checker found %q, want exactly the three code uses", got)
	}
}
