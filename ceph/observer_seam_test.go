package ceph

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"testing"
)

// tenantUses lists where src names the identifier Tenant in code, as
// "<enclosing declaration>", comments excluded.
func tenantUses(t *testing.T, name, src string) []string {
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
			if id, ok := n.(*ast.Ident); ok && id.Name == "Tenant" {
				uses = append(uses, label)
			}
			return true
		})
	}
	return uses
}

// The Observer and the Pool lease stay in package ceph when the harness moves
// out, so their files must not name the harness Tenant. ObserveTenant is the
// tenant side and lives in tenant_observe.go.
func TestObserverAndPoolLeaseNameNoTenant(t *testing.T) {
	for _, file := range []string{"observer.go", "pool.go", "pool_handle.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if uses := tenantUses(t, file, string(src)); len(uses) != 0 {
			t.Errorf("%s names Tenant in %q; hand the Observer a lease check instead", file, uses)
		}
	}
}

// Negative control: the checker sees a Tenant in a field, a parameter and a
// method receiver, and ignores comments.
func TestTenantUseCheckerSeesARegression(t *testing.T) {
	bad := `package ceph
type Observer struct { tenant *Tenant }
type poolLease struct{ tenant *Tenant }
// Tenant in a comment is ignored.
func ObserveTenant(tenant *Tenant) {}
func (t *Tenant) container() {}`
	got := tenantUses(t, "bad.go", bad)
	for _, want := range []string{"Observer", "poolLease", "ObserveTenant", "container"} {
		if !slices.Contains(got, want) {
			t.Errorf("checker missed %s in %q", want, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("checker found %q, want exactly the four code uses", got)
	}
}

// A leased Observer re-checks the lease it was given on every query, and the
// Pool values it lists check the same lease on every operation. The test drives
// the seam with a bare lease check, without any tenant.
func TestLeasedObserverChecksTheGivenLeaseOnEveryQueryAndPoolOperation(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	cluster := poolFixtureCluster(ctr, 3)
	cluster.sharing.Store(int32(sharedWithTenants))
	errExpired := invalidRequest("lease expired")
	var expired bool
	checks := 0
	live := func() error {
		checks++
		if expired {
			return errExpired
		}
		return nil
	}
	target := func() (*Container, error) {
		if err := live(); err != nil {
			return nil, err
		}
		return cluster, nil
	}
	observer := newLeasedObserver(target, live)
	if checks != 0 {
		t.Fatalf("construction checked the lease %d times", checks)
	}
	pools, err := observer.Pools(t.Context())
	if err != nil || len(pools) != 1 {
		t.Fatalf("Pools = %v, %v", pools, err)
	}
	pool := pools[0]
	if pool.lease == nil {
		t.Fatal("a pool of a leased observer carries no lease")
	}
	before := checks
	if _, err := pool.Status(t.Context()); err != nil {
		t.Fatalf("a read through a live lease failed: %v", err)
	}
	if checks <= before {
		t.Fatal("a pool operation did not re-check the lease")
	}
	if _, err := observer.Pools(t.Context()); err != nil {
		t.Fatalf("a query through a live lease failed: %v", err)
	}

	expired = true
	calls := len(ctr.Calls)
	if _, err := observer.Configuration(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("Configuration after expiry returned %v, want ErrInvalidRequest", err)
	}
	if _, err := observer.Pools(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("Pools after expiry returned %v, want ErrInvalidRequest", err)
	}
	requireNoPoolOperationRuns(t, pool, ctr, ErrInvalidRequest)
	if len(ctr.Calls) != calls {
		t.Fatalf("an expired lease reached the cluster: %v", ctr.Calls[calls:])
	}
}

// The cluster form has no lease: its observer and pools carry none.
func TestClusterObserverCarriesNoLease(t *testing.T) {
	observer, err := Observe(poolFixtureCluster(policyFixture("2", "1", "1"), 3))
	if err != nil {
		t.Fatal(err)
	}
	if observer.live != nil {
		t.Fatal("the cluster form has a lease check")
	}
	pools, err := observer.Pools(t.Context())
	if err != nil || len(pools) != 1 || pools[0].lease != nil {
		t.Fatalf("Pools = %v, %v", pools, err)
	}
}
