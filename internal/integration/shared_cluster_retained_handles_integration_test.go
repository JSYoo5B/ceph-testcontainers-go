//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_shared_cluster_retained_handles))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephharness "github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
)

// TestSharedClusterRetainedHandles observes what handles that outlive their
// interval do on a real shared cluster (issue 155, design note section
// "Exclusive stale-handle audit"). A tenant retains its client descriptors
// and a pool, the exclusive interval takes the same cluster, and the interval
// retains the lent Container, handles derived from it and the embedded
// container. The retained handles are called while the interval is active and
// after its release, and each outcome is logged.
//
// It asserts the properties of the gate: a descriptor or pool that a tenant
// obtained is refused by the gate in the exclusive interval, and everything
// that the interval lent is refused by the gate once the release retired the
// cluster. The embedded container is not behind the gate, so its outcome is
// recorded and only required to show that the terminated cluster does not run
// the command. A run of this test on one runner is an observation, not a
// proof that every operation fails safely on a terminated cluster.
func TestSharedClusterRetainedHandles(t *testing.T) {
	shared := newSharedCluster(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()

	// A tenant retains a client descriptor of each origin and a pool listed
	// through its Observer.
	tenantTB := newNamedTB(t, "tenant")
	tenant := shared.Lease(tenantTB)
	if tenant == nil {
		t.Fatalf("lease: %s", tenantTB.errorText())
	}
	fsid := sharedFSID(t, ctx, tenant)
	own := tenant.Client()
	created, err := tenant.CreateClient(ctx, "retained", cephharness.TenantReadOnly)
	if err != nil {
		t.Fatalf("Tenant.CreateClient: %v", err)
	}
	tenantPools, err := observeTenant(tenant).Pools(ctx)
	if err != nil || len(tenantPools) == 0 {
		t.Fatalf("tenant pools: %d, %v", len(tenantPools), err)
	}
	tenantClients := map[string]*ceph.ClientConfig{"Tenant.Client": own, "Tenant.CreateClient": created}
	tenantTB.finish()
	if tenantTB.Failed() {
		t.Fatalf("tenant release: %s", tenantTB.errorText())
	}

	exclusiveTB := newNamedTB(t, "exclusive")
	cluster := shared.Exclusive(exclusiveTB)
	if cluster == nil {
		t.Fatalf("Exclusive: %s", exclusiveTB.errorText())
	}
	if status, err := observeCluster(cluster).Status(ctx); err != nil || status.FSID != fsid {
		t.Fatalf("the exclusive interval got cluster %q (%v), want the cluster %s that the tenant used", status.FSID, err, fsid)
	}

	// The interval lends handles derived from the Container.
	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) == 0 {
		t.Fatalf("Monitors: %d, %v", len(monitors), err)
	}
	managers, err := cluster.Managers(ctx)
	if err != nil || len(managers) == 0 {
		t.Fatalf("Managers: %d, %v", len(managers), err)
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) == 0 {
		t.Fatalf("OSDs: %d, %v", len(osds), err)
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "retained-handles", Application: "rados", PGNum: 1})
	if err != nil {
		t.Fatalf("CreatePool under an exclusive lease: %v", err)
	}
	client, err := cluster.CreateClient(ctx, "retained-handles", ceph.ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatalf("CreateClient under an exclusive lease: %v", err)
	}

	// Gap 2: what the tenant obtained does not gain the interval.
	for name, retained := range tenantClients {
		err := retained.UpdateCaps(ctx, ceph.ClientCaps{Mon: "allow r"})
		t.Logf("exclusive interval, retained %s: ClientConfig.UpdateCaps = %v", name, err)
		if !errors.Is(err, ceph.ErrSharedCluster) {
			t.Errorf("a descriptor from %s gained the exclusive interval: %v", name, err)
		}
	}
	err = tenantPools[0].SetQuota(ctx, ceph.PoolQuota{MaxObjects: 1})
	t.Logf("exclusive interval, retained tenant pool: Pool.SetQuota = %v", err)
	if err == nil {
		t.Error("a pool that a tenant listed gained the exclusive interval")
	}
	// The interval's own handles work.
	if err := pool.SetQuota(ctx, ceph.PoolQuota{MaxObjects: 100}); err != nil {
		t.Errorf("the interval's pool during the interval: %v", err)
	}
	if _, err := monitors[0].Version(ctx); err != nil {
		t.Errorf("the interval's monitor handle during the interval: %v", err)
	}

	exclusiveTB.finish()
	if exclusiveTB.Failed() {
		t.Fatalf("exclusive release: %s", exclusiveTB.errorText())
	}

	// The release retired and terminated the cluster. Each retained handle
	// is called and its outcome logged.
	gated := map[string]func() error{
		"Container.Ceph":                 func() error { _, err := cluster.Ceph(ctx, "status"); return err },
		"Container.CreatePool":           func() error { _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "after-release"}); return err },
		"MonitorContainer.Version":       func() error { _, err := monitors[0].Version(ctx); return err },
		"ManagerContainer.Version":       func() error { _, err := managers[0].Version(ctx); return err },
		"OSDContainer.SetIn":             func() error { return osds[0].SetIn(ctx, true) },
		"Pool.SetQuota":                  func() error { return pool.SetQuota(ctx, ceph.PoolQuota{MaxObjects: 200}) },
		"ClientConfig.UpdateCaps":        func() error { return client.UpdateCaps(ctx, ceph.ClientCaps{Mon: "allow r"}) },
		"tenant pool Pool.SetQuota":      func() error { return tenantPools[0].SetQuota(ctx, ceph.PoolQuota{MaxObjects: 1}) },
		"Tenant.Client UpdateCaps":       func() error { return own.UpdateCaps(ctx, ceph.ClientCaps{Mon: "allow r"}) },
		"Tenant.CreateClient UpdateCaps": func() error { return created.UpdateCaps(ctx, ceph.ClientCaps{Mon: "allow r"}) },
	}
	for name, call := range gated {
		err := call()
		t.Logf("after release, retained %s = %v", name, err)
		if !errors.Is(err, ceph.ErrSharedCluster) {
			t.Errorf("%s after the release returned %v, want the gate's ErrSharedCluster", name, err)
		}
	}

	// Gap not closed by the gate: the embedded container is a plain field.
	state, stateErr := cluster.Container.State(ctx)
	t.Logf("after release, embedded Container.State = running %v, %v", state != nil && state.Running, stateErr)
	if stateErr == nil && state != nil && state.Running {
		t.Error("the embedded container of the retired cluster still runs")
	}
	_, _, execErr := cluster.Container.Exec(ctx, []string{"ceph", "status"})
	t.Logf("after release, embedded Container.Exec = %v", execErr)
	if execErr == nil {
		t.Error("the embedded container of the retired cluster ran a command after the release")
	}

	// A later lease gets a fresh cluster.
	lateTB := newNamedTB(t, "late")
	late := shared.Lease(lateTB)
	if late == nil {
		t.Fatalf("the lease after the interval: %s", lateTB.errorText())
	}
	if got := sharedFSID(t, ctx, late); got == fsid {
		t.Fatalf("the lease after the interval got the retired cluster %s", got)
	}
	lateTB.finish()
	if lateTB.Failed() {
		t.Fatalf("late release: %s", lateTB.errorText())
	}
}
