//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_shared_cluster))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// TestSharedCluster leases tenants of one shared cluster: tenants are
// confined to their namespaces, the shared container refuses the gated
// methods, sequential leases reuse the cluster, and a tenant that leaves an
// object outside its scope poisons it, so the next lease gets a new one.
func TestSharedCluster(t *testing.T) {
	shared := newSharedCluster(t)

	var fsid string
	t.Run("isolation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		a, b := shared.Lease(t), shared.Lease(t)
		fsid = sharedFSID(t, ctx, a)
		if got := sharedFSID(t, ctx, b); got != fsid {
			t.Fatalf("parallel tenants got clusters %s and %s", fsid, got)
		}
		ctrA, ctrB := startTenantClient(t, ctx, a), startTenantClient(t, ctx, b)
		for _, tenant := range []struct {
			tenant *ceph.Tenant
			ctr    testcontainers.Container
		}{{a, ctrA}, {b, ctrB}} {
			tenantExpect(t, ctx, tenant.tenant, tenant.ctr, true, "write own namespace", "rados", "-p", ceph.SharedPool, "-N", tenant.tenant.Namespace(), "put", "own", "/etc/hostname")
			tenantExpect(t, ctx, tenant.tenant, tenant.ctr, true, "list own namespace", "rados", "-p", ceph.SharedPool, "-N", tenant.tenant.Namespace(), "ls")
		}
		other := b.Namespace()
		for _, denied := range []struct {
			label string
			argv  []string
		}{
			{"read the other tenant's object", []string{"rados", "-p", ceph.SharedPool, "-N", other, "get", "own", "/dev/null"}},
			{"write the other tenant's namespace", []string{"rados", "-p", ceph.SharedPool, "-N", other, "put", "intruder", "/etc/hostname"}},
			{"list the other tenant's namespace", []string{"rados", "-p", ceph.SharedPool, "-N", other, "ls"}},
			{"list every namespace", []string{"rados", "-p", ceph.SharedPool, "ls", "--all"}},
			{"write the default namespace", []string{"rados", "-p", ceph.SharedPool, "put", "intruder", "/etc/hostname"}},
			{"write another pool", []string{"rados", "-p", ".mgr", "-N", a.Namespace(), "put", "intruder", "/etc/hostname"}},
			{"set a config option", []string{"ceph", "config", "set", "global", "mon_max_pg_per_osd", "500"}},
			{"set an OSD flag", []string{"ceph", "osd", "set", "noout"}},
			{"add a blocklist entry", []string{"ceph", "osd", "blocklist", "add", "192.0.2.1:0/1", "60"}},
		} {
			tenantExpect(t, ctx, a, ctrA, false, denied.label, denied.argv...)
		}
		reader, err := a.CreateClient(ctx, "reader", ceph.TenantReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		readerCtr := startTenantClientAs(t, ctx, a, reader)
		tenantExpectAs(t, ctx, a, readerCtr, reader, true, "read-only client reads", "rados", "-p", ceph.SharedPool, "-N", a.Namespace(), "get", "own", "/dev/null")
		tenantExpectAs(t, ctx, a, readerCtr, reader, false, "read-only client writes", "rados", "-p", ceph.SharedPool, "-N", a.Namespace(), "put", "x", "/etc/hostname")
		if _, err := a.RunNativeCommand(ctx, ctrB, ceph.NativeCommand{Argv: []string{"true"}}); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("tenant A ran a command in tenant B's container: %v", err)
		}
	})
	if t.Failed() {
		return
	}
	if stats := shared.Stats(); stats.Starts != 1 || stats.Poisonings != 0 {
		t.Fatalf("clean tenants poisoned the cluster: %+v", stats)
	}

	t.Run("gate", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		tenant := shared.Lease(t)
		cluster := servicehook.TenantCluster(tenant)
		if cluster == nil {
			t.Fatal("the tenant has no cluster")
		}
		for name, call := range map[string]func() error{
			"Ceph": func() error { _, err := cluster.Ceph(ctx, "status"); return err },
			"OverrideConfig": func() error {
				_, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "global", Name: "mon_max_pg_per_osd", Value: "500"})
				return err
			},
			"OverrideOSDFlag": func() error { _, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true); return err },
			"CreatePool":      func() error { _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "gated"}); return err },
			"CreateClient": func() error {
				_, err := cluster.CreateClient(ctx, "gated", ceph.ClientCaps{Mon: "allow *"})
				return err
			},
			"AddOSD": func() error { _, err := cluster.AddOSD(ctx); return err },
			"ControlContainer": func() error {
				_, err := cluster.ControlContainer(ctx)
				return err
			},
			"Exec":      func() error { _, _, err := cluster.Exec(ctx, []string{"true"}); return err },
			"Terminate": func() error { return cluster.Terminate(ctx) },
		} {
			if err := call(); !errors.Is(err, ceph.ErrSharedCluster) {
				t.Errorf("%s on the shared cluster returned %v, want ErrSharedCluster", name, err)
			}
		}
		if control, err := cluster.ControlContainer(ctx); control != nil || !errors.Is(err, ceph.ErrSharedCluster) {
			t.Errorf("ControlContainer = %v, %v, want no handle and ErrSharedCluster", control, err)
		}
		if osds, err := cluster.OSDs(ctx); osds != nil || !errors.Is(err, ceph.ErrSharedCluster) {
			t.Errorf("OSDs = %v, %v, want no handles and ErrSharedCluster", osds, err)
		}
		if status, err := cluster.Status(ctx); err != nil || status.FSID != fsid {
			t.Fatalf("a read-only view failed: %s, %v", status.FSID, err)
		}
		if flags, err := tenant.OSDFlags(ctx); err != nil || len(flags) == 0 {
			t.Fatalf("OSDFlags = %v, %v", flags, err)
		}
	})

	t.Run("sequential reuse", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		for i := range 2 {
			lease := &cleanupTB{TB: t}
			tenant := shared.Lease(lease)
			if got := sharedFSID(t, ctx, tenant); got != fsid {
				t.Fatalf("lease %d got cluster %s, want %s", i, got, fsid)
			}
			lease.finish()
		}
		if stats := shared.Stats(); stats.Starts != 1 || stats.Poisonings != 0 {
			t.Fatalf("sequential leases did not reuse the cluster: %+v", stats)
		}
	})

	t.Run("dirty tenant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		dirty := &cleanupTB{TB: t}
		tenant := shared.Lease(dirty)
		cluster := servicehook.TenantCluster(tenant)
		// An internal admin path that bypasses the public shared-cluster gate.
		if _, err := servicehook.ClientCommand(ctx, cluster, "rados", "-p", ceph.SharedPool, "put", "stray", "/etc/hostname"); err != nil {
			t.Fatal(err)
		}
		dirty.finish()
		if len(dirty.errors) != 1 || !dirty.failed {
			t.Fatalf("the dirty tenant's release reported %q", dirty.errors)
		}
		t.Logf("dirty release: %s", dirty.errors[0])
		next := shared.Lease(t)
		if got := sharedFSID(t, ctx, next); got == fsid {
			t.Fatalf("the lease after a dirty tenant got the poisoned cluster %s", got)
		}
		if stats := shared.Stats(); stats.Starts != 2 || stats.Recycles != 1 || stats.Poisonings != 1 {
			t.Fatalf("stats = %+v", stats)
		}
	})
}

// TestSharedClusterExclusive takes the whole shared cluster with Exclusive:
// a test that holds a tenant lease is refused at once, the exclusive lease
// waits for the tenant and holds back a lease that asks after it, the
// returned Container allows cluster-wide changes and a topology change but
// not Terminate, and its release retires the cluster, so the waiting lease
// gets a fresh one.
func TestSharedClusterExclusive(t *testing.T) {
	shared := newSharedCluster(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()

	tenantTB := newNamedTB(t, "tenant")
	tenant := shared.Lease(tenantTB)
	if tenant == nil {
		t.Fatalf("lease: %s", tenantTB.errorText())
	}
	fsid := sharedFSID(t, ctx, tenant)

	nested := newNamedTB(tenantTB, "nested")
	began := time.Now()
	if shared.Exclusive(nested) != nil || !strings.Contains(nested.errorText(), "holds a tenant lease") {
		t.Fatalf("an Exclusive under a tenant lease: %q", nested.errorText())
	}
	if waited := time.Since(began); waited > 5*time.Second {
		t.Fatalf("the refused Exclusive took %v", waited)
	}

	exclusiveTB := newNamedTB(t, "exclusive")
	granted := make(chan *ceph.Container, 1)
	go func() { granted <- shared.Exclusive(exclusiveTB) }()
	select {
	case <-granted:
		t.Fatalf("Exclusive did not wait for the tenant: %s", exclusiveTB.errorText())
	case <-time.After(3 * time.Second):
	}
	lateTB := newNamedTB(t, "late")
	leased := make(chan *ceph.Tenant, 1)
	go func() { leased <- shared.Lease(lateTB) }()
	select {
	case <-leased:
		t.Fatalf("a lease passed the waiting Exclusive: %s", lateTB.errorText())
	case <-time.After(3 * time.Second):
	}

	tenantTB.finish()
	if tenantTB.Failed() {
		t.Fatalf("tenant release: %s", tenantTB.errorText())
	}
	var cluster *ceph.Container
	select {
	case cluster = <-granted:
	case <-ctx.Done():
		t.Fatal("Exclusive did not return after the tenant ended")
	}
	if cluster == nil {
		t.Fatalf("Exclusive: %s", exclusiveTB.errorText())
	}
	status, err := cluster.Status(ctx)
	if err != nil || status.FSID != fsid {
		t.Fatalf("the exclusive lease got cluster %s (%v), want the shared %s", status.FSID, err, fsid)
	}
	if _, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true); err != nil {
		t.Fatalf("OverrideOSDFlag under an exclusive lease: %v", err)
	}
	if _, err := cluster.Ceph(ctx, "osd", "set", "noscrub"); err != nil {
		t.Fatalf("Ceph under an exclusive lease: %v", err)
	}
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatalf("AddOSD under an exclusive lease: %v", err)
	}
	if err := cluster.Terminate(ctx); !errors.Is(err, ceph.ErrSharedCluster) {
		t.Fatalf("Terminate under an exclusive lease returned %v", err)
	}
	select {
	case <-leased:
		t.Fatalf("a lease was granted while the exclusive lease is held: %s", lateTB.errorText())
	default:
	}

	exclusiveTB.finish()
	if exclusiveTB.Failed() {
		t.Fatalf("exclusive release: %s", exclusiveTB.errorText())
	}
	var late *ceph.Tenant
	select {
	case late = <-leased:
	case <-ctx.Done():
		t.Fatal("the waiting lease did not get a cluster after the exclusive lease ended")
	}
	if late == nil {
		t.Fatalf("the waiting lease: %s", lateTB.errorText())
	}
	if got := sharedFSID(t, ctx, late); got == fsid {
		t.Fatalf("the lease after an exclusive lease got its cluster %s", got)
	}
	if flags, err := late.OSDFlags(ctx); err != nil || slices.Contains(flags, "noout") || slices.Contains(flags, "noscrub") {
		t.Fatalf("the fresh cluster has OSD flags %v (%v)", flags, err)
	}
	lateTB.finish()
	if lateTB.Failed() {
		t.Fatalf("late release: %s", lateTB.errorText())
	}
	if stats := shared.Stats(); stats.Starts != 2 || stats.Exclusives != 1 || stats.Recycles != 0 || stats.Poisonings != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}
