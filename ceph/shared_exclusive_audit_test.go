package ceph

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Audit of the stale-handle question that the owner's Exclusive decision of
// 2026-10-28 left to the extraction (issue 155, design note section "Exclusive
// stale-handle audit"): after the move SharedCluster.Exclusive still lends the
// core *Container, so a retained Container or derived handle must not keep an
// interval's authority. These controls characterize what the current code does,
// without Docker. They pin the audited properties and the gaps the note lists;
// the gaps are findings, not accepted behavior, and a change that closes one
// inverts the matching test in the same change.

// sharingModeWrites returns "callee(argument)" for every call in src that
// stores a cluster's sharing mode: setSharing, setSharingMode and the atomic
// Store of the sharing field.
func sharingModeWrites(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var writes []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch callee := types.ExprString(sel); {
		case sel.Sel.Name == "setSharing", sel.Sel.Name == "setSharingMode", strings.HasSuffix(callee, "sharing.Store"):
			writes = append(writes, callee+"("+types.ExprString(call.Args[0])+")")
		}
		return true
	})
	return writes
}

// TestExclusiveAuditSharingModeOnlyMovesForward pins that no production code
// lowers the mode of a cluster. A cluster starts shared with tenants, becomes
// exclusive once, and is never returned to tenants, so one *Container is never
// the cluster of a tenant interval after an exclusive interval and generation
// reuse of the same cluster handle cannot happen.
func TestExclusiveAuditSharingModeOnlyMovesForward(t *testing.T) {
	// The only writers: the two orchestration sites, the backend method that
	// forwards, the core setter, and the unit-test cluster builder.
	want := map[string][]string{
		"shared.go":              {"backend.setSharing(sharedWithTenants)"},
		"shared_exclusive.go":    {"inst.backend.setSharing(sharedExclusively)"},
		"shared_backend.go":      {"b.c.setSharingMode(mode)"},
		"shared_cluster_port.go": {"c.sharing.Store(int32(mode))"},
		"unit_access.go":         {"c.sharing.Store(int32(sharedWithTenants))"},
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		got := sharingModeWrites(t, name, string(src))
		if !slices.Equal(got, want[name]) {
			t.Errorf("%s writes the sharing mode as %v, want %v: a new writer needs the stale-handle audit", name, got, want[name])
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("%s no longer exists but the audit lists it", name)
	}
}

// TestExclusiveAuditSharingModeScannerSeesADemotion is the negative control:
// the scanner reports a demotion, so the test above fails for one.
func TestExclusiveAuditSharingModeScannerSeesADemotion(t *testing.T) {
	src := `package ceph
func (s *SharedCluster) releaseExclusive(inst *sharedInstance) {
	inst.backend.setSharing(sharedWithTenants)
	c.sharing.Store(int32(notShared))
}`
	got := sharingModeWrites(t, "demote.go", src)
	want := []string{"inst.backend.setSharing(sharedWithTenants)", "c.sharing.Store(int32(notShared))"}
	if !slices.Equal(got, want) {
		t.Fatalf("scanner = %v, want %v", got, want)
	}
	if slices.Equal(got, []string{"inst.backend.setSharing(sharedExclusively)"}) {
		t.Fatal("a demoting release is indistinguishable from the real one")
	}
}

// TestExclusiveAuditInstanceOfAnExclusiveLeaseIsNeverReused follows one
// cluster through a tenant lease, the exclusive interval and a later lease, and
// shows the model evidence for the audited properties: the exclusive interval
// takes the cluster that tenants used (so a tenant-held handle can meet an
// exclusive mode, see the handle controls below), and afterwards no lease gets
// that cluster again.
func TestExclusiveAuditInstanceOfAnExclusiveLeaseIsNeverReused(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tenantTB := newFakeTB(t)
	tenant := shared.Lease(tenantTB)
	first := backendOf(tenant)
	tenantTB.finish()
	if first.mode() != sharedWithTenants {
		t.Fatalf("the tenant cluster has mode %v", first.mode())
	}

	exclusiveTB := newFakeTB(t)
	shared.Exclusive(exclusiveTB)
	if got := fleet.clusters(); len(got) != 1 || got[0] != first || first.mode() != sharedExclusively {
		t.Fatalf("the exclusive interval did not take the cluster that tenants used: %d clusters, mode %v", len(got), first.mode())
	}
	exclusiveTB.finish()
	if !first.isTerminated() {
		t.Fatal("the cluster of the exclusive interval was not terminated by its release")
	}

	laterTB := newFakeTB(t)
	later := shared.Lease(laterTB)
	if backendOf(later) == first || backendOf(later).mode() != sharedWithTenants {
		t.Fatal("a lease after the exclusive interval got the exclusive cluster")
	}
	laterTB.finish()
	if first.mode() != sharedExclusively {
		t.Fatalf("the retired cluster has mode %v, want it unchanged", first.mode())
	}
}

// TestExclusiveAuditKnownGapRetiredClusterKeepsExclusiveMode characterizes
// gap 1 of the note: the release of an exclusive interval does not lower the
// cluster's mode. When the termination fails, the cluster stays owned by Close
// and keeps the mode that admits every cluster-wide handle operation, so a
// retained Container or handle is admitted by the gate until Close ends the
// cluster. A change that makes the retired cluster refuse (a retired mode)
// inverts the mode assertion here.
func TestExclusiveAuditKnownGapRetiredClusterKeepsExclusiveMode(t *testing.T) {
	fleet := &fakeFleet{prepare: func(f *fakeShared) { f.terminateFailures = 1 }}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Exclusive(tb)
	tb.finish()
	cluster := fleet.clusters()[0]
	if cluster.isTerminated() {
		t.Fatal("the failed termination ended the cluster")
	}
	if got := cluster.mode(); got != sharedExclusively {
		t.Fatalf("the retired, still running cluster has mode %v; gap 1 is closed, update the note and this control", got)
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestExclusiveAuditHandleAdmissionFollowsTheLiveMode shows how each handle
// family that carries an owner reads the sharing mode: all but the tenant's
// Pool read the owner's current mode on every operation. A handle that a
// tenant obtained before an exclusive interval of the same cluster is
// therefore admitted by the gate once the mode is exclusive (gap 2); only the
// identity state ends its use (a released tenant's client descriptor is
// revoked by the release). The tenant's Pool is the contrast: it keeps the
// tenant admission and checks its lease, so it never gains the interval.
func TestExclusiveAuditHandleAdmissionFollowsTheLiveMode(t *testing.T) {
	cluster := &Container{}
	cluster.sharing.Store(int32(sharedWithTenants))

	type family struct {
		name  string
		op    string
		admit func() error
	}
	leased := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return nil }}}
	expired := &Pool{owner: cluster, ident: &poolIdentity{bound: true}, lease: &poolLease{live: func() error { return errors.New("released") }}}
	own := &Pool{owner: cluster, ident: &poolIdentity{bound: true}}
	detached := &DetachedOSD{cluster: cluster}
	families := []family{
		{"ClientConfig", "ClientConfig.UpdateCaps", func() error { _, err := (&ClientConfig{owner: cluster}).cluster("ClientConfig.UpdateCaps"); return err }},
		{"OSDContainer", "OSDContainer.SetIn", func() error { _, err := (&OSDContainer{owner: cluster}).cluster("OSDContainer.SetIn"); return err }},
		{"MonitorContainer", "MonitorContainer.Remove", func() error {
			_, err := (&MonitorContainer{owner: cluster}).cluster("MonitorContainer.Remove")
			return err
		}},
		{"ManagerContainer", "ManagerContainer.RotateKey", func() error {
			_, err := (&ManagerContainer{owner: cluster}).cluster("ManagerContainer.RotateKey")
			return err
		}},
		{"DetachedOSD", "DetachedOSD.Reopen", func() error { return detached.cluster.admitHandle("DetachedOSD.Reopen") }},
		{"Pool without a lease", "Pool.SetQuota", func() error { _, err := own.cluster("Pool.SetQuota"); return err }},
	}
	for _, f := range families {
		if err := f.admit(); !errors.Is(err, ErrSharedCluster) {
			t.Errorf("%s: %s under tenants returned %v, want ErrSharedCluster", f.name, f.op, err)
		}
	}
	for _, p := range []*Pool{leased, expired} {
		if _, err := p.cluster("Pool.SetQuota"); err == nil {
			t.Error("a tenant Pool was admitted for a cluster-wide operation under tenants")
		}
	}

	// The same cluster becomes exclusive, as the cluster that tenants used does
	// when an Exclusive takes it.
	cluster.sharing.Store(int32(sharedExclusively))
	for _, f := range families {
		if err := f.admit(); err != nil {
			t.Errorf("%s: %s under the exclusive mode returned %v; the family no longer reads the live mode, update the note", f.name, f.op, err)
		}
	}
	if _, err := leased.cluster("Pool.SetQuota"); !errors.Is(err, ErrSharedCluster) {
		t.Errorf("a leased tenant Pool gained the exclusive interval: %v", err)
	}
	if _, err := expired.cluster("Pool.Status"); err == nil || errors.Is(err, ErrSharedCluster) {
		t.Errorf("an expired tenant Pool returned %v, want the lease error before admission", err)
	}
}
