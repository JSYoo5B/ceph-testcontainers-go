package ceph

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// requireStale fails unless err refuses a stale or foreign handle as an
// invalid request, and returns it for further checks.
func requireStale(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("%s: got %v, want an invalid request", what, err)
	}
}

func TestOSDHandleOfARemovedOSDIsRefusedAndNeverReachesASuccessor(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	cluster.runDir.volume = &runVolume{name: "run"}
	removed := osdHandle(t, t.Context(), cluster, 3)
	if err := removed.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	actions, stops := len(control.actions), daemon.stops
	// A later OSD that took the ID must not be reached through the old handle.
	successor := &OSDContainer{ID: 3, owner: cluster, nativeUUID: osdLifecycleUUID}
	cluster.osdHandles.register(3, successor)
	ctx := t.Context()
	for name, call := range map[string]func() error{
		"Remove": func() error { return removed.Remove(ctx) },
		"SetIn":  func() error { return removed.SetIn(ctx, false) },
		"Detach": func() error { _, err := removed.Detach(ctx); return err },
		"AdminSocket": func() error {
			_, err := removed.AdminSocket(ctx, "status")
			return err
		},
		"RotateKey": func() error { return removed.RotateKey(ctx) },
	} {
		requireStale(t, name+" of a removed OSD", call())
	}
	if len(control.actions) != actions || daemon.stops != stops || successor.purgeIssued || successor.purged || successor.detaching {
		t.Fatalf("a stale handle acted on its successor: actions %v stops %d", control.actions, daemon.stops)
	}
}

func TestOSDHandleBecomesStaleWhenDetachedOrReplacedByReopen(t *testing.T) {
	f := newRetainedFixture(t)
	old := osdHandle(t, t.Context(), f.cluster, 3)
	detached, err := old.Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	requireStale(t, "Remove of a detached OSD", old.Remove(t.Context()))
	requireStale(t, "SetIn of a detached OSD", old.SetIn(t.Context(), false))
	reopened, err := detached.Reopen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if reopened == old || f.cluster.osdHandles.get(3) != reopened {
		t.Fatal("Reopen did not publish a new handle")
	}
	actions := len(f.control.actions)
	for name, err := range map[string]error{
		"Remove":    old.Remove(t.Context()),
		"SetIn":     old.SetIn(t.Context(), false),
		"RotateKey": old.RotateKey(t.Context()),
	} {
		requireStale(t, name+" through the handle that Reopen replaced", err)
	}
	if _, err := old.Detach(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Detach through the handle that Reopen replaced: %v", err)
	}
	if len(f.control.actions) != actions || reopened.purgeIssued || reopened.detaching || f.cluster.osdHandles.get(3) != reopened {
		t.Fatalf("the replaced handle acted on the new instance: %v", f.control.actions)
	}
	// The handle that Reopen returned is the one to operate on.
	if err := reopened.SetIn(t.Context(), true); err != nil {
		t.Fatal("the new handle was refused:", err)
	}
}

func TestOSDHandleOfAnotherOrATerminatedClusterIsRefused(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	other, otherControl, otherDaemon := newOSDLifecycleFixture()
	foreign := &OSDContainer{ID: 3, owner: other, nativeUUID: osdLifecycleUUID, Container: daemon}
	// other owns a different osd.3 descriptor, so the foreign descriptor is not its OSD.
	requireStale(t, "Remove of a descriptor that its cluster does not own", foreign.Remove(t.Context()))
	if len(control.actions) != 0 || len(otherControl.actions) != 0 || daemon.stops != 0 || otherDaemon.stops != 0 {
		t.Fatal("a foreign descriptor reached a cluster")
	}
	handle := osdHandle(t, t.Context(), cluster, 3)
	cluster.closed = true
	if err := handle.Remove(t.Context()); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("Remove after Terminate: %v", err)
	}
	if err := handle.SetIn(t.Context(), false); err == nil {
		t.Fatal("SetIn after Terminate was accepted")
	}
	if _, err := handle.AdminSocket(t.Context(), "status"); err == nil {
		t.Fatal("AdminSocket after Terminate was accepted")
	}
	if len(control.actions) != 0 {
		t.Fatalf("a terminated cluster ran %v", control.actions)
	}
	for name, call := range map[string]func() error{
		"nil OSD":      func() error { return (*OSDContainer)(nil).Remove(t.Context()) },
		"zero OSD":     func() error { return (&OSDContainer{}).Remove(t.Context()) },
		"nil monitor":  func() error { return (*MonitorContainer)(nil).Remove(t.Context()) },
		"zero manager": func() error { return (&ManagerContainer{DaemonName: "a"}).Remove(t.Context()) },
		"nil detached": func() error { _, err := (*DetachedOSD)(nil).Reopen(t.Context()); return err },
		"zero pool":    func() error { _, err := (&Pool{}).Status(t.Context()); return err },
		"zero client":  func() error { _, err := (&ClientConfig{}).Capabilities(t.Context()); return err },
	} {
		requireStale(t, name, call())
	}
}

func TestMonitorHandleIsMatchedByNameAndContainerNotByPointer(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "b", "c"}, quorum: []string{"a", "b", "c"}}
	cluster := monitorLifecycleFixture(control)
	cluster.runDir.volume = &runVolume{name: "run"}
	primary := monitorHandle(t, t.Context(), cluster, "a")
	// The cluster describes the primary MON by a new descriptor on every
	// listing, so a second descriptor of the same container is the same MON.
	again := monitorHandle(t, t.Context(), cluster, "a")
	if primary == again {
		t.Fatal("the fixture was expected to build a descriptor for the primary MON each time")
	}
	if !again.ownedBy(cluster) || !primary.ownedBy(cluster) {
		t.Fatal("a descriptor of the primary MON was not matched by name and container")
	}
	daemon := &monitorLifecycleDaemon{}
	cluster.roster.addMonitor("b", &MonitorContainer{Container: daemon, DaemonName: "b", owner: cluster})
	removed := cluster.roster.monitor("b")
	if err := removed.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	removals := control.removals
	// A successor with the same name is not reachable through the old handle.
	successor := &MonitorContainer{Container: &monitorLifecycleDaemon{}, DaemonName: "b", owner: cluster}
	cluster.roster.addMonitor("b", successor)
	requireStale(t, "Remove of a removed monitor", removed.Remove(t.Context()))
	if _, err := removed.AdminSocket(t.Context(), "version"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("AdminSocket of a removed monitor: %v", err)
	}
	if control.removals != removals || cluster.roster.monitor("b") != successor {
		t.Fatal("a stale monitor handle acted on its successor")
	}
	// The primary MON's descriptor goes stale when it is gone.
	cluster.monitorTerminated = true
	requireStale(t, "Remove of a terminated primary MON", primary.Remove(t.Context()))
}

func TestManagerHandleOfAReplacedManagerIsRefused(t *testing.T) {
	cluster, control, target, replacement := managerLifecycleFixture()
	cluster.runDir.volume = &runVolume{name: "run"}
	old := managerHandle(t, t.Context(), cluster, "a")
	successor := &ManagerContainer{Container: replacement, DaemonName: "a", owner: cluster, authOwned: true}
	cluster.roster.addManager("a", successor)
	requireStale(t, "Remove of a manager that another descriptor replaced", old.Remove(t.Context()))
	requireStale(t, "RotateKey of a manager that another descriptor replaced", old.RotateKey(t.Context()))
	if _, err := old.AdminSocket(t.Context(), "status"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("AdminSocket: %v", err)
	}
	if target.terminations != 0 || replacement.terminations != 0 || cluster.roster.manager("a") != successor {
		t.Fatalf("a stale manager handle acted on its successor: %v", control.calls)
	}
}

func TestPoolHandleIdentifiesTheNativePool(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("data", false)
	f.AddFixturePool("bystander", false)
	cluster := removalCluster(f)
	ctx := t.Context()

	data, err := cluster.Pool(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state, err := data.Status(ctx); err != nil || state.Name != "data" {
		t.Fatalf("status %+v %v", state, err)
	}
	if _, err := cluster.Pool(ctx, "missing"); err == nil {
		t.Fatal("a pool that does not exist has a handle")
	}
	if _, err := cluster.Pool(ctx, "--option"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an invalid name was looked up: %v", err)
	}

	// A copy shares the identity, so a removal through one stales both.
	copied := *data
	if err := data.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	// A later pool that reuses the name is another pool.
	f.AddFixturePool("data", false)
	f.Pools[len(f.Pools)-1].ID = 40
	calls := len(f.Calls)
	for name, call := range map[string]func() error{
		"Status":         func() error { _, err := copied.Status(ctx); return err },
		"Usage":          func() error { _, err := copied.Usage(ctx); return err },
		"PGs":            func() error { _, err := copied.PGs(ctx); return err },
		"SetQuota":       func() error { return copied.SetQuota(ctx, PoolQuota{MaxObjects: 1}) },
		"SetReplication": func() error { return copied.SetReplication(ctx, 2, 1) },
		"SetPGCount":     func() error { return copied.SetPGCount(ctx, 16) },
		"SetPlacement":   func() error { return copied.SetPlacement(ctx, PoolPlacement{FailureDomain: "osd"}) },
		"WaitForPGCount": func() error { return copied.WaitForPGCount(ctx, 8) },
		"Remove":         func() error { return copied.Remove(ctx) },
	} {
		err := call()
		if !errors.Is(err, errStalePool) {
			t.Fatalf("%s through the handle of a removed pool: %v", name, err)
		}
	}
	if len(f.Calls) != calls || !f.HasPool("data") || !f.HasRule("tc-data-replicated") {
		t.Fatalf("a stale pool handle reached the pool that reuses the name: %s", f)
	}

	// The same name with another native ID is refused without a removal, also
	// when the handle itself never removed anything.
	stale, err := cluster.Pool(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	other, err := cluster.Pool(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	f.AddFixturePool("data", false)
	f.Pools[len(f.Pools)-1].ID = 41
	if err := stale.Remove(ctx); !errors.Is(err, errStalePool) || !f.HasPool("data") || !f.HasRule("tc-data-replicated") {
		t.Fatalf("Remove acted on a pool that replaced the handle's pool: %v", err)
	}
	if _, err := stale.Status(ctx); !errors.Is(err, errStalePool) || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("Status of a replaced pool: %v", err)
	}
}

func TestPoolHandleFromAFailedCreationBindsOnFirstUse(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("data", false)
	cluster := removalCluster(f)
	handle := &Pool{PoolConfig: PoolConfig{Name: "data"}, owner: cluster, ident: &poolIdentity{}}
	if _, err := handle.Status(t.Context()); err != nil || !handle.ident.bound {
		t.Fatalf("the first operation did not bind the handle: %v", err)
	}
	id := handle.ident.id
	for i := range f.Pools {
		if f.Pools[i].Name == "data" {
			f.Pools[i].ID += 100
		}
	}
	if _, err := handle.Status(t.Context()); !errors.Is(err, errStalePool) || handle.ident.id != id {
		t.Fatalf("a bound handle followed the name to another pool: %v", err)
	}
}

// listingContainer answers each native pool listing from listings in turn, or
// fails it when the entry is empty, and repeats the last entry.
type listingContainer struct {
	*poolFixtureContainer
	listings []string
	reads    int
}

func (ctr *listingContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Contains(args, "detail") {
		listing := ctr.listings[min(ctr.reads, len(ctr.listings)-1)]
		ctr.reads++
		if listing == "" {
			return 0, nil, errors.New("injected listing failure")
		}
		ctr.Output["osd pool ls detail --format json"] = listing
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func idListing(id string) string {
	return `[{"pool_id":` + id + `,"pool_name":"fixture","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":3}]`
}

// A CreatePool that returns without an error has read the native ID of its
// pool, so a pool that later takes the name is refused. When the read fails,
// the creation is reported as an error that still carries the handle.
func TestCreatePoolBindsTheNativeIDBeforeReturningSuccess(t *testing.T) {
	ctr := &listingContainer{poolFixtureContainer: relocationFixture(), listings: []string{idListing("11"), idListing("12")}}
	pool, err := poolFixtureCluster(ctr, 3).CreatePool(t.Context(), PoolConfig{Name: "fixture"})
	if err != nil || pool == nil || !pool.ident.bound || pool.ident.id != 11 {
		t.Fatalf("created pool %+v, error %v", pool, err)
	}
	// The pool was removed and created again under the same name as ID 12.
	if _, err := pool.Status(t.Context()); !errors.Is(err, errStalePool) {
		t.Fatalf("the handle reached the pool that replaced it: %v", err)
	}
	if err := pool.Remove(t.Context()); !errors.Is(err, errStalePool) {
		t.Fatalf("Remove of a replaced pool: %v", err)
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "rm") {
			t.Fatalf("the replacing pool was deleted: %v", call)
		}
	}
}

func TestCreatePoolReportsAnIdentityReadFailureWithTheHandle(t *testing.T) {
	ctr := &listingContainer{poolFixtureContainer: relocationFixture(), listings: []string{"", idListing("12")}}
	pool, err := poolFixtureCluster(ctr, 3).CreatePool(t.Context(), PoolConfig{Name: "fixture"})
	if err == nil || !strings.Contains(err.Error(), "was created but its native ID could not be read") {
		t.Fatalf("a creation without an identity succeeded: %v", err)
	}
	if pool == nil || pool.ident.bound || pool.Name != "fixture" {
		t.Fatalf("the handle for cleanup was not returned unbound: %+v", pool)
	}
	// The documented fallback of this error path: the handle binds to the
	// pool of its name at its first successful operation.
	if _, err := pool.Status(t.Context()); err != nil || !pool.ident.bound || pool.ident.id != 12 {
		t.Fatalf("first use did not bind: %v", err)
	}
}
