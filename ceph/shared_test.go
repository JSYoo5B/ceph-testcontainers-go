package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// fakeShared models the state of one cluster that the shared cluster's
// verification reads; tests change it as a tenant or an admin path would.
type fakeShared struct {
	fleet *fakeFleet
	id    int

	mu         sync.Mutex
	auth       map[string]string
	pools      map[string]string
	flags      []string
	config     map[string]string
	blocklist  []string
	health     []string
	objects    map[string]map[string]bool
	exited     bool
	sharing    sharingMode
	terminated bool
	// healthReads counts invariants reads; health clears after
	// healthClearsAfter of them when it is positive.
	healthReads, healthClearsAfter int
	clients, maxClients            int
	// terminateFailures fails that many terminate calls without ending the
	// cluster, as a transient engine error does; terminateGate, when set,
	// holds terminate until it is closed.
	terminateFailures, terminateCalls int
	terminateGate                     chan struct{}
	// services are the service resources that the probes would list,
	// mapped to their owners; unsettled is what keeps admission waiting.
	services  map[string]string
	unsettled string
	// unsettledReads counts invariants reads; unsettled clears after
	// unsettledClearsAfter of them when it is positive.
	unsettledReads, unsettledClearsAfter int
}

func (f *fakeShared) prepareServices(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fleet.prepareServices != nil {
		return f.fleet.prepareServices(f)
	}
	return nil
}

func (f *fakeShared) createPool(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pools[name]; ok {
		return invalidRequest("pool %s exists", name)
	}
	f.pools[name] = "rados"
	return nil
}

func (f *fakeShared) ready(context.Context) error { return nil }

func (f *fakeShared) invariants(_ context.Context, pool string) (sharedInvariants, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.terminated {
		return sharedInvariants{}, errClusterTerminated
	}
	f.healthReads++
	if f.healthClearsAfter > 0 && f.healthReads >= f.healthClearsAfter {
		f.health = nil
	}
	namespaces := map[string]int{}
	for ns, objects := range f.objects {
		if len(objects) > 0 {
			namespaces[ns] = len(objects)
		}
	}
	f.unsettledReads++
	if f.unsettledClearsAfter > 0 && f.unsettledReads >= f.unsettledClearsAfter {
		f.unsettled = ""
	}
	return sharedInvariants{Auth: maps.Clone(f.auth), Pools: maps.Clone(f.pools), OSDFlags: slices.Sorted(slices.Values(f.flags)),
		Config: maps.Clone(f.config), Blocklist: slices.Sorted(slices.Values(f.blocklist)), Health: slices.Sorted(slices.Values(f.health)),
		Namespaces: namespaces, Unsettled: f.unsettled}, nil
}

func (f *fakeShared) resources(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.terminated {
		return nil, errClusterTerminated
	}
	return maps.Clone(f.services), nil
}

func (f *fakeShared) daemonsRunning(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exited {
		return errors.New("daemon containers exited: osd.0")
	}
	return nil
}

func (f *fakeShared) createClient(_ context.Context, entity string, caps ClientCaps) (*ClientConfig, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.auth[entity]; ok {
		return nil, false, invalidRequest("%s exists", entity)
	}
	f.auth[entity] = "mon=" + caps.Mon + ";osd=" + caps.OSD
	f.clients++
	f.maxClients = max(f.maxClients, f.clients)
	return &ClientConfig{name: entity, created: true, ready: true}, true, nil
}

func (f *fakeShared) deleteClient(_ context.Context, client *ClientConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.auth[client.name]; !ok {
		return invalidRequest("%s is missing", client.name)
	}
	delete(f.auth, client.name)
	f.clients--
	return nil
}

func (f *fakeShared) purgeNamespace(_ context.Context, pool, namespace string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, namespace)
	return nil
}

func (f *fakeShared) setSharing(mode sharingMode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sharing = mode
}

func (f *fakeShared) mode() sharingMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sharing
}

func (f *fakeShared) terminate(ctx context.Context) error {
	f.mu.Lock()
	gate := f.terminateGate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminateCalls++
	if f.terminateFailures > 0 {
		f.terminateFailures--
		return errors.New("transient engine error")
	}
	if f.terminated {
		return errors.New("terminated twice")
	}
	f.terminated = true
	return nil
}

func (f *fakeShared) ops() clusterOps { return nil }

// put writes an object as a client of the namespace would.
func (f *fakeShared) put(namespace, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putLocked(namespace, name)
}

func (f *fakeShared) putLocked(namespace, name string) {
	if f.objects[namespace] == nil {
		f.objects[namespace] = map[string]bool{}
	}
	f.objects[namespace][name] = true
}

func (f *fakeShared) change(apply func(*fakeShared)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	apply(f)
}

func (f *fakeShared) terminations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminateCalls
}

func (f *fakeShared) isTerminated() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminated
}

// fakeFleet starts fakeShared clusters.
type fakeFleet struct {
	mu       sync.Mutex
	started  []*fakeShared
	startErr error
	// prepare changes each cluster before its baseline is taken.
	prepare func(*fakeShared)
	// entered, when set, receives a value as a start begins; the start then
	// waits for gate to close.
	entered chan struct{}
	gate    chan struct{}
	// partial makes a failing start return the cluster it allocated, as
	// Run returns a partial Container with its error.
	partial bool
	// prepareServices models the services' preparation of a started
	// cluster; it runs with the cluster's lock held.
	prepareServices func(*fakeShared) error
}

func (fleet *fakeFleet) start(context.Context, string, []testcontainers.ContainerCustomizer) (sharedBackend, error) {
	if fleet.entered != nil {
		fleet.entered <- struct{}{}
		<-fleet.gate
	}
	fleet.mu.Lock()
	defer fleet.mu.Unlock()
	if fleet.startErr != nil && !fleet.partial {
		return nil, fleet.startErr
	}
	cluster := &fakeShared{fleet: fleet, id: len(fleet.started),
		auth:    map[string]string{"client.admin": "mon=allow *", "mgr.a": "mon=allow profile mgr", "osd.0": "mon=allow profile osd"},
		pools:   map[string]string{".mgr": "mgr"},
		config:  map[string]string{"global/mon_allow_pool_delete": "true"},
		objects: map[string]map[string]bool{}, services: map[string]string{}}
	if fleet.prepare != nil {
		fleet.prepare(cluster)
	}
	fleet.started = append(fleet.started, cluster)
	return cluster, fleet.startErr
}

func (fleet *fakeFleet) clusters() []*fakeShared {
	fleet.mu.Lock()
	defer fleet.mu.Unlock()
	return slices.Clone(fleet.started)
}

func newFakeShared(fleet *fakeFleet, opts ...SharedOption) *SharedCluster {
	opts = append([]SharedOption{func(s *sharedSettings) {
		s.start = fleet.start
		s.settle = 50 * time.Millisecond
	}}, opts...)
	return NewShared("fake", opts...)
}

// fakeTB records what a lease reports and runs its cleanups on finish, so
// a test can fail a tenant or expect a release error without failing.
type fakeTB struct {
	testing.TB
	ctx  context.Context
	name string

	mu       sync.Mutex
	failed   bool
	errors   []string
	logs     []string
	cleanups []func()
}

var fakeTBs atomic.Int64

// newFakeTB returns a fake test of its own name, a subtest of t.
func newFakeTB(t *testing.T) *fakeTB {
	return &fakeTB{TB: t, ctx: t.Context(), name: fmt.Sprintf("%s/fake%d", t.Name(), fakeTBs.Add(1))}
}

// subtest returns a fake subtest of f.
func (f *fakeTB) subtest(name string) *fakeTB {
	return &fakeTB{TB: f.TB, ctx: f.ctx, name: f.name + "/" + name}
}

func (f *fakeTB) Name() string             { return f.name }
func (f *fakeTB) Helper()                  {}
func (f *fakeTB) Context() context.Context { return f.ctx }

func (f *fakeTB) Failed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed
}

func (f *fakeTB) Fail() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) { f.Errorf(format, args...) }

func (f *fakeTB) Logf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Cleanup(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, fn)
}

// finish runs the cleanups as the testing package does after a test.
func (f *fakeTB) finish() {
	f.mu.Lock()
	cleanups := f.cleanups
	f.cleanups = nil
	f.mu.Unlock()
	for _, fn := range slices.Backward(cleanups) {
		fn()
	}
}

func (f *fakeTB) errorText() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.errors, "\n")
}

func backendOf(tenant *Tenant) *fakeShared { return tenant.inst.backend.(*fakeShared) }

func TestSharedClusterStartsLazilyAndReusesItForSequentialLeases(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	if len(fleet.clusters()) != 0 {
		t.Fatal("NewShared started a cluster")
	}
	var seen []*fakeShared
	for range 3 {
		tb := newFakeTB(t)
		tenant := shared.Lease(tb)
		seen = append(seen, backendOf(tenant))
		backendOf(tenant).put(tenant.Namespace(), "object")
		tb.finish()
		if tb.Failed() {
			t.Fatalf("a clean tenant failed: %s", tb.errorText())
		}
	}
	if len(fleet.clusters()) != 1 || seen[0] != seen[1] || seen[1] != seen[2] {
		t.Fatalf("sequential leases used %d clusters", len(fleet.clusters()))
	}
	if seen[0].mode() != sharedWithTenants || seen[0].isTerminated() {
		t.Fatal("the cluster is not shared and running")
	}
	if stats := shared.Stats(); stats.Starts != 1 || stats.Leases != 3 || stats.Recycles != 0 || stats.Poisonings != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := shared.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !seen[0].isTerminated() {
		t.Fatal("Close left the cluster running")
	}
	if err := shared.Close(context.Background()); err != nil {
		t.Fatalf("a second Close failed: %v", err)
	}
	if _, err := shared.lease(t.Context(), leaseSettings{}, t.Logf); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a lease after Close returned %v", err)
	}
}

func TestSharedTenantGetsScopedClientsAndReleaseRemovesThem(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	cluster := backendOf(tenant)
	if len(tenant.ID()) != 9 || tenant.ID()[0] != 't' || tenant.Namespace() != tenant.ID() || tenant.Pool() != SharedPool {
		t.Fatalf("tenant id %q, namespace %q, pool %q", tenant.ID(), tenant.Namespace(), tenant.Pool())
	}
	want := map[string]string{
		"client." + tenant.ID():           "mon=allow r;osd=allow rw pool=" + SharedPool + " namespace=" + tenant.ID(),
		"client." + tenant.ID() + "-read": "mon=allow r;osd=allow r pool=" + SharedPool + " namespace=" + tenant.ID(),
	}
	if tenant.Client().Name() != "client."+tenant.ID() {
		t.Fatalf("tenant client %s", tenant.Client().Name())
	}
	reader, err := tenant.CreateClient(t.Context(), "client.read", TenantReadOnly)
	if err != nil || reader.Name() != "client."+tenant.ID()+"-read" {
		t.Fatalf("CreateClient = %v, %v", reader, err)
	}
	if _, err := tenant.CreateClient(t.Context(), "bad name", TenantReadOnly); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an invalid name returned %v", err)
	}
	if _, err := tenant.CreateClient(t.Context(), "zero", 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an unknown access returned %v", err)
	}
	for entity, caps := range want {
		if cluster.auth[entity] != caps {
			t.Fatalf("%s has caps %q, want %q", entity, cluster.auth[entity], caps)
		}
	}
	cluster.put(tenant.Namespace(), "a")
	cluster.put(tenant.Namespace(), "b")
	tb.finish()
	if tb.Failed() {
		t.Fatalf("release failed: %s", tb.errorText())
	}
	for entity := range want {
		if _, ok := cluster.auth[entity]; ok {
			t.Fatalf("release left %s", entity)
		}
	}
	if len(cluster.objects[tenant.Namespace()]) != 0 {
		t.Fatal("release left the tenant's objects")
	}
	if _, err := tenant.CreateClient(t.Context(), "late", TenantReadOnly); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("CreateClient after release returned %v", err)
	}
	if shared.Stats().Poisonings != 0 {
		t.Fatal("a clean release poisoned the cluster")
	}
}

func TestSharedReleasePoisonsChangesOutsideTheTenantScope(t *testing.T) {
	for name, dirty := range map[string]func(cluster *fakeShared, tenant *Tenant){
		"object in the default namespace": func(cluster *fakeShared, _ *Tenant) { cluster.putLocked("", "stray") },
		"object in another namespace":     func(cluster *fakeShared, _ *Tenant) { cluster.putLocked("elsewhere", "stray") },
		"config entry":                    func(cluster *fakeShared, _ *Tenant) { cluster.config["global/osd_pool_default_size"] = "3" },
		"changed config value":            func(cluster *fakeShared, _ *Tenant) { cluster.config["global/mon_allow_pool_delete"] = "false" },
		"auth entity":                     func(cluster *fakeShared, _ *Tenant) { cluster.auth["client.stray"] = "mon=allow r" },
		"changed caps":                    func(cluster *fakeShared, _ *Tenant) { cluster.auth["client.admin"] = "mon=allow r" },
		"pool":                            func(cluster *fakeShared, _ *Tenant) { cluster.pools["stray"] = "rados" },
		"OSD flag":                        func(cluster *fakeShared, _ *Tenant) { cluster.flags = append(cluster.flags, "noout") },
		"blocklist entry":                 func(cluster *fakeShared, _ *Tenant) { cluster.blocklist = append(cluster.blocklist, "192.0.2.1:0/1") },
		"leftover tenant client": func(cluster *fakeShared, tenant *Tenant) {
			cluster.auth["client."+tenant.ID()+"-hidden"] = "mon=allow r"
		},
	} {
		t.Run(name, func(t *testing.T) {
			fleet := &fakeFleet{}
			shared := newFakeShared(fleet)
			tb := newFakeTB(t)
			tenant := shared.Lease(tb)
			first := backendOf(tenant)
			first.change(func(cluster *fakeShared) { dirty(cluster, tenant) })
			tb.finish()
			if !strings.Contains(tb.errorText(), "outside its scope") {
				t.Fatalf("the dirty tenant's test did not fail: %q", tb.errorText())
			}
			if !first.isTerminated() {
				t.Fatal("the poisoned cluster was not terminated after its last lease")
			}
			next := newFakeTB(t)
			second := backendOf(shared.Lease(next))
			next.finish()
			if second == first || next.Failed() {
				t.Fatalf("the next lease did not get a fresh, clean cluster: %s", next.errorText())
			}
			if stats := shared.Stats(); stats.Starts != 2 || stats.Recycles != 1 || stats.Poisonings != 1 {
				t.Fatalf("stats = %+v", stats)
			}
		})
	}
}

func TestSharedFailedTenantPoisonsUnlessKept(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprint("keep=", keep), func(t *testing.T) {
			fleet := &fakeFleet{}
			shared := newFakeShared(fleet)
			tb := newFakeTB(t)
			var opts []LeaseOption
			if keep {
				opts = append(opts, WithKeepOnTenantFailure())
			}
			first := backendOf(shared.Lease(tb, opts...))
			tb.Fail()
			tb.finish()
			next := newFakeTB(t)
			second := backendOf(shared.Lease(next))
			next.finish()
			if (second == first) != keep || first.isTerminated() == keep {
				t.Fatalf("keep=%v: reused=%v, first terminated=%v", keep, second == first, first.isTerminated())
			}
		})
	}
}

func TestSharedExitedDaemonPoisonsTheCluster(t *testing.T) {
	t.Run("at release", func(t *testing.T) {
		fleet := &fakeFleet{}
		shared := newFakeShared(fleet)
		tb := newFakeTB(t)
		first := backendOf(shared.Lease(tb))
		first.change(func(cluster *fakeShared) { cluster.exited = true })
		tb.finish()
		next := newFakeTB(t)
		second := backendOf(shared.Lease(next))
		next.finish()
		if second == first || !first.isTerminated() || shared.Stats().Poisonings != 1 {
			t.Fatalf("an exited daemon did not replace the cluster: %+v", shared.Stats())
		}
	})
	t.Run("at admission", func(t *testing.T) {
		fleet := &fakeFleet{}
		shared := newFakeShared(fleet)
		tb := newFakeTB(t)
		first := backendOf(shared.Lease(tb))
		tb.finish()
		first.change(func(cluster *fakeShared) { cluster.exited = true })
		next := newFakeTB(t)
		second := backendOf(shared.Lease(next))
		next.finish()
		if second == first || !first.isTerminated() || next.Failed() {
			t.Fatalf("the lease was admitted to a cluster with an exited daemon: %s", next.errorText())
		}
	})
}

func TestSharedRecyclesAreBounded(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet, WithMaxRecycles(1))
	for i := range 2 {
		tb := newFakeTB(t)
		tenant := shared.Lease(tb)
		backendOf(tenant).change(func(cluster *fakeShared) { cluster.putLocked("", fmt.Sprint("stray", i)) })
		tb.finish()
	}
	tb := newFakeTB(t)
	if tenant := shared.Lease(tb); tenant != nil || !strings.Contains(tb.errorText(), "recycles are spent") {
		t.Fatalf("a lease after the last recycle returned %v: %q", tenant, tb.errorText())
	}
	if stats := shared.Stats(); stats.Starts != 2 || stats.Recycles != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSharedPoisonedClusterEndsWithItsLastLease(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	holderTB, dirtyTB := newFakeTB(t), newFakeTB(t)
	holder := shared.Lease(holderTB)
	dirty := shared.Lease(dirtyTB)
	first := backendOf(holder)
	if backendOf(dirty) != first {
		t.Fatal("concurrent leases got different clusters")
	}
	first.put(holder.Namespace(), "kept")
	first.put("", "stray")
	dirtyTB.finish()
	if !dirtyTB.Failed() || first.isTerminated() {
		t.Fatal("the poisoned cluster ended while a tenant still held it")
	}
	if _, err := holder.CreateClient(t.Context(), "still", TenantReadWrite); err != nil {
		t.Fatalf("the remaining tenant lost its cluster: %v", err)
	}
	nextTB := newFakeTB(t)
	if backendOf(shared.Lease(nextTB)) == first {
		t.Fatal("a poisoned cluster took a new lease")
	}
	holderTB.finish()
	if !first.isTerminated() {
		t.Fatal("the poisoned cluster outlived its last lease")
	}
	nextTB.finish()
	if err := shared.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedReleaseIgnoresOtherTenantsScopes(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	aTB, bTB := newFakeTB(t), newFakeTB(t)
	a, b := shared.Lease(aTB), shared.Lease(bTB)
	cluster := backendOf(a)
	cluster.put(a.Namespace(), "a")
	cluster.put(b.Namespace(), "b")
	if _, err := b.CreateClient(t.Context(), "extra", TenantReadWrite); err != nil {
		t.Fatal(err)
	}
	aTB.finish()
	bTB.finish()
	if aTB.Failed() || bTB.Failed() || shared.Stats().Poisonings != 0 {
		t.Fatalf("a tenant was blamed for another's scope: %s %s", aTB.errorText(), bTB.errorText())
	}
}

func TestSharedReleaseWaitsForHealthToSettle(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	backendOf(tenant).change(func(cluster *fakeShared) {
		cluster.health = []string{"PG_DEGRADED"}
		cluster.healthClearsAfter = cluster.healthReads + 3
	})
	tb.finish()
	if tb.Failed() || shared.Stats().Poisonings != 0 {
		t.Fatalf("a health warning that settled poisoned the cluster: %s", tb.errorText())
	}
	tb = newFakeTB(t)
	tenant = shared.Lease(tb)
	backendOf(tenant).change(func(cluster *fakeShared) { cluster.health, cluster.healthClearsAfter = []string{"OSD_DOWN"}, 0 })
	tb.finish()
	if tb.Failed() || shared.Stats().Poisonings != 1 {
		t.Fatalf("health that did not settle: failed=%v stats=%+v", tb.Failed(), shared.Stats())
	}
}

func TestSharedMaxTenantsBoundsParallelLeases(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet, WithMaxTenants(2))
	// Each tenant holds its lease until another tenant holds one too, so two
	// leases overlap however the parallel subtests are scheduled; a fixed
	// sleep let a slow run lease them one at a time.
	partner := make(chan struct{})
	t.Run("group", func(t *testing.T) {
		for i := range 6 {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				tenant := shared.Lease(t)
				backendOf(tenant).put(tenant.Namespace(), "object")
				select {
				case partner <- struct{}{}:
				case <-partner:
				case <-time.After(10 * time.Second): // hang guard, e.g. -parallel=1
					t.Error("no other tenant held a lease at the same time")
				}
			})
		}
	})
	clusters := fleet.clusters()
	if len(clusters) != 1 {
		t.Fatalf("parallel leases started %d clusters", len(clusters))
	}
	clusters[0].mu.Lock()
	peak := clusters[0].maxClients
	clusters[0].mu.Unlock()
	if peak != 2 {
		t.Fatalf("%d tenants held the cluster at once, want 2", peak)
	}
	if stats := shared.Stats(); stats.Leases != 6 || stats.Poisonings != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := shared.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedLeaseWaitEndsWithTheContext(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet, WithMaxTenants(1))
	tb := newFakeTB(t)
	shared.Lease(tb)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := shared.lease(ctx, leaseSettings{}, t.Logf); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a lease beyond WithMaxTenants returned %v", err)
	}
	tb.finish()
	tb = newFakeTB(t)
	shared.Lease(tb)
	tb.finish()
	if tb.Failed() {
		t.Fatalf("the slot was not returned: %s", tb.errorText())
	}
}

func TestSharedStartFailureIsReportedOnce(t *testing.T) {
	fleet := &fakeFleet{startErr: classify(ErrEnvironmentUnavailable, errors.New("no docker"))}
	shared := newFakeShared(fleet)
	for range 2 {
		tb := newFakeTB(t)
		if shared.Lease(tb) != nil || !strings.Contains(tb.errorText(), "no docker") {
			t.Fatalf("lease without a cluster: %q", tb.errorText())
		}
	}
	if stats := shared.Stats(); stats.Starts != 1 {
		t.Fatalf("a failed start was retried: %+v", stats)
	}
}

func TestSharedOptionsAreValidated(t *testing.T) {
	for _, opt := range []SharedOption{WithMaxTenants(0), WithMaxRecycles(-1)} {
		shared := newFakeShared(&fakeFleet{}, opt)
		if _, err := shared.lease(t.Context(), leaseSettings{}, t.Logf); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("an invalid option returned %v", err)
		}
	}
}

func TestSharedAdmissionReportsWhatDiffers(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	first := backendOf(shared.Lease(tb))
	tb.finish()
	first.change(func(cluster *fakeShared) { cluster.flags = append(cluster.flags, "noout") })
	next := newFakeTB(t)
	second := backendOf(shared.Lease(next))
	next.finish()
	if second == first || !strings.Contains(strings.Join(next.logs, "\n"), "OSD flags are [noout]") {
		t.Fatalf("admission to a changed cluster: reused=%v, logs %q", second == first, next.logs)
	}
}

func TestSharedBaselineWaitsForLaggingHealth(t *testing.T) {
	fleet := &fakeFleet{prepare: func(cluster *fakeShared) {
		cluster.health, cluster.healthClearsAfter = []string{"POOL_APP_NOT_ENABLED"}, 3
	}}
	shared := newFakeShared(fleet)
	for range 2 {
		tb := newFakeTB(t)
		shared.Lease(tb)
		tb.finish()
		if tb.Failed() {
			t.Fatalf("lease failed: %s", tb.errorText())
		}
	}
	if stats := shared.Stats(); stats.Starts != 1 || stats.Poisonings != 0 {
		t.Fatalf("a health check that cleared after the start poisoned the cluster: %+v", stats)
	}
}

func TestSharedWarnsWithoutRunTests(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	shared.Lease(tb)
	tb.finish()
	if !strings.Contains(strings.Join(tb.logs, "\n"), "RunTests") {
		t.Fatalf("no warning about RunTests: %q", tb.logs)
	}
}

func TestSharedCloseRetriesATerminationThatFailed(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Lease(tb)
	tb.finish()
	cluster := fleet.clusters()[0]
	cluster.mu.Lock()
	cluster.terminateFailures = 1
	cluster.mu.Unlock()
	if err := shared.Close(t.Context()); err == nil {
		t.Fatal("Close reported success although the cluster could not be terminated")
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !cluster.isTerminated() {
		t.Fatal("the cluster is still running after a failed and a second Close")
	}
	if calls := cluster.terminations(); calls != 2 {
		t.Fatalf("terminate calls = %d, want 2", calls)
	}
}

func TestSharedConcurrentClosesJoinOneTermination(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Lease(tb)
	tb.finish()
	cluster := fleet.clusters()[0]
	gate := make(chan struct{})
	cluster.mu.Lock()
	cluster.terminateGate = gate
	cluster.mu.Unlock()
	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- shared.Close(t.Context()) }()
	}
	select {
	case err := <-errs:
		t.Fatalf("a Close returned (%v) while the termination was still running", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if !cluster.isTerminated() || cluster.terminations() != 1 {
		t.Fatalf("terminated=%v calls=%d, want one successful termination", cluster.isTerminated(), cluster.terminations())
	}
}

func TestSharedFirstLeaseCanBeCanceledWhileTheClusterStarts(t *testing.T) {
	fleet := &fakeFleet{entered: make(chan struct{}, 1), gate: make(chan struct{})}
	shared := newFakeShared(fleet)
	t.Cleanup(func() { _ = shared.Close(context.Background()) })
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := shared.lease(ctx, leaseSettings{}, t.Logf)
		first <- err
	}()
	<-fleet.entered
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first lease: %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		close(fleet.gate)
		t.Fatal("the first lease did not return after its context was canceled")
	}
	second := make(chan *Tenant, 1)
	tb := newFakeTB(t)
	go func() { second <- shared.Lease(tb) }()
	close(fleet.gate)
	if tenant := <-second; tenant == nil {
		t.Fatalf("second lease failed: %s", tb.errorText())
	}
	tb.finish()
	if n := len(fleet.clusters()); n != 1 {
		t.Fatalf("started %d clusters, want the one start to continue", n)
	}
}

func TestSharedRunResultKeepsAPartialCluster(t *testing.T) {
	partial := &Container{}
	backend, err := sharedRunResult(partial, errors.New("bootstrap failed"))
	if err == nil || backend == nil || backend.ops().raw() != partial {
		t.Fatalf("backend=%v err=%v, want the partial cluster with the error", backend, err)
	}
	if backend, err := sharedRunResult(nil, errors.New("create failed")); backend != nil || err == nil {
		t.Fatalf("backend=%v err=%v, want no backend", backend, err)
	}
}

func TestSharedCloseRetriesAPartialStartWhoseCleanupFailed(t *testing.T) {
	fleet := &fakeFleet{startErr: errors.New("bootstrap failed"), partial: true,
		prepare: func(f *fakeShared) { f.terminateFailures = 1 }}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	if shared.Lease(tb) != nil || !strings.Contains(tb.errorText(), "bootstrap failed") {
		t.Fatalf("lease after a failed start: %q", tb.errorText())
	}
	cluster := fleet.clusters()[0]
	if cluster.isTerminated() || cluster.terminations() != 1 {
		t.Fatalf("terminated=%v calls=%d, want one failed cleanup", cluster.isTerminated(), cluster.terminations())
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cluster.isTerminated() || cluster.terminations() != 2 {
		t.Fatalf("terminated=%v calls=%d, want Close to retry the partial cluster", cluster.isTerminated(), cluster.terminations())
	}
}
