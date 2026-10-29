package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/sharedinvariants"
	"github.com/testcontainers/testcontainers-go"
)

// SharedPool is the pool whose RADOS namespaces a shared cluster gives its
// tenants.
const SharedPool = sharedinvariants.SharedPool

const (
	defaultMaxTenants  = 4
	defaultMaxRecycles = 2
	// sharedSettleTimeout bounds the wait for PGs, health and the other
	// conditions of a lease that can lag behind a tenant's last operation.
	sharedSettleTimeout = 30 * time.Second
	// sharedReleaseTimeout bounds a release and the termination of a
	// poisoned cluster, which run after the test's context ended.
	sharedReleaseTimeout = 3 * time.Minute
	// sharedTenantLabel marks the containers that Tenant.WithClient attached.
	sharedTenantLabel = "org.testcontainers.ceph.tenant"
)

// SharedCluster lends one lazily started cluster to the tests of a test
// binary. Each Lease gets a Tenant: a RADOS namespace of SharedPool and a
// Cephx client limited to it. The cluster's *Container is never handed out;
// while it is shared, the methods that could reach outside one tenant return
// ErrSharedCluster. A release that finds the cluster changed outside the
// tenant's scope, a failed tenant test or an exited daemon poisons the
// cluster: it takes no new leases, ends when its last lease ends, and the
// next Lease starts a fresh one, at most WithMaxRecycles times.
//
// A test that needs the whole cluster takes it with Exclusive instead: it
// waits until no tenant holds a lease, gets the *Container with the gate
// open, and its release retires the cluster, so the next lease starts a
// fresh one.
//
// Create one per test binary in a package variable and end it with RunTests
// in TestMain. Its methods are safe for concurrent use, also by parallel
// tests.
type SharedCluster struct {
	image    string
	settings sharedSettings

	mu        sync.Mutex
	current   *sharedInstance
	instances map[*sharedInstance]bool
	recycle   bool
	closed    bool
	managed   bool
	warned    bool
	stats     SharedStats
	// holds counts, by test name, the tenant slots held: leases granted or
	// being set up. exclusive names the test that holds the exclusive lease,
	// and exclusiveHeld says whether one is held. waiters are the leases and
	// exclusive leases waiting for admission, in arrival order; wake is
	// closed and replaced whenever admission may have changed.
	holds         map[string]int
	slots         int
	exclusive     string
	exclusiveHeld bool
	waiters       []*sharedWaiter
	wake          chan struct{}
}

// SharedOption configures NewShared.
type SharedOption func(*sharedSettings)

type sharedSettings struct {
	runOpts     []testcontainers.ContainerCustomizer
	maxTenants  int
	maxRecycles int
	err         error
	// serviceList comes from WithServices; services is the validated set
	// that NewShared builds from it.
	serviceList []sharedService
	services    sharedServiceSet
	// start replaces Run in tests.
	start sharedStarter
	// settle replaces sharedSettleTimeout in tests.
	settle time.Duration
}

// WithSharedRunOptions passes options to the Run that starts each cluster.
func WithSharedRunOptions(opts ...testcontainers.ContainerCustomizer) SharedOption {
	return func(s *sharedSettings) { s.runOpts = append(s.runOpts, opts...) }
}

// WithMaxTenants bounds the leases that are held at once; Lease waits for a
// free one. The default is 4.
func WithMaxTenants(n int) SharedOption {
	return func(s *sharedSettings) {
		if n < 1 {
			s.err = errors.Join(s.err, fmt.Errorf("WithMaxTenants needs at least 1, got %d", n))
		}
		s.maxTenants = n
	}
}

// WithMaxRecycles bounds how many fresh clusters replace poisoned ones; a
// Lease after that fails. The default is 2.
func WithMaxRecycles(n int) SharedOption {
	return func(s *sharedSettings) {
		if n < 0 {
			s.err = errors.Join(s.err, fmt.Errorf("WithMaxRecycles needs at least 0, got %d", n))
		}
		s.maxRecycles = n
	}
}

// LeaseOption configures one Lease.
type LeaseOption func(*leaseSettings)

type leaseSettings struct {
	keepOnFailure bool
	// holder names the test that holds the lease; see related.
	holder string
}

// WithKeepOnTenantFailure keeps the cluster when the tenant's test fails but
// its release finds the cluster clean. Without it, a failed test poisons the
// cluster.
func WithKeepOnTenantFailure() LeaseOption {
	return func(s *leaseSettings) { s.keepOnFailure = true }
}

// SharedStats counts what a SharedCluster did. Durations are totals.
type SharedStats struct {
	// Starts counts the clusters started, recycled ones included; Recycles
	// counts those that replaced a poisoned cluster.
	Starts, Recycles int
	// Leases counts the leases granted, Poisonings the clusters poisoned.
	Leases, Poisonings int
	// Exclusives counts the exclusive leases granted. Each retires its
	// cluster; the clusters started after them count in Starts but not in
	// Recycles.
	Exclusives int
	// StartTime is the time spent starting clusters, LeaseWait the time
	// Lease waited for a free tenant slot or a starting cluster, LeaseSetup
	// the time admitting tenants and creating their clients, and Release the
	// time releasing and verifying them. ExclusiveWait is the time Exclusive
	// waited for the tenants to end and for the cluster.
	StartTime, LeaseWait, LeaseSetup, Release, ExclusiveWait time.Duration
}

// NewShared describes a shared cluster of image. Nothing starts until the
// first Lease.
func NewShared(image string, opts ...SharedOption) *SharedCluster {
	settings := sharedSettings{maxTenants: defaultMaxTenants, maxRecycles: defaultMaxRecycles, start: startSharedContainer, settle: sharedSettleTimeout}
	for _, opt := range opts {
		opt(&settings)
	}
	set, err := newSharedServiceSet(settings.serviceList)
	settings.err = errors.Join(settings.err, err)
	settings.services = set
	return &SharedCluster{image: image, settings: settings, instances: map[*sharedInstance]bool{},
		holds: map[string]int{}, wake: make(chan struct{})}
}

// RunTests runs m and then closes every s. It returns the exit code for
// os.Exit: m's, or 1 when m passed and a Close failed.
//
//	var cluster = harness.NewShared(ceph.DefaultImage)
//
//	func TestMain(m *testing.M) { os.Exit(harness.RunTests(m, cluster)) }
func RunTests(m *testing.M, s ...*SharedCluster) int {
	for _, cluster := range s {
		cluster.mu.Lock()
		cluster.managed = true
		cluster.mu.Unlock()
	}
	code := m.Run()
	for _, cluster := range s {
		ctx, cancel := context.WithTimeout(context.Background(), sharedReleaseTimeout)
		err := cluster.Close(ctx)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "ceph: close shared cluster:", err)
			if code == 0 {
				code = 1
			}
		}
	}
	return code
}

// Stats returns a snapshot of the counters.
func (s *SharedCluster) Stats() SharedStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Close terminates every cluster that s started, stopping one that is
// starting. Later leases fail. A tenant still held loses its cluster. A
// cluster whose termination failed stays owned by s, so a later Close tries
// again; concurrent Closes wait for the same termination.
func (s *SharedCluster) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.current = nil
	s.signalLocked()
	instances := make([]*sharedInstance, 0, len(s.instances))
	for inst := range s.instances {
		instances = append(instances, inst)
	}
	s.mu.Unlock()
	var errs []error
	for _, inst := range instances {
		// A start that is still running is stopped and joined.
		inst.stopStart()
		select {
		case <-inst.ready:
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
		errs = append(errs, s.terminate(ctx, inst))
	}
	return errors.Join(errs...)
}

// Lease grants t a tenant of the shared cluster, starting the cluster when
// none is running, and releases it in t.Cleanup. It waits while
// WithMaxTenants leases are held. It fails t when the cluster cannot start,
// the recycle budget is spent, or the tenant cannot be set up. A test that
// holds a lease must not wait for another one when the leases held by the
// running tests can reach WithMaxTenants.
//
// A Lease waits while an exclusive lease is held, and behind an Exclusive
// that asked first, unless the test or one of its parents already holds a
// lease: Exclusive waits for that lease, so the second one must not wait for
// Exclusive. A Lease by the test that holds the exclusive lease, or by one of
// its subtests, fails at once instead of waiting for itself.
func (s *SharedCluster) Lease(t testing.TB, opts ...LeaseOption) *Tenant {
	t.Helper()
	settings := leaseSettings{holder: t.Name()}
	for _, opt := range opts {
		opt(&settings)
	}
	tenant, err := s.lease(t.Context(), settings, t.Logf)
	if err != nil {
		t.Fatalf("ceph: lease shared cluster: %v", err)
		return nil
	}
	t.Cleanup(func() { s.release(tenant, t) })
	return tenant
}

// sharedInstance is one cluster that s started.
type sharedInstance struct {
	// ready is closed when the start ended; startErr then reports a failure.
	// stopStart cancels a start that is still running.
	ready     chan struct{}
	startErr  error
	stopStart context.CancelFunc
	backend   sharedBackend
	baseline  sharedInvariants
	// live holds the tenants that hold a lease, issued every tenant ID that
	// the instance gave out.
	live     map[string]*Tenant
	issued   map[string]bool
	poisoned string
	// retired is set when the first termination begins: inst takes no
	// leases and its releases skip verification. terminating is closed when
	// a running termination ends; terminated is set only when one succeeded.
	retired     bool
	terminating chan struct{}
	terminated  bool
}

func (s *SharedCluster) lease(ctx context.Context, settings leaseSettings, logf func(string, ...any)) (*Tenant, error) {
	if s.settings.err != nil {
		return nil, classify(ceph.ErrInvalidRequest, s.settings.err)
	}
	waitStart := time.Now()
	if err := s.acquire(ctx, false, settings.holder); err != nil {
		return nil, err
	}
	granted := false
	defer func() {
		if !granted {
			s.releaseSlot(settings.holder)
		}
	}()
	for {
		inst, err := s.instance(ctx, logf)
		if err != nil {
			return nil, err
		}
		tenant := &Tenant{owner: s, inst: inst, id: newTenantID(), keepOnFailure: settings.keepOnFailure, holder: settings.holder}
		if !s.reserve(inst, tenant) {
			continue
		}
		setupStart := time.Now()
		s.mu.Lock()
		s.stats.LeaseWait += setupStart.Sub(waitStart)
		s.mu.Unlock()
		if err := s.admit(ctx, inst); err != nil {
			if ctx.Err() != nil {
				s.unreserve(inst, tenant)
				return nil, errors.Join(err, ctx.Err())
			}
			// The cluster is not as its baseline: no tenant can trust it.
			s.poison(inst, "admission: "+err.Error())
			s.unreserve(inst, tenant)
			logf("ceph: shared cluster poisoned before a lease: %v", err)
			waitStart = time.Now()
			continue
		}
		client, err := tenant.newClient(ctx, tenant.id, TenantReadWrite)
		if err != nil {
			problems, verifyErr := s.cleanAndVerify(ctx, tenant)
			if len(problems) > 0 || verifyErr != nil {
				s.poison(inst, "failed tenant setup")
			}
			s.unreserve(inst, tenant)
			return nil, fmt.Errorf("create tenant client: %w", err)
		}
		tenant.client = client
		s.mu.Lock()
		s.stats.Leases++
		s.stats.LeaseSetup += time.Since(setupStart)
		s.mu.Unlock()
		granted = true
		return tenant, nil
	}
}

// instance returns the current cluster, starting one when there is none.
func (s *SharedCluster) instance(ctx context.Context, logf func(string, ...any)) (*sharedInstance, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, invalidRequest("shared cluster is closed")
	}
	inst := s.current
	if inst == nil {
		if s.recycle {
			if s.stats.Recycles >= s.settings.maxRecycles {
				s.mu.Unlock()
				return nil, invalidRequest("shared cluster was poisoned and its %d recycles are spent", s.settings.maxRecycles)
			}
			s.stats.Recycles++
			s.recycle = false
		}
		// The start belongs to the instance, not to the lease that began it:
		// every lease waits for it below, and Close stops it.
		startCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
		inst = &sharedInstance{ready: make(chan struct{}), stopStart: stop, live: map[string]*Tenant{}, issued: map[string]bool{}}
		s.current = inst
		s.instances[inst] = true
		s.stats.Starts++
		warn := !s.managed && !s.warned
		s.warned = s.warned || warn
		s.mu.Unlock()
		if warn {
			logf("ceph: the shared cluster is not registered with RunTests; unless the test binary closes it, Ryuk removes it when the process exits")
		}
		go s.start(startCtx, inst)
	} else {
		s.mu.Unlock()
	}
	select {
	case <-inst.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if inst.startErr != nil {
		return nil, fmt.Errorf("start shared cluster: %w", inst.startErr)
	}
	return inst, nil
}

// start starts inst's cluster, creates SharedPool and records the baseline.
// Leases wait for it, so a canceled lease does not stop it. A cluster that
// failed to start and cannot be terminated stays owned by s for Close.
func (s *SharedCluster) start(ctx context.Context, inst *sharedInstance) {
	began := time.Now()
	backend, err := s.settings.start(ctx, s.image, s.settings.runOpts)
	if err == nil {
		err = backend.createPool(ctx, SharedPool)
	}
	if err == nil {
		err = backend.prepareServices(ctx, s.settings.services.prepares)
	}
	if err == nil {
		settleCtx, cancel := context.WithTimeout(ctx, max(s.settings.settle, time.Second)*4)
		err = backend.ready(settleCtx)
		cancel()
	}
	var baseline sharedInvariants
	if err == nil {
		baseline, err = s.settledBaseline(ctx, backend)
	}
	if err == nil {
		backend.setSharing(sharedWithTenants)
	}
	s.mu.Lock()
	inst.backend = backend
	if err != nil {
		// Later leases fail with the same error instead of starting again.
		inst.startErr = err
		inst.poisoned = "start failed"
	} else {
		inst.baseline = baseline
	}
	s.stats.StartTime += time.Since(began)
	s.mu.Unlock()
	close(inst.ready)
	if err != nil {
		_ = s.terminate(context.WithoutCancel(ctx), inst)
	}
}

// sharedLaggingHealth lists the health checks that a new cluster can still
// report shortly after it is ready: the MON reports a new pool's missing
// application until a later health tick sees the one it was given.
var sharedLaggingHealth = []string{"POOL_APP_NOT_ENABLED"}

// settledBaseline records the baseline once the checks that lag behind the
// start cleared, waiting at most the settle timeout.
func (s *SharedCluster) settledBaseline(ctx context.Context, backend sharedBackend) (sharedInvariants, error) {
	deadline := time.Now().Add(s.settings.settle)
	lagging := func(code string) bool { return slices.Contains(sharedLaggingHealth, code) }
	for {
		baseline, err := backend.invariants(ctx, SharedPool)
		if err != nil || !slices.ContainsFunc(baseline.Health, lagging) || time.Now().After(deadline) {
			if err == nil {
				baseline.Resources, err = backend.resources(ctx, s.settings.services.probes)
			}
			return baseline, err
		}
		select {
		case <-ctx.Done():
			return baseline, ctx.Err()
		case <-time.After(sharedPollInterval(s.settings.settle)):
		}
	}
}

// reserve registers tenant with inst unless inst no longer takes leases.
func (s *SharedCluster) reserve(inst *sharedInstance, tenant *Tenant) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || inst.poisoned != "" || inst.retired || s.current != inst {
		return false
	}
	inst.live[tenant.id] = tenant
	inst.issued[tenant.id] = true
	return true
}

// unreserve forgets tenant and terminates a poisoned inst that it drained.
func (s *SharedCluster) unreserve(inst *sharedInstance, tenant *Tenant) {
	s.mu.Lock()
	delete(inst.live, tenant.id)
	tenant.released = true
	drained := inst.poisoned != "" && len(inst.live) == 0 && !inst.retired
	s.mu.Unlock()
	if drained {
		// A failure leaves inst owned by s, so Close retries and reports it.
		ctx, cancel := context.WithTimeout(context.Background(), sharedReleaseTimeout)
		defer cancel()
		_ = s.terminate(ctx, inst)
	}
}

// poison stops inst from taking leases; the next Lease starts a fresh one.
func (s *SharedCluster) poison(inst *sharedInstance, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst.poisoned != "" {
		return
	}
	inst.poisoned = reason
	s.stats.Poisonings++
	if s.current == inst {
		s.current = nil
		s.recycle = true
	}
}

// terminate ends inst's cluster. A caller that finds a termination running
// waits for it and tries again if it failed. Only a successful termination
// releases inst; after a failure s still owns it.
func (s *SharedCluster) terminate(ctx context.Context, inst *sharedInstance) error {
	for {
		s.mu.Lock()
		inst.retired = true
		if inst.terminated || inst.backend == nil {
			inst.terminated = true
			delete(s.instances, inst)
			s.mu.Unlock()
			return nil
		}
		if s.current == inst {
			s.current = nil
		}
		if running := inst.terminating; running != nil {
			s.mu.Unlock()
			select {
			case <-running:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		done := make(chan struct{})
		inst.terminating = done
		s.mu.Unlock()
		err := inst.backend.terminate(ctx)
		s.mu.Lock()
		inst.terminating = nil
		if err == nil {
			inst.terminated = true
			delete(s.instances, inst)
		}
		s.mu.Unlock()
		close(done)
		return err
	}
}

// admit checks that inst is as its baseline before a tenant uses it: its
// daemons run, PGs are active+clean, and health, OSD flags and the
// blocklist match. Conditions that can lag get the settle timeout.
func (s *SharedCluster) admit(ctx context.Context, inst *sharedInstance) error {
	if err := inst.backend.daemonsRunning(ctx); err != nil {
		return err
	}
	// The settle deadline bounds the waiting, not the queries, so that an
	// expired wait reports what differs rather than a canceled query. One
	// read of the invariants also tells whether the PGs are active+clean.
	settle, cancel := context.WithTimeout(ctx, s.settings.settle)
	defer cancel()
	for {
		current, err := inst.backend.invariants(ctx, SharedPool)
		if err != nil {
			return err
		}
		problems := inst.baseline.AdmissionDiff(current)
		if len(problems) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-settle.Done():
			return errors.New(strings.Join(problems, "; "))
		case <-time.After(sharedPollInterval(s.settings.settle)):
		}
	}
}

func sharedPollInterval(settle time.Duration) time.Duration {
	return min(time.Second, max(settle/20, time.Millisecond))
}

// release ends tenant's lease in t's cleanup.
func (s *SharedCluster) release(tenant *Tenant, t testing.TB) {
	began := time.Now()
	defer func() {
		s.mu.Lock()
		s.stats.Release += time.Since(began)
		s.mu.Unlock()
		s.releaseSlot(tenant.holder)
	}()
	inst := tenant.inst
	s.mu.Lock()
	gone := inst.retired
	s.mu.Unlock()
	if gone {
		s.unreserve(inst, tenant)
		return
	}
	failed := t.Failed()
	ctx, cancel := context.WithTimeout(context.Background(), sharedReleaseTimeout)
	defer cancel()
	problems, err := s.cleanAndVerify(ctx, tenant)
	switch {
	case len(problems) > 0:
		s.poison(inst, "tenant "+tenant.id+" left changes outside its scope")
		t.Errorf("ceph: shared cluster tenant %s changed the cluster outside its scope, so the cluster is poisoned: %s",
			tenant.id, strings.Join(problems, "; "))
	case err != nil:
		s.poison(inst, "release verification failed")
		t.Logf("ceph: shared cluster poisoned: the release of tenant %s could not be verified: %v", tenant.id, err)
	case failed && !tenant.keepOnFailure:
		s.poison(inst, "tenant "+tenant.id+" failed")
		t.Logf("ceph: shared cluster poisoned because the test of tenant %s failed; WithKeepOnTenantFailure keeps it", tenant.id)
	}
	s.unreserve(inst, tenant)
}

// cleanAndVerify removes tenant's service resources, clients and objects,
// then compares the cluster with its baseline. Problems are differences the
// tenant left; an error is a removal or check that could not run.
func (s *SharedCluster) cleanAndVerify(ctx context.Context, tenant *Tenant) ([]string, error) {
	inst := tenant.inst
	backend := inst.backend
	// Service resources go first, in reverse order of attachment, while
	// the tenant's clients still exist.
	remains, err := releaseScopes(ctx, tenant.takeScopes())
	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	tenant.mu.Lock()
	clients := slices.Clone(tenant.clients)
	tenant.mu.Unlock()
	for _, client := range slices.Backward(clients) {
		if err := backend.deleteClient(ctx, client); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", client.Name(), err))
		}
	}
	if err := backend.purgeNamespace(ctx, SharedPool, tenant.id); err != nil {
		errs = append(errs, fmt.Errorf("remove the objects of namespace %s: %w", tenant.id, err))
	}
	if err := backend.daemonsRunning(ctx); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return remains, errors.Join(errs...)
	}
	resources, err := backend.resources(ctx, s.settings.services.probes)
	if err != nil {
		return remains, err
	}
	settle, cancel := context.WithTimeout(ctx, s.settings.settle)
	defer cancel()
	for {
		current, err := backend.invariants(ctx, SharedPool)
		if err != nil {
			return remains, err
		}
		current.Resources = resources
		s.mu.Lock()
		others := map[string]bool{}
		for id := range inst.issued {
			if id != tenant.id {
				others[id] = true
			}
		}
		s.mu.Unlock()
		problems, lagging := inst.baseline.ReleaseDiff(current, SharedPool, tenant.id, others)
		if problems = append(remains, problems...); len(problems) > 0 || !lagging {
			return problems, nil
		}
		// Health can lag behind the tenant's last operation; when it does
		// not settle, the cluster is unusable but not proven dirty.
		select {
		case <-settle.Done():
			return nil, fmt.Errorf("cluster did not settle: %s", strings.Join(inst.baseline.AdmissionDiff(current), "; "))
		case <-time.After(sharedPollInterval(s.settings.settle)):
		}
	}
}

func newTenantID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "t" + hex.EncodeToString(b[:])
}
