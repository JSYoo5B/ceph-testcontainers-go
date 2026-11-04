package fault

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/system"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// fakeCPUDocker is one container's engine settings and the kernel cpu.max
// they install. An update replaces each nonzero CPU field, as Docker does,
// and installs the result in the kernel unless configuredOnly is set. The
// kernel mapping is Docker's: NanoCPUs over a 100 ms period, a quota over its
// period, and "max" for no positive quota.
type fakeCPUDocker struct {
	info       system.Info
	state      container.State
	restarts   int
	settings   cpuSettings
	kernel     cpuMax
	throttled  int64
	cgroupLine string

	// configuredOnly keeps the kernel unchanged on update; updateErr is the
	// reply of every update, which still applies unless dropUpdate is set.
	configuredOnly, dropUpdate bool
	updateErr, inspectErr      error
	readErr                    error
	warnings                   []string
	updates                    []container.Resources
	reads, closes              int
	// onUpdate runs after an update was applied.
	onUpdate func()
}

func newFakeCPUDocker(settings cpuSettings) *fakeCPUDocker {
	d := &fakeCPUDocker{
		info:     system.Info{CgroupVersion: "2", CPUCfsQuota: true, NCPU: 4},
		state:    container.State{Running: true, Pid: 42, StartedAt: "2026-11-04T00:00:00Z"},
		settings: settings,
	}
	d.kernel = fakeKernelOf(settings)
	return d
}

func fakeKernelOf(s cpuSettings) cpuMax {
	period := s.CPUPeriod
	if period == 0 {
		period = dockerCFSPeriod
	}
	switch {
	case s.NanoCPUs > 0:
		return cpuMax{Quota: s.NanoCPUs * dockerCFSPeriod / 1e9, Period: dockerCFSPeriod}
	case s.CPUQuota > 0:
		return cpuMax{Quota: s.CPUQuota, Period: period}
	}
	return cpuMax{Unlimited: true, Period: period}
}

func (d *fakeCPUDocker) Info(context.Context, mobycl.InfoOptions) (mobycl.SystemInfoResult, error) {
	return mobycl.SystemInfoResult{Info: d.info}, nil
}

func (d *fakeCPUDocker) ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if d.inspectErr != nil {
		return mobycl.ContainerInspectResult{}, d.inspectErr
	}
	state := d.state
	s := d.settings
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{
		ID: "osd", State: &state, RestartCount: d.restarts,
		HostConfig: &container.HostConfig{Resources: container.Resources{NanoCPUs: s.NanoCPUs, CPUQuota: s.CPUQuota, CPUPeriod: s.CPUPeriod, CPUShares: s.CPUShares}},
	}}, nil
}

func (d *fakeCPUDocker) ContainerUpdate(_ context.Context, _ string, options mobycl.ContainerUpdateOptions) (mobycl.ContainerUpdateResult, error) {
	r := *options.Resources
	d.updates = append(d.updates, r)
	if !d.dropUpdate {
		for _, field := range []struct {
			to   *int64
			from int64
		}{{&d.settings.NanoCPUs, r.NanoCPUs}, {&d.settings.CPUQuota, r.CPUQuota}, {&d.settings.CPUPeriod, r.CPUPeriod}, {&d.settings.CPUShares, r.CPUShares}} {
			if field.from != 0 {
				*field.to = field.from
			}
		}
		if !d.configuredOnly {
			d.kernel = fakeKernelOf(d.settings)
		}
	}
	if d.onUpdate != nil {
		d.onUpdate()
	}
	return mobycl.ContainerUpdateResult{Warnings: d.warnings}, d.updateErr
}

func (d *fakeCPUDocker) Close() error { d.closes++; return nil }

func (d *fakeCPUDocker) read(context.Context, testcontainers.Container) ([]byte, error) {
	d.reads++
	if d.readErr != nil {
		return nil, d.readErr
	}
	line := d.cgroupLine
	if line == "" {
		line = "0::/"
	}
	return fmt.Appendf(nil, "%s\n%s\n%s\nusage_usec 10\nnr_periods 20\nnr_throttled %d\nthrottled_usec %d\n", line, line, d.kernel, d.throttled, d.throttled*1000), nil
}

func (d *fakeCPUDocker) engine() cpuLimitEngine {
	return cpuLimitEngine{connect: func(context.Context) (cpuLimitClient, error) { return d, nil }, read: d.read}
}

func cpuCluster() *ceph.Container {
	return testhelper.TestCluster(pauseTestContainer{id: "control"}, testhelper.TestConfig{StartupTimeout: time.Second})
}

func limitWith(ctx context.Context, c *ceph.Container, d *fakeCPUDocker, nano int64) (*CPULimitLease, error) {
	return applyCPULimitInCluster(ctx, c, pauseTestContainer{id: "osd"}, CPULimit{NanoCPUs: nano}, d.engine())
}

func trackedCPULimit(c *ceph.Container, id string) (held any) {
	faulthook.WithTopologyNoContext(c, func() { held = testhelper.TestFaults(c, faulthook.CPULimit)[id] })
	return held
}

func TestPlanCPULimitKeepsTheBaselineEncoding(t *testing.T) {
	for name, tc := range map[string]struct {
		baseline             cpuSettings
		nano                 int64
		apply, restore       cpuSettings
		appliedMax, restored cpuMax
		restoredSettings     cpuSettings
	}{
		"nano": {baseline: cpuSettings{NanoCPUs: 2e9, CPUShares: 512}, nano: 25e7,
			apply: cpuSettings{NanoCPUs: 25e7}, restore: cpuSettings{NanoCPUs: 2e9},
			appliedMax: cpuMax{Quota: 25_000, Period: 100_000}, restored: cpuMax{Quota: 200_000, Period: 100_000},
			restoredSettings: cpuSettings{NanoCPUs: 2e9, CPUShares: 512}},
		"quota with period": {baseline: cpuSettings{CPUQuota: 100_000, CPUPeriod: 50_000}, nano: 1e9,
			apply: cpuSettings{CPUQuota: 50_000}, restore: cpuSettings{CPUQuota: 100_000},
			appliedMax: cpuMax{Quota: 50_000, Period: 50_000}, restored: cpuMax{Quota: 100_000, Period: 50_000},
			restoredSettings: cpuSettings{CPUQuota: 100_000, CPUPeriod: 50_000}},
		"quota without period": {baseline: cpuSettings{CPUQuota: 150_000}, nano: 5e8,
			apply: cpuSettings{CPUQuota: 50_000}, restore: cpuSettings{CPUQuota: 150_000},
			appliedMax: cpuMax{Quota: 50_000, Period: 100_000}, restored: cpuMax{Quota: 150_000, Period: 100_000},
			restoredSettings: cpuSettings{CPUQuota: 150_000}},
		"unset": {baseline: cpuSettings{}, nano: 25e7,
			apply: cpuSettings{CPUQuota: 25_000}, restore: cpuSettings{CPUQuota: -1},
			appliedMax: cpuMax{Quota: 25_000, Period: 100_000}, restored: cpuMax{Unlimited: true, Period: 100_000},
			restoredSettings: cpuSettings{CPUQuota: -1}},
		"unlimited quota": {baseline: cpuSettings{CPUQuota: -1, CPUPeriod: 20_000}, nano: 5e8,
			apply: cpuSettings{CPUQuota: 10_000}, restore: cpuSettings{CPUQuota: -1},
			appliedMax: cpuMax{Quota: 10_000, Period: 20_000}, restored: cpuMax{Unlimited: true, Period: 20_000},
			restoredSettings: cpuSettings{CPUQuota: -1, CPUPeriod: 20_000}},
	} {
		plan, err := planCPULimit(tc.baseline, fakeKernelOf(tc.baseline), 4, CPULimit{NanoCPUs: tc.nano})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if plan.apply != tc.apply || plan.restore != tc.restore || plan.appliedMax != tc.appliedMax || plan.restoredMax != tc.restored || plan.restored != tc.restoredSettings || plan.baseline != tc.baseline {
			t.Fatalf("%s: plan %+v", name, plan)
		}
		if plan.applied.CPUShares != tc.baseline.CPUShares || plan.applied.CPUPeriod != tc.baseline.CPUPeriod {
			t.Fatalf("%s: the limit changes settings it does not own: %+v", name, plan.applied)
		}
	}
}

func TestPlanCPULimitRefusesWhatItCannotProveOrRestore(t *testing.T) {
	nano := cpuSettings{NanoCPUs: 1e9}
	for name, tc := range map[string]struct {
		baseline cpuSettings
		kernel   cpuMax
		nano     int64
		ncpu     int
		kind     error
	}{
		"nonpositive":         {nano: 0, kind: ceph.ErrInvalidRequest},
		"not below baseline":  {baseline: nano, nano: 1e9, kind: ceph.ErrInvalidRequest},
		"not below quota":     {baseline: cpuSettings{CPUQuota: 50_000}, nano: 5e8, kind: ceph.ErrInvalidRequest},
		"engine CPU count":    {nano: 4e9, kind: ceph.ErrInvalidRequest},
		"fractional quota":    {nano: 1, kind: ceph.ErrInvalidRequest},
		"below minimum quota": {nano: 5e6, kind: ceph.ErrInvalidRequest},
		"mixed encodings":     {baseline: cpuSettings{NanoCPUs: 1e9, CPUPeriod: 100_000}, kernel: cpuMax{Quota: 100_000, Period: 100_000}, nano: 5e8, kind: ceph.ErrInvalidRequest},
		"nano kernel max":     {baseline: nano, kernel: cpuMax{Unlimited: true, Period: 100_000}, nano: 5e8, kind: ceph.ErrEnvironmentUnavailable},
		"nano kernel other":   {baseline: nano, kernel: cpuMax{Quota: 50_000, Period: 100_000}, nano: 25e7, kind: ceph.ErrEnvironmentUnavailable},
		"unset kernel finite": {kernel: cpuMax{Quota: 50_000, Period: 100_000}, nano: 25e7, kind: ceph.ErrEnvironmentUnavailable},
		"unset other period":  {kernel: cpuMax{Unlimited: true, Period: 50_000}, nano: 25e7, kind: ceph.ErrEnvironmentUnavailable},
	} {
		kernel := tc.kernel
		if kernel == (cpuMax{}) {
			kernel = fakeKernelOf(tc.baseline)
		}
		ncpu := tc.ncpu
		if ncpu == 0 {
			ncpu = 4
		}
		if plan, err := planCPULimit(tc.baseline, kernel, ncpu, CPULimit{NanoCPUs: tc.nano}); !errors.Is(err, tc.kind) {
			t.Fatalf("%s: %+v %v, want %v", name, plan, err, tc.kind)
		}
	}
}

func TestCPULimitRoundTripsFiniteAndUnlimitedBaselines(t *testing.T) {
	for name, baseline := range map[string]cpuSettings{"finite": {NanoCPUs: 2e9}, "unlimited": {}} {
		t.Run(name, func(t *testing.T) {
			c := cpuCluster()
			d := newFakeCPUDocker(baseline)
			before := d.kernel
			lease, err := limitWith(t.Context(), c, d, 25e7)
			if err != nil || lease == nil || trackedCPULimit(c, "osd") != lease {
				t.Fatalf("limit was not applied and retained: %v", err)
			}
			if d.kernel != (cpuMax{Quota: 25_000, Period: 100_000}) || len(d.updates) != 1 {
				t.Fatalf("kernel %s after %v", d.kernel, d.updates)
			}
			d.throttled = 7
			status, err := lease.Status(t.Context())
			if err != nil || status.Unlimited || status.Quota != 25*time.Millisecond || status.Period != 100*time.Millisecond || status.ThrottledPeriods != 7 || status.ThrottledTime != 7*time.Millisecond || status.Periods != 20 {
				t.Fatalf("status %+v: %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil || d.kernel != before || len(d.updates) != 2 {
				t.Fatalf("restore: %v kernel %s updates %v", err, d.kernel, d.updates)
			}
			if name == "unlimited" && d.settings != (cpuSettings{CPUQuota: -1}) {
				t.Fatalf("unlimited baseline restored as %s", d.settings)
			}
			if name == "finite" && d.settings != baseline {
				t.Fatalf("finite baseline restored as %s", d.settings)
			}
			if err := lease.Restore(t.Context()); err != nil || len(d.updates) != 2 {
				t.Fatalf("repeated restore reached Docker: %v", err)
			}
			if err := c.Terminate(t.Context()); err != nil || len(d.updates) != 2 {
				t.Fatalf("terminate repeated a complete restore: %v", err)
			}
			if d.closes == 0 {
				t.Fatal("Docker connections were not closed")
			}
		})
	}
}

func TestCPULimitRefusesUnprovableTargets(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*fakeCPUDocker)
		kind   error
	}{
		"cgroup v1":       {func(d *fakeCPUDocker) { d.info.CgroupVersion = "1" }, ceph.ErrEnvironmentUnavailable},
		"no CFS quota":    {func(d *fakeCPUDocker) { d.info.CPUCfsQuota = false }, ceph.ErrEnvironmentUnavailable},
		"host cgroupns":   {func(d *fakeCPUDocker) { d.cgroupLine = "0::/system.slice/docker-osd.scope" }, ceph.ErrEnvironmentUnavailable},
		"unreadable":      {func(d *fakeCPUDocker) { d.readErr = cpuEnvironment("cat exited 1") }, ceph.ErrEnvironmentUnavailable},
		"paused":          {func(d *fakeCPUDocker) { d.state.Paused = true }, ceph.ErrInvalidRequest},
		"stopped":         {func(d *fakeCPUDocker) { d.state.Running = false }, ceph.ErrInvalidRequest},
		"restarting":      {func(d *fakeCPUDocker) { d.state.Restarting = true }, ceph.ErrInvalidRequest},
		"kernel disagree": {func(d *fakeCPUDocker) { d.kernel = cpuMax{Quota: 1_000, Period: 100_000} }, ceph.ErrEnvironmentUnavailable},
	} {
		c := cpuCluster()
		d := newFakeCPUDocker(cpuSettings{})
		tc.change(d)
		lease, err := limitWith(t.Context(), c, d, 25e7)
		if !errors.Is(err, tc.kind) || lease != nil || len(d.updates) != 0 || trackedCPULimit(c, "osd") != nil {
			t.Fatalf("%s: lease %v err %v updates %v", name, lease, err, d.updates)
		}
	}
	c := cpuCluster()
	for name, target := range map[string]testcontainers.Container{"nil": nil, "unstarted": pauseTestContainer{}} {
		if lease, err := applyCPULimitInCluster(t.Context(), c, target, CPULimit{NanoCPUs: 25e7}, newFakeCPUDocker(cpuSettings{}).engine()); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
			t.Fatalf("%s container accepted: %v", name, err)
		}
	}
	testhelper.TestSetClosed(c, true)
	_, terminated := faulthook.Terminated()
	if lease, err := limitWith(t.Context(), c, newFakeCPUDocker(cpuSettings{}), 25e7); !errors.Is(err, terminated) || lease != nil {
		t.Fatalf("terminated cluster accepted a CPU limit: %v", err)
	}
	var missing *CPULimitLease
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("nil CPU limit restored")
	}
}

func TestCPULimitChangedBeforeDispatchSendsNothing(t *testing.T) {
	d := newFakeCPUDocker(cpuSettings{})
	reads := 0
	d.readErr = nil
	engine := d.engine()
	engine.read = func(ctx context.Context, ctr testcontainers.Container) ([]byte, error) {
		reads++
		if reads == 1 {
			// The container is restarted after its baseline was read.
			defer func() { d.state.StartedAt = "2026-11-04T00:01:00Z" }()
		}
		return d.read(ctx, ctr)
	}
	lease, err := applyCPULimitInCluster(t.Context(), cpuCluster(), pauseTestContainer{id: "osd"}, CPULimit{NanoCPUs: 25e7}, engine)
	if err == nil || lease != nil || len(d.updates) != 0 {
		t.Fatalf("a replaced generation was updated: %v %v", err, d.updates)
	}
}

func TestCPULimitUncertainApplyIsSettledByEvidence(t *testing.T) {
	// A lost reply whose update took effect is a clean application.
	d := newFakeCPUDocker(cpuSettings{NanoCPUs: 2e9})
	d.updateErr = errors.New("lost reply")
	lease, err := limitWith(t.Context(), cpuCluster(), d, 5e8)
	if err != nil || lease == nil || len(d.updates) != 1 {
		t.Fatalf("applied update with a lost reply: %v", err)
	}

	// A lost reply whose update did not arrive keeps a handle that Restore
	// completes without sending anything.
	c := cpuCluster()
	d = newFakeCPUDocker(cpuSettings{NanoCPUs: 2e9})
	d.updateErr, d.dropUpdate = errors.New("lost reply"), true
	lease, err = limitWith(t.Context(), c, d, 5e8)
	if !errors.Is(err, d.updateErr) || lease == nil || trackedCPULimit(c, "osd") != lease {
		t.Fatalf("uncertain update did not return ownership: %v", err)
	}
	if err := lease.Restore(t.Context()); err != nil || len(d.updates) != 1 || !lease.complete {
		t.Fatalf("restore of an update that never arrived: %v %v", err, d.updates)
	}

	// Warnings with a limit that is not in effect are reported.
	d = newFakeCPUDocker(cpuSettings{})
	d.warnings, d.dropUpdate = []string{"cfs quota unsupported"}, true
	if lease, err := limitWith(t.Context(), cpuCluster(), d, 5e8); err == nil || lease == nil || !strings.Contains(err.Error(), "cfs quota unsupported") {
		t.Fatalf("warnings were not reported: %v", err)
	}
}

func TestCPULimitConfiguredOnlyIsNotApplied(t *testing.T) {
	d := newFakeCPUDocker(cpuSettings{})
	d.configuredOnly = true
	lease, err := limitWith(t.Context(), cpuCluster(), d, 5e8)
	if err == nil || lease == nil || !strings.Contains(err.Error(), "configured only") {
		t.Fatalf("configured-only update was accepted: %v", err)
	}
	d.configuredOnly = false
	if err := lease.Restore(t.Context()); err != nil || d.settings != (cpuSettings{CPUQuota: -1}) || !d.kernel.Unlimited {
		t.Fatalf("configured-only limit not restored: %v %s %s", err, d.settings, d.kernel)
	}
}

func TestCPULimitRestoreRefusesForeignState(t *testing.T) {
	for name, tc := range map[string]struct {
		change   func(*fakeCPUDocker)
		complete bool
		kind     error
	}{
		"outside settings":  {change: func(d *fakeCPUDocker) { d.settings.CPUQuota = 10_000; d.kernel = fakeKernelOf(d.settings) }},
		"outside kernel":    {change: func(d *fakeCPUDocker) { d.kernel = cpuMax{Quota: 1_000, Period: 100_000} }},
		"restarted":         {change: func(d *fakeCPUDocker) { d.state.StartedAt = "2026-11-04T00:01:00Z"; d.state.Pid = 43 }},
		"restart count":     {change: func(d *fakeCPUDocker) { d.restarts = 1 }},
		"paused":            {change: func(d *fakeCPUDocker) { d.state.Paused = true }, kind: ceph.ErrInvalidRequest},
		"inspect fails":     {change: func(d *fakeCPUDocker) { d.inspectErr = errors.New("engine unavailable") }},
		"removed":           {change: func(d *fakeCPUDocker) { d.inspectErr = errdefs.ErrNotFound }, complete: true},
		"restored outside":  {change: func(d *fakeCPUDocker) { d.settings = cpuSettings{CPUQuota: -1}; d.kernel = fakeKernelOf(d.settings) }, complete: true},
		"stopped unchanged": {change: func(d *fakeCPUDocker) { d.state.Running, d.state.Pid = false, 0 }, complete: true},
	} {
		c := cpuCluster()
		d := newFakeCPUDocker(cpuSettings{})
		lease, err := limitWith(t.Context(), c, d, 5e8)
		if err != nil {
			t.Fatal(err)
		}
		tc.change(d)
		err = lease.Restore(t.Context())
		if tc.complete {
			if err != nil || !lease.complete {
				t.Fatalf("%s: %v", name, err)
			}
			continue
		}
		if err == nil || lease.complete || (tc.kind != nil && !errors.Is(err, tc.kind)) {
			t.Fatalf("%s: restore %v complete %t", name, err, lease.complete)
		}
		if len(d.updates) != 1 {
			t.Fatalf("%s: foreign state was overwritten: %v", name, d.updates)
		}
		if err := c.Terminate(t.Context()); err == nil || !strings.Contains(err.Error(), "restore CPU limit") {
			t.Fatalf("%s: terminate hid the failed restore: %v", name, err)
		}
	}
}

func TestCPULimitRestoresTheSettingsOfAStoppedTarget(t *testing.T) {
	d := newFakeCPUDocker(cpuSettings{NanoCPUs: 2e9})
	lease, err := limitWith(t.Context(), cpuCluster(), d, 5e8)
	if err != nil {
		t.Fatal(err)
	}
	d.state.Running, d.state.Pid = false, 0
	reads := d.reads
	if err := lease.Restore(t.Context()); err != nil || !lease.complete || d.settings != (cpuSettings{NanoCPUs: 2e9}) || d.reads != reads {
		t.Fatalf("stopped target: %v %s", err, d.settings)
	}
	if _, err := lease.Status(t.Context()); err == nil {
		t.Fatal("status of a stopped target")
	}
}

func TestCPULimitFailedRestoreStaysRetryableThroughTerminate(t *testing.T) {
	c := cpuCluster()
	d := newFakeCPUDocker(cpuSettings{})
	lease, err := limitWith(t.Context(), c, d, 5e8)
	if err != nil {
		t.Fatal(err)
	}
	// Overlap returns the live handle without another update.
	again, err := limitWith(t.Context(), c, d, 25e7)
	if !errors.Is(err, ceph.ErrInvalidRequest) || again != lease || len(d.updates) != 1 {
		t.Fatalf("overlap: %v %v", again, err)
	}
	d.dropUpdate, d.updateErr = true, errors.New("engine unavailable")
	if err := c.Terminate(t.Context()); !errors.Is(err, d.updateErr) || lease.complete || trackedCPULimit(c, "osd") != lease {
		t.Fatalf("failed restore lost retryability: %v", err)
	}
	d.dropUpdate, d.updateErr = false, nil
	if err := c.Terminate(t.Context()); err != nil || !lease.complete || !d.kernel.Unlimited {
		t.Fatalf("retried teardown did not restore: %v", err)
	}
}

func TestCPULimitQueuedHandleDeadlines(t *testing.T) {
	c := cpuCluster()
	assertContextLockQueued(t, testhelper.TestState(c).Mutex, func(ctx context.Context) error {
		_, err := limitWith(ctx, c, newFakeCPUDocker(cpuSettings{}), 5e8)
		return err
	})
	d := newFakeCPUDocker(cpuSettings{})
	lease, err := limitWith(t.Context(), c, d, 5e8)
	if err != nil {
		t.Fatal(err)
	}
	assertContextLockQueued(t, &lease.mu, lease.Restore)
	assertContextLockQueued(t, &lease.mu, func(ctx context.Context) error { _, err := lease.Status(ctx); return err })
	if len(d.updates) != 1 {
		t.Fatal("a queued restore reached Docker")
	}
}

func TestParseCPUFilesRefusesIncompleteEvidence(t *testing.T) {
	stat := "nr_periods 1\nnr_throttled 0\nthrottled_usec 0\n"
	for name, data := range map[string]string{
		"truncated":    "0::/\n0::/\nmax 100000",
		"cgroup v1":    "12:cpu,cpuacct:/\n0::/\nmax 100000\n" + stat,
		"other cgroup": "0::/\n0::/child\nmax 100000\n" + stat,
		"no period":    "0::/\n0::/\nmax\n" + stat,
		"zero quota":   "0::/\n0::/\n0 100000\n" + stat,
		"no stat keys": "0::/\n0::/\nmax 100000\nusage_usec 1\n",
		"duplicate":    "0::/\n0::/\nmax 100000\n" + stat + "nr_periods 2\n",
		"negative":     "0::/\n0::/\nmax 100000\nnr_periods -1\nnr_throttled 0\nthrottled_usec 0\n",
	} {
		if max, status, err := parseCPUFiles([]byte(data)); err == nil {
			t.Fatalf("%s accepted: %s %+v", name, max, status)
		}
	}
	max, status, err := parseCPUFiles([]byte("0::/\n0::/\n25000 100000\n" + stat))
	if err != nil || max != (cpuMax{Quota: 25_000, Period: 100_000}) || status.Quota != 25*time.Millisecond || status.Periods != 1 {
		t.Fatalf("valid files: %s %+v %v", max, status, err)
	}
}
