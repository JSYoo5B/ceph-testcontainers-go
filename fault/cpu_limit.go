package fault

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// CPULimit is the CPU bandwidth that ApplyCPULimit installs on one container.
type CPULimit struct {
	// NanoCPUs is the bandwidth in billionths of a CPU, as Docker's --cpus
	// option sets it: 250_000_000 is a quarter of one CPU. It must be below
	// the target's current limit and below the engine's CPU count, and give a
	// CFS quota of at least one millisecond and a whole number of
	// microseconds per period (a multiple of 10_000 for the default
	// 100 ms period).
	NanoCPUs int64
}

// CPULimitStatus is the kernel's account of the target's cgroup v2 CPU
// controller, read from cpu.max and cpu.stat inside the target.
type CPULimitStatus struct {
	// Unlimited reports a cpu.max quota of "max"; Quota is zero then.
	Unlimited bool
	// The target's processes may run for Quota in each Period.
	Quota, Period time.Duration
	// Periods, ThrottledPeriods and ThrottledTime are cpu.stat's nr_periods,
	// nr_throttled and throttled_usec. They count from the container's start
	// and never decrease: compare two readings to see throttling.
	Periods, ThrottledPeriods int64
	ThrottledTime             time.Duration
}

// CPULimitLease owns one CPU bandwidth limit that ApplyCPULimit installed on
// a running container. Restore puts back the CPU settings that the container
// had before. A non-nil handle returned with an error must be restored: the
// update was sent, and its outcome is settled by Restore from the engine's
// and the kernel's current state, never by sending it again. Restore is
// retryable and idempotent.
type CPULimitLease struct {
	ContainerID string
	mu          sync.Mutex
	engine      cpuLimitEngine
	target      testcontainers.Container
	generation  cpuGeneration
	plan        cpuPlan
	complete    bool
}

// ApplyCPULimit lowers the CPU bandwidth of an owned daemon or a caller-owned
// WithClient container while it keeps running, as with docker update
// --cpus, so that a test can watch its SUT meet a slow collaborator: an OSD
// throttled to a fraction of a CPU still answers, but serves client
// operations and heartbeats late. The limit applies to every process of the
// container through its cgroup v2 CFS quota, and the process keeps its
// identity, memory and connections.
//
// The target must be running and unpaused, in a cgroup v2 namespace of its
// own (Docker's default on cgroup v2) with the cpu controller enabled, and
// its image must provide cat. Its current CPU setting is the baseline: a
// NanoCPUs limit, a CFS quota with or without a period, or none. Settings
// that mix NanoCPUs with a quota or period are refused, and so is a target
// whose kernel cpu.max does not express its engine settings. The limit is
// installed in the encoding of the baseline. ApplyCPULimit returns without
// error only after the engine's settings and the kernel's cpu.max of the
// same container generation both show it; a lost update reply is settled
// by that evidence. Reading cpu.max and cpu.stat runs cat inside the
// target, which counts against its limit.
//
// Only one limit per container is allowed. The cluster retains the handle
// and restores it during Terminate, after paused containers are resumed;
// restore explicitly before removing a caller-owned container, and before
// stopping, restarting or pausing the target.
//
//purpose:condition
func ApplyCPULimit(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, limit CPULimit) (*CPULimitLease, error) {
	return applyCPULimitInCluster(ctx, c, ctr, limit, cpuLimitEngine{connect: connectCPULimitDocker, read: readCPUFiles})
}

// cpuLimitClient is the part of the Docker client that a CPU limit uses.
type cpuLimitClient interface {
	Info(context.Context, mobycl.InfoOptions) (mobycl.SystemInfoResult, error)
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	ContainerUpdate(context.Context, string, mobycl.ContainerUpdateOptions) (mobycl.ContainerUpdateResult, error)
	Close() error
}

// cpuLimitEngine reaches Docker and the target's cgroup files. Only these two
// are replaceable in unit tests; admission, the plan, the evidence checks,
// registry ownership and Terminate use the actual implementation.
//
//testdouble:dependency-port
type cpuLimitEngine struct {
	connect func(context.Context) (cpuLimitClient, error)
	// read returns the output of cpuFilesArgv run inside the container.
	read func(context.Context, testcontainers.Container) ([]byte, error)
}

func connectCPULimitDocker(ctx context.Context) (cpuLimitClient, error) {
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// cpuFilesArgv prints the cgroup membership of the container's init and of
// the reader itself, then the bandwidth setting and statistics of the cgroup
// at the root of the container's cgroup namespace.
var cpuFilesArgv = []string{"cat", "/proc/1/cgroup", "/proc/self/cgroup", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.stat"}

// cpuFilesLimit bounds the output read from the target.
const cpuFilesLimit = 64 << 10

func readCPUFiles(ctx context.Context, ctr testcontainers.Container) ([]byte, error) {
	code, reader, err := ctr.Exec(ctx, cpuFilesArgv)
	if err != nil {
		return nil, fmt.Errorf("read the target's CPU cgroup files: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, io.LimitReader(reader, cpuFilesLimit)); err != nil {
		return nil, fmt.Errorf("read the target's CPU cgroup files: %w", err)
	}
	if code != 0 {
		return nil, cpuEnvironment("the target's cgroup v2 CPU files are unreadable: cat exited %d: %.256s", code, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func cpuEnvironment(format string, args ...any) error {
	return nshelper.Category(fmt.Sprintf(format, args...), ceph.ErrEnvironmentUnavailable)
}

func applyCPULimitInCluster(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, limit CPULimit, engine cpuLimitEngine) (lease *CPULimitLease, err error) {
	if limit.NanoCPUs <= 0 {
		return nil, nshelper.InvalidRequest("a CPU limit needs a positive NanoCPUs")
	}
	err = faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) {
			_, terminated := faulthook.Terminated()
			return terminated
		}
		if ctr == nil || ctr.GetContainerID() == "" {
			return nshelper.InvalidRequest("a CPU limit requires a live cluster and a started container")
		}
		// An incomplete previous limit is returned with the refusal; a
		// complete one stays registered until this one replaces it.
		token, previous, err := faulthook.Reserve(ctx, c, faulthook.CPULimit, ctr.GetContainerID())
		if err != nil {
			lease, _ = previous.(*CPULimitLease)
			return err
		}
		lease, err = applyCPULimit(ctx, engine, ctr, limit)
		held := faulthook.Held{}
		if lease != nil {
			held = faulthook.Held{Handle: lease, Undo: lease.Restore, Complete: lease.isComplete}
		}
		_, settleErr := faulthook.Settle(c, token, held)
		return errors.Join(err, settleErr)
	})
	return lease, err
}

func applyCPULimit(ctx context.Context, engine cpuLimitEngine, ctr testcontainers.Container, limit CPULimit) (*CPULimitLease, error) {
	docker, err := engine.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return nil, fmt.Errorf("read the engine's cgroup support: %w", err)
	}
	if info.Info.CgroupVersion != "2" {
		return nil, cpuEnvironment("a CPU limit needs cgroup v2; the engine reports cgroup version %q", info.Info.CgroupVersion)
	}
	if !info.Info.CPUCfsQuota {
		return nil, cpuEnvironment("the engine's kernel has no CFS bandwidth control")
	}
	lease := &CPULimitLease{ContainerID: ctr.GetContainerID(), engine: engine, target: ctr}
	before, err := lease.observe(ctx, docker)
	if err != nil {
		return nil, err
	}
	if before.missing || !before.running() {
		return nil, nshelper.InvalidRequest("only a running, unpaused container can be CPU limited")
	}
	lease.generation = before.generation
	if lease.plan, err = planCPULimit(before.settings, before.kernel, info.Info.NCPU, limit); err != nil {
		return nil, err
	}
	// Refuse a target that changed since it was admitted: nothing is sent.
	guard, err := lease.inspect(ctx, docker)
	if err != nil {
		return nil, err
	}
	if guard.missing || !guard.running() || guard.generation != before.generation || guard.settings != before.settings {
		return nil, nshelper.InvalidRequest("the container changed while its CPU limit was admitted")
	}
	result, updateErr := docker.ContainerUpdate(ctx, lease.ContainerID, mobycl.ContainerUpdateOptions{Resources: lease.plan.apply.resources()})
	// From here the update may have reached the engine: the lease is
	// returned with any error.
	after, err := lease.observe(ctx, docker)
	if err != nil {
		return lease, errors.Join(updateFailure("apply", updateErr, result.Warnings), fmt.Errorf("verify the CPU limit: %w", err))
	}
	if err := lease.shows(after, lease.plan.applied, lease.plan.appliedMax); err != nil {
		return lease, errors.Join(updateFailure("apply", updateErr, result.Warnings), err)
	}
	return lease, nil
}

// updateFailure describes the reply of an update whose evidence did not
// settle it.
func updateFailure(step string, err error, warnings []string) error {
	switch {
	case err != nil:
		return fmt.Errorf("%s CPU limit: %w", step, err)
	case len(warnings) > 0:
		return fmt.Errorf("%s CPU limit: the engine warned: %s", step, strings.Join(warnings, "; "))
	}
	return nil
}

// sameGeneration refuses an observation of another run of the target. A
// stopped container keeps the start time of its last run and has no init
// process.
func (lease *CPULimitLease) sameGeneration(now cpuObservation) error {
	switch {
	case now.missing:
		return errors.New("the CPU limited container was removed")
	case now.restarting || now.generation.StartedAt != lease.generation.StartedAt || now.generation.RestartCount != lease.generation.RestartCount:
		return errors.New("the CPU limited container was restarted; its configuration may still hold the limit")
	case now.up && now.generation.PID != lease.generation.PID:
		return errors.New("the CPU limited container runs another init process")
	}
	return nil
}

// matches reports whether the target has settings and, while it runs, the
// kernel cpu.max kernel.
func (o cpuObservation) matches(settings cpuSettings, kernel cpuMax) bool {
	return o.settings == settings && (!o.running() || o.kernel == kernel)
}

// shows checks that the observation is the limited, running generation with
// the expected settings and kernel cpu.max.
func (lease *CPULimitLease) shows(now cpuObservation, settings cpuSettings, kernel cpuMax) error {
	if err := lease.sameGeneration(now); err != nil {
		return err
	}
	switch {
	case !now.running():
		return errors.New("the CPU limited container is not running")
	case now.settings != settings:
		return fmt.Errorf("the engine's CPU settings are %s, not %s", now.settings, settings)
	case now.kernel != kernel:
		return fmt.Errorf("configured only: the kernel cpu.max of the running container is %s, not %s", now.kernel, kernel)
	}
	return nil
}

func (lease *CPULimitLease) isComplete(ctx context.Context) (bool, error) {
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return false, err
	}
	defer lease.mu.Unlock()
	return lease.complete, nil
}

// Restore puts back the CPU settings that the container had before
// ApplyCPULimit and checks the engine's settings and the kernel's cpu.max
// of the same running generation. An unlimited baseline is restored as
// Docker's unlimited quota, -1, since Docker cannot clear a setting to zero;
// the kernel then reads "max" again. A removed container needs nothing. A
// stopped container whose limited run ended only gets its settings back,
// which it runs with when started. Restore refuses, and stays retryable, for
// a paused container, one that was restarted under the limit, and settings
// that were changed outside the lease, which it does not overwrite.
//
//purpose:condition,lifetime
func (lease *CPULimitLease) Restore(ctx context.Context) error {
	if lease == nil {
		return errors.New("CPU limit is unavailable")
	}
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return err
	}
	defer lease.mu.Unlock()
	if lease.complete {
		return nil
	}
	docker, err := lease.engine.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	now, err := lease.observe(ctx, docker)
	switch {
	case err != nil:
		return fmt.Errorf("inspect the CPU limited container: %w", err)
	case now.missing:
		lease.complete = true
		return nil
	case now.paused:
		return nshelper.InvalidRequest("resume the paused container before restoring its CPU limit")
	}
	if err := lease.sameGeneration(now); err != nil {
		return err
	}
	plan := lease.plan
	if now.matches(plan.restored, plan.restoredMax) || now.matches(plan.baseline, plan.restoredMax) {
		lease.complete = true
		return nil
	}
	// The limit must still be installed, possibly only in the configuration
	// when its update did not reach the kernel.
	if !now.matches(plan.applied, plan.appliedMax) && !now.matches(plan.applied, plan.restoredMax) {
		return fmt.Errorf("the container's CPU settings changed outside the lease and are not overwritten: engine %s, kernel %s", now.settings, now.kernel)
	}
	result, updateErr := docker.ContainerUpdate(ctx, lease.ContainerID, mobycl.ContainerUpdateOptions{Resources: plan.restore.resources()})
	after, err := lease.observe(ctx, docker)
	switch {
	case err != nil:
		return errors.Join(updateFailure("restore", updateErr, result.Warnings), fmt.Errorf("verify the restored CPU settings: %w", err))
	case after.missing:
		lease.complete = true
		return nil
	}
	if err := lease.sameGeneration(after); err != nil {
		return errors.Join(updateFailure("restore", updateErr, result.Warnings), err)
	}
	if !after.matches(plan.restored, plan.restoredMax) {
		return errors.Join(updateFailure("restore", updateErr, result.Warnings),
			fmt.Errorf("the restored CPU settings are not in effect: engine %s, kernel %s", after.settings, after.kernel))
	}
	lease.complete = true
	return nil
}

// Status reads the kernel's cpu.max and cpu.stat of the target's cgroup, for
// a test that needs to tell a throttled collaborator from a slow one. It
// refuses a container that is not the running, unpaused generation that
// ApplyCPULimit limited, also after Restore.
//
//purpose:observation
func (lease *CPULimitLease) Status(ctx context.Context) (CPULimitStatus, error) {
	if lease == nil {
		return CPULimitStatus{}, errors.New("CPU limit is unavailable")
	}
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return CPULimitStatus{}, err
	}
	defer lease.mu.Unlock()
	docker, err := lease.engine.connect(ctx)
	if err != nil {
		return CPULimitStatus{}, err
	}
	defer func() { _ = docker.Close() }()
	now, err := lease.observe(ctx, docker)
	if err != nil {
		return CPULimitStatus{}, err
	}
	if now.missing || !now.running() || now.generation != lease.generation {
		return CPULimitStatus{}, errors.New("the CPU limited container is no longer the running generation that was limited")
	}
	return now.status, nil
}

// cpuGeneration identifies one run of a container.
type cpuGeneration struct {
	StartedAt         string
	PID, RestartCount int
}

// cpuSettings are the engine's CPU settings of a container.
type cpuSettings struct {
	NanoCPUs, CPUQuota, CPUPeriod, CPUShares int64
}

// resources is the update request that sets the nonzero fields of s.
func (s cpuSettings) resources() *container.Resources {
	return &container.Resources{NanoCPUs: s.NanoCPUs, CPUQuota: s.CPUQuota, CPUPeriod: s.CPUPeriod, CPUShares: s.CPUShares}
}

func (s cpuSettings) String() string {
	return fmt.Sprintf("NanoCPUs=%d CPUQuota=%d CPUPeriod=%d CPUShares=%d", s.NanoCPUs, s.CPUQuota, s.CPUPeriod, s.CPUShares)
}

// cpuMax is a cpu.max reading in microseconds.
type cpuMax struct {
	Unlimited     bool
	Quota, Period int64
}

func (m cpuMax) String() string {
	if m.Unlimited {
		return fmt.Sprintf("max %d", m.Period)
	}
	return fmt.Sprintf("%d %d", m.Quota, m.Period)
}

// cpuObservation is one inspection of the target and, while it runs, one
// reading of its cgroup.
type cpuObservation struct {
	missing                bool
	generation             cpuGeneration
	up, paused, restarting bool
	settings               cpuSettings
	kernel                 cpuMax
	status                 CPULimitStatus
}

func (o cpuObservation) running() bool { return o.up && !o.paused && !o.restarting }

// inspect reads the engine's view of the target. A missing state or host
// configuration is an error, never an unlimited reading.
func (lease *CPULimitLease) inspect(ctx context.Context, docker cpuLimitClient) (cpuObservation, error) {
	result, err := docker.ContainerInspect(ctx, lease.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil {
		if faulthook.OnlyMissingHostResource(err) {
			return cpuObservation{missing: true}, nil
		}
		return cpuObservation{}, fmt.Errorf("inspect the container: %w", err)
	}
	c := result.Container
	if c.State == nil || c.HostConfig == nil || c.State.StartedAt == "" {
		return cpuObservation{}, errors.New("the container's inspection has no state or host configuration")
	}
	r := c.HostConfig.Resources
	return cpuObservation{
		generation: cpuGeneration{StartedAt: c.State.StartedAt, PID: c.State.Pid, RestartCount: c.RestartCount},
		up:         c.State.Running, paused: c.State.Paused, restarting: c.State.Restarting,
		settings: cpuSettings{NanoCPUs: r.NanoCPUs, CPUQuota: r.CPUQuota, CPUPeriod: r.CPUPeriod, CPUShares: r.CPUShares},
	}, nil
}

// observe inspects the target and, while it runs, reads its cgroup between
// that inspection and a second one, so the reading belongs to the inspected
// generation.
func (lease *CPULimitLease) observe(ctx context.Context, docker cpuLimitClient) (cpuObservation, error) {
	now, err := lease.inspect(ctx, docker)
	if err != nil || now.missing || !now.running() {
		return now, err
	}
	data, err := lease.engine.read(ctx, lease.target)
	if err != nil {
		return cpuObservation{}, err
	}
	if now.kernel, now.status, err = parseCPUFiles(data); err != nil {
		return cpuObservation{}, err
	}
	after, err := lease.inspect(ctx, docker)
	if err != nil {
		return cpuObservation{}, err
	}
	if after.missing || !after.running() || after.generation != now.generation {
		return cpuObservation{}, errors.New("the container changed while its cgroup was read")
	}
	return now, nil
}

// parseCPUFiles decodes the output of cpuFilesArgv. Both cgroup lines must
// name the root of a cgroup v2 namespace: the init process and the reader
// then share the container's own cgroup, whose files follow.
func parseCPUFiles(data []byte) (cpuMax, CPULimitStatus, error) {
	text := string(data)
	if !strings.HasSuffix(text, "\n") {
		return cpuMax{}, CPULimitStatus{}, errors.New("the target's CPU cgroup files are truncated")
	}
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if len(lines) < 3 || lines[0] != "0::/" || lines[1] != "0::/" {
		return cpuMax{}, CPULimitStatus{}, cpuEnvironment("the target does not run in a cgroup v2 namespace of its own")
	}
	max, err := parseCPUMax(lines[2])
	if err != nil {
		return cpuMax{}, CPULimitStatus{}, err
	}
	stat := map[string]int64{}
	for _, line := range lines[3:] {
		key, value, ok := strings.Cut(line, " ")
		n, err := strconv.ParseInt(value, 10, 64)
		if _, seen := stat[key]; !ok || err != nil || n < 0 || seen {
			return cpuMax{}, CPULimitStatus{}, fmt.Errorf("cpu.stat line %q is malformed", line)
		}
		stat[key] = n
	}
	for _, key := range []string{"nr_periods", "nr_throttled", "throttled_usec"} {
		if _, ok := stat[key]; !ok {
			return cpuMax{}, CPULimitStatus{}, cpuEnvironment("the target's cpu.stat has no %s: CFS bandwidth statistics are unavailable", key)
		}
	}
	status := CPULimitStatus{
		Unlimited: max.Unlimited, Quota: time.Duration(max.Quota) * time.Microsecond, Period: time.Duration(max.Period) * time.Microsecond,
		Periods: stat["nr_periods"], ThrottledPeriods: stat["nr_throttled"], ThrottledTime: time.Duration(stat["throttled_usec"]) * time.Microsecond,
	}
	return max, status, nil
}

// parseCPUMax decodes "<quota|max> <period>".
func parseCPUMax(line string) (cpuMax, error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return cpuMax{}, fmt.Errorf("cpu.max %q is malformed", line)
	}
	period, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || period <= 0 {
		return cpuMax{}, fmt.Errorf("cpu.max %q has no period", line)
	}
	if fields[0] == "max" {
		return cpuMax{Unlimited: true, Period: period}, nil
	}
	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || quota <= 0 {
		return cpuMax{}, fmt.Errorf("cpu.max %q has no quota", line)
	}
	return cpuMax{Quota: quota, Period: period}, nil
}

// cpuPlan is what one lease sends and expects to see: the update that
// installs the limit and the one that restores the baseline, with the
// engine settings and kernel cpu.max each leaves.
type cpuPlan struct {
	apply, restore          cpuSettings
	baseline                cpuSettings
	applied, restored       cpuSettings
	appliedMax, restoredMax cpuMax
}

const (
	// dockerCFSPeriod is the period Docker installs for NanoCPUs and for a
	// quota without a period, in microseconds.
	dockerCFSPeriod = 100_000
	// dockerMinQuota is the smallest quota Docker accepts, in microseconds.
	dockerMinQuota = 1_000
)

// planCPULimit admits a limit against the baseline settings and their kernel
// cpu.max, and keeps the baseline's encoding: Docker refuses to mix
// NanoCPUs with a quota or period, and ignores a setting of zero.
func planCPULimit(baseline cpuSettings, kernel cpuMax, ncpu int, limit CPULimit) (cpuPlan, error) {
	n := limit.NanoCPUs
	if n <= 0 {
		return cpuPlan{}, nshelper.InvalidRequest("a CPU limit needs a positive NanoCPUs")
	}
	if ncpu > 0 && big.NewInt(n).Cmp(new(big.Int).Mul(big.NewInt(int64(ncpu)), big.NewInt(1e9))) >= 0 {
		return cpuPlan{}, nshelper.InvalidRequest("a limit of %d NanoCPUs does not constrain an engine with %d CPUs", n, ncpu)
	}
	p := cpuPlan{baseline: baseline, applied: baseline, restored: baseline, restoredMax: kernel}
	disagree := func() error {
		return cpuEnvironment("the kernel cpu.max %s of the container does not express its engine CPU settings %s", kernel, baseline)
	}
	switch {
	case baseline.NanoCPUs > 0:
		if baseline.CPUQuota != 0 || baseline.CPUPeriod != 0 {
			return cpuPlan{}, nshelper.InvalidRequest("the container's CPU settings mix NanoCPUs with a CFS quota or period")
		}
		if kernel.Unlimited || !sameBandwidth(kernel.Quota, kernel.Period, baseline.NanoCPUs) {
			return cpuPlan{}, disagree()
		}
		if n >= baseline.NanoCPUs {
			return cpuPlan{}, nshelper.InvalidRequest("a limit of %d NanoCPUs is not below the container's %d", n, baseline.NanoCPUs)
		}
		quota, err := quotaFor(n, kernel.Period)
		if err != nil {
			return cpuPlan{}, err
		}
		p.apply, p.restore = cpuSettings{NanoCPUs: n}, cpuSettings{NanoCPUs: baseline.NanoCPUs}
		p.applied.NanoCPUs, p.appliedMax = n, cpuMax{Quota: quota, Period: kernel.Period}
	case baseline.CPUQuota > 0:
		period := effectivePeriod(baseline)
		if kernel.Unlimited || kernel.Quota != baseline.CPUQuota || kernel.Period != period {
			return cpuPlan{}, disagree()
		}
		if !belowQuota(n, period, baseline.CPUQuota) {
			return cpuPlan{}, nshelper.InvalidRequest("a limit of %d NanoCPUs is not below the container's quota of %d µs per %d µs", n, baseline.CPUQuota, period)
		}
		quota, err := quotaFor(n, period)
		if err != nil {
			return cpuPlan{}, err
		}
		p.apply, p.restore = cpuSettings{CPUQuota: quota}, cpuSettings{CPUQuota: baseline.CPUQuota}
		p.applied.CPUQuota, p.appliedMax = quota, cpuMax{Quota: quota, Period: period}
	default:
		period := effectivePeriod(baseline)
		if !kernel.Unlimited || kernel.Period != period {
			return cpuPlan{}, disagree()
		}
		quota, err := quotaFor(n, period)
		if err != nil {
			return cpuPlan{}, err
		}
		// Zero is not an update, so an unset quota comes back as Docker's
		// unlimited value.
		unlimited := baseline.CPUQuota
		if unlimited == 0 {
			unlimited = -1
		}
		p.apply, p.restore = cpuSettings{CPUQuota: quota}, cpuSettings{CPUQuota: unlimited}
		p.applied.CPUQuota, p.appliedMax = quota, cpuMax{Quota: quota, Period: period}
		p.restored.CPUQuota = unlimited
	}
	return p, nil
}

func effectivePeriod(s cpuSettings) int64 {
	if s.CPUPeriod > 0 {
		return s.CPUPeriod
	}
	return dockerCFSPeriod
}

// quotaFor converts NanoCPUs into a quota of whole microseconds per period.
func quotaFor(n, period int64) (int64, error) {
	product := new(big.Int).Mul(big.NewInt(n), big.NewInt(period))
	quota, rest := new(big.Int).QuoRem(product, big.NewInt(1e9), new(big.Int))
	if rest.Sign() != 0 {
		return 0, nshelper.InvalidRequest("a limit of %d NanoCPUs is not a whole number of microseconds per %d µs period", n, period)
	}
	if !quota.IsInt64() || quota.Int64() < dockerMinQuota {
		return 0, nshelper.InvalidRequest("a limit of %d NanoCPUs gives a quota below %d µs per %d µs period", n, dockerMinQuota, period)
	}
	return quota.Int64(), nil
}

// sameBandwidth reports whether quota per period is exactly nano CPUs.
func sameBandwidth(quota, period, nano int64) bool {
	left := new(big.Int).Mul(big.NewInt(quota), big.NewInt(1e9))
	return left.Cmp(new(big.Int).Mul(big.NewInt(nano), big.NewInt(period))) == 0
}

// belowQuota reports whether n NanoCPUs are less than quota per period.
func belowQuota(n, period, quota int64) bool {
	left := new(big.Int).Mul(big.NewInt(n), big.NewInt(period))
	return left.Cmp(new(big.Int).Mul(big.NewInt(quota), big.NewInt(1e9))) < 0
}
