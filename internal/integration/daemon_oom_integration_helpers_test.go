//go:build all || integration

package integration_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
)

// These are experiment ceilings, not product defaults. There is one pressure
// step, never a search for the smallest limit that kills a process.
const daemonMemoryBaseline int64 = 1 << 30
const daemonMemoryPressure int64 = 32 << 20

type daemonMemorySample struct {
	Max, Swap, Current uint64
	Events             map[string]uint64
}

type daemonMemoryTarget struct {
	ID, Started, ProcessStart string
	PID                       int
	Group                     string // never included in the public receipt
	Identity                  os.FileInfo
	Events                    *os.File // opened outside the victim before pressure
}

func daemonMemoryCounters(data string) (map[string]uint64, error) {
	out := map[string]uint64{}
	scan := bufio.NewScanner(strings.NewReader(data))
	for scan.Scan() {
		f := strings.Fields(scan.Text())
		if len(f) != 2 {
			return nil, errors.New("malformed local memory counter")
		}
		n, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return nil, errors.New("noninteger local memory counter")
		}
		if _, duplicate := out[f[0]]; duplicate {
			return nil, errors.New("duplicate local memory counter")
		}
		out[f[0]] = n
	}
	if err := scan.Err(); err != nil {
		return nil, errors.New("unreadable local memory counters")
	}
	for _, key := range []string{"max", "oom", "oom_kill"} {
		if _, ok := out[key]; !ok {
			return nil, errors.New("required local memory counter unavailable")
		}
	}
	return out, nil
}

func daemonProcessStart(data string) (string, error) {
	end := strings.LastIndex(data, ")")
	if end < 0 {
		return "", errors.New("malformed process identity")
	}
	fields := strings.Fields(data[end+1:])
	if len(fields) < 20 {
		return "", errors.New("incomplete process identity")
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", errors.New("invalid process start")
	}
	return fields[19], nil
}

func daemonMemoryScalar(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, errors.New("local cgroup scalar unavailable")
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, errors.New("local cgroup scalar is not finite")
	}
	return n, nil
}

func daemonMemoryBind(ctx context.Context, docker *mobycl.Client, id string) (*daemonMemoryTarget, error) {
	raw, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, err
	}
	c := raw.Container
	if c.State == nil || !c.State.Running || c.State.OOMKilled || c.State.Paused || c.State.Restarting || c.State.Pid <= 0 || c.RestartCount != 0 {
		return nil, errors.New("target is not one healthy initial generation")
	}
	if c.HostConfig == nil || c.HostConfig.Memory != daemonMemoryBaseline || c.HostConfig.MemorySwap != daemonMemoryBaseline || c.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		return nil, errors.New("finite baseline/no-swap/no-restart configuration missing")
	}
	pid := c.State.Pid
	proc := filepath.Join("/proc", strconv.Itoa(pid))
	exe, err := os.Readlink(filepath.Join(proc, "exe"))
	if errors.Is(err, os.ErrPermission) {
		return nil, errors.New("controller lacks permission to read the target PID1 executable")
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("target PID1 executable disappeared before admission")
	}
	if err != nil {
		return nil, errors.New("target PID1 executable observation unavailable")
	}
	if filepath.Base(exe) != "ceph-mon" {
		return nil, errors.New("observed target PID1 executable is not ceph-mon")
	}
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		return nil, errors.New("target process start unavailable")
	}
	started, err := daemonProcessStart(string(stat))
	if err != nil {
		return nil, err
	}
	cg, err := os.ReadFile(filepath.Join(proc, "cgroup"))
	if err != nil {
		return nil, errors.New("local target cgroup unavailable")
	}
	line := strings.TrimSpace(string(cg))
	if !strings.HasPrefix(line, "0::/") || strings.Contains(line, "\n") || !strings.Contains(line, id) {
		return nil, errors.New("target does not identify one local container cgroup v2 leaf")
	}
	group := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
	if !strings.HasPrefix(group, "/sys/fs/cgroup/") {
		return nil, errors.New("unsafe cgroup identity")
	}
	identity, err := os.Stat(group)
	if err != nil {
		return nil, errors.New("target cgroup identity unavailable")
	}
	events, err := os.Open(filepath.Join(group, "memory.events.local"))
	if err != nil {
		return nil, errors.New("target-local memory events unavailable")
	}
	return &daemonMemoryTarget{ID: id, Started: c.State.StartedAt, ProcessStart: started, PID: pid, Group: group, Identity: identity, Events: events}, nil
}

func (target *daemonMemoryTarget) sample() (daemonMemorySample, error) {
	info, err := os.Stat(target.Group)
	if err != nil || !os.SameFile(target.Identity, info) {
		return daemonMemorySample{}, errors.New("target cgroup replaced or removed")
	}
	var out daemonMemorySample
	for name, field := range map[string]*uint64{"memory.max": &out.Max, "memory.swap.max": &out.Swap, "memory.current": &out.Current} {
		*field, err = daemonMemoryScalar(filepath.Join(target.Group, name))
		if err != nil {
			return out, err
		}
	}
	out.Events, err = target.events()
	return out, err
}

func (target *daemonMemoryTarget) events() (map[string]uint64, error) {
	if _, err := target.Events.Seek(0, 0); err != nil {
		return nil, errors.New("local memory event handle unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(target.Events, 4097))
	if err != nil || len(data) > 4096 {
		return nil, errors.New("local memory event window lost or oversized")
	}
	return daemonMemoryCounters(string(data))
}

func (target *daemonMemoryTarget) fence(ctx context.Context, docker *mobycl.Client) error {
	raw, err := docker.ContainerInspect(ctx, target.ID, mobycl.ContainerInspectOptions{})
	if err != nil {
		return err
	}
	s := raw.Container.State
	if raw.Container.ID != target.ID || s == nil || !s.Running || s.Paused || s.Restarting || s.OOMKilled || s.Pid != target.PID || s.StartedAt != target.Started || raw.Container.RestartCount != 0 {
		return errors.New("target daemon generation changed")
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(target.PID), "stat"))
	if err != nil {
		return errors.New("target process identity unavailable")
	}
	start, err := daemonProcessStart(string(stat))
	if err != nil || start != target.ProcessStart {
		return errors.New("target process start changed")
	}
	// No exec allocator/observer is permitted in this cgroup. Admission remains
	// fail-closed if a second process appears before the mutation.
	procs, err := os.ReadFile(filepath.Join(target.Group, "cgroup.procs"))
	if err != nil || strings.TrimSpace(string(procs)) != strconv.Itoa(target.PID) {
		return errors.New("target cgroup is not exclusively the frozen Ceph process")
	}
	info, err := os.Stat(target.Group)
	if err != nil || !os.SameFile(target.Identity, info) {
		return errors.New("target cgroup identity changed")
	}
	return nil
}

func daemonMemoryUpdate(ctx context.Context, docker *mobycl.Client, id string, limit int64) error {
	call, done := context.WithTimeout(ctx, 20*time.Second)
	defer done()
	result, err := docker.ContainerUpdate(call, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{Memory: limit, MemorySwap: limit}})
	if err != nil {
		return fmt.Errorf("resource update reply uncertain: %w", err)
	}
	if len(result.Warnings) > 0 {
		return errors.New("resource update returned warnings; inspect before continuing")
	}
	return nil
}

func daemonMemoryOOM(before, after map[string]uint64, sameGeneration, singleton, oomKilled, stopped bool) error {
	if !sameGeneration || !singleton || !oomKilled || !stopped {
		return errors.New("Ceph victim identity/OOM exit not established")
	}
	for _, key := range []string{"max", "oom", "oom_kill"} {
		a, okA := before[key]
		b, okB := after[key]
		if !okA || !okB || b <= a {
			return fmt.Errorf("local %s event increase not observed", key)
		}
	}
	return nil
}

// oomMembershipTarget samples the frozen cgroup. The proc stat belongs to
// the same process whose start time was captured before the pressure step.
type oomMembershipTarget struct {
	group, processStat, processStart string
	identity                         os.FileInfo
	pid                              int
}

var errOOMMembershipDisappeared = errors.New("frozen cgroup or member disappeared")

func (target oomMembershipTarget) checkGroup() error {
	info, err := os.Stat(target.group)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return errOOMMembershipDisappeared
	case err != nil || !os.SameFile(target.identity, info):
		return errors.New("frozen cgroup identity is unavailable or changed")
	default:
		return nil
	}
}

// members brackets the observation with the frozen cgroup directory identity.
func (target oomMembershipTarget) members() (other bool, err error) {
	if err := target.checkGroup(); err != nil {
		return false, err
	}
	data, err := os.ReadFile(filepath.Join(target.group, "cgroup.procs"))
	if identityErr := target.checkGroup(); identityErr != nil {
		return false, identityErr
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, errOOMMembershipDisappeared
	}
	if err != nil {
		return false, errors.New("target cgroup membership unreadable")
	}
	procs, err := parseCgroupProcs(string(data))
	if err != nil {
		return false, err
	}
	if len(procs.PIDs) == 0 {
		return false, errOOMMembershipDisappeared
	}
	return slices.ContainsFunc(procs.PIDs, func(member int) bool { return member != target.pid }), nil
}

// sample separates cgroup teardown from failed attribution. Docker's exit
// timestamp can follow the point at which Linux removes a dying task from
// its cgroup. On disappearance only, a local observation that the frozen
// process is exiting or gone can account for the missing member.
func (target oomMembershipTarget) sample() (other, afterExit bool, err error) {
	other, err = target.members()
	if errors.Is(err, errOOMMembershipDisappeared) && target.processExiting() {
		return false, true, nil
	}
	return other, false, err
}

func (target oomMembershipTarget) processExiting() bool {
	if target.processStat == "" || target.processStart == "" {
		return false
	}
	raw, err := os.ReadFile(target.processStat)
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	stat := string(raw)
	start, err := daemonProcessStart(stat)
	if err != nil {
		return false
	}
	fields := strings.Fields(stat[strings.LastIndexByte(stat, ')')+1:])
	if start != target.processStart || fields[0] == "Z" || fields[0] == "X" {
		return true
	}
	// /proc/<pid>/stat field 9 is the kernel's task flags.
	const pfExiting = 0x00000004
	flags, err := strconv.ParseUint(fields[6], 10, 64)
	return err == nil && flags&pfExiting != 0
}

// oomMembershipGap excludes only samples that started at or after the
// independently reported exit. Reads are samples, not continuous coverage.
func oomMembershipGap(firstGap, finished time.Time) bool {
	return !firstGap.IsZero() && (finished.IsZero() || firstGap.Before(finished))
}

// A recorder runs outside the victim while the engine applies the limit. A
// post-update inspect is not an effective-limit observation on a stopped task.
type daemonMemoryWindow struct {
	Events           map[string]uint64
	PressureObserved bool
	OtherProcess     bool
	MembershipGapAt  time.Time
	TeardownObserved bool
	Error            error
}

func (w daemonMemoryWindow) qualify(baseline map[string]uint64, sameGeneration, oomKilled, stopped bool, finished time.Time) error {
	if oomMembershipGap(w.MembershipGapAt, finished) {
		return errors.New("target cgroup membership unavailable before the daemon exit")
	}
	if !w.PressureObserved {
		return errors.New("effective pressure ceiling was not observed")
	}
	return daemonMemoryOOM(baseline, w.Events, sameGeneration, !w.OtherProcess, oomKilled, stopped)
}

func (target *daemonMemoryTarget) record(ctx context.Context, baseline map[string]uint64) <-chan daemonMemoryWindow {
	result := make(chan daemonMemoryWindow, 1)
	go func() {
		window := daemonMemoryWindow{Events: baseline}
		membership := oomMembershipTarget{group: target.Group, identity: target.Identity, pid: target.PID,
			processStat: filepath.Join("/proc", strconv.Itoa(target.PID), "stat"), processStart: target.ProcessStart}
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			// The already-open event handle is tried even if Docker removed
			// the cgroup path. Its actual readability is not assumed.
			if events, err := target.events(); err == nil {
				window.Events = events
			} else {
				window.Error = err
			}
			sample, err := target.sample()
			at := time.Now()
			other, afterExit, memberErr := membership.sample()
			window.TeardownObserved = window.TeardownObserved || afterExit
			window.OtherProcess = window.OtherProcess || other
			if memberErr != nil {
				window.Error = memberErr
				if window.MembershipGapAt.IsZero() {
					window.MembershipGapAt = at
				}
			}
			if err == nil {
				window.Events = sample.Events
				if memberErr == nil && !afterExit && sample.Max == uint64(daemonMemoryPressure) && sample.Swap == 0 {
					window.PressureObserved = true
				}
			} else {
				window.Error = err
			}
			select {
			case <-ctx.Done():
				result <- window
				return
			case <-tick.C:
			}
		}
	}()
	return result
}
