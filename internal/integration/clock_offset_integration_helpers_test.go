//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The MON timecheck runs every mon_timecheck_interval seconds (300 by
// default) and every mon_timecheck_skew_interval while a skew is reported. A
// shorter interval set at bootstrap keeps one step of the scenario within
// about a minute.
const clockOffsetMonConfig = "[mon]\nmon_timecheck_interval = 30\nmon_timecheck_skew_interval = 10\n"

// clockHealthDeadline bounds the wait for MON_CLOCK_SKEW to appear or clear:
// two timecheck rounds and the health report after them.
const clockHealthDeadline = 3 * time.Minute

// clockHelperImage is the clock helper under test: fault.ClockHelperImage,
// or another contract-1 helper from CEPH_TEST_CLOCK_HELPER_IMAGE.
func clockHelperImage() string {
	if image := os.Getenv("CEPH_TEST_CLOCK_HELPER_IMAGE"); image != "" {
		return image
	}
	return fault.ClockHelperImage
}

// clockProcess is what the test reads from a MON container itself: PID 1,
// the process the script exec'd, and a process executed in the container.
type clockProcess struct {
	StartTicks  uint64            `json:"start_ticks"`
	Command     []string          `json:"command"`
	Environ     map[string]string `json:"environ"`
	Mapped      bool              `json:"mapped"`
	SelfEnviron map[string]string `json:"self_environ"`
	SelfMapped  bool              `json:"self_mapped"`
	ClockFiles  []string          `json:"clock_files"`
}

const clockProcessScript = `
import json, os, sys
def env(pid):
    with open('/proc/%s/environ' % pid, 'rb') as f:
        items = f.read().split(b'\0')
    out = {}
    for item in items:
        k, sep, v = item.partition(b'=')
        if sep:
            out[k.decode()] = v.decode()
    return out
def mapped(pid):
    with open('/proc/%s/maps' % pid) as f:
        return any('libfaketime' in line for line in f)
with open('/proc/1/stat') as f:
    stat = f.read()
with open('/proc/1/cmdline', 'rb') as f:
    command = [a.decode() for a in f.read().split(b'\0') if a]
files = sorted(os.listdir('/tc/clock')) if os.path.isdir('/tc/clock') else []
json.dump({'start_ticks': int(stat[stat.rindex(')') + 2:].split()[19]), 'command': command,
           'environ': env(1), 'mapped': mapped(1), 'self_environ': env('self'), 'self_mapped': mapped('self'),
           'clock_files': files}, sys.stdout)
`

func readClockProcess(t *testing.T, ctx context.Context, ctr testcontainers.Container) clockProcess {
	t.Helper()
	var p clockProcess
	if err := json.Unmarshal([]byte(harness.ExecOutput(t, ctx, ctr, "python3", "-I", "-c", clockProcessScript)), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// clockInterposerKeys are the variables that only an armed daemon may have.
var clockInterposerKeys = []string{"LD_PRELOAD", "FAKETIME", "FAKETIME_TIMESTAMP_FILE", "FAKETIME_CACHE_DURATION", "FAKETIME_DONT_FAKE_MONOTONIC"}

// requireUninstrumentedEnvironment fails when env carries an interposer
// variable.
func requireUninstrumentedEnvironment(t *testing.T, what string, env map[string]string) {
	t.Helper()
	for _, key := range clockInterposerKeys {
		if value, ok := env[key]; ok {
			t.Fatalf("%s has %s=%q", what, key, value)
		}
	}
}

// containerEnvironment is the environment Docker starts the container with:
// what the script's steps before its final exec and every executed command
// inherit.
func containerEnvironment(t *testing.T, ctx context.Context, id string) (map[string]string, []string) {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	result, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, item := range result.Container.Config.Env {
		key, value, _ := strings.Cut(item, "=")
		env[key] = value
	}
	launch := append(slices.Clone(result.Container.Config.Entrypoint), result.Container.Config.Cmd...)
	return env, launch
}

// monStatus reads the quorum and timing fields of mon_status through the
// daemon's admin socket. quorum_age is derived from the daemon's own
// CLOCK_MONOTONIC (Monitor::quorum_age).
type monStatus struct {
	State         string `json:"state"`
	ElectionEpoch int    `json:"election_epoch"`
	QuorumAge     int64  `json:"quorum_age"`
}

func readMonStatus(t *testing.T, ctx context.Context, mon *ceph.MonitorContainer) monStatus {
	t.Helper()
	out, err := mon.AdminSocket(ctx, "mon_status")
	if err != nil {
		t.Fatal(err)
	}
	var status monStatus
	if err := json.Unmarshal(out, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

// monotonicLedger checks that a MON's quorum_age advances with the test's own
// monotonic clock: a faked CLOCK_MONOTONIC would move it with the offset.
type monotonicLedger struct {
	start time.Time
	age   int64
}

func (l *monotonicLedger) check(t *testing.T, name string, status monStatus) {
	t.Helper()
	now := time.Now()
	if l.start.IsZero() {
		l.start, l.age = now, status.QuorumAge
		return
	}
	elapsed := now.Sub(l.start).Seconds()
	advanced := float64(status.QuorumAge - l.age)
	// quorum_age is truncated to whole seconds; each reading adds less than
	// one second, and an exec adds its latency.
	if advanced < elapsed-1.5 || advanced > elapsed+1.5 {
		t.Fatalf("mon.%s quorum_age advanced %.0f s while %.1f s passed: its CLOCK_MONOTONIC moved", name, advanced, elapsed)
	}
}

type clockHealth struct {
	Checks map[string]struct {
		Detail []struct {
			Message string `json:"message"`
		} `json:"detail"`
		Summary struct {
			Message string `json:"message"`
		} `json:"summary"`
	} `json:"checks"`
}

// waitClockSkew waits until MON_CLOCK_SKEW names every MON of skewed, or is
// gone when skewed is empty, and logs how long that took.
func waitClockSkew(t *testing.T, ctx context.Context, cluster *ceph.Container, skewed []string) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(clockHealthDeadline)
	var last string
	for time.Now().Before(deadline) {
		var health clockHealth
		if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "health", "detail", "--format", "json"), &health); err != nil {
			t.Fatal(err)
		}
		check, present := health.Checks["MON_CLOCK_SKEW"]
		last = check.Summary.Message
		if len(skewed) == 0 && !present {
			t.Logf("MON_CLOCK_SKEW absent after %s", time.Since(start).Round(time.Second))
			return
		}
		if len(skewed) > 0 && present {
			named := true
			for _, name := range skewed {
				named = named && strings.Contains(check.Summary.Message, "mon."+name)
			}
			if named {
				t.Logf("MON_CLOCK_SKEW %q after %s", check.Summary.Message, time.Since(start).Round(time.Second))
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("MON_CLOCK_SKEW did not become %v within %s; last %q", skewed, clockHealthDeadline, last)
}

func testClockOffsetMonitors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	config := filepath.Join(t.TempDir(), "timecheck.conf")
	if err := os.WriteFile(config, []byte(clockOffsetMonConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	helper := clockHelperImage()
	opts = append(opts, ceph.WithConfigFile(config),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithNoInitialOSDs()),
		ceph.WithClockConfig(ceph.ClockConfig{}.WithHelperImage(helper).WithMonitors("a", "b", "c")))
	cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)

	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	identity := facts.ClockInterposer
	if identity.HelperImage != helper || identity.Build != "default" || len(identity.SHA256) != 64 || len(identity.SourceCommit) != 40 {
		t.Fatalf("clock interposer identity = %+v", identity)
	}
	t.Logf("clock interposer: %+v", identity)

	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) != 3 {
		t.Fatalf("monitors = %v, %v", monitors, err)
	}
	mons := map[string]*ceph.MonitorContainer{}
	ticks := map[string]uint64{}
	for _, mon := range monitors {
		mons[mon.DaemonName] = mon
		env, _ := containerEnvironment(t, ctx, mon.GetContainerID())
		requireUninstrumentedEnvironment(t, "the container environment of mon."+mon.DaemonName, env)
		p := readClockProcess(t, ctx, mon)
		if len(p.Command) < 2 || filepath.Base(p.Command[0]) != "ceph-mon" || p.Command[1] != "-f" {
			t.Fatalf("PID 1 of mon.%s is %q, not the exec'd daemon", mon.DaemonName, p.Command)
		}
		want := map[string]string{"LD_PRELOAD": "/tc/clock/libfaketime.so.1", "FAKETIME_TIMESTAMP_FILE": "/tc/clock/offset",
			"FAKETIME_CACHE_DURATION": "1", "FAKETIME_DONT_FAKE_MONOTONIC": "1"}
		for key, value := range want {
			if p.Environ[key] != value {
				t.Fatalf("mon.%s daemon has %s=%q, want %q", mon.DaemonName, key, p.Environ[key], value)
			}
		}
		if _, ok := p.Environ["FAKETIME"]; ok || !p.Mapped {
			t.Fatalf("mon.%s daemon: FAKETIME present or interposer not mapped: %+v", mon.DaemonName, p)
		}
		requireUninstrumentedEnvironment(t, "a command executed in mon."+mon.DaemonName, p.SelfEnviron)
		if p.SelfMapped {
			t.Fatalf("a command executed in mon.%s maps the interposer", mon.DaemonName)
		}
		ticks[mon.DaemonName] = p.StartTicks
	}

	var quorum struct {
		Leader string   `json:"quorum_leader_name"`
		Names  []string `json:"quorum_names"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "quorum_status", "--format", "json"), &quorum); err != nil {
		t.Fatal(err)
	}
	var follower string
	for _, name := range quorum.Names {
		if name != quorum.Leader {
			follower = name
			break
		}
	}
	if len(quorum.Names) != 3 || mons[quorum.Leader] == nil || follower == "" {
		t.Fatalf("quorum = %+v", quorum)
	}
	others := func(name string) []string {
		var out []string
		for _, n := range quorum.Names {
			if n != name {
				out = append(out, n)
			}
		}
		return out
	}
	epoch := readMonStatus(t, ctx, mons[quorum.Leader]).ElectionEpoch
	monotonic := map[string]*monotonicLedger{}
	sample := func() {
		for name, mon := range mons {
			status := readMonStatus(t, ctx, mon)
			if status.ElectionEpoch != epoch || (status.State != "leader" && status.State != "peon") {
				t.Fatalf("mon.%s: election epoch %d (was %d), state %s: the quorum changed", name, status.ElectionEpoch, epoch, status.State)
			}
			if monotonic[name] == nil {
				monotonic[name] = &monotonicLedger{}
			}
			monotonic[name].check(t, name, status)
		}
	}
	sample()

	waitClockSkew(t, ctx, cluster, nil)
	// Ceph measures skew against the leader: a skewed follower is reported
	// itself, a skewed leader as both followers skewed the other way.
	steps := []struct {
		target string
		skewed []string
	}{{follower, []string{follower}}, {quorum.Leader, others(quorum.Leader)}}
	for _, step := range steps {
		for _, offset := range []time.Duration{2 * time.Second, -2 * time.Second} {
			name := "mon." + step.target + " " + offset.String()
			lease, err := fault.ApplyClockOffset(ctx, cluster, mons[step.target], fault.ClockOffset{Offset: offset})
			if err != nil {
				if lease != nil {
					_ = lease.Restore(context.WithoutCancel(ctx))
				}
				t.Fatalf("%s: apply: %v", name, err)
			}
			status, err := lease.Status(ctx)
			if err != nil || status.State != fault.ClockOffsetApplied || status.MeasuredMin > offset || status.MeasuredMax < offset {
				t.Fatalf("%s: status = %+v, %v", name, status, err)
			}
			t.Logf("%s: measured [%s, %s]", name, status.MeasuredMin, status.MeasuredMax)
			sample()
			waitClockSkew(t, ctx, cluster, step.skewed)
			sample()
			if err := lease.Restore(ctx); err != nil {
				t.Fatalf("%s: restore: %v", name, err)
			}
			if err := lease.Restore(ctx); err != nil {
				t.Fatalf("%s: repeated restore: %v", name, err)
			}
			sample()
			waitClockSkew(t, ctx, cluster, nil)
			sample()
		}
	}
	for name, mon := range mons {
		if now := readClockProcess(t, ctx, mon).StartTicks; now != ticks[name] {
			t.Fatalf("mon.%s was restarted: start ticks %d, were %d", name, now, ticks[name])
		}
	}

	// A held lease is restored by Terminate before the containers go.
	held, err := fault.ApplyClockOffset(ctx, cluster, mons[follower], fault.ClockOffset{Offset: 3 * time.Second})
	if err != nil {
		t.Fatalf("held lease: %v", err)
	}
	// The test's client container is attached to the cluster's network,
	// which Terminate removes.
	if err := client.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatalf("terminate with a held clock offset: %v", err)
	}
	if status, err := held.Status(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Terminate did not restore the held lease: %+v, %v", status, err)
	}
}

// testClockOffsetUnarmed checks that a cluster without ClockConfig starts its
// MON exactly as before: no clock files, no interposer variable anywhere and
// the same launch command, and that an offset is refused.
func testClockOffsetUnarmed(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithNoInitialManagers()))
	cluster, _ := harness.NewServiceClusterWithOptions(t, image, opts...)
	if facts, err := ceph.FactsOf(cluster); err != nil || facts.ClockInterposer != (ceph.ClockInterposer{}) {
		t.Fatalf("an unarmed cluster records an interposer: %+v, %v", facts.ClockInterposer, err)
	}
	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) != 1 {
		t.Fatalf("monitors = %v, %v", monitors, err)
	}
	mon := monitors[0]
	env, launch := containerEnvironment(t, ctx, mon.GetContainerID())
	requireUninstrumentedEnvironment(t, "the container environment", env)
	if want := []string{"/bin/sh", "/tc/mon.sh"}; !slices.Equal(launch, want) {
		t.Fatalf("MON launch command %q, want %q", launch, want)
	}
	p := readClockProcess(t, ctx, mon)
	requireUninstrumentedEnvironment(t, "the MON daemon", p.Environ)
	if p.Mapped || len(p.ClockFiles) != 0 {
		t.Fatalf("an unarmed MON has the interposer: mapped=%v files=%v", p.Mapped, p.ClockFiles)
	}
	lease, err := fault.ApplyClockOffset(ctx, cluster, mon, fault.ClockOffset{Offset: 2 * time.Second})
	if lease != nil || err == nil || !strings.Contains(err.Error(), "ClockConfig.WithMonitors") {
		t.Fatalf("an unarmed MON accepted an offset: %v, %v", lease, err)
	}
}
