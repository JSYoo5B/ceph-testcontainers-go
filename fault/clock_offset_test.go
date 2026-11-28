package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/testcontainers/testcontainers-go"
)

// clockFakeContainer stands in for the MON container: only its ID is read.
type clockFakeContainer struct {
	testcontainers.Container
	id string
}

func (c clockFakeContainer) GetContainerID() string { return c.id }

// fakeClockDaemon simulates one armed MON for the clock engine: its
// environment, the owned file, and a daemon that reads the file once the
// interposer's cache window after a write has passed. Time is simulated:
// sleep advances it, so the tests take no wall-clock time.
type fakeClockDaemon struct {
	mono, epoch int64
	env         map[string]string
	mapped      bool
	selfPreload bool
	ticks       uint64
	file        clockRawFile
	// shown is the offset text the daemon uses; writtenAt the time of the
	// last write, after which it switches to the file's text.
	shown      string
	writtenAt  int64
	pickUp     bool
	state      clockContainerState
	audit      []string
	writes     []string
	copies     []string
	runErr     error
	extraAudit int
}

func newFakeClockDaemon() *fakeClockDaemon {
	interposer, path := faulthook.ClockFiles()
	return &fakeClockDaemon{
		epoch: time.Date(2026, 11, 28, 1, 0, 0, 0, time.UTC).UnixNano(), mono: int64(time.Hour),
		env: map[string]string{"LD_PRELOAD": interposer, "FAKETIME_TIMESTAMP_FILE": path,
			"FAKETIME_CACHE_DURATION": "1", "FAKETIME_DONT_FAKE_MONOTONIC": "1"},
		mapped: true, ticks: 4711, shown: "+0", pickUp: true, state: clockContainerRunning,
		file: clockRawFile{Present: true, Regular: true, Device: 50, Inode: 77, Text: "+0\n"},
	}
}

func (d *fakeClockDaemon) real() int64 { return d.epoch + d.mono }

func (d *fakeClockDaemon) shownOffset() int64 {
	if d.pickUp && d.mono-d.writtenAt >= int64(2*time.Second) {
		d.shown = strings.TrimSuffix(d.file.Text, "\n")
	}
	n, err := strconv.ParseInt(d.shown, 10, 64)
	if err != nil {
		panic(err)
	}
	return n * int64(time.Second)
}

func (d *fakeClockDaemon) engine() clockEngine {
	return clockEngine{
		run: func(_ context.Context, _ testcontainers.Container, args ...string) ([]byte, error) {
			if d.runErr != nil {
				return nil, d.runErr
			}
			var out any
			switch args[0] {
			case "inspect":
				m := map[string]any{"file": d.file}
				if args[1] != "" {
					m["start_ticks"], m["environ"], m["mapped"], m["self_preload"] = d.ticks, d.env, d.mapped, d.selfPreload
				}
				out = m
			case "write":
				d.writes = append(d.writes, args[2])
				d.file.Text, d.writtenAt = args[2], d.mono
				out = map[string]any{"file": d.file}
			case "measure":
				before := d.real()
				stamp := time.Unix(0, before+int64(time.Millisecond)+d.shownOffset()).UTC()
				for range 1 + d.extraAudit {
					d.audit = append(d.audit, stamp.Format("2006-01-02T15:04:05.000-0700")+
						" 7f6b2a 0 log_channel(audit) log [DBG] : from='admin socket' entity='admin socket' cmd='mon_status' args=[]: dispatch")
				}
				d.mono += int64(3 * time.Millisecond)
				out = map[string]any{"before": before, "after": d.real(), "ticks_before": d.ticks, "ticks_after": d.ticks}
			default:
				return nil, fmt.Errorf("unknown operation %s", args[0])
			}
			return json.Marshal(out)
		},
		logs: func(context.Context, string, int64) ([]byte, error) {
			lines := d.audit
			d.audit = nil
			return []byte(strings.Join(lines, "\n")), nil
		},
		status: func(context.Context, string) (clockContainerState, error) { return d.state, nil },
		copy: func(_ context.Context, _ testcontainers.Container, _ string, data []byte) error {
			d.copies = append(d.copies, string(data))
			d.file.Text = string(data)
			return nil
		},
		now:       func() int64 { return d.mono },
		sleep:     func(_ context.Context, wait time.Duration) error { d.mono += int64(wait); return nil },
		auditWait: time.Second,
	}
}

// clockCluster is a unit-test cluster whose MON a runs in a fake container.
func clockCluster(t *testing.T, armed ...string) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	mon := clockFakeContainer{id: "mon-a"}
	c := testhelper.TestCluster(mon, testhelper.TestConfig{StartupTimeout: time.Minute})
	testhelper.TestArmClock(c, armed...)
	return c, mon
}

func TestClockOffsetAppliesReportsAndRestores(t *testing.T) {
	c, mon := clockCluster(t, "a")
	d := newFakeClockDaemon()
	lease, err := applyClockOffsetInCluster(t.Context(), c, mon, 2*time.Second, d.engine())
	if err != nil || lease == nil || lease.Daemon != "mon.a" || lease.ContainerID != "mon-a" {
		t.Fatalf("apply = %+v, %v", lease, err)
	}
	if len(d.writes) != 1 || d.writes[0] != "+2\n" {
		t.Fatalf("writes = %q", d.writes)
	}
	if held := testhelper.TestFaults(c, faulthook.ClockOffset); held["mon-a"] != lease {
		t.Fatalf("lease is not held for Terminate: %v", held)
	}
	status, err := lease.Status(t.Context())
	if err != nil || status.State != ClockOffsetApplied || status.Replaced ||
		status.MeasuredMin > 2*time.Second || status.MeasuredMax < 2*time.Second {
		t.Fatalf("status = %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(d.writes) != 2 || d.writes[1] != "+0\n" || d.shown != "+0" {
		t.Fatalf("restore writes = %q, daemon shows %s", d.writes, d.shown)
	}
	writes := len(d.writes)
	if err := lease.Restore(t.Context()); err != nil || len(d.writes) != writes {
		t.Fatalf("a repeated restore wrote again: %v %q", err, d.writes)
	}
	if status, err := lease.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || status != (ClockOffsetStatus{}) {
		t.Fatalf("status after restore = %+v, %v; want a refusal matching ceph.ErrInvalidRequest", status, err)
	}
}

func TestClockOffsetRefusesUnarmedAndForeignTargets(t *testing.T) {
	c, mon := clockCluster(t)
	d := newFakeClockDaemon()
	_, err := applyClockOffsetInCluster(t.Context(), c, mon, time.Second, d.engine())
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "ClockConfig.WithMonitors") {
		t.Fatalf("unarmed MON: %v", err)
	}
	c, _ = clockCluster(t, "a")
	_, err = applyClockOffsetInCluster(t.Context(), c, clockFakeContainer{id: "client"}, time.Second, d.engine())
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "MON that this cluster owns") {
		t.Fatalf("foreign container: %v", err)
	}
	for _, offset := range []time.Duration{0, 1500 * time.Millisecond, 400 * 24 * time.Hour} {
		if _, err := applyClockOffsetInCluster(t.Context(), c, clockFakeContainer{id: "mon-a"}, offset, d.engine()); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("offset %s: %v", offset, err)
		}
	}
	if len(d.writes) != 0 {
		t.Fatalf("a refused request wrote: %q", d.writes)
	}
}

func TestClockOffsetAdmission(t *testing.T) {
	cases := map[string]func(*fakeClockDaemon){
		"other preload":           func(d *fakeClockDaemon) { d.env["LD_PRELOAD"] = "/other.so" },
		"not mapped":              func(d *fakeClockDaemon) { d.mapped = false },
		"monotonic faked":         func(d *fakeClockDaemon) { delete(d.env, "FAKETIME_DONT_FAKE_MONOTONIC") },
		"executed commands load":  func(d *fakeClockDaemon) { d.selfPreload = true },
		"FAKETIME present":        func(d *fakeClockDaemon) { d.env["FAKETIME"] = "" },
		"file selector elsewhere": func(d *fakeClockDaemon) { d.env["FAKETIME_TIMESTAMP_FILE"] = "/tmp/x" },
		"file not at zero":        func(d *fakeClockDaemon) { d.file.Text = "+2\n" },
		"file gone":               func(d *fakeClockDaemon) { d.file = clockRawFile{} },
		"no cache duration":       func(d *fakeClockDaemon) { delete(d.env, "FAKETIME_CACHE_DURATION") },
		"daemon not at reference": func(d *fakeClockDaemon) { d.shown, d.pickUp = "+5", false },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c, mon := clockCluster(t, "a")
			d := newFakeClockDaemon()
			mutate(d)
			lease, err := applyClockOffsetInCluster(t.Context(), c, mon, 2*time.Second, d.engine())
			if !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil || len(d.writes) != 0 {
				t.Fatalf("admitted: lease=%v err=%v writes=%q", lease, err, d.writes)
			}
			if len(testhelper.TestFaults(c, faulthook.ClockOffset)) != 0 {
				t.Fatal("a refused admission stayed registered")
			}
		})
	}
}

func TestClockOffsetNotTakenReturnsTheLease(t *testing.T) {
	c, mon := clockCluster(t, "a")
	d := newFakeClockDaemon()
	d.pickUp = false
	lease, err := applyClockOffsetInCluster(t.Context(), c, mon, -2*time.Second, d.engine())
	if !errors.Is(err, ceph.ErrNativeFailure) || lease == nil {
		t.Fatalf("an offset the daemon never showed: lease=%v err=%v", lease, err)
	}
	if testhelper.TestFaults(c, faulthook.ClockOffset)["mon-a"] != lease {
		t.Fatal("the lease of an uncertain offset is not held for Terminate")
	}
	// The daemon reads the restored file; the restore must discriminate it
	// from the offset that was written.
	d.pickUp = true
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("restore after a failed apply: %v", err)
	}
}

func TestClockOffsetUncertainWriteReturnsTheLease(t *testing.T) {
	c, mon := clockCluster(t, "a")
	d := newFakeClockDaemon()
	engine := d.engine()
	run := engine.run
	engine.run = func(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
		if args[0] == "write" && len(d.writes) == 0 {
			d.writes = append(d.writes, args[2])
			d.file.Text, d.writtenAt = args[2], d.mono
			return nil, errors.New("exec reply lost")
		}
		return run(ctx, ctr, args...)
	}
	lease, err := applyClockOffsetInCluster(t.Context(), c, mon, 3*time.Second, engine)
	if err == nil || lease == nil {
		t.Fatalf("uncertain write: lease=%v err=%v", lease, err)
	}
	if _, err := lease.Status(t.Context()); err == nil {
		t.Fatal("status of an unacknowledged write reported a verdict")
	}
	if err := lease.Restore(t.Context()); err != nil || d.shown != "+0" {
		t.Fatalf("restore after an uncertain write: %v, shows %s", err, d.shown)
	}
}

func TestClockOffsetRestoreOfStoppedRemovedPausedAndRestarted(t *testing.T) {
	apply := func(t *testing.T) (*ClockOffsetLease, *fakeClockDaemon) {
		c, mon := clockCluster(t, "a")
		d := newFakeClockDaemon()
		lease, err := applyClockOffsetInCluster(t.Context(), c, mon, 2*time.Second, d.engine())
		if err != nil {
			t.Fatal(err)
		}
		return lease, d
	}
	t.Run("removed", func(t *testing.T) {
		lease, d := apply(t)
		d.state = clockContainerMissing
		if err := lease.Restore(t.Context()); err != nil || len(d.writes) != 1 || len(d.copies) != 0 {
			t.Fatalf("removed: %v writes=%q copies=%q", err, d.writes, d.copies)
		}
	})
	t.Run("stopped", func(t *testing.T) {
		lease, d := apply(t)
		d.state = clockContainerStopped
		if err := lease.Restore(t.Context()); err != nil || len(d.copies) != 1 || d.copies[0] != "+0\n" {
			t.Fatalf("stopped: %v copies=%q", err, d.copies)
		}
	})
	t.Run("paused", func(t *testing.T) {
		lease, d := apply(t)
		d.state = clockContainerPaused
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("paused: %v", err)
		}
		d.state = clockContainerRunning
		if err := lease.Restore(t.Context()); err != nil {
			t.Fatalf("retry after resume: %v", err)
		}
	})
	t.Run("restarted", func(t *testing.T) {
		lease, d := apply(t)
		d.ticks++
		if status, err := lease.Status(t.Context()); err != nil || !status.Replaced || status.State == ClockOffsetApplied {
			t.Fatalf("status of a restarted daemon = %+v, %v", status, err)
		}
		if err := lease.Restore(t.Context()); err != nil {
			t.Fatalf("restarted: %v", err)
		}
	})
	t.Run("not restored", func(t *testing.T) {
		lease, d := apply(t)
		d.pickUp = false
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrNativeFailure) {
			t.Fatalf("a daemon still at the offset was taken as restored: %v", err)
		}
		d.pickUp = true
		if err := lease.Restore(t.Context()); err != nil {
			t.Fatalf("retry: %v", err)
		}
	})
}

func TestClockOffsetRefusesASecondLease(t *testing.T) {
	c, mon := clockCluster(t, "a")
	d := newFakeClockDaemon()
	first, err := applyClockOffsetInCluster(t.Context(), c, mon, 2*time.Second, d.engine())
	if err != nil {
		t.Fatal(err)
	}
	second, err := applyClockOffsetInCluster(t.Context(), c, mon, -2*time.Second, d.engine())
	if !errors.Is(err, ceph.ErrInvalidRequest) || second != first {
		t.Fatalf("second lease: %v %v", second, err)
	}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if next, err := applyClockOffsetInCluster(t.Context(), c, mon, -2*time.Second, d.engine()); err != nil || next == first {
		t.Fatalf("a lease after restore: %v %v", next, err)
	}
}

func TestClockMeasurementRepeatsAnAmbiguousWindow(t *testing.T) {
	d := newFakeClockDaemon()
	d.extraAudit = 1
	engine := d.engine()
	if _, err := engine.measure(t.Context(), clockFakeContainer{id: "mon-a"}, "mon.a"); err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("ambiguous windows: %v", err)
	}
	d.extraAudit = 0
	sample, err := engine.measure(t.Context(), clockFakeContainer{id: "mon-a"}, "mon.a")
	if err != nil || !sample.Offset.overlaps(clockOffsetInterval{Known: true}) {
		t.Fatalf("measure = %+v, %v", sample, err)
	}
	if sample.Offset.Hi-sample.Offset.Lo > int64(5*time.Millisecond+clockLogStampLag) {
		t.Fatalf("interval wider than the bracket, the stamp resolution and the coarse stamp lag: %s", sample.Offset)
	}
}

func TestClockAuditStamps(t *testing.T) {
	logs := strings.Join([]string{
		"2026-11-28T01:00:00.123+0000 7f6b2a 0 log_channel(audit) log [DBG] : from='admin socket' entity='admin socket' cmd='mon_status' args=[]: dispatch",
		// The leader's cluster log copy has no log_channel prefix.
		"2026-11-28T01:00:00.123+0000 mon.a (mon.0) 51 : audit [DBG] from='admin socket' entity='admin socket' cmd='mon_status' args=[]: dispatch",
		"2026-11-28T01:00:00.200+0000 7f6b2a 0 log_channel(audit) log [DBG] : from='admin socket' entity='admin socket' cmd='quorum_status' args=[]: dispatch",
		"2026-11-28T01:00:01.5+0100 7f6b2a 0 log_channel(audit) log [DBG] : from='admin socket' entity='admin socket' cmd=mon_status args=[]: dispatch",
		"not a stamp 0 log_channel(audit) log [DBG] : from='admin socket' cmd='mon_status': dispatch",
	}, "\n")
	stamps := clockAuditStamps([]byte(logs))
	want := []clockStamp{
		{at: time.Date(2026, 11, 28, 1, 0, 0, 123e6, time.UTC).UnixNano(), resolution: int64(time.Millisecond)},
		{at: time.Date(2026, 11, 28, 0, 0, 1, 500e6, time.UTC).UnixNano(), resolution: int64(100 * time.Millisecond)},
	}
	if fmt.Sprint(stamps) != fmt.Sprint(want) {
		t.Fatalf("stamps = %v, want %v", stamps, want)
	}
}
