package ceph

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func TestDebugLevelDecodesConfigGet(t *testing.T) {
	if value, err := decodeDebugLevel([]byte(`{"debug_osd": "1/5"}`), "debug_osd"); err != nil || value != "1/5" {
		t.Fatalf("%q %v", value, err)
	}
	for _, out := range []string{``, `{}`, `{"debug_ms": "1/5"}`, `{"debug_osd": ""}`, `["1/5"]`} {
		if value, err := decodeDebugLevel([]byte(out), "debug_osd"); err == nil {
			t.Errorf("%q decoded as %q", out, value)
		}
	}
}

func TestOpenDebugTargetRefusesBeforeCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit   func(*Container)
		daemon string
		want   string
	}{
		"MON":            {nil, "mon.a", "must be an OSD"},
		"MGR":            {nil, "mgr.a", "must be an OSD"},
		"malformed":      {nil, "osd.01", "must be an OSD"},
		"unknown OSD":    {nil, "osd.7", "not a running-capable OSD"},
		"purged OSD":     {nil, "osd.1", "not a running-capable OSD"},
		"detaching OSD":  {func(c *Container) { c.osdHandles.get(0).detaching = true }, "osd.0", "not a running-capable OSD"},
		"purge issued":   {func(c *Container) { c.osdHandles.get(0).purgeIssued = true }, "osd.0", "not a running-capable OSD"},
		"closed cluster": {func(c *Container) { c.closed = true }, "osd.0", "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			target, err := c.openDebugTarget(t.Context(), test.daemon)
			if target != nil || err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 || len(c.debugLevels.active) != 0 {
				t.Fatalf("target %v err %v calls %v", target, err, control.Calls)
			}
		})
	}
	t.Run("owned running-capable OSD", func(t *testing.T) {
		c, control := adminSocketCluster()
		target, err := c.openDebugTarget(t.Context(), "osd.0")
		if err != nil || target == nil || target.daemon != "osd.0" || target.id != 0 || target.osd != c.osdHandles.get(0) || len(control.Calls) != 0 {
			t.Fatalf("target %+v err %v calls %v", target, err, control.Calls)
		}
		// The topology lock is released again.
		if !c.mu.TryLock() {
			t.Fatal("openDebugTarget kept the topology lock")
		}
		c.mu.Unlock()
	})
	t.Run("topology busy", func(t *testing.T) {
		c, _ := adminSocketCluster()
		c.mu.Lock()
		defer c.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if target, err := c.openDebugTarget(ctx, "osd.0"); target != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("target %v err %v", target, err)
		}
	})
}

// TestDebugLevelRegistryAdmitsOneWindowPerKey pins the registry that stays in
// the cluster: one open window per daemon and option, a release that only the
// owner of the slot can make, and a lock of its own so that neither waits
// behind a long topology operation.
func TestDebugLevelRegistryAdmitsOneWindowPerKey(t *testing.T) {
	var r debugLevelRegistry
	first, second := new(int), new(int)
	if err := r.Reserve("osd.0/debug_osd", first); err != nil {
		t.Fatal(err)
	}
	if err := r.Reserve("osd.0/debug_osd", second); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "is open; restore it first") {
		t.Fatalf("a second window was admitted: %v", err)
	}
	if err := r.Reserve("osd.0/debug_ms", second); err != nil {
		t.Fatalf("another option of the daemon: %v", err)
	}
	// A stale owner cannot release the slot of the window that holds it.
	r.Release("osd.0/debug_osd", second)
	if err := r.Reserve("osd.0/debug_osd", second); err == nil {
		t.Fatal("a release by a non-owner freed the slot")
	}
	r.Release("osd.0/debug_osd", first)
	if err := r.Reserve("osd.0/debug_osd", second); err != nil {
		t.Fatalf("the slot was not freed: %v", err)
	}
	// The registry takes only its own lock.
	c := &Container{}
	c.mu.Lock()
	defer c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.debugLevels.Reserve("osd.1/debug_osd", first)
		c.debugLevels.Release("osd.1/debug_osd", first)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the registry waited for the cluster mutex")
	}
}

// TestDebugLevelRefusalReportsChanged pins the method that the override
// package reads through an interface.
func TestDebugLevelRefusalReportsChanged(t *testing.T) {
	var changed interface{ Changed() bool }
	err := fmt.Errorf("set: %w", &debugLevelRefusal{changed: true, err: errors.New("stopped")})
	if !errors.As(err, &changed) || !changed.Changed() {
		t.Fatal("a terminal refusal does not report Changed")
	}
	err = fmt.Errorf("set: %w", &debugLevelRefusal{err: errors.New("paused")})
	if !errors.As(err, &changed) || changed.Changed() {
		t.Fatal("a non-terminal refusal reports Changed")
	}
}

func TestCheckDebugLevelGeneration(t *testing.T) {
	good := debugLevelGeneration{owned: true, identity: "osd0@start-1", running: true}
	for name, test := range map[string]struct {
		gen     debugLevelGeneration
		err     error
		changed bool
		want    string
	}{
		"same":        {good, nil, false, ""},
		"inspect":     {good, errors.New("boom"), false, "inspect osd.0"},
		"not owned":   {debugLevelGeneration{identity: "osd0@start-1", running: true}, nil, true, "no longer owned"},
		"restarted":   {debugLevelGeneration{owned: true, identity: "osd0@start-2", running: true}, nil, true, "another container generation"},
		"no identity": {debugLevelGeneration{owned: true, running: true}, nil, true, "another container generation"},
		"stopped":     {debugLevelGeneration{owned: true, identity: "osd0@start-1"}, nil, true, "is stopped"},
		"paused":      {debugLevelGeneration{owned: true, identity: "osd0@start-1", running: true, paused: true}, nil, false, "is paused"},
	} {
		t.Run(name, func(t *testing.T) {
			changed, err := checkDebugLevelGeneration(test.gen, test.err, "osd0@start-1", "osd.0")
			if changed != test.changed || (test.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("changed %v err %v", changed, err)
			}
		})
	}
}

// debugLevelOSD is an OSD container whose generation the test sets.
type debugLevelOSD struct {
	testcontainers.Container
	id    string
	state container.State
}

func (o *debugLevelOSD) GetContainerID() string { return o.id }
func (o *debugLevelOSD) State(context.Context) (*container.State, error) {
	state := o.state
	return &state, nil
}

func TestOSDDebugTargetGeneration(t *testing.T) {
	c, _ := adminSocketCluster()
	ctr := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
	c.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr})
	target := &osdDebugTarget{c: c, id: 0, daemon: "osd.0", osd: c.osdHandles.get(0)}
	gen, err := target.generation(t.Context())
	if err != nil || gen != (debugLevelGeneration{owned: true, identity: "osd-container@start-1", running: true}) {
		t.Fatalf("generation %+v %v", gen, err)
	}
	ctr.state = container.State{Running: true, Paused: true, StartedAt: "start-1"}
	if gen, _ := target.generation(t.Context()); !gen.paused {
		t.Fatalf("paused generation %+v", gen)
	}
	for name, edit := range map[string]func(){
		"reopened":   func() { c.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr}) },
		"purged":     func() { c.osdHandles.get(0).purged = true },
		"purging":    func() { c.osdHandles.get(0).purgeIssued = true },
		"detaching":  func() { c.osdHandles.get(0).detaching = true },
		"terminated": func() { c.closed = true },
	} {
		t.Run(name, func(t *testing.T) {
			saved, closed := *c.osdHandles.get(0), c.closed
			original := c.osdHandles.get(0)
			defer func() { *original, c.closed = saved, closed; c.osdHandles.register(0, original) }()
			edit()
			if gen, err := target.generation(t.Context()); err != nil || gen.owned {
				t.Fatalf("generation %+v %v", gen, err)
			}
		})
	}
}

func TestOSDDebugTargetSetAdmission(t *testing.T) {
	newTarget := func() (*Container, *poolFixtureContainer, *debugLevelOSD, *osdDebugTarget) {
		c, control := adminSocketCluster()
		ctr := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
		c.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr})
		return c, control, ctr, &osdDebugTarget{c: c, id: 0, daemon: "osd.0", osd: c.osdHandles.get(0)}
	}
	for name, test := range map[string]struct {
		edit    func(*Container, *debugLevelOSD)
		changed bool
	}{
		"restarted":  {func(_ *Container, o *debugLevelOSD) { o.state.StartedAt = "start-2" }, true},
		"replaced":   {func(c *Container, o *debugLevelOSD) { c.osdHandles.register(0, &OSDContainer{ID: 0, Container: o}) }, true},
		"stopped":    {func(_ *Container, o *debugLevelOSD) { o.state.Running = false }, true},
		"terminated": {func(c *Container, _ *debugLevelOSD) { c.closed = true }, true},
		"paused":     {func(_ *Container, o *debugLevelOSD) { o.state.Paused = true }, false},
		"no run dir": {func(c *Container, _ *debugLevelOSD) { c.runDir.volume.removed = true }, false},
	} {
		t.Run(name, func(t *testing.T) {
			c, control, ctr, target := newTarget()
			test.edit(c, ctr)
			settle, err := target.set(t.Context(), "osd-container@start-1", "debug_osd", "20")
			var refusal *debugLevelRefusal
			if settle != nil || !errors.As(err, &refusal) || refusal.changed != test.changed || len(control.Calls) != 0 {
				t.Fatalf("settle %v err %v calls %v", settle != nil, err, control.Calls)
			}
		})
	}
	t.Run("topology busy", func(t *testing.T) {
		c, control, _, target := newTarget()
		c.mu.Lock()
		defer c.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		var refusal *debugLevelRefusal
		if _, err := target.set(ctx, "osd-container@start-1", "debug_osd", "20"); !errors.As(err, &refusal) || refusal.changed || len(control.Calls) != 0 {
			t.Fatalf("err %v calls %v", err, control.Calls)
		}
	})
}

// debugLevelNativeCluster is a cluster whose control container runs its
// commands as real processes through the supervisor, with a ceph program
// that only appends its arguments to the returned log.
func debugLevelNativeCluster(t *testing.T) (*Container, *nativeTarget, *osdDebugTarget, string) {
	t.Helper()
	control := newNativeTarget(t, "control", "tc-public")
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "ceph.log")
	if err := os.WriteFile(filepath.Join(bin, "ceph"), []byte("#!/bin/sh\necho \"$*\" >> '"+log+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	c := nativeCluster()
	c.Container, c.runDir.volume = control, &runVolume{name: "run"}
	osd := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
	c.osdHandles.replace(map[int]*OSDContainer{0: {ID: 0, Container: osd}})
	return c, control, &osdDebugTarget{c: c, id: 0, daemon: "osd.0", osd: c.osdHandles.get(0)}, log
}

func cephCalls(t *testing.T, log string) string {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(data)
}

func TestOSDDebugTargetSetRunsInControl(t *testing.T) {
	requireLinuxGroups(t)
	c, _, target, log := debugLevelNativeCluster(t)
	settle, err := target.set(t.Context(), "osd-container@start-1", "debug_osd", "20")
	if settle != nil || err != nil || registered(c) != 0 {
		t.Fatalf("settle %v err %v registered %d", settle != nil, err, registered(c))
	}
	if got := cephCalls(t, log); got != "--admin-daemon /var/run/ceph/ceph-osd.0.asok config set debug_osd 20\n" {
		t.Fatalf("ceph calls %q", got)
	}
}

// A set whose exec Docker accepted but has not started, and whose
// cancellation fails, may still reach the shared socket after a replacement.
// It keeps the OSD instance's native fence closed until it is confirmed
// stopped, and once confirmed it never runs.
func TestOSDDebugTargetUnconfirmedSetHoldsTheRemovalFence(t *testing.T) {
	unconfirmedSet := func(t *testing.T) (*Container, *nativeTarget, func(context.Context) error, string, func()) {
		c, control, target, log := debugLevelNativeCluster(t)
		gate := make(chan struct{})
		control.startGate, control.refuseCancel = gate, true
		var release sync.Once
		t.Cleanup(func() { release.Do(func() { close(gate) }) })
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(100*time.Millisecond, cancel)
		settle, err := target.set(ctx, "osd-container@start-1", "debug_osd", "20")
		if settle == nil || err == nil || registered(c) != 1 {
			t.Fatalf("settle %v err %v registered %d", settle != nil, err, registered(c))
		}
		return c, control, settle, log, func() { release.Do(func() { close(gate) }) }
	}
	t.Run("failed cancellation, then settlement", func(t *testing.T) {
		c, control, settle, log, start := unconfirmedSet(t)
		if err := settle(t.Context()); err == nil || registered(c) != 1 {
			t.Fatalf("settled with a refused cancellation: %v", err)
		}
		// RemoveOSD and DetachOSD fence the instance before they replace it.
		if err := c.fenceNativeTarget(t.Context(), "osd-container"); err == nil || registered(c) != 1 {
			t.Fatalf("the fence admitted a replacement while the set may run: %v", err)
		}
		control.set(func(f *nativeTarget) { f.refuseCancel = false })
		if err := settle(t.Context()); err != nil || registered(c) != 0 {
			t.Fatalf("settle: %v registered %d", err, registered(c))
		}
		if err := c.fenceNativeTarget(t.Context(), "osd-container"); err != nil {
			t.Fatal(err)
		}
		start()
		control.wg.Wait()
		if got := cephCalls(t, log); got != "" {
			t.Fatalf("a settled set ran late: %q", got)
		}
	})
	t.Run("drained by the fence", func(t *testing.T) {
		c, control, settle, log, start := unconfirmedSet(t)
		control.set(func(f *nativeTarget) { f.refuseCancel = false })
		if err := c.fenceNativeTarget(t.Context(), "osd-container"); err != nil || registered(c) != 0 {
			t.Fatalf("fence: %v registered %d", err, registered(c))
		}
		if err := settle(t.Context()); err != nil {
			t.Fatalf("settle after the fence stopped it: %v", err)
		}
		start()
		control.wg.Wait()
		if got := cephCalls(t, log); got != "" {
			t.Fatalf("a drained set ran late: %q", got)
		}
	})
	t.Run("fenced instance", func(t *testing.T) {
		c, _, target, log := debugLevelNativeCluster(t)
		if err := c.fenceNativeTarget(t.Context(), "osd-container"); err != nil {
			t.Fatal(err)
		}
		var refusal *debugLevelRefusal
		if _, err := target.set(t.Context(), "osd-container@start-1", "debug_osd", "20"); !errors.As(err, &refusal) || !refusal.changed || cephCalls(t, log) != "" {
			t.Fatalf("set on a fenced instance: %v", err)
		}
	})
}
