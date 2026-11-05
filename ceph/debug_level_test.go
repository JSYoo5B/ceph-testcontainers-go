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
		"MON":            {nil, "mon.a", "must be an OSD, MGR or MDS"},
		"RGW":            {nil, "rgw.a", "must be an OSD, MGR or MDS"},
		"malformed":      {nil, "osd.01", "must be an OSD, MGR or MDS"},
		"malformed MDS":  {nil, "mds.-x", "must be an OSD, MGR or MDS"},
		"terminated MGR": {nil, "mgr.b", "not a running-capable"},
		"unknown MGR":    {nil, "mgr.z", "not a running-capable"},
		"unknown MDS":    {nil, "mds.fs-a", "not a running-capable"},
		"unknown OSD":    {nil, "osd.7", "not a running-capable OSD"},
		"purged OSD":     {nil, "osd.1", "not a running-capable OSD"},
		"detaching OSD":  {func(c *Container) { c.fleet.osdHandles.get(0).detaching = true }, "osd.0", "not a running-capable OSD"},
		"purge issued":   {func(c *Container) { c.fleet.osdHandles.get(0).purgeIssued = true }, "osd.0", "not a running-capable OSD"},
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
		if err != nil || target == nil || target.daemon != "osd.0" || target.kind != "osd" || target.name != "0" || target.osd != c.fleet.osdHandles.get(0) || len(control.Calls) != 0 {
			t.Fatalf("target %+v err %v calls %v", target, err, control.Calls)
		}
		// The topology lock is released again.
		if !c.mu.TryLock() {
			t.Fatal("openDebugTarget kept the topology lock")
		}
		c.mu.Unlock()
	})
	t.Run("owned MGR and MDS", func(t *testing.T) {
		c, control := adminSocketCluster()
		mds := &debugLevelOSD{id: "mds-container"}
		c.members.roster.setService("mds.fs-a", mds)
		target, err := c.openDebugTarget(t.Context(), "mgr.a")
		if err != nil || target.kind != "mgr" || target.mgr != c.members.roster.manager("a") || target.container() != control {
			t.Fatalf("MGR target %+v err %v", target, err)
		}
		target, err = c.openDebugTarget(t.Context(), "mds.fs-a")
		if err != nil || target.kind != "mds" || target.mds != mds || target.container() != mds || len(control.Calls) != 0 {
			t.Fatalf("MDS target %+v err %v calls %v", target, err, control.Calls)
		}
		// A replacement under the same service name is another descriptor.
		c.members.roster.setService("mds.fs-a", &debugLevelOSD{id: "mds-container-2"})
		if gen, err := target.generation(t.Context()); err != nil || gen.owned {
			t.Fatalf("generation of a replaced MDS %+v %v", gen, err)
		}
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
		"same":               {good, nil, false, ""},
		"inspect":            {good, errors.New("boom"), false, "inspect osd.0"},
		"not owned":          {debugLevelGeneration{identity: "osd0@start-1", running: true}, nil, true, "no longer owned"},
		"restarted":          {debugLevelGeneration{owned: true, identity: "osd0@start-2", running: true}, nil, true, "another container generation"},
		"no identity":        {debugLevelGeneration{owned: true, running: true}, nil, true, "another container generation"},
		"stopped":            {debugLevelGeneration{owned: true, identity: "osd0@start-1"}, nil, true, "is stopped"},
		"paused":             {debugLevelGeneration{owned: true, identity: "osd0@start-1", running: true, paused: true}, nil, false, "is paused"},
		"paused replacement": {debugLevelGeneration{owned: true, identity: "osd0@start-2", running: true, paused: true}, nil, true, "another container generation"},
	} {
		t.Run(name, func(t *testing.T) {
			changed, err := checkDebugLevelGeneration(test.gen, test.err, "osd0@start-1", "osd.0")
			if changed != test.changed || (test.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("changed %v err %v", changed, err)
			}
		})
	}
	// An MDS or MGR incarnation adds its process: a paused daemon is compared
	// by its container only, a running one by the whole incarnation.
	const want = "mgr0@start-1/gid=4100/asok=12@t1"
	for name, test := range map[string]struct {
		gen     debugLevelGeneration
		changed bool
		want    string
	}{
		"same":           {debugLevelGeneration{owned: true, identity: want, running: true}, false, ""},
		"respawned":      {debugLevelGeneration{owned: true, identity: "mgr0@start-1/gid=4200/asok=12@t2", running: true}, true, "another daemon process"},
		"rebound socket": {debugLevelGeneration{owned: true, identity: "mgr0@start-1/gid=4100/asok=12@t2", running: true}, true, "another daemon process"},
		"paused":         {debugLevelGeneration{owned: true, identity: "mgr0@start-1", running: true, paused: true}, false, "is paused"},
		"stopped":        {debugLevelGeneration{owned: true, identity: "mgr0@start-1"}, true, "is stopped"},
		"new container":  {debugLevelGeneration{owned: true, identity: "mgr0@start-2/gid=4100/asok=12@t1", running: true}, true, "another container generation"},
	} {
		t.Run("process "+name, func(t *testing.T) {
			changed, err := checkDebugLevelGeneration(test.gen, nil, want, "mgr.a")
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
	c.fleet.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr})
	target := &debugTarget{c: c, kind: "osd", name: "0", daemon: "osd.0", osd: c.fleet.osdHandles.get(0)}
	gen, err := target.generation(t.Context())
	if err != nil || gen != (debugLevelGeneration{owned: true, identity: "osd-container@start-1", running: true}) {
		t.Fatalf("generation %+v %v", gen, err)
	}
	ctr.state = container.State{Running: true, Paused: true, StartedAt: "start-1"}
	if gen, _ := target.generation(t.Context()); !gen.paused {
		t.Fatalf("paused generation %+v", gen)
	}
	for name, edit := range map[string]func(){
		"reopened":   func() { c.fleet.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr}) },
		"purged":     func() { c.fleet.osdHandles.get(0).purged = true },
		"purging":    func() { c.fleet.osdHandles.get(0).purgeIssued = true },
		"detaching":  func() { c.fleet.osdHandles.get(0).detaching = true },
		"terminated": func() { c.closed = true },
	} {
		t.Run(name, func(t *testing.T) {
			saved, closed := *c.fleet.osdHandles.get(0), c.closed
			original := c.fleet.osdHandles.get(0)
			defer func() { *original, c.closed = saved, closed; c.fleet.osdHandles.register(0, original) }()
			edit()
			if gen, err := target.generation(t.Context()); err != nil || gen.owned {
				t.Fatalf("generation %+v %v", gen, err)
			}
		})
	}
}

func TestOSDDebugTargetSetAdmission(t *testing.T) {
	newTarget := func() (*Container, *poolFixtureContainer, *debugLevelOSD, *debugTarget) {
		c, control := adminSocketCluster()
		ctr := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
		c.fleet.osdHandles.register(0, &OSDContainer{ID: 0, Container: ctr})
		return c, control, ctr, &debugTarget{c: c, kind: "osd", name: "0", daemon: "osd.0", osd: c.fleet.osdHandles.get(0)}
	}
	for name, test := range map[string]struct {
		edit    func(*Container, *debugLevelOSD)
		changed bool
	}{
		"restarted": {func(_ *Container, o *debugLevelOSD) { o.state.StartedAt = "start-2" }, true},
		"replaced": {func(c *Container, o *debugLevelOSD) {
			c.fleet.osdHandles.register(0, &OSDContainer{ID: 0, Container: o})
		}, true},
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
func debugLevelNativeCluster(t *testing.T) (*Container, *nativeTarget, *debugTarget, string) {
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
	c.fleet.osdHandles.replace(map[int]*OSDContainer{0: {ID: 0, Container: osd}})
	return c, control, &debugTarget{c: c, kind: "osd", name: "0", daemon: "osd.0", osd: c.fleet.osdHandles.get(0)}, log
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
		if err := c.native.fence(t.Context(), "osd-container"); err == nil || registered(c) != 1 {
			t.Fatalf("the fence admitted a replacement while the set may run: %v", err)
		}
		control.set(func(f *nativeTarget) { f.refuseCancel = false })
		if err := settle(t.Context()); err != nil || registered(c) != 0 {
			t.Fatalf("settle: %v registered %d", err, registered(c))
		}
		if err := c.native.fence(t.Context(), "osd-container"); err != nil {
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
		if err := c.native.fence(t.Context(), "osd-container"); err != nil || registered(c) != 0 {
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
		if err := c.native.fence(t.Context(), "osd-container"); err != nil {
			t.Fatal(err)
		}
		var refusal *debugLevelRefusal
		if _, err := target.set(t.Context(), "osd-container@start-1", "debug_osd", "20"); !errors.As(err, &refusal) || !refusal.changed || cephCalls(t, log) != "" {
			t.Fatalf("set on a fenced instance: %v", err)
		}
	})
}

func TestManagerGIDReadsActiveAndStandby(t *testing.T) {
	dump := []byte(`{"active_name":"a","active_gid":4100,"standbys":[{"name":"b","gid":4200}]}`)
	for name, want := range map[string]uint64{"a": 4100, "b": 4200} {
		if gid, err := managerGID(dump, name); err != nil || gid != want {
			t.Errorf("%s: %d %v", name, gid, err)
		}
	}
	for _, dump := range []string{`{}`, `{"active_name":"a","active_gid":0}`, `[]`, `{"standbys":[{"name":"c","gid":1}]}`} {
		if gid, err := managerGID([]byte(dump), "a"); err == nil {
			t.Errorf("%s: %d", dump, gid)
		}
	}
}

// TestManagerDebugTargetIncarnation pins the process part of a MGR's
// identity: its MgrMap GID and the inode and change time of its admin socket.
// A paused or stopped MGR is not asked.
func TestManagerDebugTargetIncarnation(t *testing.T) {
	c, control := adminSocketCluster()
	mgr := &debugLevelOSD{id: "mgr-container", state: container.State{Running: true, StartedAt: "start-1"}}
	c.members.roster.manager("a").Container = mgr
	// The canned control strips the first three words of every command.
	control.Output["/var/run/ceph/ceph-mgr.a.asok"] = "1234@2026-11-05 10:42:01.123456789 +0000\n"
	control.Output["mgr dump --format json"] = `{"active_name":"a","active_gid":4100}`
	target, err := c.openDebugTarget(t.Context(), "mgr.a")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := target.generation(t.Context())
	want := "mgr-container@start-1/gid=4100/asok=1234@2026-11-05_10:42:01.123456789_+0000"
	if err != nil || gen.identity != want || !gen.owned || !gen.running {
		t.Fatalf("generation %+v %v calls %v", gen, err, control.Calls)
	}
	control.Output["mgr dump --format json"] = `{"active_name":"b","active_gid":4300}`
	if _, err := target.generation(t.Context()); err == nil || !strings.Contains(err.Error(), "not in the MgrMap") {
		t.Fatalf("a MGR missing from the MgrMap: %v", err)
	}
	calls := len(control.Calls)
	mgr.state.Paused = true
	if gen, err := target.generation(t.Context()); err != nil || gen.identity != "mgr-container@start-1" || !gen.paused || len(control.Calls) != calls {
		t.Fatalf("paused generation %+v %v", gen, err)
	}
}

// TestMDSDebugTargetRunsInItsContainer pins the MDS path: the admin socket is
// reached in the MDS container, whose status names the native GID that the
// incarnation holds, and a respawn with another GID is another incarnation.
func TestMDSDebugTargetRunsInItsContainer(t *testing.T) {
	requireLinuxGroups(t)
	c, control, _, log := debugLevelNativeCluster(t)
	mds := newNativeTarget(t, "mds-container", "tc-public")
	gidFile := filepath.Join(t.TempDir(), "gid")
	if err := os.WriteFile(gidFile, []byte("4321"), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	script := "#!/bin/sh\necho \"$*\" >> '" + log + "'\ncase \"$*\" in *' status') echo \"{\\\"id\\\": $(cat '" + gidFile + "')}\";; esac\n"
	if err := os.WriteFile(filepath.Join(bin, "ceph"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	c.members.roster.setService("mds.fs-a", mds)
	target, err := c.openDebugTarget(t.Context(), "mds.fs-a")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := target.generation(t.Context())
	if err != nil || gen.identity != "mds-container@start-1/gid=4321" {
		t.Fatalf("generation %+v %v", gen, err)
	}
	controlExecs, mdsExecs := control.execCount(), mds.execCount()
	if settle, err := target.set(t.Context(), gen.identity, "debug_mds", "10"); settle != nil || err != nil || registered(c) != 0 {
		t.Fatalf("set: %v", err)
	}
	if control.execCount() != controlExecs || mds.execCount() == mdsExecs {
		t.Fatalf("the set ran in the control container: control %d mds %d", control.execCount(), mds.execCount())
	}
	if got := cephCalls(t, log); !strings.HasSuffix(got, "--admin-daemon /var/run/ceph/ceph-mds.fs-a.asok status\n--admin-daemon /var/run/ceph/ceph-mds.fs-a.asok config set debug_mds 10\n") {
		t.Fatalf("ceph calls %q", got)
	}
	// ceph mds fail respawns the daemon in place with another GID.
	if err := os.WriteFile(gidFile, []byte("4400"), 0o600); err != nil {
		t.Fatal(err)
	}
	var refusal *debugLevelRefusal
	if _, err := target.set(t.Context(), gen.identity, "debug_mds", "1/5"); !errors.As(err, &refusal) || !refusal.changed || strings.Contains(cephCalls(t, log), "1/5") {
		t.Fatalf("set into a respawned MDS: %v", err)
	}
}
