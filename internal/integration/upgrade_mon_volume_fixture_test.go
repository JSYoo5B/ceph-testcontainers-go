//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These are Docker-free controls of the private effects of the MON-step driver
// for the retained-volume experiment of #64. Docker is replaced by an
// in-memory engine, so they establish decision logic, ordering and failure
// handling only. No container, volume, image change, Ceph daemon or retained
// store was involved, and none of this is native upgrade evidence. The
// allocator and entrypoint scripts are run locally, without Docker, against
// loopback sockets and a fake ceph-mon.

const (
	fakeRunID    = "run1"
	fakeFSID     = "fsid-1"
	fakeSource   = "src-mon-a"
	fakeRunVol   = "tc-ceph-run-x"
	fakeHostIP   = "192.0.2.10"
	fakeDestVer  = "ceph version 20.2.4 (0123456789abcdef) tentacle (stable)\n"
	fakeCreated  = "2026-01-02T03:04:05Z"
	fakeStoreDir = "/var/lib/ceph/mon/ceph-a"
)

type fakeCtr struct {
	upgradeEngineContainer
	spec *upgradeDestSpec
}

type fakeLease struct {
	addr     string
	ports    []int
	released bool
	relErr   error
	e        *fakeEngine
}

func (l *fakeLease) Bound() (string, []int) { return l.addr, l.ports }
func (l *fakeLease) Release(context.Context) error {
	l.e.calls = append(l.e.calls, "lease-release")
	if l.relErr != nil {
		return l.relErr
	}
	l.released = true
	return nil
}

type fakeEngine struct {
	calls      []string
	ctrs       map[string]*fakeCtr
	vols       map[string]upgradeEngineVolume
	conf       []byte
	confErr    error
	stopErr    error
	stopLeaves bool // Stop returns nil but the container keeps running
	removeErr  error
	probeHas   bool
	probeErr   error
	leaseErr   error
	leaseBound *fakeLease // overrides what a successful lease reports
	leaseRelEr error
	lease      *fakeLease
	createErr  error
	createReg  bool // Create registers the container even when it fails
	startErr   error
	startExit  bool // Start succeeds but the container exits at once
	execOut    string
	execErr    error
	execCode   int
	volCreate  error
	volCreated bool // VolumeCreate fails after creating the volume
	volInspErr error
	volRemErr  error
	createdAt  string
}

func newFakeEngine() *fakeEngine {
	e := &fakeEngine{ctrs: map[string]*fakeCtr{}, vols: map[string]upgradeEngineVolume{}, probeHas: true, execOut: fakeDestVer, createdAt: fakeCreated}
	e.conf = []byte("[global]\nfsid = " + fakeFSID + "\nmon host = [v2:" + fakeHostIP + ":3300,v1:" + fakeHostIP + ":6789]\npublic addr = " + fakeHostIP + "\n")
	e.ctrs[fakeSource] = &fakeCtr{upgradeEngineContainer: upgradeEngineContainer{
		ID: fakeSource, Name: fakeSource, Running: true, Status: "running", HostNetwork: true, ImageArch: "amd64",
		Mounts: []upgradeEngineMount{{Volume: "tc-ceph-upgrade-mon-" + fakeRunID, Target: fakeStoreDir}, {Volume: fakeRunVol, Target: upgradeMonRunDir}},
	}}
	return e
}

func (e *fakeEngine) call(name string) { e.calls = append(e.calls, name) }

func (e *fakeEngine) count(name string) int {
	n := 0
	for _, c := range e.calls {
		if c == name {
			n++
		}
	}
	return n
}

func (e *fakeEngine) VolumeCreate(_ context.Context, name string, labels map[string]string) (upgradeEngineVolume, error) {
	e.call("volume-create")
	v := upgradeEngineVolume{Name: name, Driver: "local", CreatedAt: e.createdAt, Labels: labels}
	if e.volCreate != nil {
		if e.volCreated {
			e.vols[name] = v
		}
		return upgradeEngineVolume{}, e.volCreate
	}
	e.vols[name] = v
	return v, nil
}

func (e *fakeEngine) VolumeInspect(_ context.Context, name string) (upgradeEngineVolume, bool, error) {
	e.call("volume-inspect")
	if e.volInspErr != nil {
		return upgradeEngineVolume{}, false, e.volInspErr
	}
	v, ok := e.vols[name]
	return v, ok, nil
}

func (e *fakeEngine) VolumeRemove(_ context.Context, name string) error {
	e.call("volume-remove")
	if e.volRemErr != nil {
		return e.volRemErr
	}
	delete(e.vols, name)
	return nil
}

func (e *fakeEngine) Inspect(_ context.Context, ref string) (upgradeEngineContainer, bool, error) {
	e.call("inspect " + ref)
	c, ok := e.ctrs[ref]
	if !ok {
		return upgradeEngineContainer{}, false, nil
	}
	return c.upgradeEngineContainer, true, nil
}

func (e *fakeEngine) Stop(_ context.Context, ref string, _ time.Duration) error {
	e.call("stop " + ref)
	if e.stopErr != nil {
		return e.stopErr
	}
	if c := e.ctrs[ref]; c != nil && !e.stopLeaves {
		c.Running, c.Status, c.ExitCode = false, "exited", 0
	}
	return nil
}

func (e *fakeEngine) Remove(_ context.Context, ref string) error {
	e.call("remove " + ref)
	if e.removeErr != nil {
		return e.removeErr
	}
	delete(e.ctrs, ref)
	return nil
}

func (e *fakeEngine) CopyOut(_ context.Context, ref, path string) ([]byte, error) {
	e.call("copy-out " + ref + " " + path)
	return e.conf, e.confErr
}

func (e *fakeEngine) ProbeStore(_ context.Context, volume, dir string) (bool, error) {
	e.call("probe " + volume + " " + dir)
	return e.probeHas, e.probeErr
}

func (e *fakeEngine) ReserveExact(_ context.Context, spec upgradeLeaseSpec) (upgradeLease, error) {
	e.call("lease")
	if e.leaseErr != nil {
		return nil, e.leaseErr
	}
	l := &fakeLease{addr: spec.Address, ports: slices.Clone(spec.Ports), relErr: e.leaseRelEr, e: e}
	if e.leaseBound != nil {
		l.addr, l.ports = e.leaseBound.addr, e.leaseBound.ports
	}
	e.lease = l
	return l, nil
}

func (e *fakeEngine) Create(_ context.Context, spec upgradeDestSpec) (string, error) {
	e.call("create " + spec.Name)
	register := func() {
		s := spec
		e.ctrs[spec.Name] = &fakeCtr{spec: &s, upgradeEngineContainer: upgradeEngineContainer{
			ID: spec.Name, Name: spec.Name, Status: "created", HostNetwork: spec.HostNetwork, ImageArch: "amd64",
			Mounts: slices.Clone(spec.Mounts),
		}}
	}
	if e.createErr != nil {
		if e.createReg {
			register()
		}
		return "", e.createErr
	}
	register()
	return spec.Name, nil
}

func (e *fakeEngine) Start(_ context.Context, ref string) error {
	e.call("start " + ref)
	c := e.ctrs[ref]
	if c == nil {
		return errors.New("no such container")
	}
	if e.startErr != nil {
		return e.startErr
	}
	if e.startExit {
		c.Running, c.Status, c.ExitCode = false, "exited", 1
		return nil
	}
	c.Running, c.Status = true, "running"
	return nil
}

func (e *fakeEngine) Exec(_ context.Context, ref string, argv []string) (string, int, error) {
	e.call("exec " + ref + " " + strings.Join(argv, " "))
	return e.execOut, e.execCode, e.execErr
}

// ---- helpers -----------------------------------------------------------------------

func fakeVector() upgradeMonVector { return upgradeMonVector{monAddr(fakeHostIP, 3300, 6789)} }

type effectsEnv struct {
	t       *testing.T
	eng     *fakeEngine
	vol     *upgradeStoreVolume
	eff     *upgradeVolumeMonEffects
	view    upgradeMonView
	viewErr error
	viewN   int
	slept   int
	// quorumAfter is the number of View calls that report the MON outside
	// quorum before it joins.
	quorumAfter int
}

func newEffectsEnv(t *testing.T) *effectsEnv {
	t.Helper()
	env := &effectsEnv{t: t, eng: newFakeEngine()}
	vol, err := newUpgradeStoreVolume(fakeRunID)
	if err != nil {
		t.Fatal(err)
	}
	env.vol = vol
	if err := vol.create(context.Background(), env.eng); err != nil {
		t.Fatal(err)
	}
	env.view = upgradeMonView{FSID: fakeFSID, Addrs: fakeVector(), InQuorum: true}
	env.build(nil)
	return env
}

func (e *effectsEnv) build(edit func(*upgradeVolumeMonEffectsConfig)) {
	e.t.Helper()
	cfg := upgradeVolumeMonEffectsConfig{
		Engine: e.eng, Volume: e.vol, RunID: fakeRunID, Source: fakeSource, Name: "a", FSID: fakeFSID,
		Retained: fakeVector(), DestImage: "dest-image",
		View: func(context.Context) (upgradeMonView, error) {
			e.viewN++
			v := e.view
			v.Addrs = slices.Clone(v.Addrs)
			if e.viewN <= e.quorumAfter {
				v.InQuorum = false
			}
			return v, e.viewErr
		},
		StopTimeout: time.Second, ObserveTimeout: time.Minute, Poll: time.Second,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			e.slept++
			if e.slept > 5 {
				return context.DeadlineExceeded
			}
			return ctx.Err()
		},
	}
	if edit != nil {
		edit(&cfg)
	}
	eff, err := newUpgradeVolumeMonEffects(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.eff = eff
}

func monA() upgradeDaemonID { return upgradeDaemonID{Role: "mon", ID: "a"} }

// fenceAll runs the effects in the driver's order up to and including Retained.
func (e *effectsEnv) retained() (bool, upgradeMonVector, error) {
	e.t.Helper()
	ctx := context.Background()
	if _, err := e.eff.Fence(ctx, monA()); err != nil {
		e.t.Fatalf("fence: %v", err)
	}
	if present, err := e.eff.SourcePresent(ctx, monA()); err != nil || present {
		e.t.Fatalf("source present = %v, %v", present, err)
	}
	return e.eff.Retained(ctx, monA())
}

func goodPlan() upgradeMonPlan {
	return upgradeMonPlan{Env: map[string]string{
		"CEPH_MON_ID": "a", "CEPH_ADDRESS_FAMILY": "ipv4", "CEPH_PUBLIC_ADDRESS": fakeHostIP, "CEPH_MON_PORT_V2": "3300", "CEPH_MON_PORT_V1": "6789",
	}}
}

// launchReady takes the effects to the point where Launch may be called.
func (e *effectsEnv) launchReady() {
	e.t.Helper()
	has, vec, err := e.retained()
	if err != nil || !has || !vec.sameSet(fakeVector()) {
		e.t.Fatalf("retained = %v %v %v", has, vec, err)
	}
}

func hasEvent(events []string, sub string) bool {
	for _, ev := range events {
		if strings.Contains(ev, sub) {
			return true
		}
	}
	return false
}

// ---- the volume ledger ---------------------------------------------------------------

func TestUpgradeStoreVolumeCreateClassifiesEveryOutcome(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		setup func(*fakeEngine)
		want  upgradeVolumeState
		err   bool
	}{
		"created":                    {func(*fakeEngine) {}, upgradeVolumeCreated, false},
		"failed, definitely absent":  {func(e *fakeEngine) { e.volCreate = errors.New("boom") }, upgradeVolumeNotCreated, true},
		"failed but it exists":       {func(e *fakeEngine) { e.volCreate, e.volCreated = errors.New("reply lost"), true }, upgradeVolumeCreated, true},
		"failed, inspect also fails": {func(e *fakeEngine) { e.volCreate, e.volInspErr = errors.New("reply lost"), errors.New("daemon gone") }, upgradeVolumeCreateUnknown, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			eng := newFakeEngine()
			tc.setup(eng)
			v, err := newUpgradeStoreVolume(fakeRunID)
			if err != nil {
				t.Fatal(err)
			}
			err = v.create(ctx, eng)
			if (err != nil) != tc.err || v.state != tc.want {
				t.Fatalf("err = %v, state = %s, want %s", err, v.state, tc.want)
			}
			if r := v.Receipt(); len(r.Events) == 0 || r.State != tc.want.String() {
				t.Fatalf("receipt = %+v", r)
			}
			if err := v.create(ctx, eng); err == nil {
				t.Fatal("a second create was accepted")
			}
			if n := eng.count("volume-create"); n != 1 {
				t.Fatalf("create calls = %d", n)
			}
		})
	}
}

func TestUpgradeStoreVolumeRejectsAForeignOrChangedVolume(t *testing.T) {
	ctx := context.Background()
	good := upgradeEngineVolume{Name: "tc-ceph-upgrade-mon-" + fakeRunID, Driver: "local", CreatedAt: fakeCreated, Labels: map[string]string{upgradeMonStoreOwnerLabel: fakeRunID}}
	for name, edit := range map[string]func(*upgradeEngineVolume){
		"other owner":      func(v *upgradeEngineVolume) { v.Labels = map[string]string{upgradeMonStoreOwnerLabel: "other"} },
		"no owner":         func(v *upgradeEngineVolume) { v.Labels = nil },
		"other driver":     func(v *upgradeEngineVolume) { v.Driver = "nfs" },
		"driver options":   func(v *upgradeEngineVolume) { v.Options = map[string]string{"type": "tmpfs"} },
		"no creation time": func(v *upgradeEngineVolume) { v.CreatedAt = "" },
		"other name":       func(v *upgradeEngineVolume) { v.Name = "elsewhere" },
	} {
		t.Run(name, func(t *testing.T) {
			v, _ := newUpgradeStoreVolume(fakeRunID)
			bad := good
			edit(&bad)
			if v.validate(bad) == nil {
				t.Fatal("accepted")
			}
		})
	}
	t.Run("creation identity changed after create", func(t *testing.T) {
		eng := newFakeEngine()
		v, _ := newUpgradeStoreVolume(fakeRunID)
		if err := v.create(ctx, eng); err != nil {
			t.Fatal(err)
		}
		changed := eng.vols[v.name]
		changed.CreatedAt = "2027-01-01T00:00:00Z"
		eng.vols[v.name] = changed
		if err := v.Check(ctx, eng); err == nil {
			t.Fatal("a recreated volume passed the identity check")
		}
		if err := v.release(ctx, eng); err == nil || eng.count("volume-remove") != 0 {
			t.Fatalf("a changed volume was removed: %v", err)
		}
	})
}

func TestUpgradeStoreVolumeReleaseNeedsEveryUserGone(t *testing.T) {
	ctx := context.Background()
	eng := newFakeEngine()
	v, _ := newUpgradeStoreVolume(fakeRunID)
	if err := v.create(ctx, eng); err != nil {
		t.Fatal(err)
	}
	v.attach(fakeSource)
	if err := v.release(ctx, eng); err == nil || eng.count("volume-remove") != 0 {
		t.Fatalf("released while the source container exists: %v", err)
	}
	delete(eng.ctrs, fakeSource)
	eng.volRemErr = errors.New("busy")
	if err := v.release(ctx, eng); err == nil || v.state != upgradeVolumeReleaseUnknown {
		t.Fatalf("failed remove: %v, state %s", err, v.state)
	}
	eng.volRemErr = nil
	if err := v.release(ctx, eng); err != nil || v.state != upgradeVolumeReleased {
		t.Fatalf("retry: %v, state %s", err, v.state)
	}
	if err := v.release(ctx, eng); err != nil || eng.count("volume-remove") != 2 {
		t.Fatalf("a released volume was removed again: %v", err)
	}
	if got := v.Receipt(); got.State != "released" || !hasEvent(got.Events, "release failed") || len(got.Users) != 1 {
		t.Fatalf("receipt = %+v", got)
	}
}

func TestUpgradeStoreVolumeUnknownCreateStaysReleasable(t *testing.T) {
	ctx := context.Background()
	eng := newFakeEngine()
	eng.volCreate, eng.volCreated, eng.volInspErr = errors.New("reply lost"), true, errors.New("daemon gone")
	v, _ := newUpgradeStoreVolume(fakeRunID)
	if err := v.create(ctx, eng); err == nil || v.state != upgradeVolumeCreateUnknown {
		t.Fatalf("create: %v, state %s", err, v.state)
	}
	eng.volInspErr = nil
	if err := v.release(ctx, eng); err != nil || v.state != upgradeVolumeReleased || len(eng.vols) != 0 {
		t.Fatalf("release of an unclassified create: %v, state %s", err, v.state)
	}
}

// ---- the lease ---------------------------------------------------------------------

func TestUpgradeLeaseOutputIsStrict(t *testing.T) {
	spec := upgradeLeaseSpec{Address: "10.0.0.1", Ports: []int{3300, 6789}}
	ok := upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6789]}` + "\n"
	if addr, ports, err := parseUpgradeLeaseOutput([]byte("noise\n"+ok), spec); err != nil || addr != "10.0.0.1" || !slices.Equal(ports, []int{3300, 6789}) {
		t.Fatalf("good output: %v %v %v", addr, ports, err)
	}
	for name, out := range map[string]string{
		"empty":             "",
		"no line":           "bound\n",
		"two lines":         ok + ok,
		"not json":          upgradeLeasePrefix + "nope\n",
		"missing address":   upgradeLeasePrefix + `{"ports":[3300,6789]}` + "\n",
		"null address":      upgradeLeasePrefix + `{"address":null,"ports":[3300,6789]}` + "\n",
		"empty address":     upgradeLeasePrefix + `{"address":"","ports":[3300,6789]}` + "\n",
		"address is number": upgradeLeasePrefix + `{"address":10,"ports":[3300,6789]}` + "\n",
		"missing ports":     upgradeLeasePrefix + `{"address":"10.0.0.1"}` + "\n",
		"null ports":        upgradeLeasePrefix + `{"address":"10.0.0.1","ports":null}` + "\n",
		"empty ports":       upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[]}` + "\n",
		"ports are strings": upgradeLeasePrefix + `{"address":"10.0.0.1","ports":["3300","6789"]}` + "\n",
		"other address":     upgradeLeasePrefix + `{"address":"10.0.0.2","ports":[3300,6789]}` + "\n",
		"other ports":       upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6790]}` + "\n",
		"swapped ports":     upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[6789,3300]}` + "\n",
		"unknown field":     upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6789],"x":1}` + "\n",
		"trailing data":     upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6789]} {}` + "\n",
		"closing bracket":   upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6789]}]` + "\n",
		"closing brace":     upgradeLeasePrefix + `{"address":"10.0.0.1","ports":[3300,6789]}}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseUpgradeLeaseOutput([]byte(out), spec); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestUpgradeLeaseSpecFollowsTheRetainedVector(t *testing.T) {
	spec, err := upgradeLeaseSpecFor(fakeVector())
	if err != nil || spec.Address != fakeHostIP || !slices.Equal(spec.Ports, []int{3300, 6789}) {
		t.Fatalf("spec = %+v, %v", spec, err)
	}
	if spec, err = upgradeLeaseSpecFor(upgradeMonVector{monAddr(fakeHostIP, 3300, 0)}); err != nil || !slices.Equal(spec.Ports, []int{3300}) {
		t.Fatalf("msgr2-only spec = %+v, %v", spec, err)
	}
	for name, v := range map[string]upgradeMonVector{
		"empty":     nil,
		"two hosts": {monAddr("192.0.2.10", 3300, 6789), monAddr("192.0.2.11", 3300, 6789)},
		"ipv6":      {monAddr("2001:db8::1", 3300, 6789)},
		"no port":   {monAddr(fakeHostIP, 0, 0)},
	} {
		if _, err := upgradeLeaseSpecFor(v); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// loopbackPort returns a port that was free a moment ago.
func loopbackPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func startAllocator(t *testing.T, port int) *exec.Cmd {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required for the allocator control")
	}
	cmd := exec.Command(py, "-I", "-u", "-c", upgradeExactLeaseScript, "127.0.0.1", strconv.Itoa(port))
	return cmd
}

// The allocator script itself, run locally against loopback: it prints what it
// bound, it holds the port while it runs, and a second allocator for the same
// port fails closed without printing a lease line.
func TestUpgradeExactLeaseScriptHoldsAndFailsClosed(t *testing.T) {
	port := loopbackPort(t)
	first := startAllocator(t, port)
	stdout, err := first.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Process.Kill(); _ = first.Wait() }()
	buf := make([]byte, 256)
	n, err := stdout.Read(buf)
	if err != nil {
		t.Fatalf("read lease line: %v", err)
	}
	spec := upgradeLeaseSpec{Address: "127.0.0.1", Ports: []int{port}}
	if _, ports, err := parseUpgradeLeaseOutput(buf[:n], spec); err != nil || !slices.Equal(ports, []int{port}) {
		t.Fatalf("lease line %q: %v", buf[:n], err)
	}
	// While it is held, another listener cannot take the port.
	if l, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(port)); err == nil {
		_ = l.Close()
		t.Fatal("a listener bound a leased port")
	}
	second := startAllocator(t, port)
	out, err := second.Output()
	if err == nil {
		t.Fatalf("a second allocator got the held port: %q", out)
	}
	if strings.Contains(string(out), upgradeLeasePrefix) {
		t.Fatalf("a failed allocator printed a lease line: %q", out)
	}
}

// ---- the destination entrypoint ------------------------------------------------------

func TestUpgradeDestEntrypointNeverCreatesAStore(t *testing.T) {
	run := func(t *testing.T, withStore bool) (string, error) {
		t.Helper()
		dir := t.TempDir()
		data := filepath.Join(dir, "ceph-a")
		if err := os.MkdirAll(data, 0o755); err != nil {
			t.Fatal(err)
		}
		if withStore {
			if err := os.MkdirAll(filepath.Join(data, "store.db"), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		bin := filepath.Join(dir, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		log := filepath.Join(dir, "ceph-mon.log")
		spy := "#!/bin/sh\necho \"$@\" >> " + log + "\n"
		if err := os.WriteFile(filepath.Join(bin, "ceph-mon"), []byte(spy), 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("/bin/sh", "-c", upgradeDestEntrypoint)
		cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "CEPH_MON_DATA_DIR=" + data, "CEPH_MON_ID=a", "CEPH_RUN_DIR=" + filepath.Join(dir, "run")}
		err := cmd.Run()
		got, _ := os.ReadFile(log)
		return string(got), err
	}
	t.Run("retained store", func(t *testing.T) {
		got, err := run(t, true)
		if err != nil || got != "-f -i a\n" {
			t.Fatalf("ceph-mon invocations %q, err %v", got, err)
		}
	})
	t.Run("store missing", func(t *testing.T) {
		got, err := run(t, false)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 3 || got != "" {
			t.Fatalf("exit %v, ceph-mon invocations %q: mkfs or a start was possible", err, got)
		}
	})
}

func TestUpgradeCephVersionReportIsStrict(t *testing.T) {
	if v, err := parseUpgradeCephVersion(fakeDestVer); err != nil || v != "20.2.4" {
		t.Fatalf("version = %q, %v", v, err)
	}
	for _, bad := range []string{"", "ceph version\n", "ceph version 20.2 (x)\n", "version 20.2.4\n", "ceph version abc.2.4 (x)\n", "x\nceph version 20.2.4 (x)\n"} {
		if v, err := parseUpgradeCephVersion(bad); err == nil {
			t.Errorf("%q parsed as %q", bad, v)
		}
	}
}

// ---- Fence ------------------------------------------------------------------------

func TestUpgradeVolumeEffectsFenceGuards(t *testing.T) {
	cases := map[string]func(*effectsEnv){
		"source already absent": func(e *effectsEnv) { delete(e.eng.ctrs, fakeSource) },
		"bridge source": func(e *effectsEnv) {
			e.eng.ctrs[fakeSource].HostNetwork = false
		},
		"no architecture": func(e *effectsEnv) { e.eng.ctrs[fakeSource].ImageArch = "" },
		"no store mount": func(e *effectsEnv) {
			e.eng.ctrs[fakeSource].Mounts = e.eng.ctrs[fakeSource].Mounts[1:]
		},
		"store mount read-only": func(e *effectsEnv) { e.eng.ctrs[fakeSource].Mounts[0].ReadOnly = true },
		"another volume": func(e *effectsEnv) {
			e.eng.ctrs[fakeSource].Mounts[0].Volume = "someone-elses"
		},
		"no run volume": func(e *effectsEnv) { e.eng.ctrs[fakeSource].Mounts = e.eng.ctrs[fakeSource].Mounts[:1] },
		"volume changed": func(e *effectsEnv) {
			v := e.eng.vols[e.vol.name]
			v.CreatedAt = "2030-01-01T00:00:00Z"
			e.eng.vols[e.vol.name] = v
		},
		"conf unreadable": func(e *effectsEnv) { e.eng.confErr = errors.New("copy failed") },
		"conf other fsid": func(e *effectsEnv) { e.eng.conf = []byte("[global]\nfsid = other\npublic addr = " + fakeHostIP + "\n") },
		"conf no fsid":    func(e *effectsEnv) { e.eng.conf = []byte("[global]\npublic addr = " + fakeHostIP + "\n") },
		"conf other address": func(e *effectsEnv) {
			e.eng.conf = []byte("[global]\nfsid = " + fakeFSID + "\npublic addr = 192.0.2.99\n")
		},
		"stop fails":         func(e *effectsEnv) { e.eng.stopErr = errors.New("timeout") },
		"stop does not stop": func(e *effectsEnv) { e.eng.stopLeaves = true },
		"remove fails":       func(e *effectsEnv) { e.eng.removeErr = errors.New("busy") },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			env := newEffectsEnv(t)
			setup(env)
			if _, err := env.eff.Fence(context.Background(), monA()); err == nil {
				t.Fatal("fence succeeded")
			}
			r := env.eff.Receipt()
			if r.Fenced {
				t.Fatal("receipt says fenced")
			}
			// Nothing downstream may proceed after a failed fence.
			if _, _, err := env.eff.Retained(context.Background(), monA()); err == nil {
				t.Fatal("Retained ran without a fence")
			}
			if env.eng.count("lease") != 0 || env.eng.count("create dest") != 0 {
				t.Fatalf("calls after a failed fence: %v", env.eng.calls)
			}
		})
	}
	t.Run("wrong daemon", func(t *testing.T) {
		env := newEffectsEnv(t)
		if _, err := env.eff.Fence(context.Background(), upgradeDaemonID{Role: "mon", ID: "b"}); err == nil || env.eng.count("stop "+fakeSource) != 0 {
			t.Fatalf("fenced another MON: %v", err)
		}
	})
	t.Run("a stop error is classified from the container", func(t *testing.T) {
		env := newEffectsEnv(t)
		env.eng.stopErr = errors.New("timeout")
		_, _ = env.eff.Fence(context.Background(), monA())
		if !hasEvent(env.eff.Receipt().Events, "stop returned") || env.eng.count("remove "+fakeSource) != 0 || env.eng.count("stop "+fakeSource) != 1 {
			t.Fatalf("events %v, calls %v", env.eff.Receipt().Events, env.eng.calls)
		}
	})
	t.Run("fence is taken once", func(t *testing.T) {
		env := newEffectsEnv(t)
		env.launchReady()
		if _, err := env.eff.Fence(context.Background(), monA()); err == nil || env.eng.count("stop "+fakeSource) != 1 {
			t.Fatalf("second fence: %v", err)
		}
	})
}

func TestUpgradeVolumeEffectsFenceReportsTheOwnedIdentity(t *testing.T) {
	env := newEffectsEnv(t)
	id, err := env.eff.Fence(context.Background(), monA())
	if err != nil {
		t.Fatal(err)
	}
	want := upgradeDaemonID{Role: "mon", ID: "a", FSID: fakeFSID, StoreID: env.vol.identity(), Arch: "amd64"}
	if id != want || !strings.Contains(id.StoreID, env.vol.name) || !strings.Contains(id.StoreID, fakeCreated) {
		t.Fatalf("identity %+v, want %+v", id, want)
	}
	// The conf was copied before the stop, and the container is removed after it.
	idx := func(prefix string) int {
		for i, c := range env.eng.calls {
			if strings.HasPrefix(c, prefix) {
				return i
			}
		}
		return -1
	}
	if !(idx("copy-out") < idx("stop") && idx("stop") < idx("remove")) {
		t.Fatalf("order = %v", env.eng.calls)
	}
}

// ---- Retained and the lease -----------------------------------------------------------

func TestUpgradeVolumeEffectsRetainedTakesTheExactLeaseOnce(t *testing.T) {
	env := newEffectsEnv(t)
	has, vec, err := env.retained()
	if err != nil || !has || !vec.sameSet(fakeVector()) {
		t.Fatalf("retained = %v %v %v", has, vec, err)
	}
	if !env.eff.Receipt().LeaseHeld {
		t.Fatal("no lease is held")
	}
	if _, _, err := env.eff.Retained(context.Background(), monA()); err == nil || env.eng.count("lease") != 1 {
		t.Fatalf("a second lease was taken: %v", err)
	}
	// The vector comes from what the allocator bound, not from the request.
	env2 := newEffectsEnv(t)
	env2.eng.leaseBound = &fakeLease{addr: fakeHostIP, ports: []int{3300, 6790}}
	has, vec, err = env2.retained()
	if err != nil || !has || vec.sameSet(fakeVector()) {
		t.Fatalf("a lease on other ports was reported as the retained endpoint: %v %v %v", has, vec, err)
	}
}

func TestUpgradeVolumeEffectsRetainedFailsClosed(t *testing.T) {
	cases := map[string]struct {
		setup    func(*fakeEngine)
		wantHas  bool
		wantLeas int
	}{
		"probe fails":       {func(e *fakeEngine) { e.probeErr = errors.New("helper failed") }, false, 0},
		"no store.db":       {func(e *fakeEngine) { e.probeHas = false }, false, 0},
		"port is taken":     {func(e *fakeEngine) { e.leaseErr = errors.New("address already in use") }, true, 1},
		"unusable endpoint": {func(e *fakeEngine) { e.leaseBound = &fakeLease{addr: "not-an-address", ports: []int{3300, 6789}} }, true, 1},
		"short port list":   {func(e *fakeEngine) { e.leaseBound = &fakeLease{addr: fakeHostIP, ports: []int{3300}} }, true, 1},
		"zero port":         {func(e *fakeEngine) { e.leaseBound = &fakeLease{addr: fakeHostIP, ports: []int{0, 6789}} }, true, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newEffectsEnv(t)
			tc.setup(env.eng)
			has, vec, err := env.retained()
			if tc.wantHas == false && name == "no store.db" {
				if err != nil || has || vec != nil {
					t.Fatalf("no store: %v %v %v", has, vec, err)
				}
			} else if err == nil {
				t.Fatalf("no error: %v %v", has, vec)
			}
			if got := env.eng.count("lease"); got != tc.wantLeas {
				t.Fatalf("lease calls = %d, want %d", got, tc.wantLeas)
			}
			if env.eng.count("create "+env.eff.containerName("i")) != 0 {
				t.Fatal("a destination exists")
			}
		})
	}
}

// ---- Launch ---------------------------------------------------------------------------

func TestUpgradeVolumeEffectsLaunchBuildsTheRetainedStart(t *testing.T) {
	env := newEffectsEnv(t)
	env.launchReady()
	if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err != nil {
		t.Fatal(err)
	}
	var spec *upgradeDestSpec
	for _, c := range env.eng.ctrs {
		if c.spec != nil {
			spec = c.spec
		}
	}
	if spec == nil {
		t.Fatal("no destination was created")
	}
	if spec.Image != "dest-image" || !spec.HostNetwork || spec.Name != "tc-upgrade-"+fakeRunID+"-i1" {
		t.Fatalf("spec = %+v", spec)
	}
	if !slices.Contains(spec.Mounts, upgradeEngineMount{Volume: env.vol.name, Target: fakeStoreDir}) || !slices.Contains(spec.Mounts, upgradeEngineMount{Volume: fakeRunVol, Target: upgradeMonRunDir}) {
		t.Fatalf("mounts = %+v", spec.Mounts)
	}
	if string(spec.Files["/etc/ceph/ceph.conf"]) != string(env.eng.conf) || spec.Env["CEPH_MON_DATA_DIR"] != fakeStoreDir || spec.Env["CEPH_MON_ID"] != "a" {
		t.Fatalf("files/env = %v / %v", spec.Files, spec.Env)
	}
	if len(spec.Entrypoint) != 3 || spec.Entrypoint[2] != upgradeDestEntrypoint || spec.Labels[upgradeMonStoreOwnerLabel] != fakeRunID {
		t.Fatalf("entrypoint/labels = %v / %v", spec.Entrypoint, spec.Labels)
	}
	// The lease is released after the attempt is recorded and before create.
	release, create := -1, -1
	for i, c := range env.eng.calls {
		switch {
		case c == "lease-release":
			release = i
		case strings.HasPrefix(c, "create "):
			create = i
		}
	}
	if release < 0 || create < 0 || release > create || env.eff.Receipt().LeaseHeld {
		t.Fatalf("lease release at %d, create at %d, calls %v", release, create, env.eng.calls)
	}
	if got := env.eff.Receipt().Launches["i1"]; !strings.HasSuffix(got, "started") {
		t.Fatalf("launch record = %q", got)
	}
	if !slices.Contains(env.vol.users, "tc-upgrade-"+fakeRunID+"-i1") {
		t.Fatalf("destination is not a recorded user of the volume: %v", env.vol.users)
	}
}

func TestUpgradeVolumeEffectsLaunchGuards(t *testing.T) {
	cases := map[string]func(*upgradeMonPlan){
		"mkfs":              func(p *upgradeMonPlan) { p.Mkfs = true },
		"monmap update":     func(p *upgradeMonPlan) { p.MonmapUpdate = true },
		"membership change": func(p *upgradeMonPlan) { p.MembershipChange = true },
		"other address":     func(p *upgradeMonPlan) { p.Env["CEPH_PUBLIC_ADDRESS"] = "192.0.2.99" },
		"other v2 port":     func(p *upgradeMonPlan) { p.Env["CEPH_MON_PORT_V2"] = "3301" },
		"other v1 port":     func(p *upgradeMonPlan) { p.Env["CEPH_MON_PORT_V1"] = "6790" },
		"v1 port dropped":   func(p *upgradeMonPlan) { delete(p.Env, "CEPH_MON_PORT_V1") },
		"other MON":         func(p *upgradeMonPlan) { p.Env["CEPH_MON_ID"] = "b" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			env := newEffectsEnv(t)
			env.launchReady()
			plan := goodPlan()
			edit(&plan)
			if err := env.eff.Launch(context.Background(), "i1", plan); err == nil {
				t.Fatal("launched")
			}
			if env.eng.count("create "+env.eff.containerName("i1")) != 0 || env.eng.count("lease-release") != 0 {
				t.Fatalf("a refused plan changed state: %v", env.eng.calls)
			}
			// The same effects still accept the corrected plan once.
			if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err != nil {
				t.Fatalf("corrected plan: %v", err)
			}
		})
	}
	t.Run("no lease", func(t *testing.T) {
		env := newEffectsEnv(t)
		if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err == nil {
			t.Fatal("launched without a lease")
		}
	})
	t.Run("the same instance twice", func(t *testing.T) {
		env := newEffectsEnv(t)
		env.launchReady()
		if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err != nil {
			t.Fatal(err)
		}
		if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err == nil || env.eng.count("create "+env.eff.containerName("i1")) != 1 {
			t.Fatalf("second launch: %v", err)
		}
	})
}

func TestUpgradeVolumeEffectsLaunchOutcomesAreRecordedAndNeverReplayed(t *testing.T) {
	cases := map[string]struct {
		setup  func(*fakeEngine)
		state  string
		starts int
		exists bool
	}{
		"lease release fails": {func(e *fakeEngine) { e.lease.relErr = errors.New("busy") }, "not-created", 0, false},
		"create fails":        {func(e *fakeEngine) { e.createErr = errors.New("daemon error") }, "create-unknown", 0, false},
		"create reply lost":   {func(e *fakeEngine) { e.createErr, e.createReg = errors.New("reply lost"), true }, "create-unknown", 0, true},
		"start fails":         {func(e *fakeEngine) { e.startErr = errors.New("daemon error") }, "start-unknown", 1, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newEffectsEnv(t)
			env.launchReady()
			tc.setup(env.eng)
			if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err == nil {
				t.Fatal("no error")
			}
			rec := env.eff.Receipt().Launches["i1"]
			if !strings.Contains(rec, tc.state) {
				t.Fatalf("record = %q, want %q", rec, tc.state)
			}
			if env.eng.count("start "+env.eff.containerName("i1")) != tc.starts {
				t.Fatalf("start calls: %v", env.eng.calls)
			}
			// Observation resolves the outcome by name, without creating anything.
			before := env.eng.count("create " + env.eff.containerName("i1"))
			_, _ = env.eff.Observe(context.Background(), "i1")
			if env.eng.count("create "+env.eff.containerName("i1")) != before {
				t.Fatal("observation created a destination")
			}
			if _, ok := env.eng.ctrs[env.eff.containerName("i1")]; ok != tc.exists {
				t.Fatalf("container exists = %v, want %v", ok, tc.exists)
			}
			// A lease whose release failed stays owned until a release works.
			if name == "lease release fails" {
				if !env.eff.Receipt().LeaseHeld {
					t.Fatal("a lease whose release failed is no longer owned")
				}
				if err := env.eff.Dispose(context.Background(), []string{"i1"}); err == nil {
					t.Fatal("disposal hid the failed lease release")
				}
				env.eng.lease.relErr = nil
			}
			// Disposal removes what exists and tolerates what does not.
			if err := env.eff.Dispose(context.Background(), []string{"i1"}); err != nil {
				t.Fatalf("dispose: %v", err)
			}
			if _, ok := env.eng.ctrs[env.eff.containerName("i1")]; ok {
				t.Fatal("destination remains after disposal")
			}
		})
	}
}

// ---- Observe -----------------------------------------------------------------------

func launched(t *testing.T, edit func(*effectsEnv)) *effectsEnv {
	t.Helper()
	env := newEffectsEnv(t)
	if edit != nil {
		edit(env)
	}
	env.launchReady()
	if err := env.eff.Launch(context.Background(), "i1", goodPlan()); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestUpgradeVolumeEffectsObserveReportsNativeEvidence(t *testing.T) {
	env := launched(t, nil)
	ev, err := env.eff.Observe(context.Background(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	want := upgradeNativeEvidence{Instance: "i1", Version: "20.2.4", FSID: fakeFSID, StoreID: env.vol.identity()}
	if ev != want {
		t.Fatalf("evidence %+v, want %+v", ev, want)
	}
}

func TestUpgradeVolumeEffectsObserveIsNotFooledByAWrongDestination(t *testing.T) {
	cases := map[string]struct {
		edit  func(*effectsEnv)
		after func(*effectsEnv)
	}{
		"destination exits (bind failure)": {edit: func(e *effectsEnv) { e.eng.startExit = true }},
		"destination never created":        {edit: func(e *effectsEnv) { e.eng.createErr = errors.New("daemon error") }},
		"destination mounts another volume": {after: func(e *effectsEnv) {
			e.eng.ctrs[e.eff.containerName("i1")].Mounts[0].Volume = "other"
		}},
		"destination store read-only": {after: func(e *effectsEnv) {
			e.eng.ctrs[e.eff.containerName("i1")].Mounts[0].ReadOnly = true
		}},
		"volume replaced": {after: func(e *effectsEnv) {
			v := e.eng.vols[e.vol.name]
			v.CreatedAt = "2031-01-01T00:00:00Z"
			e.eng.vols[e.vol.name] = v
		}},
		"version unparsable":      {after: func(e *effectsEnv) { e.eng.execOut = "weird\n" }},
		"version command fails":   {after: func(e *effectsEnv) { e.eng.execCode = 1 }},
		"version exec errors":     {after: func(e *effectsEnv) { e.eng.execErr = errors.New("no exec") }},
		"monmap view unavailable": {after: func(e *effectsEnv) { e.viewErr = errors.New("quorum unavailable") }},
		"monmap address moved": {after: func(e *effectsEnv) {
			e.view.Addrs = upgradeMonVector{monAddr(fakeHostIP, 3301, 6789)}
		}},
		"monmap address family changed": {after: func(e *effectsEnv) {
			e.view.Addrs = upgradeMonVector{monAddr("2001:db8::1", 3300, 6789)}
		}},
		"monmap address empty": {after: func(e *effectsEnv) { e.view.Addrs = nil }},
		"never reaches quorum": {after: func(e *effectsEnv) { e.quorumAfter = 1000 }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := launchedAllowLaunchError(t, tc.edit)
			if tc.after != nil {
				tc.after(env)
			}
			ev, err := env.eff.Observe(context.Background(), "i1")
			if err == nil {
				t.Fatalf("evidence reported: %+v", ev)
			}
			if ev != (upgradeNativeEvidence{}) {
				t.Fatalf("evidence alongside an error: %+v", ev)
			}
		})
	}
	t.Run("joins quorum after a few observations", func(t *testing.T) {
		env := launched(t, func(e *effectsEnv) { e.quorumAfter = 2 })
		if _, err := env.eff.Observe(context.Background(), "i1"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("an unknown instance", func(t *testing.T) {
		env := launched(t, nil)
		if _, err := env.eff.Observe(context.Background(), "i2"); err == nil {
			t.Fatal("observed an instance that was never attempted")
		}
	})
	t.Run("destination stops while observed", func(t *testing.T) {
		env := launched(t, nil)
		env.eng.execOut = fakeDestVer
		env.build(func(c *upgradeVolumeMonEffectsConfig) {
			base := c.View
			c.View = func(ctx context.Context) (upgradeMonView, error) {
				v, err := base(ctx)
				if ctr := env.eng.ctrs[env.eff.containerName("i1")]; ctr != nil {
					ctr.Running, ctr.Status = false, "exited"
				}
				return v, err
			}
		})
		// build replaced the effects; restore their launch record.
		env.eff.launches["i1"] = &upgradeLaunchRecord{Name: "tc-upgrade-" + fakeRunID + "-i1", State: "started"}
		if ev, err := env.eff.Observe(context.Background(), "i1"); err == nil {
			t.Fatalf("a destination that stopped was confirmed: %+v", ev)
		}
	})
}

// launchedAllowLaunchError is launched, but a Launch error is part of the case.
func launchedAllowLaunchError(t *testing.T, edit func(*effectsEnv)) *effectsEnv {
	t.Helper()
	env := newEffectsEnv(t)
	if edit != nil {
		edit(env)
	}
	env.launchReady()
	_ = env.eff.Launch(context.Background(), "i1", goodPlan())
	return env
}

// ---- Dispose ------------------------------------------------------------------------

func TestUpgradeVolumeEffectsDisposeRemovesOnlyWhatItRecorded(t *testing.T) {
	env := launched(t, nil)
	bystander := &fakeCtr{upgradeEngineContainer: upgradeEngineContainer{ID: "bystander", Name: "bystander", Running: true, Status: "running"}}
	env.eng.ctrs["bystander"] = bystander
	if err := env.eff.Dispose(context.Background(), []string{"i1", "bystander"}); err == nil {
		t.Fatal("an unrecorded instance was accepted")
	}
	if _, ok := env.eng.ctrs["bystander"]; !ok || env.eng.count("remove bystander") != 0 {
		t.Fatal("a container that was never recorded was removed")
	}
	if _, ok := env.eng.ctrs[env.eff.containerName("i1")]; ok {
		t.Fatal("the recorded destination remains")
	}
	if env.eng.count("stop "+env.eff.containerName("i1")) != 1 {
		t.Fatalf("a running destination was not stopped first: %v", env.eng.calls)
	}
}

func TestUpgradeVolumeEffectsDisposeKeepsFailuresSeparateAndReleasesTheLease(t *testing.T) {
	env := newEffectsEnv(t)
	env.launchReady() // lease held, nothing launched
	if err := env.eff.Dispose(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if env.eff.Receipt().LeaseHeld || !env.eng.lease.released {
		t.Fatal("the lease was not released")
	}
	env2 := newEffectsEnv(t)
	env2.launchReady()
	env2.eng.leaseRelEr = nil
	env2.eng.lease.relErr = errors.New("busy")
	err := env2.eff.Dispose(context.Background(), nil)
	if err == nil || !env2.eff.Receipt().LeaseHeld {
		t.Fatalf("a failed lease release was hidden: %v", err)
	}
	env3 := launched(t, nil)
	env3.eng.removeErr = errors.New("busy")
	if err := env3.eff.Dispose(context.Background(), []string{"i1"}); err == nil || !hasEvent(env3.eff.Receipt().Events, "remove") {
		t.Fatalf("a failed remove was hidden: %v", err)
	}
}

// ---- with the merged driver ----------------------------------------------------------

// driverEnv wires the real effects over the fake engine into the merged
// driver, with the sequence plan carrying the volume's store identity.
func driverEnv(t *testing.T, edit func(*effectsEnv)) (*stepEnv, *effectsEnv) {
	t.Helper()
	ee := newEffectsEnv(t)
	if edit != nil {
		edit(ee)
	}
	env := newStepEnv(t)
	plan := seqPlan()
	plan[0].StoreID = ee.vol.identity()
	seq, err := newUpgradeSequence(newTestLedger(t), upgradePair, plan, goodFacts(t))
	if err != nil {
		t.Fatal(err)
	}
	env.seq = seq
	h, err := newUpgradeMonHandoff(upgradeMonRetained{Name: "a", FSID: fakeFSID, StoreID: ee.vol.identity(), Addrs: fakeVector()})
	if err != nil {
		t.Fatal(err)
	}
	env.handoff = h
	return env, ee
}

func TestUpgradeVolumeEffectsDriveTheMergedStep(t *testing.T) {
	env, ee := driverEnv(t, nil)
	rc := env.step(func(c *upgradeMonStepConfig) { c.Effects = ee.eff }).run(context.Background())
	if rc.Err != nil || rc.CleanupErr != nil {
		t.Fatalf("err = %v, cleanup = %v, events = %v", rc.Err, rc.CleanupErr, rc.Events)
	}
	if rc.Step != upgradeNativeConfirmed || !rc.Settled || len(rc.Instances) != 1 {
		t.Fatalf("receipt = %+v", rc)
	}
	// Disposal removed the destination; only then may the volume go.
	if _, ok := ee.eng.ctrs[ee.eff.containerName(rc.Instances[0])]; ok {
		t.Fatal("destination remains after the step")
	}
	if err := ee.vol.release(context.Background(), ee.eng); err != nil || ee.vol.state != upgradeVolumeReleased {
		t.Fatalf("volume release: %v", err)
	}
	if got := ee.vol.Receipt(); len(got.Users) != 2 {
		t.Fatalf("volume users = %v", got.Users)
	}
	if ee.eng.count("create "+ee.eff.containerName(rc.Instances[0])) != 1 {
		t.Fatal("the destination was created more than once")
	}
}

func TestUpgradeVolumeEffectsStopTheStepOnEveryFailure(t *testing.T) {
	cases := map[string]struct {
		edit     func(*effectsEnv)
		step     upgradeStep
		launched bool
	}{
		"fence fails":                      {func(e *effectsEnv) { e.eng.stopErr = errors.New("timeout") }, upgradeNotAttempted, false},
		"remove fails":                     {func(e *effectsEnv) { e.eng.removeErr = errors.New("busy") }, upgradeNotAttempted, false},
		"no store.db":                      {func(e *effectsEnv) { e.eng.probeHas = false }, upgradeOldAbsent, false},
		"port taken":                       {func(e *effectsEnv) { e.eng.leaseErr = errors.New("address already in use") }, upgradeOldAbsent, false},
		"destination exits":                {func(e *effectsEnv) { e.eng.startExit = true }, upgradeStartAttempted, true},
		"create reply lost, never created": {func(e *effectsEnv) { e.eng.createErr = errors.New("reply lost") }, upgradeStartAttempted, true},
		"monmap address differs": {func(e *effectsEnv) {
			e.view.Addrs = upgradeMonVector{monAddr(fakeHostIP, 3301, 6789)}
		}, upgradeStartAttempted, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, ee := driverEnv(t, tc.edit)
			var receipts []upgradeVolumeMonEffectsReceipt
			rc := env.step(func(c *upgradeMonStepConfig) {
				c.Effects = ee.eff
				base := c.OnReceipt
				c.OnReceipt = func(r upgradeMonReceipt) {
					receipts = append(receipts, ee.eff.Receipt()) // before cleanup
					base(r)
				}
			}).run(context.Background())
			if rc.Err == nil || rc.Settled {
				t.Fatalf("err = %v settled = %v", rc.Err, rc.Settled)
			}
			if rc.Step != tc.step {
				t.Fatalf("step = %s, want %s", rc.Step, tc.step)
			}
			if len(receipts) != 1 || len(receipts[0].Events) == 0 {
				t.Fatalf("receipt before cleanup: %+v", receipts)
			}
			if tc.launched != (len(rc.Instances) == 1) {
				t.Fatalf("instances = %v", rc.Instances)
			}
			if n := ee.eng.count("create " + ee.eff.containerName("dst-t1-mon.a")); n > 1 {
				t.Fatalf("destination create calls = %d", n)
			}
			if rc.CleanupErr != nil {
				t.Fatalf("cleanup: %v", rc.CleanupErr)
			}
			if ee.eff.Receipt().LeaseHeld && len(rc.Instances) > 0 {
				t.Fatal("lease still held after cleanup")
			}
			if _, _, err := env.seq.admit(); err == nil {
				t.Fatal("the halted sequence admitted another transition")
			}
		})
	}
}

func TestUpgradeVectorFromMonmapIsStrict(t *testing.T) {
	good := []upgradeMonmapAddr{{"v2", fakeHostIP + ":3300"}, {"v1", fakeHostIP + ":6789"}}
	if v, err := upgradeVectorFromMonmap(good); err != nil || !v.sameSet(fakeVector()) {
		t.Fatalf("good = %v, %v", v, err)
	}
	if v, err := upgradeVectorFromMonmap(good[:1]); err != nil || !v.sameSet(upgradeMonVector{monAddr(fakeHostIP, 3300, 0)}) {
		t.Fatalf("msgr2 only = %v, %v", v, err)
	}
	for name, in := range map[string][]upgradeMonmapAddr{
		"empty":          nil,
		"v1 only":        {{"v1", fakeHostIP + ":6789"}},
		"other type":     {{"any", fakeHostIP + ":3300"}},
		"empty type":     {{"", fakeHostIP + ":3300"}},
		"no port":        {{"v2", fakeHostIP}},
		"port zero":      {{"v2", fakeHostIP + ":0"}},
		"not an address": {{"v2", "mon-a:3300"}},
		"repeated v2":    {{"v2", fakeHostIP + ":3300"}, {"v2", fakeHostIP + ":3301"}},
		"repeated v1":    {{"v2", fakeHostIP + ":3300"}, {"v1", fakeHostIP + ":6789"}, {"v1", fakeHostIP + ":6790"}},
		"same port both": {{"v2", fakeHostIP + ":3300"}, {"v1", fakeHostIP + ":3300"}},
	} {
		if v, err := upgradeVectorFromMonmap(in); err == nil {
			t.Errorf("%s: accepted as %v", name, v)
		}
	}
}
