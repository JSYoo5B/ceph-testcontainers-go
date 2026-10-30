package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// initialOSDMon models the MON state that OSD registration and startup
// change: the OSDMap with its next free ID and up/in flags, and the CRUSH
// buckets in their creation order.
type initialOSDMon struct {
	testcontainers.Container
	mu           sync.Mutex
	native       map[int]initialOSDState
	uuids        map[int]string
	buckets      []string
	classes      map[int]string
	terminations int
}

type initialOSDState struct{ up, in bool }

func newInitialOSDMon() *initialOSDMon {
	return &initialOSDMon{native: map[int]initialOSDState{}, uuids: map[int]string{}, classes: map[int]string{}}
}

func (m *initialOSDMon) CopyToContainer(ctx context.Context, _ []byte, _ string, _ int64) error {
	return ctx.Err()
}

func (m *initialOSDMon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminations++
	return nil
}

func (m *initialOSDMon) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.Equal(args, []string{"ceph-authtool", "--gen-print-key"}) {
		return 0, noInitialOSDStream("AQBsynthetickey=="), nil
	}
	if len(args) < 4 || args[0] != "ceph" {
		return 1, noInitialOSDStream("unexpected command " + strings.Join(args, " ")), nil
	}
	action := args[3:]
	switch {
	case len(action) == 5 && action[0] == "osd" && action[1] == "new":
		id := 0
		for {
			if _, used := m.native[id]; !used {
				break
			}
			id++
		}
		m.native[id] = initialOSDState{}
		m.uuids[id] = action[2]
		return 0, noInitialOSDStream(strconv.Itoa(id) + "\n"), nil
	case len(action) >= 5 && action[0] == "osd" && action[1] == "crush" && action[2] == "add-bucket":
		m.buckets = append(m.buckets, strings.Join(action[3:], " "))
		return 0, noInitialOSDStream(""), nil
	case slices.Equal(action, []string{"osd", "dump", "--format", "json"}):
		rows := []map[string]any{}
		for id, state := range m.native {
			up, in := 0, 0
			if state.up {
				up = 1
			}
			if state.in {
				in = 1
			}
			rows = append(rows, map[string]any{"osd": id, "uuid": m.uuids[id], "up": up, "in": in})
		}
		data, _ := json.Marshal(map[string]any{"osds": rows})
		return 0, noInitialOSDStream(string(data)), nil
	case len(action) == 4 && action[0] == "osd" && action[1] == "crush" && action[2] == "rm-device-class":
		return 0, noInitialOSDStream(""), nil
	case len(action) == 5 && action[0] == "osd" && action[1] == "crush" && action[2] == "set-device-class":
		id, _ := strconv.Atoi(strings.TrimPrefix(action[4], "osd."))
		m.classes[id] = action[3]
		return 0, noInitialOSDStream(""), nil
	}
	return 1, noInitialOSDStream("unexpected command " + strings.Join(args, " ")), nil
}

// boot models a started OSD daemon joining the OSDMap.
func (m *initialOSDMon) boot(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.native[id] = initialOSDState{up: true, in: true}
}

// initialOSDDaemon is an OSD container created by the fake runner.
type initialOSDDaemon struct {
	testcontainers.Container
	id, terminations int
	env              map[string]string
}

func (d *initialOSDDaemon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	d.terminations++
	return nil
}

// initialOSDRunner models Docker creating OSD containers. fail names an OSD
// whose container is created but whose start fails, as a failed mkfs does.
// gate, when set, is called inside each start before it completes.
type initialOSDRunner struct {
	mon              *initialOSDMon
	mu               sync.Mutex
	fail             map[int]error
	gate             func(ctx context.Context, id int)
	started          []*initialOSDDaemon
	inflight, maxRun int
}

func (r *initialOSDRunner) run(ctx context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	var req testcontainers.GenericContainerRequest
	for _, opt := range opts {
		if err := opt.Customize(&req); err != nil {
			return nil, err
		}
	}
	id, err := strconv.Atoi(req.Env["CEPH_OSD_ID"])
	if err != nil {
		return nil, fmt.Errorf("OSD container without ID: %w", err)
	}
	daemon := &initialOSDDaemon{id: id, env: req.Env}
	r.mu.Lock()
	r.inflight++
	r.maxRun = max(r.maxRun, r.inflight)
	r.started = append(r.started, daemon)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.inflight--
		r.mu.Unlock()
	}()
	if r.gate != nil {
		r.gate(ctx, id)
	}
	if err := r.fail[id]; err != nil {
		return daemon, err
	}
	r.mon.boot(id)
	return daemon, nil
}

func (r *initialOSDRunner) daemons() map[int]*initialOSDDaemon {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[int]*initialOSDDaemon, len(r.started))
	for _, daemon := range r.started {
		result[daemon.id] = daemon
	}
	return result
}

// initialOSDBarrier lets n starts wait for each other. A start gives up after
// a second, so that code which never runs n starts at once fails the test
// instead of hanging it.
func initialOSDBarrier(n int) func() {
	var mu sync.Mutex
	arrived := 0
	all := make(chan struct{})
	return func() {
		mu.Lock()
		arrived++
		if arrived == n {
			close(all)
		}
		mu.Unlock()
		select {
		case <-all:
		case <-time.After(time.Second):
		}
	}
}

func newInitialOSDCluster(settings options) (*Container, *initialOSDMon, *initialOSDRunner) {
	mon := newInitialOSDMon()
	runner := &initialOSDRunner{mon: mon}
	if settings.startupTimeout == 0 {
		settings.startupTimeout = 5 * time.Second
	}
	cluster := &Container{Container: mon, settings: settings, osdHandles: osdRegistry{owned: map[int]*OSDContainer{}}, stores: osdStorageState{runner: runner.run}}
	return cluster, mon, runner
}

func initialOSDLayout() []OSDConfig {
	return []OSDConfig{
		{Host: "host-a", Rack: "rack-1"},
		{},
		{Host: "host-a", Rack: "rack-1", DeviceClass: "fast"},
		{Host: "host-b", Root: "default"},
	}
}

func TestInitialOSDsKeepSequentialIDsAndPlacement(t *testing.T) {
	layout := initialOSDLayout()
	cluster, mon, runner := newInitialOSDCluster(options{osds: len(layout), initialOSDs: layout})
	if err := cluster.startInitialOSDs(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Runtime additions of the same layout, one after another, are the
	// reference: the order the module used before the concurrent start.
	sequential, _, _ := newInitialOSDCluster(options{osds: len(layout)})
	for _, config := range layout {
		if _, err := sequential.AddOSD(t.Context(), OSDOptions{}.WithPlacement(config)); err != nil {
			t.Fatal(err)
		}
	}
	if cluster.osdHandles.count() != len(layout) || sequential.osdHandles.count() != len(layout) {
		t.Fatalf("owned %d initial and %d sequential OSDs", cluster.osdHandles.count(), sequential.osdHandles.count())
	}
	daemons := runner.daemons()
	for id := range layout {
		got, want := cluster.osdHandles.get(id), sequential.osdHandles.get(id)
		if got == nil || want == nil || got.Placement() != want.Placement() {
			t.Fatalf("osd.%d placement %+v, sequential %+v", id, got, want)
		}
		daemon := daemons[id]
		if daemon == nil || got.Container != daemon || daemon.env["CEPH_OSD_UUID"] != mon.uuids[id] ||
			daemon.env["CEPH_OSD_HOST"] != got.Placement().Host || daemon.env["CEPH_OSD_RACK"] != got.Placement().Rack {
			t.Fatalf("osd.%d container does not carry its registration: %+v", id, daemon)
		}
	}
	if got := cluster.osdHandles.get(1).Placement().Host; got != "osd-1" {
		t.Fatalf("omitted host resolved to %q", got)
	}
	wantBuckets := []string{"host-a host root=default rack=rack-1", "osd-1 host root=default", "host-b host root=default"}
	if !slices.Equal(mon.buckets, wantBuckets) {
		t.Fatalf("CRUSH host buckets %q, want %q in registration order", mon.buckets, wantBuckets)
	}
	if mon.classes[2] != "fast" || len(mon.classes) != 1 {
		t.Fatalf("device classes %v", mon.classes)
	}
}

func TestInitialOSDStartFailureKeepsEveryCreatedOSDOwned(t *testing.T) {
	cluster, _, runner := newInitialOSDCluster(options{osds: 3})
	mkfs := errors.New("ceph-osd --mkfs exited 1")
	runner.fail = map[int]error{1: mkfs}
	// Every start is under way before osd.1 fails.
	arrive := initialOSDBarrier(3)
	runner.gate = func(context.Context, int) { arrive() }
	err := cluster.startInitialOSDs(t.Context())
	if !errors.Is(err, mkfs) || !strings.Contains(err.Error(), "osd.1") {
		t.Fatalf("error does not name the failed OSD: %v", err)
	}
	daemons := runner.daemons()
	if cluster.osdHandles.count() != 3 || len(daemons) != 3 {
		t.Fatalf("owned %d OSDs after %d container creations", cluster.osdHandles.count(), len(daemons))
	}
	for id, daemon := range daemons {
		if cluster.osdHandles.get(id) == nil || cluster.osdHandles.get(id).Container != daemon {
			t.Fatalf("created osd.%d container is not owned", id)
		}
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id, daemon := range daemons {
		if daemon.terminations != 1 {
			t.Fatalf("osd.%d terminated %d times", id, daemon.terminations)
		}
	}
	if cluster.osdHandles.count() != 0 {
		t.Fatalf("%d OSDs remain owned after Terminate", cluster.osdHandles.count())
	}
}

func TestInitialOSDStartFailureStartsNoFurtherOSD(t *testing.T) {
	cluster, _, runner := newInitialOSDCluster(options{osds: 6})
	refused := errors.New("container start refused")
	runner.fail = map[int]error{0: refused}
	// The first initialOSDStarts starts meet before osd.0 fails, so that the
	// launcher has scheduled all of them. osd.1 to osd.3 are held until the
	// launcher waits for its starts: freeing their slots earlier could let it
	// schedule a further OSD before it observes the failure of osd.0.
	var mu sync.Mutex
	entered, further := 0, 0
	all := make(chan struct{})
	release := make(chan struct{})
	runner.gate = func(_ context.Context, id int) {
		mu.Lock()
		if id >= initialOSDStarts {
			further++
			mu.Unlock()
			return
		}
		entered++
		if entered == initialOSDStarts {
			close(all)
		}
		mu.Unlock()
		<-all
		if id != 0 {
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- cluster.startInitialOSDs(t.Context()) }()
	select {
	case <-all:
	case err := <-done:
		t.Fatalf("returned %v before %d starts met", err, initialOSDStarts)
	case <-time.After(5 * time.Second):
		t.Fatalf("%d starts did not meet", initialOSDStarts)
	}
	waitOSDLauncherJoins(t)
	close(release)
	if err := <-done; !errors.Is(err, refused) {
		t.Fatalf("got %v", err)
	}
	daemons := runner.daemons()
	if further != 0 || len(daemons) != initialOSDStarts || cluster.osdHandles.count() != 6 {
		t.Fatalf("created %d containers (%d after the failure) for %d registered OSDs", len(daemons), further, cluster.osdHandles.count())
	}
	for id := range 6 {
		if (daemons[id] != nil) != (cluster.osdHandles.get(id).Container != nil) {
			t.Fatalf("osd.%d ownership does not match its container", id)
		}
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id, daemon := range daemons {
		if daemon.terminations != 1 {
			t.Fatalf("osd.%d terminated %d times", id, daemon.terminations)
		}
	}
}

// waitOSDLauncherJoins returns once startOSDs has stopped scheduling and waits
// for the starts it began, which its goroutine stack shows. The launcher
// exposes no other signal, and the test must not change it.
func waitOSDLauncherJoins(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, ".osdFleet.startOSDs(") && strings.Contains(g, "sync.(*WaitGroup).Wait(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("startOSDs did not wait for its starts")
}

func TestInitialOSDStartsAreBounded(t *testing.T) {
	cluster, _, runner := newInitialOSDCluster(options{osds: 7})
	// The first starts wait for each other: a bound above initialOSDStarts
	// lets a further start join them, and one below it never releases them.
	arrive := initialOSDBarrier(initialOSDStarts)
	var mu sync.Mutex
	entered := 0
	runner.gate = func(context.Context, int) {
		mu.Lock()
		entered++
		first := entered <= initialOSDStarts
		mu.Unlock()
		if first {
			arrive()
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := cluster.startInitialOSDs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runner.maxRun != initialOSDStarts || len(runner.daemons()) != 7 {
		t.Fatalf("%d concurrent starts of %d, want at most %d", runner.maxRun, len(runner.daemons()), initialOSDStarts)
	}
}

func TestInitialOSDCancellationStartsNoFurtherOSD(t *testing.T) {
	cluster, _, runner := newInitialOSDCluster(options{osds: 6})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	arrive := initialOSDBarrier(initialOSDStarts)
	runner.gate = func(context.Context, int) {
		arrive()
		cancel()
	}
	err := cluster.startInitialOSDs(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	daemons := runner.daemons()
	if len(daemons) != initialOSDStarts || cluster.osdHandles.count() != 6 {
		t.Fatalf("created %d containers for %d registered OSDs after cancellation", len(daemons), cluster.osdHandles.count())
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id, daemon := range daemons {
		if daemon.terminations != 1 {
			t.Fatalf("osd.%d terminated %d times", id, daemon.terminations)
		}
	}
}
