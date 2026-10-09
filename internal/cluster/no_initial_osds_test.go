package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestNoInitialOSDsRejectsContradictionsBeforeRuntime(t *testing.T) {
	for _, option := range []struct {
		name string
		opt  Option
	}{
		{"positive count", WithOSDCount(1)},
		{"positive layout", WithInitialOSDs(OSDConfig{Host: "first"})},
		{"zero count", WithOSDCount(0)},
		{"negative count", WithOSDCount(-1)},
		{"empty layout", WithInitialOSDs()},
		{"pool", WithPools(PoolConfig{Name: "data"})},
		{"filesystem", WithCephFS()},
		{"gateway", WithRGW()},
		{"invalid pool default", WithPoolDefaults(0, 1)},
	} {
		for _, first := range []bool{false, true} {
			name := option.name + "/zero-last"
			opts := []testcontainers.ContainerCustomizer{option.opt, WithNoInitialOSDs()}
			if first {
				name, opts = option.name+"/zero-first", []testcontainers.ContainerCustomizer{WithNoInitialOSDs(), option.opt}
			}
			t.Run(name, func(t *testing.T) {
				var customizers atomic.Int32
				opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
				cluster, err := Run(t.Context(), DefaultImage, opts...)
				if cluster != nil || err == nil || customizers.Load() != 0 {
					t.Fatalf("invalid zero-storage setup reached runtime/customizer: cluster=%v err=%v calls=%d", cluster, err, customizers.Load())
				}
			})
		}
	}
}

func TestNoInitialOSDsPreservesProspectiveDefaultsAndFinalComposition(t *testing.T) {
	for _, test := range []struct {
		name                        string
		opts                        []Option
		wantOSDs, wantSize, wantMin int
	}{
		{"ordinary default", nil, 2, 2, 1},
		{"ordinary one", []Option{WithOSDCount(1)}, 1, 1, 1},
		{"ordinary layout count replaces previous count", []Option{WithOSDCount(1), WithInitialOSDs(OSDConfig{}, OSDConfig{})}, 2, 2, 1},
		{"zero default", []Option{WithNoInitialOSDs()}, 0, 2, 1},
		{"zero explicit defaults", []Option{WithPoolDefaults(1, 1), WithNoInitialOSDs()}, 0, 1, 1},
		{"zero defaults last", []Option{WithNoInitialOSDs(), WithPoolDefaults(3, 2)}, 0, 3, 2},
		{"zero custom root", []Option{WithDefaultCRUSHRoot("storage"), WithNoInitialOSDs()}, 0, 2, 1},
		{"zero empty final pool list", []Option{WithPools(PoolConfig{Name: "discarded"}), WithPools(), WithNoInitialOSDs()}, 0, 2, 1},
		{"idempotent zero", []Option{WithNoInitialOSDs(), WithNoInitialOSDs()}, 0, 2, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := options{osds: 2}
			for _, opt := range test.opts {
				if err := opt(&settings); err != nil {
					t.Fatal(err)
				}
			}
			if !settings.poolDefaultsSet {
				settings.poolReplicas, settings.poolMinSize = min(2, settings.osds), 1
			}
			if err := prepareInitialComposition(&settings); err != nil {
				t.Fatal(err)
			}
			if settings.osds != test.wantOSDs || settings.poolReplicas != test.wantSize || settings.poolMinSize != test.wantMin {
				t.Fatalf("initial/prospective defaults changed: count=%d size=%d min=%d", settings.osds, settings.poolReplicas, settings.poolMinSize)
			}
		})
	}
}

func TestNoInitialOSDsCanceledRunDoesNotAllocateOrCustomize(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithNoInitialOSDs()},
		{WithNoInitialOSDs(), WithHostNetwork()},
		{WithNoInitialOSDs(), WithSeparateClusterNetwork(), WithMonitorCount(3), WithManagerCount(2)},
	} {
		var customizers atomic.Int32
		opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cluster, err := Run(ctx, DefaultImage, opts...)
		if cluster != nil || !errors.Is(err, context.Canceled) || customizers.Load() != 0 {
			t.Fatalf("canceled zero-storage constructor allocated/customized: cluster=%v err=%v calls=%d", cluster, err, customizers.Load())
		}
	}
}

func TestNoInitialOSDsPreservesOldNetworkAndPlacementValidation(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithNoInitialOSDs(), WithHostAddress("127.0.0.1")},
		{WithNoInitialOSDs(), WithHostNetwork(), WithSeparateClusterNetwork()},
		{WithOSDCount(1), WithPoolDefaults(2, 1)},
		{WithInitialOSDs(OSDConfig{Root: "storage"})},
	} {
		cluster, err := Run(t.Context(), DefaultImage, opts...)
		if cluster != nil || err == nil {
			t.Fatal("existing invalid network or positive placement reached runtime")
		}
	}
}

func TestNoInitialOSDsDoesNotImplyPGCleanOrPoolProvisioning(t *testing.T) {
	for _, pgs := range []int{0, 8} {
		control := &noInitialOSDControl{pgs: pgs}
		cluster := &Container{Container: control, settings: options{noInitialOSDs: true, startupTimeout: time.Second, poolReplicas: 2, poolMinSize: 1}, osds: map[int]*OSDContainer{}}
		if pool, err := cluster.CreatePool(t.Context(), PoolConfig{Name: "before-storage"}); pool != nil || err == nil || len(control.actions) != 0 {
			t.Fatal("empty storage bypassed existing CreatePool domain preflight")
		}
		if status, err := cluster.Status(t.Context()); err != nil || !status.MgrMap.Available || status.OSDMap.NumOSDs != 0 || status.PGMap.NumPGs != pgs {
			t.Fatalf("zero-storage native status fixture invalid: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		err := cluster.WaitForClean(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || control.statusCalls == 0 {
			t.Fatalf("zero/inactive PGs became clean: pgs=%d calls=%d err=%v", pgs, control.statusCalls, err)
		}
	}
}

func TestNoInitialOSDsFirstAddHonorsOwnerDeadlineAndFreshRetry(t *testing.T) {
	control := &noInitialOSDControl{}
	cluster := &Container{Container: control, settings: options{noInitialOSDs: true, defaultCRUSHRoot: "osd-9", startupTimeout: time.Second}, osds: map[int]*OSDContainer{}}
	noInitialOSDQueuedCall(t, &cluster.mu, func(ctx context.Context) error { _, err := cluster.AddOSD(ctx); return err })
	if len(control.actions) != 0 || len(cluster.osds) != 0 {
		t.Fatal("queued first Add changed native or owned storage")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if osd, err := cluster.AddOSD(ctx); osd != nil || !errors.Is(err, context.Canceled) || len(control.actions) != 0 {
		t.Fatal("canceled first Add made a native attempt")
	}
	// An omitted host is resolved only after native registration. ID 9 collides
	// with the preselected root osd-9, returning the existing partial descriptor
	// before any Docker OSD launch; no test-only production factory is needed.
	osd, err := cluster.AddOSD(t.Context())
	if err == nil || osd == nil || osd.Container != nil || osd.ID != 9 || cluster.osds[9] != osd || osd.Placement().Host != "osd-9" {
		t.Fatalf("fresh first Add lost registered partial identity: osd=%v err=%v", osd, err)
	}
	id, parseErr := uuid.Parse(osd.nativeUUID)
	if parseErr != nil || id == uuid.Nil || !slices.Equal(control.actions, []string{"key", "register"}) || control.copies != 1 {
		t.Fatal("partial first Add did not retain exact UUID/native registration")
	}
	if err := cluster.Terminate(t.Context()); err != nil || len(cluster.osds) != 0 || control.terminations != 1 {
		t.Fatal("whole-cluster cleanup did not dispose of partial zero-storage startup")
	}
}

func TestNoInitialOSDsEmptyAndPartialControlCleanupRemainsRetryable(t *testing.T) {
	mon := &noInitialOSDControl{}
	mgr := &hostNetworkFixtureContainer{failTerminationOnce: true}
	cluster := &Container{Container: mon, manager: mgr, managers: map[string]*ManagerContainer{"a": {Container: mgr, DaemonName: "a"}}, osds: map[int]*OSDContainer{}, settings: options{noInitialOSDs: true}}
	if err := cluster.Terminate(t.Context()); err == nil || !cluster.closed || cluster.manager != mgr || mon.terminations != 1 {
		t.Fatal("failed manager cleanup lost zero-storage resources")
	}
	if err := cluster.Terminate(t.Context()); err != nil || cluster.manager != nil || len(cluster.managers) != 0 || mgr.terminations != 2 || mon.terminations != 1 {
		t.Fatal("retry repeated completed MON cleanup or skipped partial MGR")
	}
	if osd, err := cluster.AddOSD(t.Context()); osd != nil || err == nil || len(mon.actions) != 0 {
		t.Fatal("terminated zero-storage cluster admitted first Add")
	}
}

func noInitialOSDQueuedCall(t *testing.T, gate *sync.Mutex, call func(context.Context) error) {
	t.Helper()
	gate.Lock()
	var once sync.Once
	unlock := func() { once.Do(gate.Unlock) }
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	result := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		cancel()
		unlock()
		if !joined {
			select {
			case <-result:
			case <-time.After(time.Second):
				t.Error("queued first Add did not join after cleanup")
			}
		}
	})
	go func() { result <- call(ctx) }()
	select {
	case err := <-result:
		joined = true
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("owner admission returned %v", err)
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("first Add ignored owner admission deadline")
	}
	unlock()
}

type noInitialOSDControl struct {
	testcontainers.Container
	actions                                []string
	copies, statusCalls, terminations, pgs int
}

func (c *noInitialOSDControl) GetContainerID() string { return "zero-storage-monitor" }
func (c *noInitialOSDControl) CopyToContainer(context.Context, []byte, string, int64) error {
	c.copies++
	return nil
}
func (c *noInitialOSDControl) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminations++
	return nil
}
func (c *noInitialOSDControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Equal(args, []string{"ceph-authtool", "--gen-print-key"}) {
		c.actions = append(c.actions, "key")
		return 0, noInitialOSDStream("synthetic-key"), nil
	}
	if len(args) >= 5 && args[0] == "ceph" && args[3] == "osd" && args[4] == "new" {
		c.actions = append(c.actions, "register")
		return 0, noInitialOSDStream("9"), nil
	}
	if slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}) {
		c.statusCalls++
		states := []map[string]any{}
		if c.pgs != 0 {
			states = append(states, map[string]any{"state_name": "unknown", "count": c.pgs})
		}
		data, _ := json.Marshal(map[string]any{"health": map[string]string{"status": "HEALTH_WARN"}, "mgrmap": map[string]bool{"available": true}, "osdmap": map[string]int{"num_osds": 0, "num_up_osds": 0, "num_in_osds": 0}, "pgmap": map[string]any{"num_pgs": c.pgs, "pgs_by_state": states}})
		return 0, noInitialOSDStream(string(data)), nil
	}
	return 1, noInitialOSDStream("unexpected zero-storage test command"), nil
}

func noInitialOSDStream(output string) io.Reader {
	var result bytes.Buffer
	var header [8]byte
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	result.Write(header[:])
	result.WriteString(output)
	return &result
}
