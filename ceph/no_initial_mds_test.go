package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const coldMDSFixtureFSID = "a011602b-37aa-496b-a8b5-b3e7a5d13ff3"

// This fixture executes the actual constructor and real MDS publication path.
// Its native catalog changes only in response to the emitted CLI commands.
type coldMDSFixture struct {
	testcontainers.Container
	mu       sync.Mutex
	calls    []string
	pools    map[string]int64
	nextPool int64
	native   map[string]any
	auth     string
	fsid     string
	fail     string
	hook     func(string)
	removes  int
}

func newColdMDSFixture() (*Container, *coldMDSFixture) {
	native := &coldMDSFixture{pools: map[string]int64{}, nextPool: 0, fsid: coldMDSFixtureFSID, auth: `{"auth_dump":[{"entity":"client.admin","key":"never-log-this-auth-key"}]}`, native: map[string]any{"standbys": []any{}, "filesystems": []any{}}}
	c := poolFixtureCluster(native, 2)
	c.settings.osds = 2
	c.settings.mdsImage = "selected-mds-image"
	c.services = map[string]testcontainers.Container{}
	return c, native
}
func (n *coldMDSFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	n.mu.Lock()
	command := strings.Join(args[3:], " ")
	n.calls = append(n.calls, command)
	a := args[3:]
	output := ""
	code := 0
	switch {
	case command == "fsid":
		output = n.fsid + "\n"
	case command == "fs dump --format json":
		output = coldMDSTestJSON(n.native)
	case command == "auth ls --format json":
		output = n.auth
	case command == "osd pool ls --format json":
		names := make([]string, 0, len(n.pools))
		for name := range n.pools {
			names = append(names, name)
		}
		slices.Sort(names)
		output = coldMDSTestJSON(names)
	case command == "osd pool ls detail --format json":
		pools := make([]any, 0, len(n.pools))
		for name, id := range n.pools {
			pools = append(pools, map[string]any{"pool_id": id, "pool_name": name, "type": 1, "size": 2, "min_size": 1, "pg_num": 8})
		}
		output = coldMDSTestJSON(pools)
	case command == "osd crush rule ls --format json":
		output = "[]"
	case len(a) > 3 && slices.Equal(a[:3], []string{"osd", "pool", "create"}):
		n.pools[a[3]] = n.nextPool
		n.nextPool++
	case len(a) > 4 && slices.Equal(a[:2], []string{"fs", "new"}):
		n.native["filesystems"] = []any{map[string]any{"id": int64(7), "mdsmap": coldMDSTestMap(a[2], n.pools[a[3]], n.pools[a[4]])}}
	case len(a) > 4 && slices.Equal(a[:2], []string{"fs", "set"}):
		m := n.target()
		switch a[3] {
		case "allow_standby_replay", "refuse_standby_for_another_fs":
			m["flags_state"].(map[string]any)[a[3]] = a[4] == "true"
		case "max_mds":
			m["max_mds"] = 1
		case "standby_count_wanted":
			m["standby_count_wanted"] = 0
		}
	case len(a) == 4 && slices.Equal(a[:2], []string{"fs", "add_data_pool"}):
		m := n.target()
		m["data_pools"] = append(m["data_pools"].([]int64), n.pools[a[3]])
	case len(a) > 2 && slices.Equal(a[:2], []string{"auth", "get-or-create"}):
		output = "[" + a[2] + "]\nkey = never-log-this-generated-key\n"
	}
	if command == n.fail {
		code = 1
		output = "never-log-this-native-secret"
	}
	hook := n.hook
	n.mu.Unlock()
	if hook != nil {
		hook(command)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return code, bytes.NewReader(append(header[:], []byte(output)...)), nil
}
func (n *coldMDSFixture) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	n.removes++
	return nil
}
func (n *coldMDSFixture) target() map[string]any {
	return n.native["filesystems"].([]any)[0].(map[string]any)["mdsmap"].(map[string]any)
}
func (n *coldMDSFixture) mutationCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, call := range n.calls {
		if call != "fsid" && call != "fs dump --format json" && call != "auth ls --format json" && call != "osd pool ls detail --format json" {
			count++
		}
	}
	return count
}
func coldMDSTestJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(data)
}
func coldMDSTestMap(name string, metadata, data int64) map[string]any {
	return map[string]any{"fs_name": name, "max_mds": 1, "standby_count_wanted": 0, "metadata_pool": metadata, "data_pools": []int64{data}, "info": map[string]any{}, "up": map[string]any{}, "in": []int{}, "failed": []int{}, "damaged": []int{}, "stopped": []int{}, "flags_state": map[string]any{"joinable": true, "allow_standby_replay": false, "refuse_standby_for_another_fs": true}}
}
func coldMDSTestRow(name, state string, gid uint64, rank int, join int64) map[string]any {
	return map[string]any{"name": name, "state": state, "gid": gid, "rank": rank, "join_fscid": join}
}
func coldMDSTestConstruct(t *testing.T, extra bool) (*CephFSContainer, *coldMDSFixture) {
	t.Helper()
	c, n := newColdMDSFixture()
	config := CephFSConfig{Name: "cold-target", NoInitialMDS: true}
	if extra {
		config.AdditionalDataPools = []PoolConfig{{Name: "extra"}}
	}
	fs, err := c.StartCephFSWithConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("cold constructor: %v", err)
	}
	return fs, n
}

func TestNoInitialMDSDefaultsAndContradictionsBeforeNative(t *testing.T) {
	ordinary, err := normalizeCephFSConfig(CephFSConfig{})
	if err != nil || ordinary.ActiveMDS != 1 || ordinary.StandbyMDS != 0 || ordinary.NoInitialMDS {
		t.Fatalf("ordinary defaults changed: %+v %v", ordinary, err)
	}
	for _, config := range []CephFSConfig{{NoInitialMDS: true, ActiveMDS: 1}, {NoInitialMDS: true, ActiveMDS: 2}, {NoInitialMDS: true, ActiveMDS: -1}, {NoInitialMDS: true, StandbyMDS: 1}, {NoInitialMDS: true, StandbyMDS: -1}, {NoInitialMDS: true, StandbyReplay: true}, {ActiveMDS: -1}, {StandbyMDS: -1}} {
		c, n := newColdMDSFixture()
		customizers := 0
		fs, err := c.StartCephFSWithConfig(t.Context(), config, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers++; return nil }))
		if fs != nil || err == nil || len(n.calls) != 0 || customizers != 0 || len(c.filesystems) != 0 {
			t.Fatalf("invalid config reached native allocation: %+v %v", config, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cluster, runErr := Run(ctx, DefaultImage, WithCephFS(config))
		if cluster != nil || runErr == nil || errors.Is(runErr, context.Canceled) {
			t.Fatalf("initial config reached runtime: %+v %v", config, runErr)
		}
	}
	for _, zero := range []Option{WithNoInitialOSDs(), WithNoInitialManagers()} {
		for _, first := range []bool{false, true} {
			opts := []testcontainers.ContainerCustomizer{zero, WithCephFS(CephFSConfig{NoInitialMDS: true})}
			if first {
				opts[0], opts[1] = opts[1], opts[0]
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			c, err := Run(ctx, DefaultImage, opts...)
			if c != nil || err == nil || errors.Is(err, context.Canceled) {
				t.Fatalf("cold FS waived existing OSD/MGR composition guard: %v", err)
			}
		}
	}
}

func TestNoInitialMDSCompositionAndRuntimeKeepOmittedCounts(t *testing.T) {
	config := CephFSConfig{Name: "cold-target", NoInitialMDS: true, AdditionalDataPools: []PoolConfig{{Name: "extra"}}}
	settings := options{osds: 2, poolReplicas: 2, poolMinSize: 1}
	if err := WithCephFS(config)(&settings); err != nil {
		t.Fatal(err)
	}
	config.AdditionalDataPools[0].Name = "caller-mutated"
	if err := prepareInitialComposition(&settings); err != nil {
		t.Fatal(err)
	}
	retained := settings.filesystems[0]
	if retained.ActiveMDS != 0 || !retained.NoInitialMDS || retained.AdditionalDataPools[0].Name != "extra" {
		t.Fatalf("initial normalized cold request cannot be readmitted: %+v", retained)
	}
	c, n := newColdMDSFixture()
	customizers := 0
	fs, err := c.StartCephFSWithConfig(t.Context(), retained, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers++; return nil }))
	if err != nil || fs == nil || fs.Container != nil || len(fs.MDSs()) != 0 || len(c.services) != 0 || customizers != 0 || fs.config.ActiveMDS != 1 || fs.coldMDS == nil || fs.coldMDS.attempted || fs.coldMDS.fsid != coldMDSFixtureFSID || fs.coldMDS.pools["cold-target-metadata"] != 0 || !slices.Equal(fs.coldMDS.dataPools, []int64{1, 2}) || c.settings.mdsImage != "selected-mds-image" {
		t.Fatalf("cold constructor did not retain exact zero topology/pool0: fs=%+v err=%v customizers=%d", fs, err, customizers)
	}
	if got := c.Filesystems(); len(got) != 1 || got[0] != fs {
		t.Fatal("successful cold descriptor is not owned")
	}
	for _, call := range n.calls {
		if strings.HasPrefix(call, "auth get-or-create") {
			t.Fatal("cold constructor created MDS auth", call)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := fs.WaitReady(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cold FS claimed metadata availability: %v", err)
	}
}

func TestNoInitialMDSPartialConstructionNeverConfirmsCold(t *testing.T) {
	for _, point := range []string{"fs set cold-target refuse_standby_for_another_fs true", "auth ls --format json", "cancel-final-auth", "drift-final-auth"} {
		t.Run(point, func(t *testing.T) {
			c, n := newColdMDSFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if point == "cancel-final-auth" {
				n.hook = func(cmd string) {
					if cmd == "auth ls --format json" {
						cancel()
					}
				}
			} else if point == "drift-final-auth" {
				n.hook = func(cmd string) {
					if cmd == "auth ls --format json" {
						n.target()["max_mds"] = 2
					}
				}
			} else {
				n.fail = point
			}
			fs, err := c.StartCephFSWithConfig(ctx, CephFSConfig{Name: "cold-target", NoInitialMDS: true})
			if fs == nil || err == nil || fs.coldMDS != nil || fs.Container != nil || len(fs.MDSs()) != 0 || c.filesystems["cold-target"] != fs {
				t.Fatalf("partial setup was not retained or adopted cold: fs=%v err=%v", fs, err)
			}
			before := n.mutationCount()
			if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || n.mutationCount() != before {
				t.Fatalf("partial constructor gained first-start permission: %v", err)
			}
		})
	}
}

func TestNoInitialMDSColdScopeGuardsAndForeignStandby(t *testing.T) {
	for _, fault := range []string{"fsid", "metadata", "default", "additional-id", "extra-attachment", "reordered", "max", "wanted", "replay", "refusal", "joinable", "joinable-missing", "joinable-null", "info", "up", "in", "failed", "damaged", "stopped", "target-standby", "unqualified-standby", "unknown-join", "reserved-foreign", "malformed-state", "reserved-auth", "auth-null", "auth-entity-null", "auth-entity-empty", "auth-duplicate", "auth-trailing"} {
		t.Run(fault, func(t *testing.T) {
			fs, n := coldMDSTestConstruct(t, true)
			m := n.target()
			row := coldMDSTestRow("foreign", "up:standby", 30, -1, 9)
			switch fault {
			case "fsid":
				n.fsid = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
			case "metadata":
				m["metadata_pool"] = int64(90)
			case "default":
				m["data_pools"] = []int64{90, 2}
			case "additional-id":
				n.pools["extra"] = 90
			case "extra-attachment":
				m["data_pools"] = []int64{1, 2, 3}
			case "reordered":
				m["data_pools"] = []int64{2, 1}
			case "max":
				m["max_mds"] = 2
			case "wanted":
				m["standby_count_wanted"] = 1
			case "replay":
				m["flags_state"].(map[string]any)["allow_standby_replay"] = true
			case "refusal":
				m["flags_state"].(map[string]any)["refuse_standby_for_another_fs"] = false
			case "joinable":
				m["flags_state"].(map[string]any)["joinable"] = false
			case "joinable-missing":
				delete(m["flags_state"].(map[string]any), "joinable")
			case "joinable-null":
				m["flags_state"].(map[string]any)["joinable"] = nil
			case "info":
				m["info"] = map[string]any{"gid_30": coldMDSTestRow("foreign", "up:active", 30, 0, 7)}
			case "up":
				m["up"] = map[string]any{"mds_0": 30}
			case "in", "failed", "damaged", "stopped":
				m[fault] = []int{0}
			case "target-standby":
				row["join_fscid"] = 7
				n.native["standbys"] = []any{row}
			case "unqualified-standby":
				row["join_fscid"] = -1
				n.native["standbys"] = []any{row}
			case "unknown-join":
				n.native["standbys"] = []any{row}
			case "reserved-foreign", "malformed-state":
				other := coldMDSTestMap("other", 30, 31)
				r := coldMDSTestRow("cold-target-0", "up:active", 30, 0, 9)
				if fault == "malformed-state" {
					r["name"] = "other-0"
					r["state"] = "up:invented"
				}
				other["info"] = map[string]any{"gid_30": r}
				n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 9, "mdsmap": other})
			case "reserved-auth":
				n.auth = `{"auth_dump":[{"entity":"mds.cold-target-0","key":"never-log-this-auth-key"}]}`
			case "auth-null":
				n.auth = `{"auth_dump":null}`
			case "auth-entity-null":
				n.auth = `{"auth_dump":[{"entity":null}]}`
			case "auth-entity-empty":
				n.auth = `{"auth_dump":[{"entity":""}]}`
			case "auth-duplicate":
				n.auth = `{"auth_dump":[{"entity":"client.admin"},{"entity":"client.admin"}]}`
			case "auth-trailing":
				n.auth = `{"auth_dump":[]} {"key":"never-log-this-auth-key"}`
			}
			before := n.mutationCount()
			err := fs.ScaleMDS(t.Context(), 1, 0)
			if err == nil || n.mutationCount() != before || fs.coldMDS.attempted || fs.Container != nil || strings.Contains(err.Error(), "never-log") {
				t.Fatalf("scope guard mutated/consumed/leaked or accepted drift: %v", err)
			}
		})
	}
	// An explicitly affiliated sibling is protected by the target refusal flag.
	fs, n := coldMDSTestConstruct(t, false)
	other := coldMDSTestMap("_other", 30, 31)
	other["info"] = map[string]any{"gid_40": coldMDSTestRow("_other-0", "up:active", 40, 0, 9)}
	n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 9, "mdsmap": other})
	n.native["standbys"] = []any{coldMDSTestRow("_other-1", "up:standby", 41, -1, 9)}
	if err := fs.verifyColdMDS(t.Context(), fs.coldMDS); err != nil {
		t.Fatalf("protected independent sibling rejected: %v", err)
	}
}

func TestNoInitialMDSStrictNativeAbsenceSchema(t *testing.T) {
	fs, n := coldMDSTestConstruct(t, false)
	good := coldMDSTestJSON(n.native)
	for _, field := range []string{"up", "in", "info", "failed", "damaged", "stopped", "flags_state", "standby_count_wanted", "max_mds"} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missing=%v", field, missing), func(t *testing.T) {
				var native map[string]any
				_ = json.Unmarshal([]byte(good), &native)
				m := native["filesystems"].([]any)[0].(map[string]any)["mdsmap"].(map[string]any)
				if missing {
					delete(m, field)
				} else {
					m[field] = nil
				}
				if err := validateColdMDSMap([]byte(coldMDSTestJSON(native)), fs.config.Name, fs.coldMDS, "cold-target-0"); err == nil {
					t.Fatal("missing/null absence authority accepted")
				}
			})
		}
	}
	for _, bad := range []string{good + " {}", `{"standbys":[],"standbys":[],"filesystems":[]}`, "null", "[]", `{"standbys":null,"filesystems":[]}`, `{"standbys":[],"filesystems":null}`, "never-log-this-native-secret"} {
		if err := validateColdMDSMap([]byte(bad), fs.config.Name, fs.coldMDS, "cold-target-0"); err == nil || strings.Contains(err.Error(), "never-log") {
			t.Fatalf("malformed map accepted or leaked: %v", err)
		}
	}
}

func TestNoInitialMDSFirstScaleCountsPreservePermission(t *testing.T) {
	fs, n := coldMDSTestConstruct(t, false)
	before := n.mutationCount()
	for _, counts := range [][2]int{{0, 0}, {-1, 0}, {1, -1}, {2, 0}, {1, 1}, {256, 0}} {
		if err := fs.ScaleMDS(t.Context(), counts[0], counts[1]); err == nil || n.mutationCount() != before || fs.coldMDS.attempted || fs.nextMDSIndex != 0 {
			t.Fatalf("unsupported first scale consumed or mutated: %v %v", counts, err)
		}
	}
	n.fail = "auth get-or-create mds.cold-target-0 mon allow profile mds mgr allow profile mds osd allow rw tag cephfs *=* mds allow"
	if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || !fs.coldMDS.attempted || fs.nextMDSIndex != 1 || n.mutationCount() != before+1 || strings.Contains(err.Error(), "never-log") {
		t.Fatalf("first native auth failure was retriable/leaked: %v", err)
	}
	before = n.mutationCount()
	if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || n.mutationCount() != before || fs.Container != nil {
		t.Fatalf("uncertain first auth attempt silently retried: %v", err)
	}
}

// Cleanup is installed before launch; cancel/unlock/join are idempotent even if
// an admission regression stalls beyond the short caller deadline.
func coldMDSTestGate(t *testing.T, lock, unlock func(), operation func(context.Context) error) {
	t.Helper()
	lock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	result, exited := make(chan error, 1), make(chan struct{})
	var release sync.Once
	cleanup := func() {
		cancel()
		release.Do(unlock)
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("cold MDS admission worker did not exit after release")
		}
	}
	t.Cleanup(cleanup)
	go func() { defer close(exited); result <- operation(ctx) }()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("held gate lost explicit deadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		cleanup()
		t.Fatal("held gate ignored caller deadline")
	}
	cleanup()
}
func TestNoInitialMDSRealAdmissionGatesAndCancellation(t *testing.T) {
	for _, method := range []string{"constructor", "scale", "starter"} {
		for _, gate := range []string{"setup", "owner", "control"} {
			if method == "starter" && gate == "setup" {
				continue
			}
			t.Run(method+"/"+gate, func(t *testing.T) {
				c, n := newColdMDSFixture()
				var fs *CephFSContainer
				if method != "constructor" {
					var err error
					fs, err = c.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "cold-target", NoInitialMDS: true})
					if err != nil {
						t.Fatal(err)
					}
				}
				before := n.mutationCount()
				beforeQueries := len(n.calls)
				operation := func(ctx context.Context) error {
					switch method {
					case "constructor":
						_, err := c.StartCephFSWithConfig(ctx, CephFSConfig{Name: "cold-target", NoInitialMDS: true})
						return err
					case "scale":
						return fs.ScaleMDS(ctx, 1, 0)
					default:
						return fs.startMDSWithService(ctx, func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
							t.Error("blocked starter reached service launch")
							return nil, nil
						})
					}
				}
				switch gate {
				case "setup":
					coldMDSTestGate(t, c.cephfsSetupMu.Lock, c.cephfsSetupMu.Unlock, operation)
				case "owner":
					coldMDSTestGate(t, c.mu.Lock, c.mu.Unlock, operation)
				case "control":
					coldMDSTestGate(t, c.controlMu.Lock, c.controlMu.Unlock, operation)
				}
				if n.mutationCount() != before || len(n.calls) != beforeQueries || fs != nil && (fs.coldMDS.attempted || fs.nextMDSIndex != 0) {
					t.Fatal("failed admission queried, mutated or consumed first permission")
				}
				assertCephFSContextAdmissionReleased(t, c)
			})
		}
	}
	for _, method := range []string{"constructor", "scale"} {
		c, n := newColdMDSFixture()
		var fs *CephFSContainer
		if method == "scale" {
			fs, _ = c.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "cold-target", NoInitialMDS: true})
		}
		before := len(n.calls)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var err error
		if method == "scale" {
			err = fs.ScaleMDS(ctx, 1, 0)
		} else {
			fs, err = c.StartCephFSWithConfig(ctx, CephFSConfig{Name: "cold-target", NoInitialMDS: true})
		}
		if err == nil || !errors.Is(err, context.Canceled) || len(n.calls) != before || fs != nil && fs.coldMDS != nil && fs.coldMDS.attempted {
			t.Fatalf("canceled admission changed state: %v", err)
		}
	}
}

func coldMDSTestScaleWithService(ctx context.Context, fs *CephFSContainer, start func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)) error {
	if err := lockTopologyMutex(ctx, &fs.cluster.cephfsSetupMu); err != nil {
		return err
	}
	defer fs.cluster.cephfsSetupMu.Unlock()
	return fs.scaleColdMDS(ctx, 1, 0, func(ctx context.Context) error { return fs.startMDSWithService(ctx, start) })
}
func coldMDSTestService(t *testing.T, fs *CephFSContainer, n *coldMDSFixture, worker testcontainers.Container, serviceErr error, active bool) func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	t.Helper()
	return func(_ context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		if name != "mds.cold-target-0" || image != "selected-mds-image" {
			t.Fatalf("first launch changed identity/image: %s %s", name, image)
		}
		request := testcontainers.GenericContainerRequest{}
		for _, opt := range opts {
			if err := opt.Customize(&request); err != nil {
				t.Fatal(err)
			}
		}
		if request.Env["CEPH_MDS_ID"] != "cold-target-0" || request.Env["CEPH_FILESYSTEM"] != "cold-target" || !slices.Equal(request.Entrypoint, []string{"/bin/sh", "/tc/mds.sh"}) || request.WaitingFor == nil {
			t.Fatal("existing MDS runtime contract/customizers lost")
		}
		fs.cluster.mu.Lock()
		if worker != nil {
			fs.cluster.services[name] = worker
		}
		fs.cluster.mu.Unlock()
		if active {
			m := n.target()
			m["info"] = map[string]any{"gid_40": coldMDSTestRow("cold-target-0", "up:active", 40, 0, 7)}
			m["up"] = map[string]any{"mds_0": 40}
			m["in"] = []int{0}
		}
		return worker, serviceErr
	}
}

func TestNoInitialMDSFirstPublicationAndOrdinaryScaling(t *testing.T) {
	c, n := newColdMDSFixture()
	customizers := 0
	fs, err := c.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "cold-target", NoInitialMDS: true, AdditionalDataPools: []PoolConfig{{Name: "extra"}}}, testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
		customizers++
		req.Env["CALLER_CUSTOMIZER"] = "kept"
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	identity := fs.nativeIdentity
	beforeData := slices.Clone(fs.coldMDS.dataPools)
	worker := &mdsScaleFixtureContainer{id: "original-first-mds"}
	if err := coldMDSTestScaleWithService(t.Context(), fs, coldMDSTestService(t, fs, n, worker, nil, true)); err != nil {
		t.Fatalf("first provision: %v", err)
	}
	if customizers != 1 || fs.Container != worker || len(fs.MDSs()) != 1 || fs.MDSs()[0].ID != "cold-target-0" || c.services["mds.cold-target-0"] != worker || !fs.coldMDS.attempted || fs.nativeIdentity != identity || fs.nextMDSIndex != 1 || !slices.Equal(n.target()["data_pools"].([]int64), beforeData) {
		t.Fatal("first native daemon did not retain original resources/publication/customizer")
	}
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil {
		t.Fatalf("initialized cold fixture did not enter ordinary scaling: %v", err)
	}
	if customizers != 1 || fs.Container != worker || fs.nextMDSIndex != 1 {
		t.Fatal("ordinary same-count scale launched replacement")
	}
	c.osds = nil
	if err := c.Terminate(t.Context()); err != nil || worker.removes != 1 || len(c.services) != 0 {
		t.Fatalf("first owned MDS cleanup: %v removes=%d", err, worker.removes)
	}
}

func TestNoInitialMDSPartialDaemonOwnershipAndNoImplicitRetry(t *testing.T) {
	for _, withHandle := range []bool{false, true} {
		t.Run(fmt.Sprintf("handle=%v", withHandle), func(t *testing.T) {
			fs, n := coldMDSTestConstruct(t, false)
			worker := &mdsScaleFixtureContainer{id: "partial-first-mds"}
			var handle testcontainers.Container
			if withHandle {
				handle = worker
			}
			err := coldMDSTestScaleWithService(t.Context(), fs, coldMDSTestService(t, fs, n, handle, errors.New("never-log-this-service-secret"), false))
			if err == nil || strings.Contains(err.Error(), "never-log") || !fs.coldMDS.attempted || fs.nextMDSIndex != 1 {
				t.Fatalf("partial daemon failure consumed wrongly/leaked: %v", err)
			}
			if withHandle {
				if fs.Container != worker || len(fs.MDSs()) != 1 || fs.cluster.services["mds.cold-target-0"] != worker {
					t.Fatal("nonnil partial handle was lost")
				}
			} else if fs.Container != nil || len(fs.MDSs()) != 0 || len(fs.cluster.services) != 0 {
				t.Fatal("nil partial gained owned resource")
			}
			before := n.mutationCount()
			if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || n.mutationCount() != before {
				t.Fatalf("partial first daemon was automatically retried: %v", err)
			}
			fs.cluster.osds = nil
			if err := fs.cluster.Terminate(t.Context()); err != nil || len(fs.cluster.services) != 0 || worker.removes != btoi(withHandle) {
				t.Fatalf("partial cleanup lost handle: %v removes=%d", err, worker.removes)
			}
		})
	}
}

func TestNoInitialMDSFinalPoolGuardRetainsAttemptedDaemon(t *testing.T) {
	fs, n := coldMDSTestConstruct(t, true)
	worker := &mdsScaleFixtureContainer{id: "started-before-pool-drift"}
	start := coldMDSTestService(t, fs, n, worker, nil, true)
	err := coldMDSTestScaleWithService(t.Context(), fs, func(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		ctr, err := start(ctx, name, image, opts...)
		n.pools["extra"] = 99
		return ctr, err
	})
	if err == nil || fs.Container != worker || len(fs.MDSs()) != 1 || !fs.coldMDS.attempted || fs.cluster.services["mds.cold-target-0"] != worker {
		t.Fatalf("late pool replacement was adopted or lost cleanup ownership: %v", err)
	}
}

func TestNoInitialMDSSelectedControlAndLateAdmissionPreserveAuthority(t *testing.T) {
	t.Run("legitimate owned CLI switch", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		other := &coldMDSFixture{pools: n.pools, native: n.native, auth: n.auth, fsid: n.fsid}
		fs.cluster.controlPlane = other
		if err := fs.verifyColdMDS(t.Context(), fs.coldMDS); err != nil || len(other.calls) == 0 {
			t.Fatalf("stable cluster behind changed CLI was rejected: %v", err)
		}
	})
	t.Run("foreign exported primary", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		foreign := &coldMDSFixture{pools: n.pools, native: n.native, auth: n.auth, fsid: "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"}
		fs.cluster.Container = foreign
		if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || foreign.mutationCount() != 0 || fs.coldMDS.attempted {
			t.Fatalf("foreign same-named FS/pool was adopted: %v", err)
		}
	})
	t.Run("selected control drift", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		n.fsid = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
		before := n.mutationCount()
		err := fs.startMDSWithService(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			t.Error("foreign selected control started daemon")
			return nil, nil
		})
		if err == nil || fs.coldMDS.attempted || fs.nextMDSIndex != 0 || n.mutationCount() != before {
			t.Fatalf("selected control guard failed: %v", err)
		}
	})
	t.Run("late readonly cancellation", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		n.hook = func(cmd string) {
			if cmd == "auth ls --format json" {
				cancel()
			}
		}
		before := n.mutationCount()
		err := fs.ScaleMDS(ctx, 1, 0)
		if err == nil || !errors.Is(err, context.Canceled) || n.mutationCount() != before || fs.coldMDS.attempted {
			t.Fatalf("late read-only cancel consumed first permission: %v", err)
		}
	})
}

func TestNoInitialMDSActualAuthAndPostLaunchCancellationAreCanonical(t *testing.T) {
	t.Run("auth context cause", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		n.hook = func(cmd string) {
			if strings.HasPrefix(cmd, "auth get-or-create") {
				cancel()
			}
		}
		err := fs.ScaleMDS(ctx, 1, 0)
		if err == nil || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "never-log") || !fs.coldMDS.attempted {
			t.Fatalf("actual auth cancellation lost attempt/cause or leaked: %v", err)
		}
	})
	t.Run("post-launch retained", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		worker := &mdsScaleFixtureContainer{id: "partial-canceled"}
		start := coldMDSTestService(t, fs, n, worker, nil, true)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		err := coldMDSTestScaleWithService(ctx, fs, func(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			ctr, err := start(ctx, name, image, opts...)
			cancel()
			return ctr, err
		})
		if err == nil || !errors.Is(err, context.Canceled) || fs.Container != worker || len(fs.MDSs()) != 1 || !fs.coldMDS.attempted {
			t.Fatalf("post-launch cancellation lost cleanup ownership/cause: %v", err)
		}
	})
}

func TestNoInitialMDSConstructorPinsClusterBeforeMutations(t *testing.T) {
	c, n := newColdMDSFixture()
	n.hook = func(cmd string) {
		if strings.HasPrefix(cmd, "fs new ") {
			n.fsid = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
		}
	}
	fs, err := c.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "cold-target", NoInitialMDS: true})
	if err == nil || fs == nil || fs.coldMDS != nil || c.filesystems["cold-target"] != fs {
		t.Fatalf("constructor adopted a switched cluster after mutation: %v", err)
	}
	before := n.mutationCount()
	if err := fs.ScaleMDS(t.Context(), 1, 0); err == nil || n.mutationCount() != before {
		t.Fatalf("changed cluster descriptor gained first MDS authority: %v", err)
	}
}

func TestNoInitialMDSOrdinaryStarterKeepsLegacyPartialError(t *testing.T) {
	fs, n := coldMDSTestConstruct(t, false)
	fs.coldMDS = nil
	worker := &mdsScaleFixtureContainer{id: "legacy-partial"}
	cause := errors.New("ordinary native service cause")
	err := fs.startMDSWithService(t.Context(), coldMDSTestService(t, fs, n, worker, cause, false))
	if !errors.Is(err, cause) || fs.nextMDSIndex != 1 || fs.Container != worker || len(fs.MDSs()) != 1 || fs.cluster.services["mds.cold-target-0"] != worker {
		t.Fatalf("ordinary start contract changed under private service seam: %v", err)
	}
}
