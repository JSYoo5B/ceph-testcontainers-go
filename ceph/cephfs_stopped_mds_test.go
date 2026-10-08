package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type stoppedMDSControl struct {
	testcontainers.Container
	native, quorum map[string]any
	pools          []any
	calls          []string
	raw, fail      string
	hook           func(string)
	terminated     int
}

func (n *stoppedMDSControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	call := strings.Join(monitorQuorumTestModuleArgs(args), " ")
	n.calls = append(n.calls, call)
	var output string
	switch {
	case call == "quorum_status --format json":
		output = coldMDSTestJSON(n.quorum)
	case call == "fs dump --format json":
		output = coldMDSTestJSON(n.native)
		if n.raw != "" {
			output = n.raw
		}
	case call == "osd pool ls detail --format json":
		output = coldMDSTestJSON(n.pools)
	case strings.HasPrefix(call, "auth get-or-create mds."):
		output = "key = never-log-stopped-mds-auth\n"
	case strings.HasPrefix(call, "fs set target standby_count_wanted "):
		var value int
		fmt.Sscanf(strings.TrimPrefix(call, "fs set target standby_count_wanted "), "%d", &value)
		n.target()["standby_count_wanted"] = value
	default:
		return 0, nil, fmt.Errorf("unexpected stopped-MDS fixture command: %s", call)
	}
	if n.hook != nil {
		n.hook(call)
	}
	code := 0
	if n.fail == call {
		code, output = 1, "never-log-stopped-mds-native-secret"
	}
	if monitorQuorumTestCommand(args) {
		return monitorQuorumTestReader(args, code, []byte(output))
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return code, bytes.NewReader(append(header[:], []byte(output)...)), nil
}
func (n *stoppedMDSControl) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	n.terminated++
	return nil
}
func (n *stoppedMDSControl) target() map[string]any {
	return n.native["filesystems"].([]any)[0].(map[string]any)["mdsmap"].(map[string]any)
}
func (n *stoppedMDSControl) readsOnly(t *testing.T) {
	t.Helper()
	for _, call := range n.calls {
		if !slices.Contains([]string{"quorum_status --format json", "fs dump --format json", "osd pool ls detail --format json"}, call) {
			t.Fatal("retirement sent a native mutation or auth command", call)
		}
	}
}

type stoppedMDSDaemon struct {
	testcontainers.Container
	cid, returnedCID    string
	state               container.State
	inspectErr, termErr error
	missing, nilInfo    bool
	removeOnError       bool
	inspects, removes   int
	inspectHook         func(int)
	termHook            func()
}

func (d *stoppedMDSDaemon) GetContainerID() string { return d.cid }
func (d *stoppedMDSDaemon) State(ctx context.Context) (*container.State, error) {
	info, err := d.Inspect(ctx)
	if err != nil || info == nil {
		return nil, err
	}
	return info.State, nil
}
func (d *stoppedMDSDaemon) Inspect(ctx context.Context) (*container.InspectResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.inspects++
	if d.inspectHook != nil {
		d.inspectHook(d.inspects)
	}
	if d.inspectErr != nil {
		return nil, d.inspectErr
	}
	if d.missing {
		return nil, errdefs.ErrNotFound
	}
	if d.nilInfo {
		return nil, nil
	}
	state, cid := d.state, d.cid
	if d.returnedCID != "" {
		cid = d.returnedCID
	}
	return &container.InspectResponse{ID: cid, State: &state}, nil
}
func (d *stoppedMDSDaemon) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.removes++
	if d.termErr == nil || d.removeOnError {
		d.missing = true
	}
	if d.termHook != nil {
		d.termHook()
	}
	return d.termErr
}

// Native pool identity is captured by the real helper and both daemons are
// published by the real starter. The callback supplies only a fake Run result
// and the same service registry publication that startService owns in Docker.
func stoppedMDSFixture(t *testing.T) (*CephFSContainer, *stoppedMDSControl, *MDSContainer, *stoppedMDSDaemon, *stoppedMDSDaemon) {
	t.Helper()
	n := &stoppedMDSControl{native: map[string]any{"standbys": []any{}, "filesystems": []any{map[string]any{"id": 7, "mdsmap": coldMDSTestMap("target", 0, 1)}}}, quorum: map[string]any{"quorum_names": []string{"a"}, "monmap": map[string]any{"fsid": coldMDSFixtureFSID, "mons": []any{map[string]any{"name": "a", "public_addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "127.0.0.1:3300/0"}}}}}}}}
	for id, name := range []string{"target-metadata", "target-data", "extra"} {
		n.pools = append(n.pools, map[string]any{"pool_id": id, "pool_name": name, "type": 1, "size": 2, "min_size": 1, "pg_num": 8})
	}
	c := poolFixtureCluster(n, 2)
	c.services = map[string]testcontainers.Container{}
	c.settings.startupTimeout = time.Second
	c.config = []byte("[global]\nfsid = " + coldMDSFixtureFSID + "\nmon_host = [v2:127.0.0.1:3300]\n")
	c.keyring = []byte("private-confirmed-keyring")
	config, err := normalizeCephFSConfig(CephFSConfig{Name: "target", StandbyMDS: 1})
	if err != nil {
		t.Fatal(err)
	}
	fs := &CephFSContainer{cluster: c, config: config, FilesystemName: config.Name, MetadataPool: config.MetadataPool.Name, DataPool: config.DataPool.Name}
	c.filesystems = map[string]*CephFSContainer{"target": fs}
	if err := fs.captureNativePoolIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	first := &stoppedMDSDaemon{cid: strings.Repeat("a", 64), state: container.State{Status: "exited"}}
	second := &stoppedMDSDaemon{cid: strings.Repeat("b", 64), state: container.State{Status: "running", Running: true, Pid: 9}}
	stoppedMDSPublish(t, fs, first, nil)
	stoppedMDSPublish(t, fs, second, nil)
	m := n.target()
	m["standby_count_wanted"] = 1
	m["info"] = map[string]any{"gid_12": coldMDSTestRow("target-1", "up:active", 12, 0, 7)}
	m["up"], m["in"] = map[string]any{"mds_0": uint64(12)}, []int{0}
	n.calls = nil
	return fs, n, fs.mdss[0], first, second
}

func stoppedMDSPublish(t *testing.T, fs *CephFSContainer, daemon *stoppedMDSDaemon, startErr error) {
	t.Helper()
	err := fs.startMDSWithService(t.Context(), func(ctx context.Context, name, _ string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		if daemon != nil {
			if err := fs.cluster.lockTopology(ctx); err != nil {
				return nil, err
			}
			fs.cluster.services[name] = daemon
			fs.cluster.mu.Unlock()
		}
		return daemon, startErr
	})
	if !errors.Is(err, startErr) || (startErr == nil && err != nil) {
		t.Fatalf("real MDS publication changed starter receipt: %v", err)
	}
}

func TestStoppedMDSPublicationRetainsOriginalAndPartialIdentity(t *testing.T) {
	for _, kind := range []string{"success", "partial", "invalid-cid", "unconfirmed-bootstrap"} {
		t.Run(kind, func(t *testing.T) {
			fs, _, _, _, _ := stoppedMDSFixture(t)
			cid := strings.Repeat("c", 64)
			var cause error
			if kind == "partial" {
				cause = errors.New("starter failed")
			}
			if kind == "invalid-cid" {
				cid = "short-cid"
			}
			if kind == "unconfirmed-bootstrap" {
				fs.cluster.config = nil
			}
			d := &stoppedMDSDaemon{cid: cid}
			stoppedMDSPublish(t, fs, d, cause)
			owned := fs.mdss[2]
			i := owned.identity
			if i == nil || i.descriptor != owned || i.container != d || i.cid != cid || i.name != "target-2" || i.native != fs.nativeIdentity || i.filesystem != fs || i.confirmed != (kind == "success") || fs.cluster.services["mds.target-2"] != d || fs.nextMDSIndex != 3 {
				t.Fatal("actual publication lost original or partial ownership")
			}
		})
	}
}

func TestStoppedMDSNilAndCanceledPublicationPreservesOldReceipt(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			fs, _, _, _, _ := stoppedMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			daemon := &stoppedMDSDaemon{cid: strings.Repeat("c", 64)}
			err := fs.startMDSWithService(ctx, func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				if !canceled {
					return nil, nil
				}
				fs.cluster.services["mds.target-2"] = daemon
				cancel()
				return daemon, nil
			})
			if err != nil || fs.nextMDSIndex != 3 {
				t.Fatalf("additive metadata changed old starter receipt: %v", err)
			}
			if !canceled && len(fs.mdss) != 2 {
				t.Fatal("nil starter gained descriptor")
			}
			if canceled && (len(fs.mdss) != 3 || fs.mdss[2].Container != daemon || fs.mdss[2].identity.confirmed || fs.cluster.services["mds.target-2"] != daemon) {
				t.Fatal("canceled metadata capture lost partial owned handle or adopted authority")
			}
		})
	}
}

func TestStoppedMDSRemovalPreservesPolicyAuthAndOriginalSurvivor(t *testing.T) {
	fs, native, handle, old, survivor := stoppedMDSFixture(t)
	before := coldMDSTestJSON(native.native)
	desired, identity, next := fs.config, fs.nativeIdentity, fs.nextMDSIndex
	copy := *handle
	if err := fs.RemoveStoppedMDS(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	if old.removes != 1 || !old.missing || survivor.removes != 0 || handle.identity != copy.identity || !handle.identity.removed || !handle.identity.removalAttempted || len(fs.MDSs()) != 1 || fs.mdss[0].Container != survivor || fs.Container != survivor || fs.cluster.services["mds.target-0"] != nil || fs.cluster.services["mds.target-1"] != survivor || !reflect.DeepEqual(fs.config, desired) || fs.nativeIdentity != identity || fs.nextMDSIndex != next || before != coldMDSTestJSON(native.native) {
		t.Fatal("retirement changed policy/native auth/survivor or lost exact ownership")
	}
	native.readsOnly(t)
	if err := fs.RemoveStoppedMDS(t.Context(), handle); err != nil || old.removes != 1 {
		t.Fatalf("completed original handle repeated removal: %v", err)
	}
	// Existing Scale accepts the healthy retained active while explicitly
	// reducing desired standby to zero; no new Run or fake Scale seam is used.
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil || fs.config.StandbyMDS != 0 {
		t.Fatalf("existing ordinary Scale could not use remaining active: %v", err)
	}
}

func TestStoppedMDSRemovalPreservesEveryDesiredActiveRank(t *testing.T) {
	fs, native, target, old, first := stoppedMDSFixture(t)
	second := &stoppedMDSDaemon{cid: strings.Repeat("c", 64), state: container.State{Status: "running", Running: true, Pid: 10}}
	stoppedMDSPublish(t, fs, second, nil)
	fs.config.ActiveMDS = 2
	m := native.target()
	m["max_mds"] = 2
	m["info"].(map[string]any)["gid_13"] = coldMDSTestRow("target-2", "up:active", 13, 1, 7)
	m["up"], m["in"] = map[string]any{"mds_0": uint64(12), "mds_1": uint64(13)}, []int{0, 1}
	native.calls = nil
	before, desired, identity, next := coldMDSTestJSON(native.native), fs.config, fs.nativeIdentity, fs.nextMDSIndex
	if err := fs.RemoveStoppedMDS(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if old.removes != 1 || !old.missing || first.removes != 0 || second.removes != 0 || first.inspects == 0 || second.inspects == 0 || len(fs.MDSs()) != 2 || fs.mdss[0].Container != first || fs.mdss[1].Container != second || fs.Container != first || fs.cluster.services["mds.target-0"] != nil || fs.cluster.services["mds.target-1"] != first || fs.cluster.services["mds.target-2"] != second || !reflect.DeepEqual(fs.config, desired) || fs.nativeIdentity != identity || fs.nextMDSIndex != next || before != coldMDSTestJSON(native.native) {
		t.Fatal("retirement changed desired ranks, original survivors or native policy")
	}
	native.readsOnly(t)
}

func TestStoppedMDSOwnerGuardsCannotRedirectRemoval(t *testing.T) {
	for _, fault := range []string{"name", "filesystem", "container", "canonical-name", "cid", "service", "filesystem-owner", "native-identity", "embed", "metadata-description", "data-description", "closed", "duplicate-target", "unconfirmed"} {
		t.Run(fault, func(t *testing.T) {
			fs, native, target, old, _ := stoppedMDSFixture(t)
			copy := *target
			switch fault {
			case "name":
				copy.ID = "foreign"
			case "filesystem":
				copy.FilesystemName = "foreign"
			case "container":
				copy.Container = &stoppedMDSDaemon{cid: old.cid}
			case "canonical-name":
				target.ID = "foreign"
			case "cid":
				old.cid = strings.Repeat("c", 64)
			case "service":
				fs.cluster.services["mds.target-0"] = &stoppedMDSDaemon{cid: old.cid}
			case "filesystem-owner":
				fs.cluster.filesystems["target"] = &CephFSContainer{}
			case "native-identity":
				fs.nativeIdentity = &cephFSNativeIdentity{}
			case "embed":
				fs.Container = &stoppedMDSDaemon{cid: strings.Repeat("d", 64)}
			case "metadata-description":
				fs.MetadataPool = "foreign"
			case "data-description":
				fs.DataPool = "foreign"
			case "closed":
				fs.cluster.closed = true
			case "duplicate-target":
				fs.mdss = append(fs.mdss, target)
			case "unconfirmed":
				target.identity.confirmed = false
			}
			if err := fs.RemoveStoppedMDS(t.Context(), &copy); err == nil || old.removes != 0 || target.identity.removalAttempted || len(native.calls) != 0 {
				t.Fatalf("bad owner reached read/mutation: %s %v", fault, err)
			}
		})
	}
}

func TestStoppedMDSRequiresPositiveStoppedOriginalCID(t *testing.T) {
	for _, fault := range []string{"running", "restarting", "paused", "dead", "positive-pid", "created", "nil-info", "wrong-inspect-id", "missing", "inspect-error"} {
		t.Run(fault, func(t *testing.T) {
			fs, native, target, old, _ := stoppedMDSFixture(t)
			switch fault {
			case "running":
				old.state.Running = true
			case "restarting":
				old.state.Restarting = true
			case "paused":
				old.state.Paused = true
			case "dead":
				old.state.Dead = true
			case "positive-pid":
				old.state.Pid = 8
			case "created":
				old.state.Status = "created"
			case "nil-info":
				old.nilInfo = true
			case "wrong-inspect-id":
				old.returnedCID = strings.Repeat("d", 64)
			case "missing":
				old.missing = true
			case "inspect-error":
				old.inspectErr = errors.Join(context.DeadlineExceeded, errors.New("never-log-stopped-mds-docker-secret"))
			}
			err := fs.RemoveStoppedMDS(t.Context(), target)
			if err == nil || old.removes != 0 || target.identity.removalAttempted || strings.Contains(err.Error(), "never-log") {
				t.Fatalf("stopped proof adopted/changed/leaked: %s %v", fault, err)
			}
			if fault == "inspect-error" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("canonical inspection deadline lost", err)
			}
			native.readsOnly(t)
		})
	}
}

func TestStoppedMDSNativeNameAndHealthySurvivorGuards(t *testing.T) {
	for _, fault := range []string{"registered-target", "global-standby-target", "sibling-target", "sibling-up-without-info", "sibling-bad-up-key", "target-up-without-info", "active-gap", "foreign-active", "unregistered-survivor", "laggy", "null-laggy", "survivor-stopped", "survivor-restarting", "survivor-paused", "survivor-dead", "survivor-zero-pid", "survivor-error", "survivor-wrong-cid", "last-member"} {
		t.Run(fault, func(t *testing.T) {
			fs, native, target, old, survivor := stoppedMDSFixture(t)
			m := native.target()
			row := m["info"].(map[string]any)["gid_12"].(map[string]any)
			switch fault {
			case "registered-target":
				m["info"].(map[string]any)["gid_11"] = coldMDSTestRow(target.ID, "up:standby-replay", 11, 0, 7)
			case "global-standby-target":
				native.native["standbys"] = []any{coldMDSTestRow(target.ID, "up:standby", 11, -1, 7)}
			case "sibling-target", "sibling-up-without-info", "sibling-bad-up-key":
				sibling := coldMDSTestMap("sibling", 9, 10)
				sibling["up"] = map[string]any{"mds_0": uint64(11)}
				sibling["info"] = map[string]any{"gid_11": coldMDSTestRow(target.ID, "up:active", 11, 0, 8)}
				if fault == "sibling-up-without-info" {
					sibling["info"] = map[string]any{}
				}
				if fault == "sibling-bad-up-key" {
					sibling["up"] = map[string]any{"mds_00": uint64(11)}
					sibling["info"].(map[string]any)["gid_11"].(map[string]any)["name"] = "sibling-0"
				}
				native.native["filesystems"] = append(native.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
			case "target-up-without-info":
				m["up"] = map[string]any{"mds_0": uint64(999)}
			case "active-gap":
				m["up"], m["info"] = map[string]any{}, map[string]any{}
			case "foreign-active":
				row["name"] = "foreign-0"
			case "unregistered-survivor":
				m["info"], m["up"] = map[string]any{"gid_13": coldMDSTestRow("foreign-0", "up:active", 13, 0, 7)}, map[string]any{"mds_0": uint64(13)}
			case "laggy":
				row["laggy_since"] = "2026-10-08T01:00:00Z"
			case "null-laggy":
				row["laggy_since"] = nil
			case "survivor-stopped":
				survivor.state.Running = false
			case "survivor-restarting":
				survivor.state.Restarting = true
			case "survivor-paused":
				survivor.state.Paused = true
			case "survivor-dead":
				survivor.state.Dead = true
			case "survivor-zero-pid":
				survivor.state.Pid = 0
			case "survivor-error":
				survivor.state.Error = "never-log-survivor-state-error"
			case "survivor-wrong-cid":
				survivor.returnedCID = strings.Repeat("c", 64)
			case "last-member":
				fs.mdss = fs.mdss[:1]
			}
			err := fs.RemoveStoppedMDS(t.Context(), target)
			if err == nil || old.removes != 0 || target.identity.removalAttempted || strings.Contains(err.Error(), "never-log") {
				t.Fatalf("bad global/survivor proof reached removal: %s %v", fault, err)
			}
			if slices.Contains([]string{"registered-target", "global-standby-target", "sibling-target"}, fault) && err.Error() != "original stopped MDS name is still registered" {
				t.Fatal("registered target lost its fixed preflight classification", err)
			}
			native.readsOnly(t)
		})
	}
}

func TestStoppedMDSRejectsMalformedGlobalMaps(t *testing.T) {
	for _, fault := range []string{"null", "trailing", "duplicate", "standbys-missing", "standbys-null", "info-null", "up-null", "info-key", "row-gid-null", "row-name-null", "row-state", "row-rank-null", "row-join-null", "up-rank", "in-null", "flags-null", "standby-count-drift"} {
		t.Run(fault, func(t *testing.T) {
			fs, native, target, old, _ := stoppedMDSFixture(t)
			m := native.target()
			row := m["info"].(map[string]any)["gid_12"].(map[string]any)
			switch fault {
			case "null":
				native.raw = "null"
			case "trailing":
				native.raw = coldMDSTestJSON(native.native) + " {}"
			case "duplicate":
				native.raw = strings.Replace(coldMDSTestJSON(native.native), `"standbys":[]`, `"standbys":[],"standbys":[]`, 1)
			case "standbys-missing":
				delete(native.native, "standbys")
			case "standbys-null":
				native.native["standbys"] = nil
			case "info-null":
				m["info"] = nil
			case "up-null":
				m["up"] = nil
			case "info-key":
				m["info"] = map[string]any{"gid_012": row}
			case "row-gid-null":
				row["gid"] = nil
			case "row-name-null":
				row["name"] = nil
			case "row-state":
				row["state"] = "up:unknown"
			case "row-rank-null":
				row["rank"] = nil
			case "row-join-null":
				row["join_fscid"] = nil
			case "up-rank":
				m["up"] = map[string]any{"mds_1": uint64(12)}
			case "in-null":
				m["in"] = nil
			case "flags-null":
				m["flags_state"] = nil
			case "standby-count-drift":
				m["standby_count_wanted"] = 0
			}
			if err := fs.RemoveStoppedMDS(t.Context(), target); err == nil || old.removes != 0 || target.identity.removalAttempted {
				t.Fatalf("malformed map authorized removal: %s %v", fault, err)
			}
		})
	}
}

func TestStoppedMDSOriginalClusterAndPoolsCannotBeAdopted(t *testing.T) {
	for _, fault := range []string{"native-fsid", "bootstrap-fsid", "quorum-missing", "filesystem-id", "metadata-id", "default-id", "pool-rename", "native-error"} {
		t.Run(fault, func(t *testing.T) {
			fs, native, target, old, _ := stoppedMDSFixture(t)
			switch fault {
			case "native-fsid":
				native.quorum["monmap"].(map[string]any)["fsid"] = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
			case "bootstrap-fsid":
				fs.cluster.config = []byte("[global]\nfsid = b011602b-37aa-496b-a8b5-b3e7a5d13ff3\n")
			case "quorum-missing":
				native.quorum["quorum_names"] = nil
			case "filesystem-id":
				native.native["filesystems"].([]any)[0].(map[string]any)["id"] = 99
			case "metadata-id":
				native.target()["metadata_pool"] = 2
			case "default-id":
				native.target()["data_pools"] = []int64{2}
			case "pool-rename":
				native.pools[0].(map[string]any)["pool_name"] = "foreign-metadata"
			case "native-error":
				native.fail = "fs dump --format json"
			}
			err := fs.RemoveStoppedMDS(t.Context(), target)
			if err == nil || old.removes != 0 || target.identity.removalAttempted || strings.Contains(err.Error(), "never-log") {
				t.Fatalf("replacement native authority adopted/leaked: %s %v", fault, err)
			}
		})
	}
}

func TestStoppedMDSAllowsIndependentSiblingAndCurrentAttachments(t *testing.T) {
	fs, native, target, old, _ := stoppedMDSFixture(t)
	native.target()["data_pools"] = []int64{1, 2}
	sibling := coldMDSTestMap("sibling", 9, 10)
	row := coldMDSTestRow("sibling-0", "up:replay", 90, 0, 8)
	row["laggy_since"] = "2026-10-08T01:00:00Z"
	sibling["info"], sibling["up"] = map[string]any{"gid_90": row}, map[string]any{"mds_0": uint64(90)}
	native.native["filesystems"] = append(native.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
	native.native["standbys"] = []any{coldMDSTestRow("independent-standby", "up:standby", 91, -1, 8)}
	before := coldMDSTestJSON(native.native)
	if err := fs.RemoveStoppedMDS(t.Context(), target); err != nil || old.removes != 1 || coldMDSTestJSON(native.native) != before || !slices.Equal(target.identity.scope.dataPools, []int64{1, 2}) || !slices.Equal(target.identity.scope.poolNames, []string{"target-data", "extra"}) {
		t.Fatalf("independent sibling/current attachment was destroyed or overconstrained: %v", err)
	}
	native.readsOnly(t)
}

func TestStoppedMDSRealAdmissionDeadlinesAndCancellation(t *testing.T) {
	for _, gate := range []string{"setup", "owner", "control", "config"} {
		t.Run(gate, func(t *testing.T) {
			fs, native, target, old, survivor := stoppedMDSFixture(t)
			c := fs.cluster
			lock, unlock := c.mu.Lock, c.mu.Unlock
			switch gate {
			case "setup":
				lock, unlock = c.cephfsSetupMu.Lock, c.cephfsSetupMu.Unlock
			case "control":
				lock, unlock = c.controlMu.Lock, c.controlMu.Unlock
			case "config":
				lock, unlock = c.configMu.Lock, c.configMu.Unlock
			}
			coldMDSTestGate(t, lock, unlock, func(ctx context.Context) error { return fs.RemoveStoppedMDS(ctx, target) })
			if old.removes != 0 || old.inspects != 0 || survivor.inspects != 0 || len(native.calls) != 0 || target.identity.removalAttempted {
				t.Fatal("held admission performed native/Docker work or consumed attempt")
			}
			if err := fs.RemoveStoppedMDS(t.Context(), target); err != nil {
				t.Fatal("fresh context could not use released owner", err)
			}
		})
	}
	fs, native, target, old, _ := stoppedMDSFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := fs.RemoveStoppedMDS(ctx, target); err == nil || !errors.Is(err, context.Canceled) || len(native.calls) != 0 || old.inspects != 0 || old.removes != 0 || target.identity.removalAttempted {
		t.Fatalf("canceled admission consumed or lost canonical cause: %v", err)
	}
}

func TestStoppedMDSAmbiguousTerminationRetainsExactRetryAuthority(t *testing.T) {
	for _, outcome := range []string{"failed-retained", "lost-reply-removed", "joined-notfound-hook-error"} {
		t.Run(outcome, func(t *testing.T) {
			fs, native, target, old, survivor := stoppedMDSFixture(t)
			old.termErr = errors.Join(context.DeadlineExceeded, errors.New("never-log-stopped-mds-transport"))
			if outcome == "lost-reply-removed" {
				old.removeOnError = true
			}
			if outcome == "joined-notfound-hook-error" {
				old.termErr = errors.Join(errdefs.ErrNotFound, errors.New("never-log-stopped-mds-hook"))
			}
			err := fs.RemoveStoppedMDS(t.Context(), target)
			if err == nil || strings.Contains(err.Error(), "never-log") || !target.identity.removalAttempted || target.identity.removed || old.removes != 1 || len(fs.mdss) != 2 || fs.Container != old || fs.cluster.services["mds.target-0"] != old {
				t.Fatalf("ambiguous receipt discarded authority or leaked: %v", err)
			}
			if outcome != "joined-notfound-hook-error" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("termination lost canonical deadline", err)
			}
			old.termErr = nil
			copy := *target
			if err := fs.RemoveStoppedMDS(t.Context(), &copy); err != nil || !target.identity.removed || fs.Container != survivor || len(fs.mdss) != 1 || old.removes != map[bool]int{true: 1, false: 2}[outcome == "lost-reply-removed"] {
				t.Fatalf("explicit exact-CID retry failed: %v", err)
			}
			native.readsOnly(t)
		})
	}
}

func TestStoppedMDSLatenessPreservesPendingOwnership(t *testing.T) {
	for _, when := range []string{"preflight-canceled", "preflight-native-name", "after-terminate-canceled", "after-terminate-native-name", "after-terminate-registry", "after-terminate-attachments"} {
		t.Run(when, func(t *testing.T) {
			fs, native, target, old, _ := stoppedMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			name := func() { native.native["standbys"] = []any{coldMDSTestRow("target-0", "up:standby", 111, -1, 7)} }
			if strings.HasPrefix(when, "preflight") {
				queries := 0
				native.hook = func(call string) {
					if call == "fs dump --format json" {
						queries++
						if queries == 3 {
							if when == "preflight-canceled" {
								cancel()
							} else {
								name()
							}
						}
					}
				}
			} else {
				old.termHook = func() {
					switch when {
					case "after-terminate-canceled":
						cancel()
					case "after-terminate-native-name":
						name()
					case "after-terminate-registry":
						fs.cluster.services["mds.target-0"] = &stoppedMDSDaemon{cid: old.cid}
					case "after-terminate-attachments":
						native.target()["data_pools"] = []int64{1, 2}
					}
				}
			}
			err := fs.RemoveStoppedMDS(ctx, target)
			wantAttempt := !strings.HasPrefix(when, "preflight")
			if err == nil || target.identity.removed || target.identity.removalAttempted != wantAttempt || old.removes != map[bool]int{true: 1, false: 0}[wantAttempt] || len(fs.mdss) != 2 || fs.Container != old {
				t.Fatalf("late change lost original/pending ownership: %s %v", when, err)
			}
			if strings.Contains(when, "canceled") && !errors.Is(err, context.Canceled) {
				t.Fatal("late canonical cancellation lost", err)
			}
		})
	}
}

func TestStoppedMDSReplacementPublicationAndCompletedHandleRemainSeparate(t *testing.T) {
	fs, native, target, old, _ := stoppedMDSFixture(t)
	if err := fs.RemoveStoppedMDS(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	replacement := &stoppedMDSDaemon{cid: strings.Repeat("c", 64), state: container.State{Status: "running", Running: true, Pid: 10}}
	stoppedMDSPublish(t, fs, replacement, nil)
	native.native["standbys"] = []any{coldMDSTestRow("target-2", "up:standby", 14, -1, 7)}
	native.calls = nil
	if err := fs.RemoveStoppedMDS(t.Context(), target); err != nil || old.removes != 1 || replacement.removes != 0 || len(fs.mdss) != 2 || fs.mdss[1].ID != "target-2" || fs.mdss[1].identity.cid == target.identity.cid || fs.cluster.services["mds.target-2"] != replacement {
		t.Fatalf("old completed identity adopted or removed replacement: %v", err)
	}
	native.readsOnly(t)
	native.quorum["monmap"].(map[string]any)["fsid"] = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
	if err := fs.RemoveStoppedMDS(t.Context(), target); err == nil || old.removes != 1 {
		t.Fatal("completed handle rebased original cluster")
	}
}

func TestStoppedMDSWholeClusterCleanupRetainsPartialAndAmbiguousHandles(t *testing.T) {
	fs, _, target, old, survivor := stoppedMDSFixture(t)
	partial := &stoppedMDSDaemon{cid: strings.Repeat("c", 64), state: container.State{Status: "exited"}}
	stoppedMDSPublish(t, fs, partial, errors.New("partial startup"))
	if err := fs.RemoveStoppedMDS(t.Context(), fs.mdss[2]); err == nil || partial.removes != 0 {
		t.Fatal("partial startup adopted removal authority")
	}
	old.termErr = errors.New("response unavailable")
	// Remove the partial from this temporary topology solely to exercise a
	// real pending attempt; it remains independently in the cleanup registry.
	fs.mdss = fs.mdss[:2]
	if err := fs.RemoveStoppedMDS(t.Context(), target); err == nil || !target.identity.removalAttempted {
		t.Fatal("missing pending attempt")
	}
	old.termErr = nil
	fs.cluster.osds = map[int]*OSDContainer{}
	if err := fs.cluster.Terminate(t.Context()); err != nil || old.removes != 2 || survivor.removes != 1 || partial.removes != 1 || len(fs.cluster.services) != 0 || !fs.cluster.closed {
		t.Fatalf("whole cleanup lost a retained owned handle: %v", err)
	}
}

func TestStoppedMDSDecoderDoesNotPanicOnMissingNativeRow(t *testing.T) {
	fs, native, target, old, _ := stoppedMDSFixture(t)
	data, _ := json.Marshal(native.native)
	var copy map[string]any
	if err := json.Unmarshal(data, &copy); err != nil {
		t.Fatal(err)
	}
	copy["filesystems"].([]any)[0].(map[string]any)["mdsmap"].(map[string]any)["up"] = map[string]any{"mds_0": uint64(111)}
	native.raw = coldMDSTestJSON(copy)
	if err := fs.RemoveStoppedMDS(t.Context(), target); err == nil || old.removes != 0 {
		t.Fatal("missing assigned row was admitted")
	}
}
