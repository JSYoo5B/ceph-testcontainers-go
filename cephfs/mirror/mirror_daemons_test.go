package mirror

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

var _ testcontainers.Container = (*Daemon)(nil)

type cephFSMirrorDaemonFake struct {
	testcontainers.Container
	id             string
	terminateErr   error
	terminateCalls int
}

func (daemon *cephFSMirrorDaemonFake) GetContainerID() string { return daemon.id }
func (daemon *cephFSMirrorDaemonFake) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	daemon.terminateCalls++
	return daemon.terminateErr
}

func TestCephFSMirrorDaemonCountDefaultsAndValidation(t *testing.T) {
	config := Config{SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}}
	if normalized, err := normalizeCephFSMirrorConfig(config); err != nil || normalized.DaemonCount != 1 {
		t.Fatalf("default daemon count: config=%+v error=%v", normalized, err)
	}
	config.DaemonCount = 2
	if normalized, err := normalizeCephFSMirrorConfig(config); err != nil || normalized.DaemonCount != 2 {
		t.Fatalf("multiple daemon count: config=%+v error=%v", normalized, err)
	}
	config.DaemonCount = -1
	if _, err := normalizeCephFSMirrorConfig(config); err == nil {
		t.Fatal("negative count was accepted")
	}
	for _, name := range []string{"", "a/b", "with space", "a\x00b", strings.Repeat("a", 64)} {
		if err := validateCephFSMirrorDaemonName(name); err == nil {
			t.Errorf("invalid daemon name %q was accepted", name)
		}
	}
	if err := validateCephFSMirrorDaemonName("custom-a.2_3"); err != nil {
		t.Fatal(err)
	}
}

// daemonNamed returns the owned daemon called name.
func daemonNamed(t *testing.T, m *Mirror, name string) *Daemon {
	t.Helper()
	for _, daemon := range m.Daemons() {
		if daemon.DaemonName == name {
			return daemon
		}
	}
	t.Fatalf("CephFS mirror owns no daemon %q", name)
	return nil
}

func TestCephFSMirrorDaemonRemovalRetainsFailedResourceAndPolicy(t *testing.T) {
	aContainer := &cephFSMirrorDaemonFake{id: "daemon-a", terminateErr: errors.New("Docker temporarily unavailable")}
	bContainer := &cephFSMirrorDaemonFake{id: "daemon-b"}
	a := &Daemon{Container: aContainer, DaemonName: "a"}
	b := &Daemon{Container: bContainer, DaemonName: "b"}
	mirror := &Mirror{Container: aContainer, daemons: []*Daemon{a, b}, initialDaemonAssigned: true,
		peerID: "owned-peer", SourceClientEntity: "client.owned", DestinationClientEntity: "client.peer", Directories: []string{"/data"}}
	a.owner, b.owner = mirror, mirror
	inventory := mirror.Daemons()
	inventory[0] = nil
	if mirror.Daemons()[0] != a {
		t.Fatal("caller changed owned inventory through the returned slice")
	}
	if err := (&Daemon{Container: aContainer, DaemonName: "a", owner: mirror}).Remove(t.Context()); err == nil || aContainer.terminateCalls != 0 || bContainer.terminateCalls != 0 {
		t.Fatal("removal touched an unowned daemon")
	}
	if err := a.Remove(t.Context()); err == nil || len(mirror.Daemons()) != 2 || mirror.Container != aContainer || a.removed {
		t.Fatal("failed termination lost retry ownership or legacy handle")
	}
	aContainer.terminateErr = nil
	if err := a.Remove(t.Context()); err != nil || len(mirror.Daemons()) != 1 || mirror.Container != nil || !a.removed {
		t.Fatalf("initial daemon removal did not clear legacy handle: %v", err)
	}
	// A container removed externally is already gone and may be removed from
	// inventory. The zero-daemon topology must retain owned peer/auth/data policy.
	bContainer.terminateErr = errdefs.ErrNotFound
	if err := b.Remove(t.Context()); err != nil || len(mirror.Daemons()) != 0 {
		t.Fatalf("last daemon or already-missing removal failed: %v", err)
	}
	if mirror.peerID != "owned-peer" || mirror.SourceClientEntity != "client.owned" || mirror.DestinationClientEntity != "client.peer" || !reflect.DeepEqual(mirror.Directories, []string{"/data"}) {
		t.Fatal("daemon lifecycle changed peer/auth/directory policy")
	}
}

func TestCephFSMirrorDaemonMutationsRefuseUninitializedAndClosedFixtures(t *testing.T) {
	mirror := &Mirror{}
	if _, err := mirror.AddDaemon(t.Context(), "a"); err == nil {
		t.Fatal("uninitialized fixture started a daemon")
	}
	if err := mirror.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.AddDaemon(t.Context(), "a"); err == nil {
		t.Fatal("closed fixture started a daemon")
	}
	if err := (&Daemon{DaemonName: "a", owner: mirror}).Remove(t.Context()); err == nil {
		t.Fatal("closed fixture accepted daemon removal")
	}
}

func TestCephFSMirrorDaemonCleanupSkipsRemovedAndRetriesFailedProcess(t *testing.T) {
	mirror := &Mirror{}
	a := &cephFSMirrorDaemonFake{id: "a"}
	b := &cephFSMirrorDaemonFake{id: "b", terminateErr: errors.New("cleanup transport failed")}
	first := mirror.registerDaemonContainer(a, "a")
	mirror.registerDaemonContainer(b, "b")
	if err := first.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := mirror.Terminate(t.Context()); err == nil || a.terminateCalls != 1 || b.terminateCalls != 1 {
		t.Fatalf("cleanup lost a partial failure or repeated removed daemon: a=%d b=%d error=%v", a.terminateCalls, b.terminateCalls, err)
	}
	b.terminateErr = nil
	if err := mirror.Terminate(t.Context()); err != nil || a.terminateCalls != 1 || b.terminateCalls != 2 {
		t.Fatalf("cleanup retry lost failure ownership or repeated completed removal: a=%d b=%d error=%v", a.terminateCalls, b.terminateCalls, err)
	}
	if err := mirror.Terminate(t.Context()); err != nil || b.terminateCalls != 2 {
		t.Fatal("completed daemon cleanup was not idempotent")
	}
	if len(mirror.Daemons()) != 0 || mirror.Container != nil {
		t.Fatal("successful fixture cleanup retained current inventory or legacy handle")
	}
}

func TestCephFSMirrorDaemonDirectTerminationAndSortedInventory(t *testing.T) {
	mirror := &Mirror{}
	z := &cephFSMirrorDaemonFake{id: "first"}
	a := &cephFSMirrorDaemonFake{id: "second"}
	first := mirror.registerDaemonContainer(z, "z")
	second := mirror.registerDaemonContainer(a, "a")
	daemons := mirror.Daemons()
	if len(daemons) != 2 || daemons[0] != second || daemons[1] != first {
		t.Fatal("daemon snapshot is not sorted while retaining identities")
	}
	if err := first.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := mirror.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if z.terminateCalls != 1 || a.terminateCalls != 1 {
		t.Fatalf("direct daemon removal repeated container cleanup hooks: first=%d second=%d", z.terminateCalls, a.terminateCalls)
	}
}

func TestCephFSMirrorDaemonRegistrationRequiresNativeIdentityFilesystemAndPeer(t *testing.T) {
	data := []byte(`[
		{"daemon_id":101,"filesystems":[{"name":"source","peers":[{"uuid":"owned-peer"}]}]},
		{"daemon_id":102,"filesystems":[{"name":"source","peers":[{"uuid":"owned-peer"}]}]},
		{"daemon_id":101,"filesystems":[{"name":"source","peers":[{"uuid":"owned-peer"}]}]},
		{"daemon_id":0,"filesystems":[{"name":"source","peers":[{"uuid":"owned-peer"}]}]},
		{"daemon_id":103,"filesystems":[{"name":"foreign","peers":[{"uuid":"owned-peer"}]}]},
		{"daemon_id":104,"filesystems":[{"name":"source","peers":[{"uuid":"foreign-peer"}]}]},
		{"daemon_id":105,"filesystems":[]}
	]`)
	owned := map[string]bool{"101": true, "102": true}
	if count, err := countCephFSMirrorRegistrations(data, "source", "owned-peer", owned); err != nil || count != 2 {
		t.Fatalf("native registration count adopted invalid, duplicate or unrelated daemon: count=%d error=%v", count, err)
	}
	if _, err := countCephFSMirrorRegistrations([]byte(`{"not":"an-array"}`), "source", "owned-peer", owned); err == nil {
		t.Fatal("malformed native daemon status was accepted")
	}
}

func TestCephFSMirrorRegistrationBarrierFiltersOwnedGIDsAndWaitsForPolicyDiscovery(t *testing.T) {
	service := []byte(`{"services":{"cephfs-mirror":{"daemons":{
		"101":{"metadata":{"id":"owned-auth"}},
		"102":{"metadata":{"id":"owned-auth"}},
		"103":{"metadata":{"id":"another-auth"}},
		"0":{"metadata":{"id":"owned-auth"}},
		"summary":""
	}}}}`)
	ids, err := cephFSMirrorOwnedInstanceIDs(service, "owned-auth")
	if err != nil || !reflect.DeepEqual(ids, map[string]bool{"101": true, "102": true}) {
		t.Fatalf("native service ownership: ids=%v error=%v", ids, err)
	}
	if err := cephFSMirrorPolicyKnowsInstances([]byte(`{"mapping":{"101":"0 directories","103":"0 directories"}}`), ids); err == nil {
		t.Fatal("foreign native policy instance satisfied missing owned registration")
	}
	if err := cephFSMirrorPolicyKnowsInstances([]byte(`{"mapping":{"101":"0 directories","102":"0 directories","103":"0 directories"}}`), ids); err == nil {
		t.Fatal("foreign or retired policy instance remained eligible for re-added directory assignment")
	}
	if err := cephFSMirrorPolicyKnowsInstances([]byte(`{"mapping":{"101":"0 directories","102":"0 directories"}}`), ids); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSMirrorWatcherGIDsAreIndependentOfServiceRegistration(t *testing.T) {
	watchers, err := parseCephFSMirrorWatchers([]byte("watcher=172.20.0.8:0/1234 client.4262 cookie=1\nwatcher=172.20.0.9:0/5678 client.4266 cookie=2\n"))
	if err != nil || !reflect.DeepEqual(watchers, map[string]string{"172.20.0.8:0/1234": "4262", "172.20.0.9:0/5678": "4266"}) {
		t.Fatalf("filesystem watcher sessions: watchers=%v error=%v", watchers, err)
	}
	service := []byte(`{"services":{"cephfs-mirror":{"daemons":{"summary":"","4249":{"metadata":{"id":"owned-auth"}},"4253":{"metadata":{"id":"owned-auth"}}}}}}`)
	serviceIDs, err := cephFSMirrorOwnedInstanceIDs(service, "owned-auth")
	if err != nil {
		t.Fatal(err)
	}
	watcherIDs := map[string]bool{"4262": true, "4266": true}
	distribution := []byte(`{"mapping":{"4262":"0 directories","4266":"0 directories"}}`)
	if err := cephFSMirrorPolicyKnowsInstances(distribution, serviceIDs); err == nil {
		t.Fatal("service session GIDs were incorrectly treated as filesystem watcher GIDs")
	}
	if err := cephFSMirrorPolicyKnowsInstances(distribution, watcherIDs); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSMirrorExplicitRebalanceWaitsForNativeReleaseAndPreservesForeignPolicy(t *testing.T) {
	intent := []string{"/owned-a", "/owned-b"}
	native := []string{"/owned-a", "/owned-b", "/foreign"}
	listCalls := 0
	actions := []string{}
	pending := map[string]bool{}
	list := func(context.Context) ([]string, error) {
		listCalls++
		if listCalls >= 3 {
			retained := native[:0]
			for _, directory := range native {
				if !pending[directory] {
					retained = append(retained, directory)
				}
			}
			native = retained
		}
		return append([]string(nil), native...), nil
	}
	mutate := func(_ context.Context, operation, directory string) error {
		actions = append(actions, operation+":"+directory)
		if operation == "remove" {
			pending[directory] = true
			return nil
		}
		if listCalls < 3 {
			t.Fatal("directory was restored before native release completed")
		}
		native = append(native, directory)
		return nil
	}
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mappedState, mutate, make(map[string]bool), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actions, []string{"remove:/owned-a", "remove:/owned-b", "add:/owned-a", "add:/owned-b"}) || !reflect.DeepEqual(native, []string{"/foreign", "/owned-a", "/owned-b"}) {
		t.Fatalf("rebalance changed unrelated policy or order: actions=%v native=%v", actions, native)
	}
	if !reflect.DeepEqual(intent, []string{"/owned-a", "/owned-b"}) {
		t.Fatal("reconciliation mutated desired membership")
	}
}

func TestCephFSMirrorExplicitRebalanceRetriesUncertainPartialMutations(t *testing.T) {
	for _, failedOperation := range []string{"remove", "add"} {
		t.Run(failedOperation, func(t *testing.T) {
			intent := []string{"/owned-a", "/owned-b"}
			native := map[string]bool{"/owned-a": true, "/owned-b": true, "/foreign": true}
			failed := false
			pending := make(map[string]bool)
			list := func(context.Context) ([]string, error) {
				directories := []string{}
				for directory := range native {
					directories = append(directories, directory)
				}
				return directories, nil
			}
			mutate := func(_ context.Context, operation, directory string) error {
				if operation == "remove" {
					delete(native, directory)
				} else {
					native[directory] = true
				}
				if !failed && operation == failedOperation && directory == "/owned-a" {
					failed = true
					return errors.New("CLI response lost after applying mutation")
				}
				return nil
			}
			if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mappedState, mutate, pending, time.Millisecond); err == nil {
				t.Fatal("partial mutation error was lost")
			}
			if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mappedState, mutate, pending, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(native, map[string]bool{"/owned-a": true, "/owned-b": true, "/foreign": true}) || !reflect.DeepEqual(intent, []string{"/owned-a", "/owned-b"}) {
				t.Fatalf("retry lost intended or caller-owned policy: native=%v intent=%v", native, intent)
			}
		})
	}
}

func TestCephFSMirrorExplicitRebalanceHonorsCanceledReleaseWithoutRestoringEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listCalls := 0
	err := rebalanceCephFSMirrorDirectories(ctx, []string{"/owned"}, func(context.Context) ([]string, error) {
		listCalls++
		if listCalls == 2 {
			cancel()
		}
		return []string{"/owned", "/foreign"}, nil
	}, mappedState, func(_ context.Context, operation, _ string) error {
		if operation == "add" {
			t.Fatal("canceled release restored an in-flight assignment")
		}
		return nil
	}, make(map[string]bool), time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("rebalance lost cancellation: %v", err)
	}
}

func TestCephFSMirrorExplicitRebalanceRetriesLostResponseWhileNativeRemovalIsPending(t *testing.T) {
	intent := []string{"/owned"}
	pending := make(map[string]bool)
	removing := false
	removed := false
	restored := false
	removeCalls := 0
	listCalls := 0
	list := func(context.Context) ([]string, error) {
		listCalls++
		if removing && listCalls >= 4 {
			removed = true
			removing = false
		}
		if removed && !restored {
			return []string{"/foreign"}, nil
		}
		return []string{"/owned", "/foreign"}, nil
	}
	mutate := func(_ context.Context, operation, directory string) error {
		if operation == "remove" {
			removeCalls++
			if removing {
				return errors.New("Error EINVAL: directory /owned is under removal")
			}
			removing = true
			return errors.New("CLI response lost after asynchronous removal began")
		}
		if !removed {
			t.Fatal("retry restored directory before asynchronous release completed")
		}
		restored = true
		return nil
	}
	state := func(_ context.Context, directory string) (string, error) {
		if directory == "/owned" && removing {
			return "unmapping", nil
		}
		return "mapped", nil
	}
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, state, mutate, pending, time.Millisecond); err == nil || !pending["/owned"] {
		t.Fatal("uncertain async removal was not retained")
	}
	// The retry waits for the removal that the lost response started instead
	// of sending another remove into it.
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, state, mutate, pending, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if removeCalls != 1 || !restored || len(pending) != 0 {
		t.Fatalf("retry lost pending release or skipped recovery: removeCalls=%d restored=%t pending=%v", removeCalls, restored, pending)
	}
}

type cephFSMirrorNativeControlFake struct {
	testcontainers.Container
	directories map[string]bool
	mutations   []string
	commands    [][]string
}

func (control *cephFSMirrorNativeControlFake) IsRunning() bool        { return true }
func (control *cephFSMirrorNativeControlFake) GetContainerID() string { return "native-control" }
func (control *cephFSMirrorNativeControlFake) Exec(ctx context.Context, arguments []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	control.commands = append(control.commands, append([]string(nil), arguments...))
	args := arguments
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		args = args[3:]
	}
	var output, stderr []byte
	code := 0
	switch {
	case len(args) == 6 && reflect.DeepEqual(args[:4], []string{"fs", "snapshot", "mirror", "dirmap"}):
		if control.directories[args[5]] {
			output = []byte(`{"instance_id": "4262", "last_shuffled": 1791898606.5, "state": "mapped"}`)
		} else {
			code, stderr = 2, []byte("Error ENOENT: "+args[5]+" is not tracked\n")
		}
	case len(args) >= 3 && args[0] == "fs" && args[1] == "get":
		output = []byte(`{"id":1,"mdsmap":{"metadata_pool":7}}`)
	case len(args) >= 4 && reflect.DeepEqual(args[:4], []string{"osd", "pool", "ls", "detail"}):
		output = []byte(`[{"pool_id":1,"pool_name":".mgr"},{"pool_id":7,"pool_name":"source-metadata"}]`)
	case len(args) >= 2 && args[0] == "service" && args[1] == "dump":
		output = []byte(`{"services":{"cephfs-mirror":{"daemons":{"summary":"","4249":{"metadata":{"id":"owned-auth","instance_id":"4249"}}}}}}`)
	case args[0] == "rados":
		if !reflect.DeepEqual(args, []string{"rados", "--pool", "source-metadata", "listwatchers", "cephfs_mirror"}) {
			return 0, nil, errors.New("incorrect metadata watcher query")
		}
		output = []byte("watcher=172.20.0.8:0/1234 client.4262 cookie=1\n")
	case args[0] == "ceph" && len(args) >= 2 && args[1] == "--admin-daemon":
		if !reflect.DeepEqual(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "source@1"}) {
			return 0, nil, errors.New("incorrect mirror admin socket query")
		}
		output = []byte(`{"rados_inst":"172.20.0.8:0/1234","peers":{"owned-peer":{}},"snap_dirs":{"dir_count":0}}`)
	case len(args) == 5 && reflect.DeepEqual(args, []string{"fs", "snapshot", "mirror", "daemon", "status"}):
		output = []byte(`[{"daemon_id":4249,"filesystems":[{"name":"source","peers":[{"uuid":"owned-peer"}]}]}]`)
	case len(args) >= 6 && reflect.DeepEqual(args[:5], []string{"fs", "snapshot", "mirror", "show", "distribution"}):
		output = []byte(`{"mapping":{"4262":"0 directories"}}`)
	case len(args) >= 5 && reflect.DeepEqual(args[:4], []string{"fs", "snapshot", "mirror", "ls"}):
		directories := []string{}
		for directory := range control.directories {
			directories = append(directories, directory)
		}
		output, _ = json.Marshal(directories)
	case len(args) >= 6 && reflect.DeepEqual(args[:3], []string{"fs", "snapshot", "mirror"}) && (args[3] == "remove" || args[3] == "add"):
		operation, directory := args[3], args[5]
		if directory == "/foreign" {
			return 0, nil, errors.New("caller-owned directory was modified")
		}
		control.mutations = append(control.mutations, operation+":"+directory)
		if operation == "remove" {
			delete(control.directories, directory)
		} else {
			control.directories[directory] = true
		}
		output = []byte(`{}`)
	default:
		return 0, nil, errors.New("unexpected native command: " + strings.Join(arguments, " "))
	}
	var framed bytes.Buffer
	for _, stream := range []struct {
		kind byte
		data []byte
	}{{byte(stdcopy.Stdout), output}, {byte(stdcopy.Stderr), stderr}} {
		if stream.kind == byte(stdcopy.Stderr) && len(stream.data) == 0 {
			continue
		}
		header := [8]byte{stream.kind}
		binary.BigEndian.PutUint32(header[4:], uint32(len(stream.data)))
		framed.Write(header[:])
		framed.Write(stream.data)
	}
	return code, bytes.NewReader(framed.Bytes()), nil
}

func TestCephFSMirrorNativeBarrierAndRebalanceUseRealJSONAndPrivateOwnership(t *testing.T) {
	control := &cephFSMirrorNativeControlFake{directories: map[string]bool{"/owned": true, "/foreign": true}}
	mirror := &Mirror{
		source: &ceph.Container{Container: control}, sourceID: "owned-auth", peerID: "owned-peer", SourceFilesystem: "source",
		daemons:     []*Daemon{{Container: control, DaemonName: "a"}},
		Directories: []string{"/owned", "/foreign"}, ownedDirectories: map[string]bool{"/owned": true},
	}
	if err := mirror.loadFilesystemIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if mirror.filesystemID != 1 || mirror.metadataPool != "source-metadata" {
		t.Fatalf("native metadata identity mismatch: id=%d pool=%q", mirror.filesystemID, mirror.metadataPool)
	}
	if err := mirror.RebalanceDirectories(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(control.mutations, []string{"remove:/owned", "add:/owned"}) || !control.directories["/foreign"] || !reflect.DeepEqual(mirror.Directories, []string{"/owned", "/foreign"}) {
		t.Fatalf("rebalance adopted public caller-added membership: mutations=%v policies=%v", control.mutations, control.directories)
	}
}

// mappedState reports every tracked directory idle.
func mappedState(context.Context, string) (string, error) { return "mapped", nil }

// mirrorPolicyModel is the native mirroring policy as rebalance sees it. A
// removed directory stays "unmapping" for a few list calls until its release
// is acknowledged. A remove issued while another directory has an action in
// flight deadlocks the module (its policy lock against the librados callback
// strand), so the model blocks it until the caller's deadline. "mapped" and
// "stalled" directories have no action in flight. A remove can fail before it
// reaches the MGR (unsent) or lose its response after the removal began
// (lostResponse).
type mirrorPolicyModel struct {
	states       map[string]string
	releasing    map[string]int
	unsent       map[string]bool
	lostResponse map[string]bool
	deadlocked   bool
	actions      []string
}

func (m *mirrorPolicyModel) list(context.Context) ([]string, error) {
	for directory, polls := range m.releasing {
		if polls <= 1 {
			delete(m.releasing, directory)
			delete(m.states, directory)
			continue
		}
		m.releasing[directory] = polls - 1
	}
	directories := make([]string, 0, len(m.states))
	for directory := range m.states {
		directories = append(directories, directory)
	}
	slices.Sort(directories)
	return directories, nil
}

func (m *mirrorPolicyModel) state(_ context.Context, directory string) (string, error) {
	value, ok := m.states[directory]
	if !ok {
		return "", fmt.Errorf("Error ENOENT: %s is not tracked: %w", directory, fs.ErrNotExist)
	}
	return value, nil
}

func (m *mirrorPolicyModel) mutate(ctx context.Context, operation, directory string) error {
	m.actions = append(m.actions, operation+":"+directory)
	if operation == "add" {
		m.states[directory] = "mapped"
		return nil
	}
	if m.unsent[directory] {
		delete(m.unsent, directory)
		return errors.New("connect to the MGR: connection refused")
	}
	if m.states[directory] == "" {
		return fmt.Errorf("Error ENOENT: %s is not tracked: %w", directory, fs.ErrNotExist)
	}
	if m.releasing[directory] > 0 {
		return fmt.Errorf("Error EINVAL: directory %s is under removal", directory)
	}
	for other, value := range m.states {
		if other != directory && value != "mapped" && value != "stalled" {
			m.deadlocked = true
			<-ctx.Done()
			return ctx.Err()
		}
	}
	m.states[directory] = "unmapping"
	m.releasing[directory] = 3
	if m.lostResponse[directory] {
		delete(m.lostResponse, directory)
		return errors.New("exec ceph: response lost")
	}
	return nil
}

func TestCephFSMirrorRebalanceNeverRemovesWhileAReleaseIsInFlight(t *testing.T) {
	model := &mirrorPolicyModel{
		states:    map[string]string{"/daemon-a": "mapped", "/daemon-b": "mapped", "/daemon-c": "mapped", "/foreign": "mapped"},
		releasing: map[string]int{},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	intent := []string{"/daemon-a", "/daemon-b", "/daemon-c"}
	if err := rebalanceCephFSMirrorDirectories(ctx, intent, model.list, model.state, model.mutate, map[string]bool{}, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("rebalance removed a directory while another release was in flight: %v, actions %v", err, model.actions)
	}
	want := []string{"remove:/daemon-a", "remove:/daemon-b", "remove:/daemon-c", "add:/daemon-a", "add:/daemon-b", "add:/daemon-c"}
	if !reflect.DeepEqual(model.actions, want) || model.states["/foreign"] != "mapped" || len(model.states) != 4 {
		t.Fatalf("actions %v, states %v", model.actions, model.states)
	}

	// A caller's directory still acquiring delays the first remove too.
	model = &mirrorPolicyModel{states: map[string]string{"/daemon-a": "mapped", "/foreign": "mapping"}, releasing: map[string]int{}}
	acquired := 0
	list := func(ctx context.Context) ([]string, error) {
		if acquired++; acquired == 3 {
			model.states["/foreign"] = "mapped"
		}
		return model.list(ctx)
	}
	if err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, list, model.state, model.mutate, map[string]bool{}, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("rebalance removed while a caller's directory was acquiring: %v", err)
	}
}

func TestCephFSMirrorRebalanceProceedsPastStalledDirectories(t *testing.T) {
	// A directory waiting for a daemon has no action in flight.
	model := &mirrorPolicyModel{states: map[string]string{"/daemon-a": "mapped", "/foreign": "stalled"}, releasing: map[string]int{}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, model.list, model.state, model.mutate, map[string]bool{}, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("rebalance did not proceed past a stalled directory: %v, actions %v", err, model.actions)
	}
	if want := []string{"remove:/daemon-a", "add:/daemon-a"}; !reflect.DeepEqual(model.actions, want) || model.states["/foreign"] != "stalled" {
		t.Fatalf("actions %v, states %v", model.actions, model.states)
	}
}

func TestCephFSMirrorRebalanceSkipsADirectoryThatLeavesThePolicyDuringInspection(t *testing.T) {
	// A retry lists the directory, then its earlier removal completes before
	// its state is read. It must not be removed again.
	model := &mirrorPolicyModel{states: map[string]string{"/daemon-a": "mapped", "/daemon-b": "mapped"}, releasing: map[string]int{}}
	list := func(ctx context.Context) ([]string, error) {
		directories, err := model.list(ctx)
		if len(model.actions) == 0 {
			delete(model.states, "/daemon-a")
		}
		return directories, err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pending := map[string]bool{"/daemon-a": true}
	if err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a", "/daemon-b"}, list, model.state, model.mutate, pending, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("rebalance failed on a directory that left the policy: %v, actions %v", err, model.actions)
	}
	if want := []string{"remove:/daemon-b", "add:/daemon-a", "add:/daemon-b"}; !reflect.DeepEqual(model.actions, want) || len(pending) != 0 {
		t.Fatalf("actions %v, pending %v", model.actions, pending)
	}
}

func TestCephFSMirrorRebalanceNeverResendsAnUncertainRemove(t *testing.T) {
	// The remove fails, and the policy still shows the path mapped. That does
	// not prove the MGR dropped it, so no retry sends it again.
	model := &mirrorPolicyModel{
		states:    map[string]string{"/daemon-a": "mapped", "/foreign": "mapped"},
		releasing: map[string]int{},
		unsent:    map[string]bool{"/daemon-a": true},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pending := map[string]bool{}
	if err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, model.list, model.state, model.mutate, pending, time.Millisecond); err == nil || !pending["/daemon-a"] {
		t.Fatalf("uncertain removal was not reported and retained: %v, pending %v", err, pending)
	}
	for range 2 {
		err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, model.list, model.state, model.mutate, pending, time.Millisecond)
		if !errors.Is(err, errCephFSUncertainRemoval) || !pending["/daemon-a"] || model.deadlocked {
			t.Fatalf("retry over a still-mapped path: %v, pending %v", err, pending)
		}
	}
	if want := []string{"remove:/daemon-a"}; !reflect.DeepEqual(model.actions, want) {
		t.Fatalf("an uncertain remove was sent again: %v", model.actions)
	}
	// Once the path has left the policy, the retry restores it without
	// another remove.
	delete(model.states, "/daemon-a")
	if err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, model.list, model.state, model.mutate, pending, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("retry after the path left the policy: %v, actions %v", err, model.actions)
	}
	if want := []string{"remove:/daemon-a", "add:/daemon-a"}; !reflect.DeepEqual(model.actions, want) || len(pending) != 0 {
		t.Fatalf("actions %v, pending %v", model.actions, pending)
	}
}

func TestCephFSMirrorRebalanceWaitsForItsOwnLostResponseRemovalOnRetry(t *testing.T) {
	model := &mirrorPolicyModel{
		states:       map[string]string{"/daemon-a": "mapped", "/daemon-b": "mapped"},
		releasing:    map[string]int{},
		lostResponse: map[string]bool{"/daemon-a": true},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pending := map[string]bool{}
	intent := []string{"/daemon-a", "/daemon-b"}
	if err := rebalanceCephFSMirrorDirectories(ctx, intent, model.list, model.state, model.mutate, pending, time.Millisecond); err == nil || !pending["/daemon-a"] {
		t.Fatalf("lost response was not reported and retained: %v", err)
	}
	// /daemon-a is still unmapping, so /daemon-b must wait for it, and
	// /daemon-a is neither removed again nor restored early.
	if err := rebalanceCephFSMirrorDirectories(ctx, intent, model.list, model.state, model.mutate, pending, time.Millisecond); err != nil || model.deadlocked {
		t.Fatalf("retry failed: %v, actions %v", err, model.actions)
	}
	if want := []string{"remove:/daemon-a", "remove:/daemon-b", "add:/daemon-a", "add:/daemon-b"}; !reflect.DeepEqual(model.actions, want) || len(pending) != 0 {
		t.Fatalf("actions %v, pending %v", model.actions, pending)
	}
}

func TestCephFSMirrorRebalanceHonorsCancellationWhileWaitingForAnIdlePolicy(t *testing.T) {
	model := &mirrorPolicyModel{states: map[string]string{"/daemon-a": "mapped", "/foreign": "mapping"}, releasing: map[string]int{}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	polls := 0
	list := func(ctx context.Context) ([]string, error) {
		if polls++; polls == 3 {
			cancel()
		}
		return model.list(ctx)
	}
	err := rebalanceCephFSMirrorDirectories(ctx, []string{"/daemon-a"}, list, model.state, model.mutate, map[string]bool{}, time.Millisecond)
	if !errors.Is(err, context.Canceled) || len(model.actions) != 0 {
		t.Fatalf("cancellation during the idle wait: %v, actions %v", err, model.actions)
	}
}
