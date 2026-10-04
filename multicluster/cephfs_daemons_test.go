package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

var _ testcontainers.Container = (*CephFSMirrorDaemon)(nil)

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
	config := CephFSMirrorConfig{SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}}
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

func TestCephFSMirrorDaemonRemovalRetainsFailedResourceAndPolicy(t *testing.T) {
	aContainer := &cephFSMirrorDaemonFake{id: "daemon-a", terminateErr: errors.New("Docker temporarily unavailable")}
	bContainer := &cephFSMirrorDaemonFake{id: "daemon-b"}
	a := &CephFSMirrorDaemon{Container: aContainer, DaemonName: "a"}
	b := &CephFSMirrorDaemon{Container: bContainer, DaemonName: "b"}
	mirror := &CephFSMirror{Container: aContainer, daemons: []*CephFSMirrorDaemon{a, b}, initialDaemonAssigned: true,
		peerID: "owned-peer", SourceClientEntity: "client.owned", DestinationClientEntity: "client.peer", Directories: []string{"/data"}}
	inventory := mirror.Daemons()
	inventory[0] = nil
	if mirror.Daemons()[0] != a {
		t.Fatal("caller changed owned inventory through the returned slice")
	}
	if err := mirror.RemoveDaemon(t.Context(), "foreign"); err == nil || aContainer.terminateCalls != 0 || bContainer.terminateCalls != 0 {
		t.Fatal("removal touched an unowned daemon")
	}
	if err := mirror.RemoveDaemon(t.Context(), "a"); err == nil || len(mirror.Daemons()) != 2 || mirror.Container != aContainer || a.removed {
		t.Fatal("failed termination lost retry ownership or legacy handle")
	}
	aContainer.terminateErr = nil
	if err := mirror.RemoveDaemon(t.Context(), "a"); err != nil || len(mirror.Daemons()) != 1 || mirror.Container != nil || !a.removed {
		t.Fatalf("initial daemon removal did not clear legacy handle: %v", err)
	}
	// A container removed externally is already gone and may be removed from
	// inventory. The zero-daemon topology must retain owned peer/auth/data policy.
	bContainer.terminateErr = errdefs.ErrNotFound
	if err := mirror.RemoveDaemon(t.Context(), "b"); err != nil || len(mirror.Daemons()) != 0 {
		t.Fatalf("last daemon or already-missing removal failed: %v", err)
	}
	if mirror.peerID != "owned-peer" || mirror.SourceClientEntity != "client.owned" || mirror.DestinationClientEntity != "client.peer" || !reflect.DeepEqual(mirror.Directories, []string{"/data"}) {
		t.Fatal("daemon lifecycle changed peer/auth/directory policy")
	}
}

func TestCephFSMirrorDaemonMutationsRefuseUninitializedAndClosedFixtures(t *testing.T) {
	mirror := &CephFSMirror{}
	if _, err := mirror.AddDaemon(t.Context(), "a"); err == nil {
		t.Fatal("uninitialized fixture started a daemon")
	}
	if err := mirror.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.AddDaemon(t.Context(), "a"); err == nil {
		t.Fatal("closed fixture started a daemon")
	}
	if err := mirror.RemoveDaemon(t.Context(), "a"); err == nil {
		t.Fatal("closed fixture accepted daemon removal")
	}
}

func TestCephFSMirrorDaemonCleanupSkipsRemovedAndRetriesFailedProcess(t *testing.T) {
	mirror := &CephFSMirror{}
	a := &cephFSMirrorDaemonFake{id: "a"}
	b := &cephFSMirrorDaemonFake{id: "b", terminateErr: errors.New("cleanup transport failed")}
	mirror.registerDaemonContainer(a, "a")
	mirror.registerDaemonContainer(b, "b")
	if err := mirror.RemoveDaemon(t.Context(), "a"); err != nil {
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
	mirror := &CephFSMirror{}
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
	if err := mirror.RemoveDaemon(t.Context(), "z"); err != nil {
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
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mutate, make(map[string]bool), time.Millisecond); err != nil {
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
			if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mutate, pending, time.Millisecond); err == nil {
				t.Fatal("partial mutation error was lost")
			}
			if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mutate, pending, time.Millisecond); err != nil {
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
	}, func(_ context.Context, operation, _ string) error {
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
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mutate, pending, time.Millisecond); err == nil || !pending["/owned"] {
		t.Fatal("uncertain async removal was not retained")
	}
	if err := rebalanceCephFSMirrorDirectories(t.Context(), intent, list, mutate, pending, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if removeCalls != 2 || !restored || len(pending) != 0 {
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
	var output []byte
	switch {
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
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	framed.Write(header[:])
	framed.Write(output)
	return 0, bytes.NewReader(framed.Bytes()), nil
}

func TestCephFSMirrorNativeBarrierAndRebalanceUseRealJSONAndPrivateOwnership(t *testing.T) {
	control := &cephFSMirrorNativeControlFake{directories: map[string]bool{"/owned": true, "/foreign": true}}
	mirror := &CephFSMirror{
		source: &ceph.Container{Container: control}, sourceID: "owned-auth", peerID: "owned-peer", SourceFilesystem: "source",
		daemons:     []*CephFSMirrorDaemon{{Container: control, DaemonName: "a"}},
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
