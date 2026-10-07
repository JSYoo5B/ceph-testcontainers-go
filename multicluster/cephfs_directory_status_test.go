package multicluster

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
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const cephFSObserverDraftPeer = "02117353-8cd1-44db-976b-eb20609aa160"

func cephFSObserverDraftStream(data string) io.Reader {
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.WriteString(data)
	return bytes.NewReader(stream.Bytes())
}

type cephFSObserverDraftControl struct {
	*cephFSMirrorNativeControlFake
	filesystemID   int
	metadataPool   int64
	peer, mapping  string
	peerQueryError error
	calls          [][]string
	hook           func(*cephFSObserverDraftControl, []string)
}

func (c *cephFSObserverDraftControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	c.calls = append(c.calls, slices.Clone(args))
	if c.hook != nil {
		c.hook(c, args)
	}
	arguments := args
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		arguments = args[3:]
	}
	var data string
	switch {
	case len(arguments) > 2 && slices.Equal(arguments[:2], []string{"fs", "get"}):
		data = fmt.Sprintf(`{"id":%d,"mdsmap":{"metadata_pool":%d}}`, c.filesystemID, c.metadataPool)
	case len(arguments) > 4 && slices.Equal(arguments[:4], []string{"fs", "snapshot", "mirror", "peer_list"}):
		if c.peerQueryError != nil {
			return 0, nil, c.peerQueryError
		}
		data = c.peer
	case len(arguments) > 5 && slices.Equal(arguments[:4], []string{"fs", "snapshot", "mirror", "dirmap"}):
		data = c.mapping
	case len(arguments) >= 2 && arguments[0] == "rados":
		data = "watcher=172.20.0.8:0/1234 client.4262 cookie=1\nwatcher=172.20.0.9:0/5678 client.4266 cookie=2\n"
	default:
		return c.cephFSMirrorNativeControlFake.Exec(ctx, args, opts...)
	}
	return 0, cephFSObserverDraftStream(data), nil
}

type cephFSObserverDraftDaemon struct {
	testcontainers.Container
	running                  bool
	paused, restarting, dead bool
	session, replay          string
	calls                    [][]string
}

func (d *cephFSObserverDraftDaemon) State(context.Context) (*container.State, error) {
	return &container.State{Running: d.running, Paused: d.paused, Restarting: d.restarting, Dead: d.dead}, nil
}
func (d *cephFSObserverDraftDaemon) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	d.calls = append(d.calls, slices.Clone(args))
	if slices.Equal(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "source@1"}) {
		return 0, cephFSObserverDraftStream(d.session), nil
	}
	if slices.Equal(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", "source@1", cephFSObserverDraftPeer}) {
		return 0, cephFSObserverDraftStream(d.replay), nil
	}
	return 0, nil, errors.New("wrong native filesystem/peer admin command")
}

func newCephFSObserverDraft() (*CephFSMirror, *cephFSObserverDraftControl, *cephFSObserverDraftControl, *cephFSObserverDraftDaemon, *cephFSObserverDraftDaemon) {
	peer := fmt.Sprintf(`{%q:{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}}`, cephFSObserverDraftPeer)
	source := &cephFSObserverDraftControl{cephFSMirrorNativeControlFake: &cephFSMirrorNativeControlFake{}, filesystemID: 1, metadataPool: 7, peer: peer, mapping: `{"state":"mapped","instance_id":"4262"}`}
	destination := &cephFSObserverDraftControl{cephFSMirrorNativeControlFake: &cephFSMirrorNativeControlFake{}, filesystemID: 2, metadataPool: 9}
	session := func(address string) string {
		return fmt.Sprintf(`{"rados_inst":%q,"peers":{%q:{"remote":{"client_name":"client.peer","cluster_name":"destination-site","fs_name":"destination"}}},"snap_dirs":{"dir_count":1}}`, address, cephFSObserverDraftPeer)
	}
	a := &cephFSObserverDraftDaemon{running: true, session: session("172.20.0.8:0/1234"), replay: `{"/owned":{"state":"idle","last_synced_snap":{"id":120,"name":"checkpoint"},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0},"/foreign":null}`}
	b := &cephFSObserverDraftDaemon{running: true, session: session("172.20.0.9:0/5678"), replay: a.replay}
	mirror := &CephFSMirror{
		source: &ceph.Container{Container: source}, destination: &ceph.Container{Container: destination},
		SourceFilesystem: "source", DestinationFilesystem: "destination", SourceClientEntity: "client.owned-auth", DestinationClientEntity: "client.peer",
		filesystemID: 1, destinationFilesystemID: 2, metadataPool: "source-metadata", metadataPoolID: 7, destinationMetadataPoolID: 9, destinationSite: "destination-site", peerID: cephFSObserverDraftPeer,
		ownedDirectories: map[string]bool{"/owned": true}, Directories: []string{"/owned"},
		daemons: []*CephFSMirrorDaemon{{Container: a, DaemonName: "a"}, {Container: b, DaemonName: "b"}},
	}
	return mirror, source, destination, a, b
}

func TestCephFSObserverDraftExactOwnedWatcherAndSnapshotIdentity(t *testing.T) {
	mirror, source, _, a, _ := newCephFSObserverDraft()
	status, err := mirror.DirectoryStatus(t.Context(), "/a/../owned")
	if err != nil || !status.Ready || status.Directory != "/owned" || status.InstanceID != "4262" || status.DaemonName != "a" || status.LastSyncedSnapshot == nil || status.LastSyncedSnapshot.ID != 120 || status.LastSyncedSnapshot.Name != "checkpoint" {
		t.Fatalf("directory observer: %+v %v", status, err)
	}
	if len(source.mutations) != 0 || len(a.calls) != 2 {
		t.Fatal("observer mutated native policy or used wrong daemon")
	}
	if status, err := mirror.WaitSnapshotSynced(t.Context(), "/owned", CephFSMirrorSnapshot{ID: 120, Name: "checkpoint"}); err != nil || !status.Ready {
		t.Fatalf("exact source checkpoint was not reported: %+v %v", status, err)
	}
	// Source/destination IDs must be captured during construction, not inferred
	// from same-name filesystems during every standalone observation.
	if err := mirror.loadFilesystemIdentity(t.Context()); err != nil || mirror.metadataPoolID != 7 || mirror.destinationFilesystemID != 2 || mirror.destinationMetadataPoolID != 9 {
		t.Fatalf("pair identity capture: %v", err)
	}
}

func TestCephFSObserverDraftFailoverUsesFilesystemGIDAndResetsCounters(t *testing.T) {
	mirror, source, _, a, b := newCephFSObserverDraft()
	a.running = false
	status, err := mirror.DirectoryStatus(t.Context(), "/owned")
	if err == nil || status.Ready || status.DaemonProblems["a"] == "" {
		t.Fatalf("stopped owner accepted native mapping: %+v %v", status, err)
	}
	// Mapping GIDs refer to filesystem watchers (4266), not service GID 4249.
	source.mapping = `{"state":"mapped","instance_id":"4266"}`
	b.replay = strings.Replace(b.replay, `"snaps_synced":1`, `"snaps_synced":0`, 1)
	status, err = mirror.DirectoryStatus(t.Context(), "/owned")
	var partial *cephFSObservationPartialError
	if !errors.As(err, &partial) || !status.Ready || status.DaemonName != "b" || status.SnapshotsSynced != 0 || status.LastSyncedSnapshot == nil || status.LastSyncedSnapshot.ID != 120 || status.DaemonProblems["a"] == "" {
		t.Fatalf("reassignment/counter reset invalidated checkpoint: %+v %v", status, err)
	}
	if status, err := mirror.WaitSnapshotSynced(t.Context(), "/owned", CephFSMirrorSnapshot{ID: 120, Name: "checkpoint"}); err != nil || !status.Ready || status.DaemonProblems["a"] == "" {
		t.Fatalf("unrelated stopped member blocked selected directory checkpoint or was hidden: %+v %v", status, err)
	}
	for _, mapping := range []string{`{"state":"mapping"}`, `{"state":"stalled"}`, `{"state":null,"instance_id":""}`, `{"state":"mapped","instance_id":"4249"}`} {
		source.mapping = mapping
		status, err = mirror.DirectoryStatus(t.Context(), "/owned")
		if status.Ready || (strings.Contains(mapping, `"state":"mapped"`) && err == nil) {
			t.Fatalf("transition/foreign service GID became ready: mapping=%s status=%+v err=%v", mapping, status, err)
		}
	}
}

func TestCephFSObserverDraftDockerProcessReadinessAndUnmappedGuards(t *testing.T) {
	for _, unavailable := range []string{"paused", "restarting", "dead"} {
		t.Run(unavailable, func(t *testing.T) {
			mirror, _, _, a, _ := newCephFSObserverDraft()
			switch unavailable {
			case "paused":
				a.paused = true
			case "restarting":
				a.restarting = true
			case "dead":
				a.dead = true
			}
			status, err := mirror.DirectoryStatus(t.Context(), "/owned")
			if err == nil || status.Ready || status.DaemonProblems["a"] == "" || len(a.calls) != 0 {
				t.Fatalf("unavailable Docker process reached native exec: %+v %v", status, err)
			}
		})
	}
	for _, fault := range []string{"cancel", "destination-identity", "peer-identity"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination, _, _ := newCephFSObserverDraft()
			source.mapping = `{"state":"mapping"}`
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source.hook = func(_ *cephFSObserverDraftControl, args []string) {
				if !slices.Contains(args, "dirmap") {
					return
				}
				switch fault {
				case "cancel":
					cancel()
				case "destination-identity":
					destination.filesystemID++
				case "peer-identity":
					source.peer = strings.Replace(source.peer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1)
				}
			}
			status, err := mirror.DirectoryStatus(ctx, "/owned")
			if err == nil || status.Ready || status.MappingState != "mapping" {
				t.Fatalf("unmapped early return ignored %s: %+v %v", fault, status, err)
			}
			if fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("unmapped early return lost cancellation: %v", err)
			}
		})
	}
}

func TestCephFSObserverDraftRejectsReplacedFilesystemAndPeerPolicy(t *testing.T) {
	for _, fault := range []string{"source-FSID", "destination-FSID", "metadata-pool", "peer", "peer-destination", "daemon-destination", "ownership", "mapping-missing"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination, a, _ := newCephFSObserverDraft()
			switch fault {
			case "source-FSID":
				source.filesystemID++
			case "destination-FSID":
				destination.filesystemID++
			case "metadata-pool":
				source.metadataPool++
			case "peer":
				source.peer = strings.Replace(source.peer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1)
			case "peer-destination":
				source.peer = strings.Replace(source.peer, `"client.peer"`, `"client.foreign"`, 1)
			case "daemon-destination":
				a.session = strings.Replace(a.session, `"destination-site"`, `"foreign-site"`, 1)
			case "ownership":
				delete(mirror.ownedDirectories, "/owned")
			case "mapping-missing":
				source.mapping = `{}`
			}
			status, err := mirror.DirectoryStatus(t.Context(), "/owned")
			if err == nil || status.Ready {
				t.Fatalf("native %s adopted: %+v %v", fault, status, err)
			}
			if len(source.mutations) != 0 {
				t.Fatal("observer modified foreign policy")
			}
		})
	}
}

func TestCephFSObserverDraftReplaySchemaAndFailureStates(t *testing.T) {
	for _, field := range []string{"state", "snaps_synced", "snaps_deleted", "snaps_renamed", "last_synced_snap.id", "last_synced_snap.name"} {
		for _, null := range []bool{false, true} {
			mirror, _, _, a, _ := newCephFSObserverDraft()
			var fields map[string]any
			_ = json.Unmarshal([]byte(a.replay), &fields)
			parts := strings.Split(field, ".")
			target := fields["/owned"].(map[string]any)
			key := parts[0]
			if len(parts) == 2 {
				target = target[key].(map[string]any)
				key = parts[1]
			}
			if null {
				target[key] = nil
			} else {
				delete(target, key)
			}
			bad, _ := json.Marshal(fields)
			a.replay = string(bad)
			status, err := mirror.DirectoryStatus(t.Context(), "/owned")
			if err == nil || status.Ready {
				t.Fatalf("missing/null %s accepted: %+v %v", field, status, err)
			}
		}
	}
	for _, session := range []string{`{"state":"failed"}`, `{"state":"blocklisted"}`} {
		mirror, _, _, a, _ := newCephFSObserverDraft()
		a.session = session
		status, err := mirror.DirectoryStatus(t.Context(), "/owned")
		if err == nil || status.Ready || status.DaemonProblems["a"] == "" {
			t.Fatalf("failed/blocklisted process accepted: %+v %v", status, err)
		}
	}
	for _, session := range []string{`{"state":null}`, `{"state":"unknown"}`} {
		mirror, _, _, a, _ := newCephFSObserverDraft()
		a.session = session
		if status, err := mirror.DirectoryStatus(t.Context(), "/owned"); err == nil || status.Ready {
			t.Fatalf("malformed native session accepted: %+v %v", status, err)
		}
	}
	mirror, _, _, a, _ := newCephFSObserverDraft()
	a.replay = `{"/owned":{"state":"failed","failure_reason":"snapshot metadata differs","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`
	status, err := mirror.DirectoryStatus(t.Context(), "/owned")
	if err != nil || status.Ready || status.State != "failed" || status.FailureReason == "" {
		t.Fatalf("native failure was hidden or accepted as ready: %+v %v", status, err)
	}
	a.replay = `{"/owned":{"state":"syncing","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`
	if status, err := mirror.DirectoryStatus(t.Context(), "/owned"); err == nil || status.Ready {
		t.Fatal("syncing without current snapshot became ready")
	}
	for _, field := range []string{"current_syncing_snap", "last_synced_snap"} {
		a.replay = fmt.Sprintf(`{"/owned":{"state":"idle",%q:null,"snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`, field)
		if status, err := mirror.DirectoryStatus(t.Context(), "/owned"); err == nil || status.Ready {
			t.Fatalf("explicit null snapshot accepted: %+v %v", status, err)
		}
	}
}

func TestCephFSObserverDraftWaitCheckpointIdentityAndContext(t *testing.T) {
	base := CephFSMirrorDirectoryStatus{SourceFilesystemID: 1, DestinationFilesystemID: 2, PeerID: cephFSObserverDraftPeer, Directory: "/owned", Ready: true, LastSyncedSnapshot: &CephFSMirrorSnapshot{ID: 119, Name: "checkpoint"}}
	expected := CephFSMirrorSnapshot{ID: 120, Name: "checkpoint"}
	calls := 0
	status, err := waitCephFSObservedDirectory(t.Context(), time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		calls++
		s := base
		if calls > 1 {
			s.LastSyncedSnapshot = &expected
		}
		return s, nil
	}, func(s CephFSMirrorDirectoryStatus) bool {
		return s.Ready && s.LastSyncedSnapshot != nil && *s.LastSyncedSnapshot == expected
	})
	if err != nil || calls != 2 || status.LastSyncedSnapshot.ID != 120 {
		t.Fatalf("same name wrong source ID satisfied checkpoint: %+v %v", status, err)
	}
	calls = 0
	_, err = waitCephFSObservedDirectory(t.Context(), time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		calls++
		s := base
		s.Ready = false
		if calls > 1 {
			s.PeerID = "replacement"
			s.Ready = true
		}
		return s, nil
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if err == nil || calls != 2 {
		t.Fatal("wait adopted a replaced peer")
	}
	calls = 0
	_, err = waitCephFSObservedDirectory(t.Context(), time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		calls++
		s := base
		s.Ready = false
		if calls > 1 {
			s.Directory = "/replacement"
			s.Ready = true
		}
		return s, nil
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if err == nil || calls != 2 {
		t.Fatal("wait adopted a replaced directory policy")
	}
	ctx, cancel := context.WithCancel(t.Context())
	queryErr := errors.New("private-native-output")
	status, err = waitCephFSObservedDirectory(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		cancel()
		return base, cephFSObserveQuery("native query", queryErr)
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, queryErr) || strings.Contains(err.Error(), queryErr.Error()) || status.SourceFilesystemID != 1 {
		t.Fatalf("wait lost observation/cancel/error or leaked output: %+v %v", status, err)
	}
	mirror, source, _, a, _ := newCephFSObserverDraft()
	mirror.mu.Lock()
	deadline, stop := context.WithTimeout(t.Context(), 15*time.Millisecond)
	_, err = mirror.WaitDirectoryReady(deadline, "/owned")
	stop()
	mirror.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || len(source.calls) != 0 {
		t.Fatal("lock acquisition ignored deadline")
	}
	a.running = false
	started := make(chan struct{})
	source.hook = func(*cephFSObserverDraftControl, []string) {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	deadline, stop = context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer stop()
	finished := make(chan error, 1)
	go func() { _, err := mirror.WaitDirectoryReady(deadline, "/owned"); finished <- err }()
	<-started
	if err := lockRGWSyncObservation(deadline, &mirror.mu); err != nil {
		t.Fatal("wait held lock across poll sleep")
	}
	mirror.mu.Unlock()
	if err := <-finished; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait lost deadline: %v", err)
	}
}

func TestCephFSObserverDraftPartialMemberExceptionIsNarrow(t *testing.T) {
	base := CephFSMirrorDirectoryStatus{SourceFilesystemID: 1, DestinationFilesystemID: 2, PeerID: cephFSObserverDraftPeer, Directory: "/owned", Ready: true, DaemonProblems: map[string]string{"b": "owned process stopped"}}
	partial := &cephFSObservationPartialError{problems: []error{cephFSObserveQuery("owned process stopped", nil)}}
	status, err := waitCephFSObservedDirectory(t.Context(), time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		return base, partial
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if err != nil || status.DaemonProblems["b"] == "" {
		t.Fatal("valid per-directory success hid unrelated partial member issue")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	selectedError := errors.New("private selected native output")
	status, err = waitCephFSObservedDirectory(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		return base, errors.Join(partial, cephFSObserveQuery("selected owner query", selectedError))
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, selectedError) || status.Ready || strings.Contains(err.Error(), selectedError.Error()) {
		t.Fatalf("broader error was ignored via partial exception: %+v %v", status, err)
	}
	calls := 0
	guardContext, guardCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer guardCancel()
	status, err = waitCephFSObservedDirectory(guardContext, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		calls++
		return base, errors.Join(partial, cephFSObserveGuard("native schema changed"))
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if err == nil || status.Ready || calls != 1 {
		t.Fatal("permanent native schema failure was retried or ignored")
	}
	ctx, cancel = context.WithCancel(t.Context())
	status, err = waitCephFSObservedDirectory(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
		cancel()
		return base, partial
	}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
	if !errors.Is(err, context.Canceled) || status.Ready || status.DaemonProblems["b"] == "" {
		t.Fatal("partial exception ignored cancel or hid last member problem")
	}
}

func TestCephFSObserverDraftPartialHandleAndCheckpointInputsFailClosed(t *testing.T) {
	for _, checkpoint := range []CephFSMirrorSnapshot{
		{ID: 0, Name: "checkpoint"},
		{ID: cephFSObservationMaxSnapshotID + 1, Name: "checkpoint"},
		{ID: cephFSObservationMaxSnapshotID + 2, Name: "checkpoint"},
		{ID: 120, Name: "../checkpoint"},
		{ID: 120, Name: ""},
	} {
		mirror, source, _, _, _ := newCephFSObserverDraft()
		if status, err := mirror.WaitSnapshotSynced(t.Context(), "/owned", checkpoint); err == nil || status.Ready || len(source.calls) != 0 {
			t.Fatalf("invalid checkpoint started native work: %+v %v", checkpoint, err)
		}
	}
	for _, missing := range []string{"source-FSID", "destination-FSID", "destination", "peer", "pending-import", "closed"} {
		mirror, source, _, _, _ := newCephFSObserverDraft()
		switch missing {
		case "source-FSID":
			mirror.filesystemID = 0
		case "destination-FSID":
			mirror.destinationFilesystemID = 0
		case "destination":
			mirror.destination = nil
		case "peer":
			mirror.peerID = ""
		case "pending-import":
			mirror.pendingPeerImport = &cephFSPeerIdentity{}
		case "closed":
			mirror.closed = true
		}
		if status, err := mirror.DirectoryStatus(t.Context(), "/owned"); err == nil || status.Ready || len(source.calls) != 0 {
			t.Fatalf("partial handle adopted native identities (%s): %+v %v", missing, status, err)
		}
	}
}

func TestCephFSObserverDraftWaitPeerSchemaFailsPermanently(t *testing.T) {
	validPeer := fmt.Sprintf(`{%q:{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}}`, cephFSObserverDraftPeer)
	faults := map[string]string{
		"malformed-json": `{`,
		"null-object":    `null`,
		"array-root":     `[]`,
		"invalid-uuid":   strings.Replace(validPeer, cephFSObserverDraftPeer, "not-a-uuid", 1),
		"foreign-uuid":   strings.Replace(validPeer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1),
		"null-entry":     fmt.Sprintf(`{%q:null}`, cephFSObserverDraftPeer),
		"array-entry":    fmt.Sprintf(`{%q:[]}`, cephFSObserverDraftPeer),
	}
	for _, field := range []string{"client_name", "site_name", "fs_name"} {
		for _, value := range []string{"missing", "null", "wrong-type", "empty"} {
			var peer map[string]map[string]any
			_ = json.Unmarshal([]byte(validPeer), &peer)
			entry := peer[cephFSObserverDraftPeer]
			switch value {
			case "missing":
				delete(entry, field)
			case "null":
				entry[field] = nil
			case "wrong-type":
				entry[field] = 7
			case "empty":
				entry[field] = ""
			}
			raw, _ := json.Marshal(peer)
			faults[field+"-"+value] = string(raw)
		}
	}
	for name, response := range faults {
		for _, phase := range []string{"before", "after"} {
			t.Run(name+"/"+phase, func(t *testing.T) {
				mirror, source, _, _, _ := newCephFSObserverDraft()
				peerQueries := 0
				source.hook = func(control *cephFSObserverDraftControl, args []string) {
					if !slices.Contains(args, "peer_list") {
						return
					}
					peerQueries++
					if phase == "before" || peerQueries == 2 {
						control.peer = response
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
				defer cancel()
				status, err := mirror.WaitSnapshotSynced(ctx, "/owned", CephFSMirrorSnapshot{ID: 120, Name: "checkpoint"})
				expectedQueries := 1
				if phase == "after" {
					expectedQueries = 2
				}
				if !errors.Is(err, errCephFSObservationGuard) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || status.Ready || status.SourceFilesystemID != 1 || peerQueries != expectedQueries {
					t.Fatalf("wait retried/adopted malformed peer policy: %+v err=%v queries=%d", status, err, peerQueries)
				}
			})
		}
	}
	// A failed CLI command is transient and preserves the original error on
	// timeout; successful malformed JSON above must never take this path.
	mirror, source, _, _, _ := newCephFSObserverDraft()
	queryErr := errors.New("private peer transport output")
	source.peerQueryError = queryErr
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	status, err := mirror.WaitDirectoryReady(ctx, "/owned")
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, queryErr) || errors.Is(err, errCephFSObservationGuard) || status.Ready || strings.Contains(err.Error(), queryErr.Error()) {
		t.Fatalf("peer CLI transport failure classified as schema guard or leaked/lost: %+v %v", status, err)
	}
}
