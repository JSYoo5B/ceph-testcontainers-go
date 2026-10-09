package multicluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	cephFSRefreshDestinationFSID = "f5d4aeee-b09b-4b0e-8712-b3f0af0b64ad"
	cephFSRefreshAddresses       = "[v2:172.20.0.10:3300,v1:172.20.0.10:6789] [v2:172.20.0.11:3300,v1:172.20.0.11:6789]"
)

type cephFSRefreshControl struct {
	*cephFSObserverDraftControl
	fsid, config                string
	reads, writes               int
	readErr, writeErr           error
	applyLostWrite, ignoreWrite bool
	hook                        func(*cephFSRefreshControl, []string)
	configCalls                 [][]string
	files                       map[string][]byte
	fileModes                   []int64
	copyErr, cleanupErr         error
}

func (control *cephFSRefreshControl) CopyToContainer(ctx context.Context, data []byte, file string, mode int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	control.fileModes = append(control.fileModes, mode)
	if control.files == nil {
		control.files = make(map[string][]byte)
	}
	control.files[file] = slices.Clone(data)
	return control.copyErr
}

func (control *cephFSRefreshControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	arguments := args
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		arguments = args[3:]
	}
	if control.hook != nil {
		control.hook(control, arguments)
	}
	if len(arguments) == 4 && slices.Equal(arguments[:3], []string{"rm", "-f", "--"}) {
		if control.cleanupErr != nil {
			return 0, nil, control.cleanupErr
		}
		delete(control.files, arguments[3])
		return 0, cephFSObserverDraftStream(""), nil
	}
	if slices.Equal(arguments, []string{"fsid"}) {
		return 0, cephFSObserverDraftStream(control.fsid + "\n"), nil
	}
	if len(arguments) >= 3 && arguments[0] == "config-key" {
		control.configCalls = append(control.configCalls, slices.Clone(arguments))
		if arguments[2] != "cephfs/mirror/peer/source/"+cephFSObserverDraftPeer {
			return 0, nil, errors.New("unowned config-key path")
		}
		switch arguments[1] {
		case "get":
			control.reads++
			if control.readErr != nil {
				return 0, nil, control.readErr
			}
			return 0, cephFSObserverDraftStream(control.config), nil
		case "set":
			control.writes++
			if len(arguments) != 5 || arguments[3] != "-i" || len(control.files[arguments[4]]) == 0 {
				return 0, nil, errors.New("bootstrap payload was placed in argv or lacks private file")
			}
			if !control.ignoreWrite && (control.writeErr == nil || control.applyLostWrite) {
				control.config = string(control.files[arguments[4]])
			}
			return 0, cephFSObserverDraftStream(""), control.writeErr
		}
	}
	return control.cephFSObserverDraftControl.Exec(ctx, args, opts...)
}

func newCephFSRefreshFixture() (*CephFSMirror, *cephFSRefreshControl, *cephFSRefreshControl) {
	mirror, source, destination, _, _ := newCephFSObserverDraft()
	sourceControl := &cephFSRefreshControl{cephFSObserverDraftControl: source, config: fmt.Sprintf(`{"fsid":%q,"key":"fixture-private-key","mon_host":"v2:old:3300","unknown":{"large":9007199254740993},"extension":[null,true]}`, cephFSRefreshDestinationFSID)}
	destinationControl := &cephFSRefreshControl{cephFSObserverDraftControl: destination, fsid: cephFSRefreshDestinationFSID}
	mirror.source = &ceph.Container{Container: sourceControl}
	mirror.destination = &ceph.Container{Container: destinationControl}
	mirror.daemons = []*CephFSMirrorDaemon{
		{Container: &cephFSMirrorDaemonFake{id: "a"}, DaemonName: "a"},
		{Container: &cephFSMirrorDaemonFake{id: "b"}, DaemonName: "b"},
	}
	return mirror, sourceControl, destinationControl
}

func cephFSRefreshCurrentAddresses(context.Context) (string, error) {
	return cephFSRefreshAddresses, nil
}

func TestCephFSRefreshLocalCurrentOwnershipPartialRetryAndNoRuntimeChanges(t *testing.T) {
	mirror, source, _ := newCephFSRefreshFixture()
	a, b := mirror.daemons[0], mirror.daemons[1]
	removed := &cephFSMirrorDaemonFake{id: "removed"}
	mirror.daemons = append(mirror.daemons,
		&CephFSMirrorDaemon{Container: removed, DaemonName: "removed", removed: true},
		&CephFSMirrorDaemon{Container: a.Container, DaemonName: "alias"},
	)
	// The callback deliberately exposes no State/Exec/Start/Stop implementation:
	// a stopped member must reach the archive refresh without any runtime call.
	written := make(map[string]bool)
	writes := make(map[string]int)
	failed := errors.New("archive temporarily unavailable")
	refresh := func(_ context.Context, client testcontainers.Container) error {
		id := client.GetContainerID()
		if id == "b" && failed != nil {
			return failed
		}
		if !written[id] {
			written[id] = true
			writes[id]++
		}
		return nil
	}
	if err := mirror.refreshMonitorConfig(t.Context(), refresh); !errors.Is(err, failed) || writes["a"] != 1 || written["b"] || writes["removed"] != 0 || len(source.mutations) != 0 {
		t.Fatalf("partial archive refresh lost successful copy or ownership: writes=%v err=%v", writes, err)
	}
	failed = nil
	if err := mirror.refreshMonitorConfig(t.Context(), refresh); err != nil || writes["a"] != 1 || writes["b"] != 1 || writes["removed"] != 0 {
		t.Fatalf("archive retry repeated successful writes or skipped failed member: %v %v", writes, err)
	}
	if a.Container.(*cephFSMirrorDaemonFake).terminateCalls != 0 || b.Container.(*cephFSMirrorDaemonFake).terminateCalls != 0 || removed.terminateCalls != 0 || mirror.peerID != cephFSObserverDraftPeer {
		t.Fatal("refresh changed process lifetime or peer identity")
	}
}

func TestCephFSRefreshLocalIdentityAndContextGuards(t *testing.T) {
	for _, fault := range []string{"source-filesystem", "destination-filesystem", "metadata", "peer", "peer-schema", "closed", "partial"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination := newCephFSRefreshFixture()
			switch fault {
			case "source-filesystem":
				source.filesystemID++
			case "destination-filesystem":
				destination.filesystemID++
			case "metadata":
				destination.metadataPool++
			case "peer":
				source.peer = strings.Replace(source.peer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1)
			case "peer-schema":
				source.peer = "null"
			case "closed":
				mirror.closed = true
			case "partial":
				mirror.destinationFilesystemID = 0
			}
			calls := 0
			err := mirror.refreshMonitorConfig(t.Context(), func(context.Context, testcontainers.Container) error { calls++; return nil })
			if err == nil || calls != 0 {
				t.Fatalf("foreign/partial identity refreshed files: %s calls=%d err=%v", fault, calls, err)
			}
		})
	}
	for _, lock := range []string{"fixture", "daemon"} {
		t.Run(lock, func(t *testing.T) {
			mirror, _, _ := newCephFSRefreshFixture()
			mutex := &mirror.mu
			if lock == "daemon" {
				mutex = &mirror.daemons[0].mu
			}
			mutex.Lock()
			defer mutex.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
			defer cancel()
			calls := 0
			err := mirror.refreshMonitorConfig(ctx, func(context.Context, testcontainers.Container) error { calls++; return nil })
			if !errors.Is(err, context.DeadlineExceeded) || calls != 0 {
				t.Fatalf("lock acquisition ignored deadline: %v", err)
			}
		})
	}
	mirror, source, _ := newCephFSRefreshFixture()
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := mirror.refreshMonitorConfig(ctx, func(context.Context, testcontainers.Container) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 || len(source.mutations) != 0 {
		t.Fatalf("cancel after partial copy lost cancellation: %v", err)
	}
	mirror, source, _ = newCephFSRefreshFixture()
	err = mirror.refreshMonitorConfig(t.Context(), func(context.Context, testcontainers.Container) error {
		source.peer = strings.Replace(source.peer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1)
		return nil
	})
	if !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("post-copy peer drift was not rejected")
	}
	var nilMirror *CephFSMirror
	if err := nilMirror.RefreshMonitorConfig(t.Context()); err == nil {
		t.Fatal("nil fixture accepted refresh")
	}
	if err := nilMirror.RefreshPeerMonitorConfig(t.Context()); err == nil {
		t.Fatal("nil fixture accepted peer refresh")
	}
	if _, err := nilMirror.RebootstrapPeer(t.Context()); err == nil {
		t.Fatal("nil fixture accepted bootstrap")
	}
}

func TestCephFSRefreshPeerPreservesExactOwnedConfigurationAndUUID(t *testing.T) {
	mirror, source, _ := newCephFSRefreshFixture()
	before, err := decodeCephFSBootstrapFields([]byte(source.config), cephFSRefreshDestinationFSID, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.refreshPeerMonitorConfig(t.Context(), cephFSRefreshCurrentAddresses); err != nil {
		t.Fatal(err)
	}
	after, err := decodeCephFSBootstrapFields([]byte(source.config), cephFSRefreshDestinationFSID, false)
	if err != nil {
		t.Fatal(err)
	}
	before["mon_host"], _ = json.Marshal(cephFSRefreshAddresses)
	if !cephFSBootstrapFieldsEqual(before, after) || source.writes != 1 || mirror.peerID != cephFSObserverDraftPeer || mirror.pendingPeerImport != nil || len(source.mutations) != 0 {
		t.Fatal("refresh changed fields besides owned peer mon_host")
	}
	if err := mirror.refreshPeerMonitorConfig(t.Context(), cephFSRefreshCurrentAddresses); err != nil || source.writes != 1 {
		t.Fatalf("current peer refresh was not idempotent: %v", err)
	}
	for _, args := range source.configCalls {
		if args[2] != "cephfs/mirror/peer/source/"+cephFSObserverDraftPeer {
			t.Fatal("refresh targeted another peer")
		}
	}
}

func TestCephFSRefreshPeerLostWriteReadbackAndIdentityDrift(t *testing.T) {
	mirror, source, _ := newCephFSRefreshFixture()
	lost := errors.New("private config-key write output")
	source.writeErr = lost
	source.applyLostWrite = true
	if err := mirror.refreshPeerMonitorConfig(t.Context(), cephFSRefreshCurrentAddresses); !errors.Is(err, lost) || strings.Contains(err.Error(), lost.Error()) || source.writes != 1 {
		t.Fatalf("lost response was leaked/lost: %v", err)
	}
	source.writeErr = nil
	if err := mirror.refreshPeerMonitorConfig(t.Context(), cephFSRefreshCurrentAddresses); err != nil || source.writes != 1 {
		t.Fatalf("reconciliation repeated an applied write: %v", err)
	}
	for _, fault := range []string{"ignored-write", "readback-error", "changed-key", "peer-drift", "destination-FSID", "destination-addresses"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination := newCephFSRefreshFixture()
			queryErr := errors.New("private readback output")
			addressCalls := 0
			addresses := func(context.Context) (string, error) {
				addressCalls++
				if fault == "destination-addresses" && addressCalls > 1 {
					return cephFSRefreshAddresses + " [v2:172.20.0.12:3300]", nil
				}
				return cephFSRefreshAddresses, nil
			}
			source.ignoreWrite = fault == "ignored-write"
			source.hook = func(control *cephFSRefreshControl, args []string) {
				if len(args) < 2 || args[0] != "config-key" || args[1] != "get" || control.writes == 0 {
					return
				}
				switch fault {
				case "readback-error":
					control.readErr = queryErr
				case "changed-key":
					control.config = strings.Replace(control.config, "fixture-private-key", "foreign-key", 1)
				case "peer-drift":
					control.peer = strings.Replace(control.peer, cephFSObserverDraftPeer, "12117353-8cd1-44db-976b-eb20609aa160", 1)
				case "destination-FSID":
					destination.fsid = "12117353-8cd1-44db-976b-eb20609aa160"
				}
			}
			err := mirror.refreshPeerMonitorConfig(t.Context(), addresses)
			if err == nil || source.writes != 1 || mirror.peerID != cephFSObserverDraftPeer || strings.Contains(err.Error(), "fixture-private-key") || strings.Contains(err.Error(), queryErr.Error()) {
				t.Fatalf("unconfirmed refresh accepted/leaked or replaced peer: %v", err)
			}
			if fault == "readback-error" && !errors.Is(err, queryErr) {
				t.Fatal("readback error lost from chain")
			}
		})
	}
}

func TestCephFSRefreshPeerRefusesMalformedBootstrapBeforeWrites(t *testing.T) {
	for _, fault := range []string{"object-null", "object-array", "malformed", "foreign-fsid", "missing-fsid", "null-key", "empty-key", "wrong-mon-host", "peer-schema", "original-filesystem", "addresses-query"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination := newCephFSRefreshFixture()
			var fields map[string]any
			_ = json.Unmarshal([]byte(source.config), &fields)
			addresses := cephFSRefreshCurrentAddresses
			switch fault {
			case "object-null":
				source.config = "null"
			case "object-array":
				source.config = "[]"
			case "malformed":
				source.config = "{"
			case "foreign-fsid":
				fields["fsid"] = "12117353-8cd1-44db-976b-eb20609aa160"
			case "missing-fsid":
				delete(fields, "fsid")
			case "null-key":
				fields["key"] = nil
			case "empty-key":
				fields["key"] = ""
			case "wrong-mon-host":
				fields["mon_host"] = 7
			case "peer-schema":
				source.peer = "null"
			case "original-filesystem":
				destination.filesystemID++
			case "addresses-query":
				addresses = func(context.Context) (string, error) { return "", errors.New("private quorum output") }
			}
			if fieldsChanged := slices.Contains([]string{"foreign-fsid", "missing-fsid", "null-key", "empty-key", "wrong-mon-host"}, fault); fieldsChanged {
				raw, _ := json.Marshal(fields)
				source.config = string(raw)
			}
			if err := mirror.refreshPeerMonitorConfig(t.Context(), addresses); err == nil || source.writes != 0 || strings.Contains(err.Error(), "fixture-private-key") || strings.Contains(err.Error(), "private quorum output") {
				t.Fatalf("malformed bootstrap reached writes or leaked: %v", err)
			}
		})
	}
}

func TestCephFSRefreshPeerPrivateFileCleanupAndCanceledWrite(t *testing.T) {
	for _, fault := range []string{"success", "copy-error", "cleanup-error", "cancel-after-set"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, _ := newCephFSRefreshFixture()
			before := source.config
			privateErr := errors.New("fixture-private-key in transport output")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "copy-error":
				source.copyErr = privateErr
			case "cleanup-error":
				source.cleanupErr = privateErr
			case "cancel-after-set":
				source.hook = func(_ *cephFSRefreshControl, args []string) {
					if len(args) > 1 && args[0] == "config-key" && args[1] == "set" {
						cancel()
					}
				}
			}
			err := mirror.refreshPeerMonitorConfig(ctx, cephFSRefreshCurrentAddresses)
			if len(source.fileModes) != 1 || source.fileModes[0] != 0o600 {
				t.Fatalf("bootstrap file was not private: %v", source.fileModes)
			}
			for _, args := range source.configCalls {
				if strings.Contains(strings.Join(args, " "), "fixture-private-key") {
					t.Fatal("secret payload entered command argv")
				}
				if args[1] == "set" && (len(args) != 5 || args[3] != "-i" || !strings.HasPrefix(args[4], "/tmp/tc-cephfs-peer-refresh-")) {
					t.Fatal("config-key did not read the owned private temporary file")
				}
			}
			if fault != "cleanup-error" && len(source.files) != 0 {
				t.Fatal("private file was not cleaned after success/error/cancellation")
			}
			if fault == "success" && err != nil {
				t.Fatal(err)
			}
			if fault == "copy-error" && (!errors.Is(err, privateErr) || source.writes != 0 || source.config != before) {
				t.Fatalf("copy error changed peer metadata or lost original error: %v", err)
			}
			if fault == "cleanup-error" && (!errors.Is(err, privateErr) || len(source.files) != 1 || source.writes != 1) {
				t.Fatalf("cleanup failure lost owned partial write/error: %v", err)
			}
			if fault == "cancel-after-set" && (!errors.Is(err, context.Canceled) || source.writes != 1) {
				t.Fatalf("canceled applied write was adopted as success or lost: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "fixture-private-key") {
				t.Fatal("private transport output leaked from error")
			}
		})
	}
}

func TestCephFSRebootstrapOriginalIdentityAndContextGuardsBeforeManagers(t *testing.T) {
	for _, fault := range []string{"closed", "partial", "source-filesystem", "destination-filesystem", "destination-metadata"} {
		t.Run(fault, func(t *testing.T) {
			mirror, source, destination := newCephFSRefreshFixture()
			switch fault {
			case "closed":
				mirror.closed = true
			case "partial":
				mirror.destinationFilesystemID = 0
			case "source-filesystem":
				source.filesystemID++
			case "destination-filesystem":
				destination.filesystemID++
			case "destination-metadata":
				destination.metadataPool++
			}
			if _, err := mirror.RebootstrapPeer(t.Context()); !errors.Is(err, errCephFSObservationGuard) || len(source.mutations) != 0 || len(destination.mutations) != 0 {
				t.Fatalf("bootstrap crossed original identity guard: %v", err)
			}
		})
	}
	mirror, source, _ := newCephFSRefreshFixture()
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()
	if _, err := mirror.RebootstrapPeer(ctx); !errors.Is(err, context.DeadlineExceeded) || len(source.calls) != 0 {
		t.Fatalf("bootstrap mutex ignored context: %v", err)
	}
}

func TestCephFSBootstrapTokenUsesCurrentDestinationMonitorsAndPreservesCredentials(t *testing.T) {
	peer := cephFSPeerIdentity{ClientName: "client.peer", SiteName: "destination-site", FilesystemName: "destination"}
	identity := cephFSRemoteMonitorIdentity{FSID: cephFSRefreshDestinationFSID, Addresses: cephFSRefreshAddresses}
	data := fmt.Sprintf(`{"fsid":%q,"filesystem":"destination","user":"client.peer","site_name":"destination-site","key":"fixture-private-key","mon_host":"v2:warm-mgr-old:3300","unknown":{"large":9007199254740993}}`, identity.FSID)
	token := base64.StdEncoding.EncodeToString([]byte(data))
	normalized, err := normalizeCephFSBootstrapToken(token, peer, identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(normalized)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := decodeCephFSBootstrapFields([]byte(data), identity.FSID, true)
	after, err := decodeCephFSBootstrapFields(raw, identity.FSID, true)
	before["mon_host"], _ = json.Marshal(identity.Addresses)
	if err != nil || !cephFSBootstrapFieldsEqual(before, after) {
		t.Fatal("token normalization changed credentials or extensions")
	}
	for _, field := range []string{"fsid", "filesystem", "user", "site_name", "key", "mon_host"} {
		for _, fault := range []string{"missing", "null", "wrong-type", "empty", "foreign"} {
			t.Run(field+"/"+fault, func(t *testing.T) {
				var fields map[string]any
				_ = json.Unmarshal([]byte(data), &fields)
				switch fault {
				case "missing":
					delete(fields, field)
				case "null":
					fields[field] = nil
				case "wrong-type":
					fields[field] = 7
				case "empty":
					fields[field] = ""
				case "foreign":
					if field == "key" || field == "mon_host" {
						return
					}
					fields[field] = "foreign"
				}
				bad, _ := json.Marshal(fields)
				encoded := base64.StdEncoding.EncodeToString(bad)
				if _, err := normalizeCephFSBootstrapToken(encoded, peer, identity); err == nil || strings.Contains(err.Error(), "fixture-private-key") || strings.Contains(err.Error(), encoded) {
					t.Fatalf("invalid token accepted/leaked: %v", err)
				}
			})
		}
	}
	if _, err := normalizeCephFSBootstrapToken("not-base64", peer, identity); err == nil {
		t.Fatal("malformed token accepted")
	}
}
