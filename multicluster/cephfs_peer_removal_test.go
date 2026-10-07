package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const peerRemovalSourceFSID = "7875b9c0-2eac-44d7-8358-888700000001"
const peerRemovalDestinationFSID = "7875b9c0-2eac-44d7-8358-888700000002"
const peerRemovalNewPeer = "7875b9c0-2eac-44d7-8358-888700000003"

var peerRemovalTransport = errors.New("native transport unavailable")

// This fake exercises the production orchestration/parser with native shapes.
// Only original private cluster attestation uses a private callback; the public
// API separately rejects incomplete fixtures instead of admitting native FSID.
type peerRemovalControlFake struct {
	testcontainers.Container
	name                        string
	fsid                        string
	id                          int
	pool                        int64
	fsPeers, managerPeers       string
	watchers                    string
	fsOverride, managerOverride string
	queryError                  error
	removeError                 error
	applyRemoval                bool
	calls                       [][]string
	removals                    int
	hook                        func(*peerRemovalControlFake, []string)
}

func (f *peerRemovalControlFake) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	f.calls = append(f.calls, slices.Clone(args))
	if f.hook != nil {
		f.hook(f, args)
	}
	if f.queryError != nil {
		return 0, nil, f.queryError
	}
	a := args
	if len(a) >= 3 && a[0] == "ceph" && a[1] == "--connect-timeout" {
		a = a[3:]
	}
	var out string
	switch {
	case slices.Equal(a, []string{"fsid"}):
		out = f.fsid + "\n"
	case len(a) == 5 && slices.Equal(a[:2], []string{"fs", "get"}) && a[2] == f.name:
		if f.fsOverride != "" {
			out = f.fsOverride
		} else {
			out = fmt.Sprintf(`{"id":%d,"mdsmap":{"fs_name":%q,"metadata_pool":%d},"mirror_info":{"peers":%s}}`, f.id, f.name, f.pool, f.fsPeers)
		}
	case len(a) == 5 && slices.Equal(a[:4], []string{"fs", "snapshot", "mirror", "peer_list"}) && a[4] == f.name:
		out = f.managerPeers
		if f.managerOverride != "" {
			out = f.managerOverride
		}
	case len(a) == 6 && slices.Equal(a[:4], []string{"fs", "snapshot", "mirror", "peer_remove"}) && a[4] == f.name && a[5] == cephFSObserverDraftPeer:
		f.removals++
		if f.applyRemoval {
			f.fsPeers = "{}"
			f.managerPeers = "{}"
		}
		if f.removeError != nil {
			return 0, nil, f.removeError
		}
		out = "{}"
	case slices.Equal(a, []string{"rados", "--pool", "source-metadata", "listwatchers", "cephfs_mirror"}):
		out = f.watchers
	default:
		return 0, nil, errors.New("unexpected native peer removal command")
	}
	return 0, cephFSObserverDraftStream(out), nil
}

type peerRemovalDaemonFake struct {
	testcontainers.Container
	id, address                      string
	state                            container.State
	peer, catalogPeer                bool
	sessionOverride, catalogOverride string
	queryError                       error
	calls                            [][]string
	stateHook                        func(*peerRemovalDaemonFake)
	execHook                         func(*peerRemovalDaemonFake, []string)
	terminated                       int
}

func (f *peerRemovalDaemonFake) GetContainerID() string { return f.id }
func (f *peerRemovalDaemonFake) State(ctx context.Context) (*container.State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.stateHook != nil {
		f.stateHook(f)
	}
	s := f.state
	return &s, nil
}
func (f *peerRemovalDaemonFake) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	f.terminated++
	return nil
}
func (f *peerRemovalDaemonFake) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	f.calls = append(f.calls, slices.Clone(args))
	if f.execHook != nil {
		f.execHook(f, args)
	}
	if f.queryError != nil {
		return 0, nil, f.queryError
	}
	var out string
	switch {
	case slices.Equal(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "source@1"}):
		out = f.sessionOverride
		if out == "" {
			peers := "{}"
			if f.peer {
				peers = peerRemovalRemotePeers()
			}
			out = fmt.Sprintf(`{"rados_inst":%q,"peers":%s,"snap_dirs":{"dir_count":1}}`, f.address, peers)
		}
	case slices.Equal(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help"}):
		out = f.catalogOverride
		if out == "" {
			commands := map[string]string{"help": "help", "get_command_descriptions": "list", "fs mirror status source@1": "get filesystem mirror status"}
			if f.catalogPeer {
				commands["fs mirror peer status source@1 "+cephFSObserverDraftPeer] = "get peer mirror status"
			}
			v, _ := json.Marshal(commands)
			out = string(v)
		}
	default:
		return 0, nil, errors.New("unexpected peer removal admin command")
	}
	return 0, cephFSObserverDraftStream(out), nil
}
func peerRemovalRemotePeers() string {
	return fmt.Sprintf(`{%q:{"remote":{"client_name":"client.peer","cluster_name":"destination-site","fs_name":"destination"}}}`, cephFSObserverDraftPeer)
}
func peerRemovalManagerPeers() string {
	return fmt.Sprintf(`{%q:{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}}`, cephFSObserverDraftPeer)
}
func peerRemovalClusterReader(context.Context) ([2]string, error) {
	return [2]string{peerRemovalSourceFSID, peerRemovalDestinationFSID}, nil
}
func newPeerRemovalFixture() (*CephFSMirror, *peerRemovalControlFake, *peerRemovalControlFake, *peerRemovalDaemonFake, *peerRemovalDaemonFake) {
	source := &peerRemovalControlFake{name: "source", fsid: peerRemovalSourceFSID, id: 1, pool: 7, fsPeers: peerRemovalRemotePeers(), managerPeers: peerRemovalManagerPeers(), applyRemoval: true, watchers: "watcher=172.20.0.8:0/1234 client.4262 cookie=1\nwatcher=172.20.0.9:0/5678 client.4266 cookie=2\n"}
	destination := &peerRemovalControlFake{name: "destination", fsid: peerRemovalDestinationFSID, id: 2, pool: 9, fsPeers: "{}", managerPeers: "{}"}
	makeDaemon := func(id, address string) *peerRemovalDaemonFake {
		return &peerRemovalDaemonFake{id: id, address: address, state: container.State{Running: true, StartedAt: "2026-10-07T01:00:00Z"}, peer: true, catalogPeer: true}
	}
	a := makeDaemon(strings.Repeat("a", 64), "172.20.0.8:0/1234")
	b := makeDaemon(strings.Repeat("b", 64), "172.20.0.9:0/5678")
	mirror := &CephFSMirror{source: &ceph.Container{Container: source}, destination: &ceph.Container{Container: destination}, SourceFilesystem: "source", DestinationFilesystem: "destination", SourceClientEntity: "client.owned-auth", DestinationClientEntity: "client.peer", filesystemID: 1, destinationFilesystemID: 2, metadataPool: "source-metadata", metadataPoolID: 7, destinationMetadataPoolID: 9, destinationSite: "destination-site", peerID: cephFSObserverDraftPeer, Directories: []string{"/owned"}, ownedDirectories: map[string]bool{"/owned": true}, daemonImage: "unchanged", daemonKeyring: []byte("private-fixture-secret"), daemons: []*CephFSMirrorDaemon{{Container: a, DaemonName: "a"}, {Container: b, DaemonName: "b"}}}
	for _, d := range mirror.daemons {
		mirror.owned.addContainer(d)
	}
	return mirror, source, destination, a, b
}
func beginPeerRemovalFixture(t *testing.T) (*CephFSMirrorPeerRemoval, *CephFSMirror, *peerRemovalControlFake, *peerRemovalControlFake, *peerRemovalDaemonFake, *peerRemovalDaemonFake) {
	t.Helper()
	m, s, d, a, b := newPeerRemovalFixture()
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if err != nil || r == nil {
		t.Fatalf("begin original removal: %v", err)
	}
	return r, m, s, d, a, b
}
func drainPeerRemovalDaemons(ds ...*peerRemovalDaemonFake) {
	for _, d := range ds {
		d.peer = false
		d.catalogPeer = false
	}
}

func TestCephFSPeerRemovalProductionCommandUnregisterBarrier(t *testing.T) {
	r, m, s, _, a, b := beginPeerRemovalFixture(t)
	if s.removals != 1 || m.peerID != "" || len(r.cohort) != 2 {
		t.Fatal("original native request/cohort not retained")
	}
	a.peer = false
	b.peer = false
	status, err := r.Status(t.Context())
	if err != nil || status.Drained || !status.PolicyRemoved || status.Daemons["a"].State != "awaiting-command-unregister" {
		t.Fatalf("peer map absence mistaken for worker teardown: %+v %v", status, err)
	}
	a.catalogPeer = false
	status, err = r.Status(t.Context())
	if err != nil || status.Drained || status.Daemons["a"].State != "drained" || status.Daemons["b"].State != "awaiting-command-unregister" {
		t.Fatalf("partial cohort: %+v %v", status, err)
	}
	b.catalogPeer = false
	status, err = r.WaitDrained(t.Context())
	if err != nil || !status.Drained || !r.completed {
		t.Fatalf("same-session exact unregister: %+v %v", status, err)
	}
	if a.terminated+b.terminated != 0 || s.removals != 1 || len(m.owned.actions) != 2 {
		t.Fatal("observer changed lifetime/cleanup/native policy")
	}
}

func TestCephFSPeerRemovalProductionRefusesInvalidPreflightWithoutMutation(t *testing.T) {
	for _, fault := range []string{"zero", "stopped", "paused", "restarting", "dead", "generation", "catalog-missing", "session-missing", "watcher-missing", "watcher-foreign", "duplicate-CID", "partial-CID", "foreign-UUID", "null-peers", "disabled", "metadata", "name", "source-client", "peer-pending", "equal-clusters", "unavailable-original"} {
		t.Run(fault, func(t *testing.T) {
			m, s, _, a, b := newPeerRemovalFixture()
			reader := peerRemovalClusterReader
			switch fault {
			case "zero":
				m.daemons = nil
			case "stopped":
				a.state.Running = false
			case "paused":
				a.state.Paused = true
			case "restarting":
				a.state.Restarting = true
			case "dead":
				a.state.Dead = true
			case "generation":
				a.state.StartedAt = "0001-01-01T00:00:00Z"
			case "catalog-missing":
				a.catalogPeer = false
			case "session-missing":
				a.peer = false
			case "watcher-missing":
				s.watchers = "watcher=172.20.0.9:0/5678 client.4266 cookie=2\n"
			case "watcher-foreign":
				s.watchers += "watcher=172.20.0.10:0/9 client.4999 cookie=3\n"
			case "duplicate-CID":
				b.id = a.id
			case "partial-CID":
				a.id = "partial"
			case "foreign-UUID":
				s.managerPeers = strings.Replace(s.managerPeers, cephFSObserverDraftPeer, peerRemovalNewPeer, 1)
			case "null-peers":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7},"mirror_info":{"peers":null}}`
			case "disabled":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7,"enabled":true}}`
			case "metadata":
				s.pool = 8
			case "name":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"substituted","metadata_pool":7},"mirror_info":{"peers":{}}}`
			case "source-client":
				m.SourceClientEntity = ""
			case "peer-pending":
				m.pendingPeerImport = &cephFSPeerIdentity{}
			case "equal-clusters":
				reader = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalSourceFSID, peerRemovalSourceFSID}, nil
				}
			case "unavailable-original":
				reader = nil
			}
			r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, reader)
			if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 || m.peerRemoval != nil {
				t.Fatalf("preflight changed policy: receipt=%v err=%v calls=%d", r, err, s.removals)
			}
		})
	}
	m, s, _, _, _ := newPeerRemovalFixture()
	r, err := m.BeginPeerRemoval(t.Context(), cephFSObserverDraftPeer)
	if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 {
		t.Fatalf("public API adopted incomplete fixture/native FSID: %v", err)
	}
}

func TestCephFSPeerRemovalProductionRetainsLostReplyAndReconcilesNativeViews(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			m, s, _, a, b := newPeerRemovalFixture()
			s.removeError = peerRemovalTransport
			s.applyRemoval = applied
			r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			if r == nil || !errors.Is(err, peerRemovalTransport) || m.peerRemoval != r {
				t.Fatalf("lost response erased intent: %v %v", r, err)
			}
			s.removeError = nil
			s.applyRemoval = true
			if applied {
				s.managerPeers = peerRemovalManagerPeers()
			}
			again, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			if again != r {
				t.Fatal("retry replaced original receipt")
			}
			if applied {
				if err == nil || s.removals != 1 {
					t.Fatalf("cached MGR state caused duplicate delete: %v calls=%d", err, s.removals)
				}
				s.managerPeers = "{}"
				again, err = m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			}
			expected := 2
			if applied {
				expected = 1
			}
			if err != nil || again != r || s.removals != expected {
				t.Fatalf("original retry: %v calls=%d", err, s.removals)
			}
			drainPeerRemovalDaemons(a, b)
			status, err := r.Status(t.Context())
			if err != nil || !status.Drained {
				t.Fatalf("lost reply reconciliation did not finish: %+v %v", status, err)
			}
		})
	}
}

func TestCephFSPeerRemovalProductionOverlapGuardsAndLegacyContract(t *testing.T) {
	r, m, s, _, a, b := beginPeerRemovalFixture(t)
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"rebootstrap", func() error { _, err := m.RebootstrapPeer(t.Context()); return err }},
		{"add-daemon", func() error { _, err := m.AddDaemon(t.Context(), "new"); return err }},
		{"add-directory", func() error { return m.AddDirectory(t.Context(), "/new") }},
		{"remove-directory", func() error { return m.RemoveDirectory(t.Context(), "/owned") }},
		{"rebalance", func() error { return m.RebalanceDirectories(t.Context()) }},
		{"remove-unowned", func() error { return m.RemovePeer(t.Context(), peerRemovalNewPeer) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, errCephFSObservationGuard) {
				t.Fatalf("pending explicit intent allowed overlap: %v", err)
			}
		})
	}
	if s.removals != 1 || len(m.Directories) != 1 {
		t.Fatal("pending gate mutated native policy")
	}
	if err := m.RemovePeer(t.Context(), r.peerID); err != nil || s.removals != 1 {
		t.Fatalf("legacy same-intent retry resent accepted delete: %v", err)
	}
	drainPeerRemovalDaemons(a, b)
	if st, err := r.Status(t.Context()); err != nil || !st.Drained {
		t.Fatal(err)
	}
	if err := m.guardPeerRemovalOverlap(); err != nil {
		t.Fatal("completed receipt keeps import gate closed")
	}
	legacy, ls, _, _, _ := newPeerRemovalFixture()
	legacy.daemons = nil
	if err := legacy.RemovePeer(t.Context(), cephFSObserverDraftPeer); err != nil || ls.removals != 1 || legacy.peerRemoval != nil || legacy.peerID != "" {
		t.Fatalf("legacy zero-daemon contract changed: %v", err)
	}
}

func TestCephFSPeerRemovalProductionOriginalIdentityAndSchemaArePermanent(t *testing.T) {
	for _, fault := range []string{"CID", "daemon-name", "generation", "session", "watcher", "watcher-set", "peer-schema", "peer-null", "peer-UUID", "peer-tuple", "fs-schema", "fs-disabled", "fs-name", "fs-ID", "fs-pool", "destination-ID", "closed", "generation-superseded", "fixture-field", "cluster", "catalog-null", "catalog-field", "catalog-FS", "session-null", "session-state"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, d, a, b := beginPeerRemovalFixture(t)
			drainPeerRemovalDaemons(a, b)
			switch fault {
			case "CID":
				a.id = strings.Repeat("c", 64)
			case "daemon-name":
				m.daemons[0].DaemonName = "changed"
			case "generation":
				a.state.StartedAt = "2026-10-07T01:01:00Z"
			case "session":
				a.address = "172.20.0.18:0/1"
			case "watcher":
				s.watchers = strings.Replace(s.watchers, "client.4262", "client.9999", 1)
			case "watcher-set":
				s.watchers += "watcher=172.20.0.18:0/1 client.4999 cookie=3\n"
			case "peer-schema":
				s.managerOverride = `[]`
			case "peer-null":
				s.managerOverride = `null`
			case "peer-UUID":
				s.managerOverride = strings.Replace(peerRemovalManagerPeers(), cephFSObserverDraftPeer, peerRemovalNewPeer, 1)
			case "peer-tuple":
				s.managerOverride = strings.Replace(peerRemovalManagerPeers(), "client.peer", "client.substituted", 1)
			case "fs-schema":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7},"mirror_info":{"peers":[]}}`
			case "fs-disabled":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7}}`
			case "fs-name":
				s.fsOverride = `{"id":1,"mdsmap":{"fs_name":"substituted","metadata_pool":7},"mirror_info":{"peers":{}}}`
			case "fs-ID":
				s.id = 3
			case "fs-pool":
				s.pool = 9
			case "destination-ID":
				d.id = 4
			case "closed":
				m.closed = true
			case "generation-superseded":
				m.peerGeneration++
			case "fixture-field":
				m.DestinationFilesystem = "other"
			case "cluster":
				r.readClusters = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalDestinationFSID, peerRemovalSourceFSID}, nil
				}
			case "catalog-null":
				a.catalogOverride = `null`
			case "catalog-field":
				a.catalogOverride = `{"help":"help","get_command_descriptions":"list","fs mirror status source@1":null}`
			case "catalog-FS":
				a.catalogOverride = `{"help":"help","get_command_descriptions":"list","fs mirror status other@1":"status"}`
			case "session-null":
				a.sessionOverride = `null`
			case "session-state":
				a.sessionOverride = `{"state":"unknown"}`
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			status, err := r.WaitDrained(ctx)
			if status.Drained || !errors.Is(err, errCephFSObservationGuard) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("schema/identity drift retried/accepted: %+v %v", status, err)
			}
		})
	}
}

func TestCephFSPeerRemovalProductionKnownNativeFailureAndCancellationPreserveLast(t *testing.T) {
	r, _, _, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	a.queryError = peerRemovalTransport
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Complete the actual native-error observation before triggering cancellation.
	// A short wall-clock deadline can expire before the first observation under
	// compiler/native-suite load, which cannot legitimately retain an unseen cause.
	status, err := waitCephFSPeerDrain(ctx, time.Millisecond, func(queryCtx context.Context) (CephFSMirrorPeerRemovalStatus, error) {
		observed, observationErr := r.Status(queryCtx)
		cancel()
		return observed, observationErr
	})
	if status.Drained || status.PeerID != cephFSObserverDraftPeer || status.Daemons["a"].Problem == "" || !errors.Is(err, context.Canceled) || !errors.Is(err, peerRemovalTransport) {
		t.Fatalf("cancellation lost report/native cause: %+v %v", status, err)
	}
	a.queryError = nil
	a.sessionOverride = `{"state":"blocklisted"}`
	status, err = r.Status(t.Context())
	if status.Drained || err == nil || errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("known failure state treated as valid session/permanent schema: %+v %v", status, err)
	}
	a.sessionOverride = ""
	cancelCtx, stop := context.WithCancel(t.Context())
	status, err = waitCephFSPeerDrain(cancelCtx, time.Millisecond, func(context.Context) (CephFSMirrorPeerRemovalStatus, error) {
		stop()
		return CephFSMirrorPeerRemovalStatus{PeerID: "last", Drained: true}, peerRemovalTransport
	})
	if status.Drained || status.PeerID != "last" || !errors.Is(err, context.Canceled) || !errors.Is(err, peerRemovalTransport) {
		t.Fatalf("cancel after ready lost report/cause: %+v %v", status, err)
	}
	status, err = waitCephFSPeerDrain(t.Context(), time.Millisecond, func(context.Context) (CephFSMirrorPeerRemovalStatus, error) {
		return CephFSMirrorPeerRemovalStatus{Drained: true}, errors.Join(peerRemovalTransport, cephFSObserveGuard("permanent"))
	})
	if status.Drained || !errors.Is(err, errCephFSObservationGuard) || !errors.Is(err, peerRemovalTransport) {
		t.Fatalf("joined guard hidden by transport failure: %+v %v", status, err)
	}
}

func TestCephFSPeerRemovalProductionContextLocksAndWaitRelease(t *testing.T) {
	r, m, _, _, a, b := beginPeerRemovalFixture(t)
	for _, mutex := range []string{"fixture", "daemon"} {
		t.Run(mutex, func(t *testing.T) {
			unlock := m.mu.Unlock
			if mutex == "fixture" {
				m.mu.Lock()
			} else {
				m.daemons[0].mu.Lock()
				unlock = m.daemons[0].mu.Unlock
			}
			defer unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			start := time.Now()
			st, err := r.Status(ctx)
			if !errors.Is(err, context.DeadlineExceeded) || st.Drained || time.Since(start) > time.Second {
				t.Fatalf("caller deadline blocked on %s: %v", mutex, err)
			}
		})
	}
	// The waiting goroutine polls while a mutation-free same-lock operation can
	// make the cohort advance; this catches holding mirror.mu across poll/sleep.
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	go func() {
		st, err := waitCephFSPeerDrain(ctx, time.Millisecond, r.Status)
		if err == nil && !st.Drained {
			err = errors.New("missing proof")
		}
		done <- err
	}()
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		t.Fatal(err)
	}
	drainPeerRemovalDaemons(a, b)
	m.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("poll held topology lock: %v", err)
	}
}

func TestCephFSPeerRemovalProductionCleanupRetainsOriginalCohortWithoutAdoptingNew(t *testing.T) {
	r, m, s, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	if err := m.RemoveDaemon(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if len(m.daemons) != 1 || len(r.cohort) != 2 || r.cohort[0].daemon.Container != a || a.terminated != 1 {
		t.Fatal("cleanup lost retained original handle")
	}
	status, err := r.Status(t.Context())
	if status.Drained || err == nil || status.Daemons["a"].State != "retired" {
		t.Fatalf("removed inventory member silently omitted: %+v %v", status, err)
	}
	if s.removals != 1 {
		t.Fatal("cleanup/observer repeated policy mutation")
	}
	// Strict same-session cannot finish after its witnessed cohort is stopped;
	// caller retains fixture cleanup ownership even with pending explicit intent.
	if err := m.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a.terminated != 1 || b.terminated != 1 {
		t.Fatalf("observer broke cleanup idempotence: %d/%d", a.terminated, b.terminated)
	}
}

func TestCephFSPeerRemovalProductionSupersedesReceiptAfterNewPeerReconcile(t *testing.T) {
	r, m, _, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	if st, err := r.Status(t.Context()); err != nil || !st.Drained {
		t.Fatal(err)
	}
	expected := r.peer
	m.pendingPeerImport = &expected
	var peers map[string]json.RawMessage
	json.Unmarshal([]byte(strings.Replace(peerRemovalManagerPeers(), cephFSObserverDraftPeer, peerRemovalNewPeer, 1)), &peers)
	id, err := m.reconcilePendingPeer(peers)
	if err != nil || id != peerRemovalNewPeer || m.peerGeneration == r.generation {
		t.Fatalf("new ownership did not supersede old receipt: %s %v", id, err)
	}
	st, err := r.Status(t.Context())
	if st.Drained || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("old receipt adopted new peer: %+v %v", st, err)
	}
}

func TestCephFSPeerRemovalProductionExactCatalogAndTimeInstant(t *testing.T) {
	fs := "fs mirror status source@1"
	peer := "fs mirror peer status source@1 " + cephFSObserverDraftPeer
	commands := map[string]string{"help": "help", "get_command_descriptions": "list", fs: "status", peer + "suffix": "foreign"}
	data, _ := json.Marshal(commands)
	if present, err := decodeCephFSRemovalCatalog(data, fs, peer); err != nil || present {
		t.Fatalf("substring command accepted: %v %v", present, err)
	}
	commands[peer] = "status"
	data, _ = json.Marshal(commands)
	if present, err := decodeCephFSRemovalCatalog(data, fs, peer); err != nil || !present {
		t.Fatal("exact command missing")
	}
	r, _, _, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	a.state.StartedAt = "2026-10-07T10:00:00+09:00"
	st, err := r.Status(t.Context())
	if err != nil || !st.Drained {
		t.Fatalf("equivalent timestamp classified as new generation: %+v %v", st, err)
	}
}

func TestCephFSPeerRemovalProductionCommonOriginalAttestationOrderingAndRetry(t *testing.T) {
	var calls []string
	bootstrap := func(context.Context) (string, error) {
		calls = append(calls, "common-original")
		return "[v2:127.0.0.1:3300/0,v1:127.0.0.1:6789/0]", nil
	}
	quorum := func(context.Context) (string, error) {
		calls = append(calls, "monmap")
		return peerRemovalSourceFSID, nil
	}
	native := func(context.Context) (string, error) {
		calls = append(calls, "native-fsid")
		return peerRemovalSourceFSID, nil
	}
	id, err := readCephFSRemovalClusterIdentity(t.Context(), bootstrap, quorum, native)
	if err != nil || id != peerRemovalSourceFSID || !slices.Equal(calls, []string{"common-original", "monmap", "native-fsid", "common-original"}) {
		t.Fatalf("original attestation sequencing: %v %v %s", calls, err, id)
	}
	for _, fault := range []string{"common-query", "quorum-query", "native-mismatch", "monmap-null", "post-common", "context"} {
		t.Run(fault, func(t *testing.T) {
			count := 0
			common := func(context.Context) (string, error) {
				count++
				if fault == "common-query" || (fault == "post-common" && count == 2) {
					return "", peerRemovalTransport
				}
				if fault == "context" {
					return "", context.Canceled
				}
				return "addresses", nil
			}
			q := quorum
			n := native
			if fault == "monmap-null" {
				q = func(context.Context) (string, error) { return "", nil }
			}
			if fault == "quorum-query" {
				q = func(context.Context) (string, error) { return "", peerRemovalTransport }
			}
			if fault == "native-mismatch" {
				n = func(context.Context) (string, error) { return peerRemovalDestinationFSID, nil }
			}
			id, err := readCephFSRemovalClusterIdentity(t.Context(), common, q, n)
			if id != "" || err == nil {
				t.Fatalf("unattested native identity adopted: %s %v", id, err)
			}
			if fault == "context" {
				if !errors.Is(err, context.Canceled) || errors.Is(err, errCephFSObservationGuard) {
					t.Fatalf("context cause classified as identity: %v", err)
				}
			} else if !errors.Is(err, errCephFSObservationGuard) {
				t.Fatalf("common/schema attestation failure retried: %v", err)
			}
		})
	}
	r, m, s, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	r.readClusters = func(ctx context.Context) ([2]string, error) {
		_, err := readCephFSRemovalClusterIdentity(ctx, func(context.Context) (string, error) { return "", peerRemovalTransport }, quorum, native)
		return [2]string{}, err
	}
	status, err := r.WaitDrained(t.Context())
	if status.Drained || !errors.Is(err, errCephFSObservationGuard) || !errors.Is(err, peerRemovalTransport) || m.peerRemoval != r || s.removals != 1 {
		t.Fatalf("common failure discarded accepted intent: %+v %v", status, err)
	}
	r.readClusters = peerRemovalClusterReader
	status, err = r.WaitDrained(t.Context())
	if err != nil || !status.Drained || s.removals != 1 {
		t.Fatalf("fresh wait could not recover retained intent: %+v %v", status, err)
	}
}

func TestCephFSPeerRemovalProductionAcknowledgedRequestDoesNotRepeatOnStaleViews(t *testing.T) {
	m, s, _, a, b := newPeerRemovalFixture()
	s.applyRemoval = false
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if err != nil || r == nil || !r.requestAcknowledged || s.removals != 1 {
		t.Fatalf("native acknowledgement not retained: %v", err)
	}
	again, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if again != r || err == nil || s.removals != 1 {
		t.Fatalf("acknowledged request resent against stale policy views: %v calls=%d", err, s.removals)
	}
	s.fsPeers = "{}"
	s.managerPeers = "{}"
	drainPeerRemovalDaemons(a, b)
	st, err := r.Status(t.Context())
	if err != nil || !st.Drained || s.removals != 1 {
		t.Fatalf("policy convergence could not finish original ack: %+v %v", st, err)
	}
}

func TestCephFSPeerRemovalProductionJoinedDaemonGuardOverridesEarlierQuery(t *testing.T) {
	r, _, _, _, a, b := beginPeerRemovalFixture(t)
	drainPeerRemovalDaemons(a, b)
	a.queryError = peerRemovalTransport
	b.catalogOverride = "null"
	st, err := r.WaitDrained(t.Context())
	if st.Drained || !errors.Is(err, peerRemovalTransport) || !errors.Is(err, errCephFSObservationGuard) || st.Daemons["a"].Problem == "" || st.Daemons["b"].Problem == "" {
		t.Fatalf("later guard hidden behind earlier member query: %+v %v", st, err)
	}
}
