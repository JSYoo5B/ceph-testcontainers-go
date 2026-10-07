package multicluster

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Native CLI/schema orchestration uses the production receipt and old peer
// fake. Only private original-template attestation uses the existing callback
// seam; public Begin separately rejects missing original templates. No native
// FSID is adopted in production and no runtime test helper is exported.
type directoryRemovalControlFake struct {
	*peerRemovalControlFake
	directories                     []string
	policyOverride, mappingOverride string
	removeError, addError           error
	applyRemoval, applyAdd          bool
	removals, adds                  int
	hook                            func(*directoryRemovalControlFake, []string)
}

func (f *directoryRemovalControlFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	a := args
	if len(a) >= 3 && a[0] == "ceph" && a[1] == "--connect-timeout" {
		a = a[3:]
	}
	if f.hook != nil {
		f.hook(f, a)
	}
	var out string
	switch {
	case slices.Equal(a, []string{"fs", "snapshot", "mirror", "ls", "source"}):
		out = f.policyOverride
		if out == "" {
			out = "["
			for i, path := range f.directories {
				if i > 0 {
					out += ","
				}
				out += `"` + path + `"`
			}
			out += "]"
		}
	case slices.Equal(a, []string{"fs", "snapshot", "mirror", "dirmap", "source", "/owned"}):
		out = f.mappingOverride
		if out == "" {
			out = `{"state":"mapped","instance_id":"4262","last_shuffled":0}`
		}
	case slices.Equal(a, []string{"service", "dump", "--format", "json"}):
		out = `{"services":{"cephfs-mirror":{"daemons":{"100":{"metadata":{"id":""}},"101":{"metadata":{"id":""}}}}}}`
	case slices.Equal(a, []string{"fs", "snapshot", "mirror", "daemon", "status"}):
		out = `[{"daemon_id":100,"filesystems":[{"name":"source","peers":[{"uuid":"` + cephFSObserverDraftPeer + `"}]}]},{"daemon_id":101,"filesystems":[{"name":"source","peers":[{"uuid":"` + cephFSObserverDraftPeer + `"}]}]}]`
	case slices.Equal(a, []string{"fs", "snapshot", "mirror", "show", "distribution", "source"}):
		out = `{"mapping":{"4262":"1 directories","4266":"0 directories"}}`
	case len(a) == 6 && slices.Equal(a[:5], []string{"fs", "snapshot", "mirror", "remove", "source"}):
		f.removals++
		if f.applyRemoval {
			f.directories = slices.DeleteFunc(f.directories, func(name string) bool { return name == a[5] })
		}
		if f.removeError != nil {
			return 0, nil, f.removeError
		}
		out = "{}"
	case len(a) == 6 && slices.Equal(a[:5], []string{"fs", "snapshot", "mirror", "add", "source"}):
		f.adds++
		if f.applyAdd {
			f.directories = append(f.directories, a[5])
		}
		if f.addError != nil {
			return 0, nil, f.addError
		}
		out = "{}"
	default:
		return f.peerRemovalControlFake.Exec(ctx, args, opts...)
	}
	f.calls = append(f.calls, slices.Clone(args))
	return 0, cephFSObserverDraftStream(out), nil
}

type directoryRemovalDaemonFake struct {
	*peerRemovalDaemonFake
	stats      string
	statsError error
	statsHook  func(*directoryRemovalDaemonFake)
}

func (f *directoryRemovalDaemonFake) IsRunning() bool { return f.state.Running }

func (f *directoryRemovalDaemonFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Equal(args, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", "source@1", cephFSObserverDraftPeer}) {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		f.calls = append(f.calls, slices.Clone(args))
		if f.statsHook != nil {
			f.statsHook(f)
		}
		if f.statsError != nil {
			return 0, nil, f.statsError
		}
		return 0, cephFSObserverDraftStream(f.stats), nil
	}
	return f.peerRemovalDaemonFake.Exec(ctx, args, opts...)
}

const directoryRemovalTracked = `{"/owned":{"state":"idle","snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}`

func newDirectoryRemovalFixture() (*CephFSMirror, *directoryRemovalControlFake, *peerRemovalControlFake, *directoryRemovalDaemonFake, *directoryRemovalDaemonFake) {
	m, source, destination, a, b := newPeerRemovalFixture()
	s := &directoryRemovalControlFake{peerRemovalControlFake: source, directories: []string{"/owned"}, applyRemoval: true, applyAdd: true}
	da := &directoryRemovalDaemonFake{peerRemovalDaemonFake: a, stats: directoryRemovalTracked}
	db := &directoryRemovalDaemonFake{peerRemovalDaemonFake: b, stats: `{}`}
	m.source.Container = s
	m.daemons[0].Container, m.daemons[1].Container = da, db
	m.directoryGenerations = map[string]uint64{"/owned": 1}
	return m, s, destination, da, db
}

func beginDirectoryRemovalFixture(t *testing.T) (*CephFSMirrorDirectoryRemoval, *CephFSMirror, *directoryRemovalControlFake, *peerRemovalControlFake, *directoryRemovalDaemonFake, *directoryRemovalDaemonFake) {
	t.Helper()
	m, s, d, a, b := newDirectoryRemovalFixture()
	r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
	if err != nil || r == nil {
		t.Fatalf("strict directory preflight: %v", err)
	}
	return r, m, s, d, a, b
}

func TestCephFSDirectoryRemovalProductionExactCycleRelease(t *testing.T) {
	r, m, s, _, a, b := beginDirectoryRemovalFixture(t)
	if m.directoryRemoval != r || s.removals != 1 || m.peerRemoval != nil || m.peerID != cephFSObserverDraftPeer || m.ownedDirectories["/owned"] {
		t.Fatal("directory intent altered peer ownership or failed to retain path")
	}
	for _, stats := range []string{
		directoryRemovalTracked,
		`{"/owned":{"state":"failed","failure_reason":"copy","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`,
		`{"/owned":{"state":"syncing","current_syncing_snap":{"id":7,"name":"checkpoint"},"snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`,
	} {
		a.stats = stats
		st, err := r.Status(t.Context())
		if err != nil || !st.PolicyRemoved || st.Released || st.Daemons["a"].State != "tracked" || st.Daemons["b"].State != "released" {
			t.Fatalf("entry state incorrectly proved release: %+v %v", st, err)
		}
	}
	a.stats, b.stats = `{}`, directoryRemovalTracked
	st, err := r.Status(t.Context())
	if err != nil || st.Released || st.Daemons["b"].State != "tracked" {
		t.Fatalf("checked mapped owner only: %+v %v", st, err)
	}
	b.stats = `{}`
	st, err = r.WaitReleased(t.Context())
	if err != nil || !st.Released || !st.PolicyRemoved || !r.completed || st.Directory != "/owned" || st.PeerID != cephFSObserverDraftPeer || st.SourceFilesystemID != 1 || st.DestinationFilesystemID != 2 || len(st.Daemons) != 2 {
		t.Fatalf("normal original cohort release: %+v %v", st, err)
	}
	for _, report := range st.Daemons {
		if report.State != "released" || report.ContainerID == "" || report.InstanceID == "" || report.Problem != "" {
			t.Fatalf("lost original identity: %+v", report)
		}
	}
}

func TestCephFSDirectoryRemovalProductionPreflightNeverMutatesInvalidOwnership(t *testing.T) {
	cases := []string{"public-missing-template", "owned-path", "generation", "zero-daemons", "stopped", "paused", "restarting", "dead", "missing-original-fs", "missing-source-auth", "missing-pool", "mapping", "foreign-owner", "absent-policy", "owner-path-absent", "malformed-peer-stats", "foreign-watcher", "peer-missing", "peer-command-missing"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			m, s, _, a, _ := newDirectoryRemovalFixture()
			switch name {
			case "owned-path":
				delete(m.ownedDirectories, "/owned")
			case "generation":
				delete(m.directoryGenerations, "/owned")
			case "zero-daemons":
				m.daemons = nil
			case "stopped":
				a.state.Running = false
			case "paused":
				a.state.Paused = true
			case "restarting":
				a.state.Restarting = true
			case "dead":
				a.state.Dead = true
			case "missing-original-fs":
				m.filesystemID = 0
			case "missing-source-auth":
				m.SourceClientEntity = ""
			case "missing-pool":
				m.metadataPool = ""
			case "mapping":
				s.mappingOverride = `{"state":"mapping"}`
			case "foreign-owner":
				s.mappingOverride = `{"state":"mapped","instance_id":"999"}`
			case "absent-policy":
				s.directories = nil
			case "owner-path-absent":
				a.stats = `{}`
			case "malformed-peer-stats":
				a.stats = `null`
			case "foreign-watcher":
				s.watchers += "watcher=172.20.0.10:0/999 client.999 cookie=3\n"
			case "peer-missing":
				s.fsPeers = `{}`
			case "peer-command-missing":
				a.catalogPeer = false
			}
			var r *CephFSMirrorDirectoryRemoval
			var err error
			if name == "public-missing-template" {
				r, err = m.BeginDirectoryRemoval(t.Context(), "/owned")
			} else {
				r, err = m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			}
			if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 || m.directoryRemoval != nil {
				t.Fatalf("invalid preflight mutated/retained intent: receipt=%v removals=%d error=%v", r, s.removals, err)
			}
		})
	}
}

func TestCephFSDirectoryRemovalProductionRetainsLostReplyAndNoAbsentResend(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(map[bool]string{true: "applied", false: "not-applied"}[applied], func(t *testing.T) {
			m, s, _, a, _ := newDirectoryRemovalFixture()
			s.removeError, s.applyRemoval = peerRemovalTransport, applied
			s.hook = func(_ *directoryRemovalControlFake, args []string) {
				if len(args) == 6 && args[3] == "remove" && m.directoryRemoval == nil {
					t.Fatal("native mutation preceded intent storage")
				}
			}
			r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			if r == nil || !errors.Is(err, peerRemovalTransport) || m.directoryRemoval != r || !m.ownedDirectories["/owned"] || s.removals != 1 {
				t.Fatalf("lost response discarded intent: %v %v", r, err)
			}
			s.removeError, s.applyRemoval = nil, true
			again, err := m.BeginDirectoryRemoval(t.Context(), "/owned")
			want := 1
			if !applied {
				want = 2
			}
			if err != nil || again != r || s.removals != want {
				t.Fatalf("retained path retry failed: same=%v count=%d error=%v", again == r, s.removals, err)
			}
			if err := m.RemoveDirectory(t.Context(), "/owned"); err != nil || s.removals != want {
				t.Fatalf("legacy same-path retry bypassed receipt: %v", err)
			}
			a.stats = `{}`
			st, err := r.WaitReleased(t.Context())
			if err != nil || !st.Released || s.removals != want {
				t.Fatalf("lost response could not observe original release: %+v %v", st, err)
			}
		})
	}
}

func TestCephFSDirectoryRemovalProductionPolicyRetryTransitions(t *testing.T) {
	for _, state := range []string{"unmapping", "mapping", "shuffling", "resolving", "stalled", "null"} {
		t.Run(state, func(t *testing.T) {
			m, s, _, _, _ := newDirectoryRemovalFixture()
			s.removeError, s.applyRemoval = peerRemovalTransport, false
			r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			if r == nil || !errors.Is(err, peerRemovalTransport) {
				t.Fatal("missing retained native uncertainty")
			}
			s.removeError = nil
			s.mappingOverride = `{"state":"` + state + `"}`
			if state == "null" {
				s.mappingOverride = `{"state":null,"instance_id":"4262"}`
			}
			again, err := m.BeginDirectoryRemoval(t.Context(), "/owned")
			if again != r || err == nil || errors.Is(err, errCephFSObservationGuard) || s.removals != 1 || !m.ownedDirectories["/owned"] {
				t.Fatalf("ambiguous transition resent/adopted: %v %v", again, err)
			}
			s.mappingOverride = ""
			s.applyRemoval = true
			again, err = m.BeginDirectoryRemoval(t.Context(), "/owned")
			if err != nil || again != r || s.removals != 2 {
				t.Fatalf("fresh mapped retry failed: %v", err)
			}
		})
	}
	r, m, s, _, _, _ := beginDirectoryRemovalFixture(t)
	s.directories = []string{"/owned"}
	again, err := m.BeginDirectoryRemoval(t.Context(), "/owned")
	if again != r || err == nil || s.removals != 1 {
		t.Fatal("acknowledged request repeated against stale MGR policy")
	}
}

func TestCephFSDirectoryRemovalProductionOverlapLegacyAndGenerations(t *testing.T) {
	r, m, s, _, a, _ := beginDirectoryRemovalFixture(t)
	for _, mutate := range []func() error{
		func() error { return m.AddDirectory(t.Context(), "/owned") },
		func() error { return m.AddDirectory(t.Context(), "/new") },
		func() error { _, err := m.AddDaemon(t.Context(), "new"); return err },
		func() error { return m.RebalanceDirectories(t.Context()) },
		func() error { return m.RemovePeer(t.Context(), cephFSObserverDraftPeer) },
		func() error { _, err := m.BeginPeerRemoval(t.Context(), cephFSObserverDraftPeer); return err },
		func() error { _, err := m.RebootstrapPeer(t.Context()); return err },
		func() error { _, err := m.BeginDirectoryRemoval(t.Context(), "/other"); return err },
		func() error { return m.RemoveDirectory(t.Context(), "/other") },
	} {
		if err := mutate(); !errors.Is(err, errCephFSObservationGuard) || s.adds != 0 || s.removals != 1 {
			t.Fatalf("pending explicit release allowed native overlap: %v", err)
		}
	}
	a.stats = `{}`
	if st, err := r.Status(t.Context()); err != nil || !st.Released {
		t.Fatalf("release failed: %+v %v", st, err)
	}
	before := m.directoryGenerations["/owned"]
	s.addError = peerRemovalTransport
	s.applyAdd = false
	if err := m.AddDirectory(t.Context(), "/owned"); !errors.Is(err, peerRemovalTransport) || m.directoryGenerations["/owned"] != before {
		t.Fatal("failed native add advanced ownership generation")
	}
	if m.ownedDirectories["/owned"] || !r.superseded {
		t.Fatal("uncertain re-add adopted ownership or reused old cycle receipt")
	}
	if st, err := r.Status(t.Context()); st.Released || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("old cycle receipt survived an uncertain re-add: %+v %v", st, err)
	}
	s.addError = nil
	s.applyAdd = true
	if err := m.AddDirectory(t.Context(), "/owned"); err != nil || m.directoryGenerations["/owned"] != before+1 || !m.ownedDirectories["/owned"] {
		t.Fatalf("owned re-add did not advance generation: %v", err)
	}
	if st, err := r.Status(t.Context()); st.Released || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("old receipt adopted re-added path: %+v %v", st, err)
	}
	a.stats = directoryRemovalTracked
	newReceipt, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
	if err != nil || newReceipt == nil || newReceipt == r || newReceipt.generation != before+1 {
		t.Fatalf("new original registration reused old receipt: %v %v", newReceipt, err)
	}
	if st, err := r.Status(t.Context()); st.Released || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("old receipt adopted later removal: %+v %v", st, err)
	}
	legacy, ls, _, _, _ := newDirectoryRemovalFixture()
	legacy.daemons = nil
	legacy.directoryGenerations = nil
	if err := legacy.RemoveDirectory(t.Context(), "/owned"); err != nil || ls.removals != 1 || legacy.directoryRemoval != nil || legacy.ownedDirectories["/owned"] {
		t.Fatalf("legacy zero-daemon contract changed: %v", err)
	}
	stopped, ss, _, sa, _ := newDirectoryRemovalFixture()
	sa.state.Running = false
	if err := stopped.RemoveDirectory(t.Context(), "/owned"); err != nil || ss.removals != 1 || stopped.directoryRemoval != nil {
		t.Fatalf("legacy stopped-daemon contract changed: %v", err)
	}
}

func TestCephFSDirectoryRemovalProductionAppliedReaddReplyLossCannotReuseOldProof(t *testing.T) {
	r, m, s, _, a, _ := beginDirectoryRemovalFixture(t)
	a.stats = `{}`
	if st, err := r.WaitReleased(t.Context()); err != nil || !st.Released {
		t.Fatalf("initial release: %+v %v", st, err)
	}
	s.addError, s.applyAdd = peerRemovalTransport, true
	if err := m.AddDirectory(t.Context(), "/owned"); !errors.Is(err, peerRemovalTransport) {
		t.Fatal("real-shaped re-add lost reply did not fail")
	}
	if !slices.Contains(s.directories, "/owned") || m.ownedDirectories["/owned"] || m.directoryGenerations["/owned"] != r.generation || !r.superseded {
		t.Fatal("uncertain re-add did not distinguish native application from private ownership")
	}
	// Even a stale MGR ls + empty async stats cannot resurrect the old proof.
	s.policyOverride = `[]`
	st, err := r.Status(t.Context())
	if st.Released || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("old cycle was reused after applied re-add uncertainty: %+v %v", st, err)
	}
}

func TestCephFSDirectoryRemovalProductionOwnedRegistrationGenerations(t *testing.T) {
	m, s, _, _, _ := newDirectoryRemovalFixture()
	m.directoryGenerations = nil
	if err := m.AddDirectory(t.Context(), "/new"); err != nil || m.directoryGenerations["/new"] != 1 || !m.ownedDirectories["/new"] {
		t.Fatalf("acknowledged legacy add did not establish private generation: %v", err)
	}
	before := m.directoryGenerations["/new"]
	if err := m.RebalanceDirectories(t.Context()); err != nil || m.directoryGenerations["/new"] != before+1 || m.directoryGenerations["/owned"] != 1 || s.removals != 2 {
		t.Fatalf("acknowledged rebalance add did not advance current ownership: %v", err)
	}
	before = m.directoryGenerations["/new"]
	s.addError, s.applyAdd = peerRemovalTransport, false
	if err := m.RebalanceDirectories(t.Context()); !errors.Is(err, peerRemovalTransport) || m.directoryGenerations["/new"] != before {
		t.Fatalf("uncertain rebalance add adopted a registration: %v", err)
	}
}

func TestCephFSDirectoryRemovalProductionRechecksPolicyAndSessions(t *testing.T) {
	t.Run("policy-reappeared", func(t *testing.T) {
		r, _, s, _, a, _ := beginDirectoryRemovalFixture(t)
		a.stats = `{}`
		reads := 0
		s.hook = func(f *directoryRemovalControlFake, args []string) {
			if len(args) == 5 && args[3] == "ls" {
				reads++
				if reads == 2 {
					f.directories = []string{"/owned"}
				}
			}
		}
		st, err := r.Status(t.Context())
		if err != nil || st.PolicyRemoved || st.Released || r.completed {
			t.Fatalf("before-only policy absence proved release: %+v %v", st, err)
		}
	})
	t.Run("command-disappeared-after-stats", func(t *testing.T) {
		r, _, _, _, a, _ := beginDirectoryRemovalFixture(t)
		a.stats = `{}`
		a.statsHook = func(f *directoryRemovalDaemonFake) { f.catalogPeer = false }
		st, err := r.Status(t.Context())
		if st.Released || !errors.Is(err, errCephFSObservationGuard) {
			t.Fatalf("before-only catalog proved release: %+v %v", st, err)
		}
	})
	t.Run("owner-drift-before-mutation", func(t *testing.T) {
		m, s, _, _, _ := newDirectoryRemovalFixture()
		reads := 0
		s.hook = func(f *directoryRemovalControlFake, args []string) {
			if len(args) == 6 && args[3] == "dirmap" {
				reads++
				if reads == 2 {
					f.mappingOverride = `{"state":"mapped","instance_id":"4266"}`
				}
			}
		}
		r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
		if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 || m.directoryRemoval != nil {
			t.Fatalf("drifted owner mutated native policy: %v %v", r, err)
		}
	})
}

func TestCephFSDirectoryRemovalProductionCommonGuardRetainsFreshRetry(t *testing.T) {
	r, m, s, _, a, _ := beginDirectoryRemovalFixture(t)
	a.stats = `{}`
	r.original.readClusters = func(context.Context) ([2]string, error) {
		return [2]string{}, errors.Join(cephFSObserveGuard("original-template quorum unavailable"), peerRemovalTransport)
	}
	st, err := r.WaitReleased(t.Context())
	if st.Released || !errors.Is(err, errCephFSObservationGuard) || !errors.Is(err, peerRemovalTransport) || m.directoryRemoval != r || s.removals != 1 {
		t.Fatalf("common original attestation failure erased intent: %+v %v", st, err)
	}
	r.original.readClusters = peerRemovalClusterReader
	st, err = r.WaitReleased(t.Context())
	if err != nil || !st.Released {
		t.Fatalf("fresh same-original environment failed retry: %+v %v", st, err)
	}
}

func TestCephFSDirectoryRemovalProductionSchemaIdentityAndCohortFailClosed(t *testing.T) {
	cases := []string{"null-policy", "policy-alias", "policy-duplicate", "stats-null", "stats-error", "stats-wrapper", "stats-malformed-unrelated", "stats-alias", "stats-duplicate", "stats-trailing", "peer-MON-removed", "peer-MGR-removed", "peer-command-removed", "restart", "replacement", "watcher-retired", "foreign-watcher", "FSID", "filesystem", "metadata", "cohort-expansion", "cohort-retirement"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			r, m, s, d, a, b := beginDirectoryRemovalFixture(t)
			a.stats = `{}`
			switch name {
			case "null-policy":
				s.policyOverride = `null`
			case "policy-alias":
				s.policyOverride = `["/owned/../owned"]`
			case "policy-duplicate":
				s.policyOverride = `["/other","/other"]`
			case "stats-null":
				a.stats = `null`
			case "stats-error":
				a.stats = `{"error":"ENOENT"}`
			case "stats-wrapper":
				a.stats = `{"stats":{}}`
			case "stats-malformed-unrelated":
				a.stats = `{"/other":{"state":"idle"}}`
			case "stats-alias":
				a.stats = strings.Replace(directoryRemovalTracked, "/owned", "/owned/../owned", 1)
			case "stats-duplicate":
				a.stats = `{"/other":{"state":"idle","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0},"/other":{"state":"idle","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`
			case "stats-trailing":
				a.stats = `{} {}`
			case "peer-MON-removed":
				s.fsPeers = `{}`
			case "peer-MGR-removed":
				s.managerPeers = `{}`
			case "peer-command-removed":
				a.catalogPeer = false
			case "restart":
				a.state.StartedAt = "2026-10-07T01:00:01Z"
			case "replacement":
				a.id = strings.Repeat("c", 64)
			case "watcher-retired":
				s.watchers = "watcher=172.20.0.9:0/5678 client.4266 cookie=2\n"
			case "foreign-watcher":
				s.watchers += "watcher=172.20.0.10:0/999 client.999 cookie=3\n"
			case "FSID":
				r.original.readClusters = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalDestinationFSID, peerRemovalSourceFSID}, nil
				}
			case "filesystem":
				d.id++
			case "metadata":
				d.pool++
			case "cohort-expansion":
				m.daemons = append(m.daemons, &CephFSMirrorDaemon{Container: b, DaemonName: "new"})
			case "cohort-retirement":
				m.daemons = m.daemons[:1]
			}
			st, err := r.WaitReleased(t.Context())
			if st.Released || !errors.Is(err, errCephFSObservationGuard) || r.completed || m.directoryRemoval != r || s.removals != 1 {
				t.Fatalf("original identity/schema adopted: %+v %v", st, err)
			}
		})
	}
}

func TestCephFSDirectoryRemovalProductionTransientQueriesFreshRetryAndCausalCancel(t *testing.T) {
	r, m, s, _, a, b := beginDirectoryRemovalFixture(t)
	a.stats, b.stats = `{}`, `{}`
	a.statsError = peerRemovalTransport
	st, err := r.Status(t.Context())
	if st.Released || !st.PolicyRemoved || !errors.Is(err, peerRemovalTransport) || errors.Is(err, errCephFSObservationGuard) || st.Daemons["a"].Problem == "" {
		t.Fatalf("query failure treated as release: %+v %v", st, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan struct{}, 1)
	a.statsHook = func(*directoryRemovalDaemonFake) {
		select {
		case first <- struct{}{}:
		default:
		}
	}
	type observed struct {
		status CephFSMirrorDirectoryRemovalStatus
		err    error
	}
	done := make(chan observed, 1)
	go func() { current, err := r.WaitReleased(ctx); done <- observed{current, err} }()
	select {
	case <-first:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("public Wait never observed the known native failure")
	}
	gateCtx, gateCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	gateErr := lockRGWSyncObservation(gateCtx, &m.mu)
	gateCancel()
	if gateErr != nil {
		cancel()
		<-done
		t.Fatal("public Wait held gate after known query failure", gateErr)
	}
	cancel()
	m.mu.Unlock()
	result := <-done
	st, err = result.status, result.err
	if st.Released || st.Directory != "/owned" || st.Daemons["a"].Problem == "" || !errors.Is(err, context.Canceled) || !errors.Is(err, peerRemovalTransport) {
		t.Fatalf("deadline/cause/report lost: %+v %v", st, err)
	}
	a.statsError = nil
	a.statsHook = nil
	st, err = r.WaitReleased(t.Context())
	if err != nil || !st.Released || m.directoryRemoval != r || s.removals != 1 {
		t.Fatalf("fresh context could not recover same intent: %+v %v", st, err)
	}
}

func TestCephFSDirectoryRemovalProductionBeginQueuedBeforeMutation(t *testing.T) {
	for _, name := range []string{"fixture", "daemon"} {
		t.Run(name, func(t *testing.T) {
			m, s, _, a, _ := newDirectoryRemovalFixture()
			unlock := m.mu.Unlock
			if name == "fixture" {
				m.mu.Lock()
			} else {
				m.daemons[0].mu.Lock()
				unlock = m.daemons[0].mu.Unlock
			}
			locked := true
			defer func() {
				if locked {
					unlock()
				}
			}()
			beforeSelected := len(a.calls)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			type result struct {
				receipt *CephFSMirrorDirectoryRemoval
				err     error
			}
			done := make(chan result, 1)
			go func() {
				r, err := m.beginDirectoryRemoval(ctx, "/owned", peerRemovalClusterReader)
				done <- result{r, err}
			}()
			var got result
			select {
			case got = <-done:
			case <-time.After(500 * time.Millisecond):
				unlock()
				locked = false
				cancel()
				select {
				case <-done:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("queued Begin did not join after unlock")
				}
				t.Fatalf("Begin ignored caller deadline on %s", name)
			}
			if got.receipt != nil || !errors.Is(got.err, context.DeadlineExceeded) || m.directoryRemoval != nil || s.removals != 0 || len(a.calls) != beforeSelected || !m.ownedDirectories["/owned"] {
				t.Fatalf("queued preflight adopted/mutated original fixture: %+v", got)
			}
			if name == "fixture" && len(s.calls) != 0 {
				t.Fatal("queued fixture preflight reached native query")
			}
			unlock()
			locked = false
			r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			if err != nil || r == nil || s.removals != 1 || m.directoryRemoval != r {
				t.Fatalf("fresh-context Begin lost original fixture: %v %v", r, err)
			}
		})
	}
}

func TestCephFSDirectoryRemovalProductionBusyLocksAndPollRelease(t *testing.T) {
	r, m, s, _, a, b := beginDirectoryRemovalFixture(t)
	type observation struct {
		status  CephFSMirrorDirectoryRemovalStatus
		receipt *CephFSMirrorDirectoryRemoval
		err     error
	}
	for _, name := range []string{"fixture", "daemon", "expired-Begin"} {
		t.Run(name, func(t *testing.T) {
			unlock := m.mu.Unlock
			if name == "daemon" {
				m.daemons[0].mu.Lock()
				unlock = m.daemons[0].mu.Unlock
			} else {
				m.mu.Lock()
			}
			locked := true
			defer func() {
				if locked {
					unlock()
				}
			}()
			beforeCommands, beforeSelected, beforeRemovals := len(s.calls), len(a.calls), s.removals
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			want := error(context.DeadlineExceeded)
			if name == "expired-Begin" {
				cancel()
				want = context.Canceled
			}
			done := make(chan observation, 1)
			go func() {
				if name == "expired-Begin" {
					receipt, err := m.BeginDirectoryRemoval(ctx, "/owned")
					done <- observation{receipt: receipt, err: err}
					return
				}
				st, err := r.Status(ctx)
				done <- observation{status: st, err: err}
			}()
			var observed observation
			select {
			case observed = <-done:
			case <-time.After(500 * time.Millisecond):
				// A blocking-lock regression must not hang its own test teardown.
				unlock()
				locked = false
				cancel()
				select {
				case <-done:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("queued observer did not join after gate release")
				}
				t.Fatalf("caller context blocked on held %s gate", name)
			}
			if observed.status.Released || !errors.Is(observed.err, want) || observed.receipt != nil || s.removals != beforeRemovals || m.directoryRemoval != r || r.completed {
				t.Fatalf("queued gate altered intent/proof: %+v", observed)
			}
			if name == "daemon" {
				if len(a.calls) != beforeSelected {
					t.Fatal("held selected daemon reached native observation")
				}
			} else if len(s.calls) != beforeCommands || len(a.calls) != beforeSelected {
				t.Fatal("held fixture reached native observation")
			}
			unlock()
			locked = false
			st, err := r.Status(t.Context())
			if err != nil || st.Released || st.Daemons["a"].State != "tracked" || len(a.calls) <= beforeSelected {
				t.Fatalf("same original fixture failed fresh-context gate retry: %+v %v", st, err)
			}
		})
	}
	// A selected real stats Exec callback attests that Wait made its first
	// pending observation before this independent gate request. The 200ms gate
	// deadline is shorter than the public 500ms poll interval, so carrying the
	// fixture lock through the sleep fails this test deterministically.
	first := make(chan string, 1)
	a.statsHook = func(f *directoryRemovalDaemonFake) {
		select {
		case first <- f.stats:
		default:
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		st, err := r.WaitReleased(ctx)
		if err == nil && !st.Released {
			err = errors.New("missing release proof")
		}
		done <- err
	}()
	select {
	case stats := <-first:
		if stats != directoryRemovalTracked {
			cancel()
			<-done
			t.Fatal("first Wait observation did not positively witness pending path")
		}
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("Wait never observed the original tracked path")
	}
	gateCtx, gateCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	err := lockRGWSyncObservation(gateCtx, &m.mu)
	gateCancel()
	if err != nil {
		cancel()
		<-done
		t.Fatalf("Wait held fixture gate between polls: %v", err)
	}
	a.stats, b.stats = `{}`, `{}`
	m.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("public Wait did not continue after independent gate release: %v", err)
	}
}

func TestCephFSDirectoryRemovalProductionJoinedGuardAndCleanupDoNotBecomeCompletion(t *testing.T) {
	r, m, _, _, a, b := beginDirectoryRemovalFixture(t)
	a.statsError = peerRemovalTransport
	b.catalogPeer = false
	st, err := r.WaitReleased(t.Context())
	if st.Released || !errors.Is(err, peerRemovalTransport) || !errors.Is(err, errCephFSObservationGuard) || st.Daemons["a"].Problem == "" || st.Daemons["b"].Problem == "" {
		t.Fatalf("later guard hidden: %+v %v", st, err)
	}
	m.closed = true
	if st, err := r.Status(t.Context()); st.Released || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("cleanup adopted as cycle proof: %+v %v", st, err)
	}
}
