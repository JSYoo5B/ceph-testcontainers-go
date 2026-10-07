package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
)

func beginPeerAcknowledgmentFixture(t *testing.T, lostReply bool) (*CephFSMirrorPeerRemoval, *CephFSMirror, *peerRemovalControlFake, *peerRemovalControlFake, []*processRawFake) {
	t.Helper()
	m, s, d, a, b := newPeerRemovalFixture()
	raws := enableProcessDraftBindings(m, a, b)
	if lostReply {
		s.removeError = peerRemovalTransport
	}
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if r == nil || (err == nil) == lostReply {
		t.Fatalf("original peer request fixture: %v", err)
	}
	s.removeError = nil
	return r, m, s, d, raws
}

func beginDirectoryAcknowledgmentFixture(t *testing.T, lostReply bool) (*CephFSMirrorDirectoryRemoval, *CephFSMirror, *directoryRemovalControlFake, *peerRemovalControlFake, []*processRawFake) {
	t.Helper()
	m, s, d, a, b := newDirectoryRemovalFixture()
	raws := enableProcessDraftBindings(m, a.peerRemovalDaemonFake, b.peerRemovalDaemonFake)
	if lostReply {
		s.removeError = peerRemovalTransport
	}
	r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
	if r == nil || (err == nil) == lostReply {
		t.Fatalf("original directory request fixture: %v", err)
	}
	s.removeError = nil
	return r, m, s, d, raws
}

func removeAcknowledgmentOriginals(t *testing.T, m *CephFSMirror, raw []*processRawFake) {
	t.Helper()
	for _, d := range m.Daemons() {
		if err := m.RemoveDaemon(t.Context(), d.DaemonName); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range raw {
		f.inspectErr = errdefs.ErrNotFound
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentPeerAcceptanceAndFreshRetry(t *testing.T) {
	r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
	if m.peerID != r.peerID || r.requestAcknowledged {
		t.Fatal("lost reply fixture did not retain stale peer intent")
	}
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	if st, err := r.ProcessQuiescence(t.Context()); err != nil || !st.OriginalQuiescent || m.guardPeerRemovalOverlap() == nil {
		t.Fatalf("read-only proof changed gate: %+v %v", st, err)
	}
	first, err := r.AcknowledgeProcessQuiescence(t.Context())
	if err != nil || !first.Acknowledged || !first.Observation.OriginalQuiescent || !r.processQuiescenceAcknowledged || !r.terminal() || r.completed || r.requestAcknowledged || m.peerID != "" || m.guardPeerRemovalOverlap() != nil {
		t.Fatalf("peer alternative terminal decision: %+v %v", first, err)
	}
	before := raw[0].inspects
	second, err := r.AcknowledgeProcessQuiescence(t.Context())
	if err != nil || !second.Acknowledged || raw[0].inspects < before+2 || s.removals != 1 {
		t.Fatal("repeat acknowledgment reused cached evidence or replayed deletion")
	}
	before = raw[0].inspects
	if same, err := m.BeginPeerRemoval(t.Context(), r.peerID); err != nil || same != r {
		t.Fatalf("same acknowledged receipt retry: %v", err)
	}
	if err := m.RemovePeer(t.Context(), r.peerID); err != nil || s.removals != 1 || raw[0].inspects != before {
		t.Fatal("legacy acknowledged retry re-probed process or replayed deletion")
	}
	if st, _ := r.Status(t.Context()); st.Drained || r.completed {
		t.Fatal("process acknowledgment promoted native Drained")
	}
	for _, f := range raw {
		if f.closes != 0 {
			t.Fatal("acknowledgment closed retained observer")
		}
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentDirectoryReconcilesExactLostReply(t *testing.T) {
	r, m, s, _, raw := beginDirectoryAcknowledgmentFixture(t, true)
	m.Directories = append(m.Directories, "/other")
	m.ownedDirectories["/other"] = true
	m.pendingDirectoryRelease = map[string]bool{"/owned": true, "/other": true}
	if !m.ownedDirectories["/owned"] || r.requestAcknowledged {
		t.Fatal("lost reply fixture did not retain desired path")
	}
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	st, err := r.AcknowledgeProcessQuiescence(t.Context())
	if err != nil || !st.Acknowledged || st.Observation.Directory != "/owned" || !st.Observation.OriginalQuiescent || !r.processQuiescenceAcknowledged || r.completed || r.requestAcknowledged || m.guardDirectoryRemovalOverlap() != nil {
		t.Fatalf("directory alternative terminal decision: %+v %v", st, err)
	}
	if slices.Contains(m.Directories, "/owned") || m.ownedDirectories["/owned"] || m.pendingDirectoryRelease["/owned"] || !slices.Contains(m.Directories, "/other") || !m.ownedDirectories["/other"] || !m.pendingDirectoryRelease["/other"] || m.peerID != cephFSObserverDraftPeer {
		t.Fatal("acknowledgment failed exact desired-state reconciliation")
	}
	before := raw[0].inspects
	if next, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !next.Acknowledged || raw[0].inspects < before+2 {
		t.Fatal("directory retry did not use fresh proof")
	}
	before = raw[0].inspects
	if same, err := m.BeginDirectoryRemoval(t.Context(), "/owned"); err != nil || same != r {
		t.Fatalf("retired directory same receipt retry: %v", err)
	}
	if err := m.RemoveDirectory(t.Context(), "/owned"); err != nil || s.removals != 1 || raw[0].inspects != before {
		t.Fatal("retired directory replayed removal or raw proof")
	}
	if old, _ := r.Status(t.Context()); old.Released || r.completed {
		t.Fatal("process acknowledgment promoted native Released")
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentRequiresRemovedInventoryAndExactCID(t *testing.T) {
	for _, fault := range []string{"exited-owned", "later-owned", "direct-terminate", "mixed-inventory", "inventory-cleared-without-remove", "exited-cid", "later-cid", "mixed-cid", "old-watcher", "no-capability"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
			s.watchers = ""
			switch fault {
			case "exited-owned", "later-owned":
				for _, f := range raw {
					f.state = draftExited(processDraftStart)
					if fault == "later-owned" {
						f.state = draftRunning(processDraftLater)
					}
				}
				if g, err := r.ProcessQuiescence(t.Context()); err != nil || !g.OriginalQuiescent {
					t.Fatal("original-only diagnostic fixture did not prove old task")
				}
			case "direct-terminate":
				for _, d := range m.Daemons() {
					if err := d.Terminate(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			case "mixed-inventory":
				if err := m.RemoveDaemon(t.Context(), "a"); err != nil {
					t.Fatal(err)
				}
			case "inventory-cleared-without-remove":
				m.daemons = nil
			default:
				removeAcknowledgmentOriginals(t, m, raw)
				if fault == "exited-cid" || fault == "later-cid" || fault == "mixed-cid" {
					raw[0].inspectErr = nil
					raw[0].state = draftExited(processDraftStart)
					if fault == "later-cid" {
						raw[0].state = draftRunning(processDraftLater)
					} else if fault == "mixed-cid" {
						raw[0].state = draftRunning(processDraftStart)
					}
				}
				if fault == "old-watcher" {
					s.watchers = "watcher=moved-address client.04262 cookie=1\n"
				}
				if fault == "no-capability" {
					r.cohort[0].originalProcess = nil
				}
			}
			queries := len(s.calls)
			st, err := r.AcknowledgeProcessQuiescence(t.Context())
			if err == nil || st.Acknowledged || r.processQuiescenceAcknowledged || r.completed || r.requestAcknowledged || m.peerID != r.peerID || m.guardPeerRemovalOverlap() == nil || s.removals != 1 {
				t.Fatalf("invalid acknowledgment unblocked intent: %+v %v", st, err)
			}
			if len(m.daemons) != 0 && len(s.calls) != queries {
				t.Fatal("owned inventory refusal queried native authority")
			}
			beforeOverlap := len(s.calls)
			if err := m.AddDirectory(t.Context(), "/other"); err == nil || len(s.calls) != beforeOverlap || s.removals != 1 {
				t.Fatal("refused acknowledgment allowed overlap")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentFailureAndFinalEvidence(t *testing.T) {
	for _, fault := range []string{"engine", "Info404", "transport", "mixed-error", "final-policy", "final-watchers", "final-cancel", "generation", "closed", "canonical-causes"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			queries := 0
			switch fault {
			case "engine":
				raw[0].engineID = "other-engine"
			case "Info404":
				raw[0].infoErr = errdefs.ErrNotFound
			case "transport":
				raw[0].inspectErr = errors.New("private backend secret 4829")
			case "mixed-error":
				raw[0].inspectErr = errors.Join(errdefs.ErrNotFound, errors.New("private backend secret 4829"))
			case "canonical-causes":
				raw[0].inspectErr = errors.Join(context.Canceled, context.DeadlineExceeded, errors.New("private backend secret 4829"))
			case "generation":
				m.peerGeneration++
			case "closed":
				m.closed = true
			case "final-policy", "final-cancel":
				s.hook = func(f *peerRemovalControlFake, args []string) {
					if slices.Contains(args, "peer_list") {
						queries++
						if queries == 2 {
							if fault == "final-cancel" {
								cancel()
							} else {
								f.fsPeers, f.managerPeers = peerRemovalRemotePeers(), peerRemovalManagerPeers()
							}
						}
					}
				}
			case "final-watchers":
				s.hook = func(f *peerRemovalControlFake, args []string) {
					if slices.Contains(args, "listwatchers") {
						queries++
						if queries == 2 {
							f.watchers = "watcher=moved-address client.4262 cookie=1\n"
						}
					}
				}
			}
			st, err := r.AcknowledgeProcessQuiescence(ctx)
			if err == nil || st.Acknowledged || r.processQuiescenceAcknowledged || r.completed || m.peerID != r.peerID || s.removals != 1 || strings.Contains(err.Error(), "4829") {
				t.Fatalf("failed/final proof committed or leaked: %+v %v", st, err)
			}
			if fault == "final-cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("final cancellation cause lost")
			}
			if fault == "canonical-causes" && (!errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded)) {
				t.Fatal("joined canonical context causes lost")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentReplayReattestsNativeIdentity(t *testing.T) {
	for _, fault := range []string{"FSID", "source-ID", "destination-ID", "pool", "peer-reappears", "new-native-UUID", "query", "private-generation"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, d, raw := beginPeerAcknowledgmentFixture(t, false)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			if st, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !st.Acknowledged {
				t.Fatal(err)
			}
			switch fault {
			case "FSID":
				r.readClusters = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalDestinationFSID, peerRemovalSourceFSID}, nil
				}
			case "source-ID":
				s.id++
			case "destination-ID":
				d.id++
			case "pool":
				s.pool++
			case "peer-reappears":
				s.fsPeers, s.managerPeers = peerRemovalRemotePeers(), peerRemovalManagerPeers()
			case "new-native-UUID":
				s.fsPeers = strings.ReplaceAll(peerRemovalRemotePeers(), r.peerID, peerRemovalNewPeer)
				s.managerPeers = strings.ReplaceAll(peerRemovalManagerPeers(), r.peerID, peerRemovalNewPeer)
			case "query":
				s.queryError = peerRemovalTransport
			case "private-generation":
				m.peerGeneration++
			}
			before := raw[0].inspects
			if same, err := m.BeginPeerRemoval(t.Context(), r.peerID); err == nil || same != r {
				t.Fatal("acknowledged Begin trusted only cached owner")
			}
			if err := m.RemovePeer(t.Context(), r.peerID); err == nil || s.removals != 1 || raw[0].inspects != before || !r.processQuiescenceAcknowledged || r.completed {
				t.Fatal("acknowledged legacy retry removed recreated native policy")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentDirectoryReaddSupersedesBeforeAttempt(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-applied", true: "applied-reply-lost"}[applied], func(t *testing.T) {
			r, m, s, _, raw := beginDirectoryAcknowledgmentFixture(t, false)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			if st, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !st.Acknowledged {
				t.Fatal(err)
			}
			before := m.directoryGenerations["/owned"]
			s.applyAdd, s.addError = applied, peerRemovalTransport
			if err := m.AddDirectory(t.Context(), "/owned"); !errors.Is(err, peerRemovalTransport) || !r.superseded || m.ownedDirectories["/owned"] || m.directoryGenerations["/owned"] != before {
				t.Fatal("uncertain add reused old acknowledgment or adopted ownership")
			}
			if st, err := r.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) || !r.processQuiescenceAcknowledged {
				t.Fatal("old acknowledgment adopted uncertain path generation")
			}
			if st, err := r.ProcessQuiescence(t.Context()); st.OriginalQuiescent || !errors.Is(err, errCephFSObservationGuard) {
				t.Fatal("old read-only receipt adopted re-add")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentDirectoryReplayAndSuccessfulReadd(t *testing.T) {
	r, m, s, _, raw := beginDirectoryAcknowledgmentFixture(t, false)
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !st.Acknowledged {
		t.Fatal(err)
	}
	for _, fault := range []string{"path", "peer", "pool"} {
		oldPool := s.pool
		if fault == "path" {
			s.directories = []string{"/owned"}
		} else if fault == "peer" {
			s.fsPeers, s.managerPeers = "{}", "{}"
		} else {
			s.pool++
		}
		if same, err := m.BeginDirectoryRemoval(t.Context(), "/owned"); err == nil || same != r {
			t.Fatalf("directory replay adopted native %s drift", fault)
		}
		if err := m.RemoveDirectory(t.Context(), "/owned"); err == nil || s.removals != 1 {
			t.Fatal("directory replay deleted recreated policy")
		}
		s.pool, s.directories, s.fsPeers, s.managerPeers = oldPool, nil, peerRemovalRemotePeers(), peerRemovalManagerPeers()
	}
	before := m.directoryGenerations["/owned"]
	if err := m.AddDirectory(t.Context(), "/owned"); err != nil || !r.superseded || !m.ownedDirectories["/owned"] || m.directoryGenerations["/owned"] != before+1 {
		t.Fatal("new owned registration did not supersede old terminal identity")
	}
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("old receipt acknowledged new path generation")
	}
	// Legacy removal addresses the newly owned path, not the superseded receipt.
	if err := m.RemoveDirectory(t.Context(), "/owned"); err != nil || s.removals != 2 || m.ownedDirectories["/owned"] {
		t.Fatalf("new-generation legacy removal was blocked by old receipt: %v", err)
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentDirectoryFinalAuthorityFailureKeepsDesiredIntent(t *testing.T) {
	for _, fault := range []string{"path-reappears", "peer-disappears", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, _, raw := beginDirectoryAcknowledgmentFixture(t, true)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			s.hook = func(f *directoryRemovalControlFake, args []string) {
				if slices.Contains(args, "ls") {
					reads++
					if reads == 2 {
						if fault == "cancel" {
							cancel()
						} else if fault == "path-reappears" {
							f.directories = []string{"/owned"}
						} else {
							f.fsPeers, f.managerPeers = "{}", "{}"
						}
					}
				}
			}
			if fault == "peer-disappears" {
				// Change the peer at its final authoritative query rather than after
				// that query has already completed and only ls remains.
				s.peerRemovalControlFake.hook = func(f *peerRemovalControlFake, args []string) {
					if slices.Contains(args, "peer_list") {
						reads++
						if reads == 2 {
							f.fsPeers, f.managerPeers = "{}", "{}"
						}
					}
				}
				s.hook = nil
			}
			st, err := r.AcknowledgeProcessQuiescence(ctx)
			if err == nil || st.Acknowledged || r.processQuiescenceAcknowledged || r.completed || r.requestAcknowledged || !m.ownedDirectories["/owned"] || !slices.Contains(m.Directories, "/owned") || m.guardDirectoryRemovalOverlap() == nil || s.removals != 1 {
				t.Fatalf("failed directory proof changed desired intent: %+v %v", st, err)
			}
			if fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("directory final cancellation cause lost")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentLaterMembershipAndPeerGeneration(t *testing.T) {
	r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, false)
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !st.Acknowledged {
		t.Fatal(err)
	}
	// Model ordinary successful inventory registration without invoking Docker.
	m.registerDaemonContainer(&peerRemovalDaemonFake{id: strings.Repeat("c", 64)}, "new")
	if same, err := m.BeginPeerRemoval(t.Context(), r.peerID); err != nil || same != r {
		t.Fatal("same-generation accepted retry required old inventory")
	}
	if err := m.RemovePeer(t.Context(), r.peerID); err != nil || s.removals != 1 {
		t.Fatal("accepted retry with new cohort replayed deletion")
	}
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || err == nil || !r.processQuiescenceAcknowledged || m.guardPeerRemovalOverlap() != nil {
		t.Fatal("failed fresh retry rolled back historical terminal decision")
	}
	m.pendingPeerImport = &cephFSPeerIdentity{"client.peer", "destination-site", "destination"}
	peers := map[string]json.RawMessage{peerRemovalNewPeer: json.RawMessage(`{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}`)}
	if _, err := m.reconcilePendingPeer(peers); err != nil || m.peerGeneration == r.generation {
		t.Fatal("new peer reconciliation failed to advance generation")
	}
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("old acknowledgment adopted new peer generation")
	}
	if same, err := m.BeginPeerRemoval(t.Context(), r.peerID); same != r || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("old Begin adopted new peer generation")
	}
	if err := m.RemovePeer(t.Context(), r.peerID); !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("old legacy intent adopted new peer")
	}
	// A new explicit receipt's pending state belongs to that receipt, not fixture.
	m.peerRemoval = &CephFSMirrorPeerRemoval{mirror: m, peerID: peerRemovalNewPeer, generation: m.peerGeneration}
	if m.guardPeerRemovalOverlap() == nil {
		t.Fatal("new receipt inherited historical acknowledgment")
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentFailedFreshRepeatKeepsHistoricalDecision(t *testing.T) {
	for _, fault := range []string{"engine", "policy", "transport"} {
		t.Run(fault, func(t *testing.T) {
			r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, false)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			if st, err := r.AcknowledgeProcessQuiescence(t.Context()); err != nil || !st.Acknowledged {
				t.Fatal(err)
			}
			switch fault {
			case "engine":
				raw[0].engineID = "foreign-engine"
			case "policy":
				s.fsPeers, s.managerPeers = peerRemovalRemotePeers(), peerRemovalManagerPeers()
			case "transport":
				raw[0].inspectErr = errors.New("private repeat secret 4829")
			}
			before := raw[0].infos
			st, err := r.AcknowledgeProcessQuiescence(t.Context())
			if err == nil || st.Acknowledged || st.Observation.OriginalQuiescent || !r.processQuiescenceAcknowledged || r.completed || m.peerID != "" || m.guardPeerRemovalOverlap() != nil || s.removals != 1 || strings.Contains(err.Error(), "4829") {
				t.Fatalf("failed repeat reused proof or revoked historical decision: %+v %v", st, err)
			}
			if fault != "policy" && raw[0].infos <= before {
				t.Fatal("repeat did not query retained raw engine freshly")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentNilGracefulAndCanceled(t *testing.T) {
	var peer *CephFSMirrorPeerRemoval
	var dir *CephFSMirrorDirectoryRemoval
	if st, err := peer.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("nil peer admitted acknowledgment")
	}
	if st, err := dir.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("nil directory admitted acknowledgment")
	}
	r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if st, err := r.AcknowledgeProcessQuiescence(ctx); st.Acknowledged || !errors.Is(err, context.Canceled) || r.processQuiescenceAcknowledged || m.peerID != r.peerID {
		t.Fatal("canceled acknowledgment mutated historical state")
	}
	r.completed = true
	if st, err := r.AcknowledgeProcessQuiescence(t.Context()); st.Acknowledged || !errors.Is(err, errCephFSObservationGuard) || r.processQuiescenceAcknowledged || !r.completed {
		t.Fatal("alternative acknowledgment rewrote graceful disposition")
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentBusyGatesHaveWatchdog(t *testing.T) {
	for _, gate := range []string{"fixture", "member", "raw"} {
		t.Run(gate, func(t *testing.T) {
			r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
			removeAcknowledgmentOriginals(t, m, raw)
			s.watchers = ""
			var unlock func()
			switch gate {
			case "fixture":
				m.mu.Lock()
				unlock = m.mu.Unlock
			case "member":
				r.cohort[0].daemon.mu.Lock()
				unlock = r.cohort[0].daemon.mu.Unlock
			case "raw":
				r.cohort[0].originalProcess.mu.Lock()
				unlock = r.cohort[0].originalProcess.mu.Unlock
			}
			locked, joined := true, false
			done := make(chan error, 1)
			t.Cleanup(func() {
				if locked {
					unlock()
					locked = false
				}
				if !joined {
					select {
					case <-done:
						joined = true
					case <-time.After(500 * time.Millisecond):
						t.Error("acknowledgment did not join after cleanup unlock")
					}
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			t.Cleanup(cancel)
			go func() { _, err := r.AcknowledgeProcessQuiescence(ctx); done <- err }()
			select {
			case err := <-done:
				joined = true
				if !errors.Is(err, context.DeadlineExceeded) || r.processQuiescenceAcknowledged || m.peerID != r.peerID {
					t.Fatal("busy gate ignored context or committed after deadline")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("busy acknowledgment ignored watchdog")
			}
		})
	}
}

func TestCephFSProcessQuiescenceAcknowledgmentFinalProofRetainsFixtureGate(t *testing.T) {
	r, m, s, _, raw := beginPeerAcknowledgmentFixture(t, true)
	removeAcknowledgmentOriginals(t, m, raw)
	s.watchers = ""
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	queries := 0
	s.hook = func(_ *peerRemovalControlFake, args []string) {
		if slices.Contains(args, "peer_list") {
			queries++
			if queries == 2 {
				close(entered)
				<-release
			}
		}
	}
	type acknowledgmentResult struct {
		status CephFSMirrorProcessQuiescenceAcknowledgment
		err    error
	}
	done := make(chan acknowledgmentResult, 1)
	overlapDone := make(chan error, 1)
	joined, overlapStarted, overlapJoined := false, false, false
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Error("blocked final proof did not join")
			}
		}
		if overlapStarted && !overlapJoined {
			select {
			case <-overlapDone:
			case <-time.After(500 * time.Millisecond):
				t.Error("blocked overlap did not join")
			}
		}
	})
	go func() { st, err := r.AcknowledgeProcessQuiescence(t.Context()); done <- acknowledgmentResult{st, err} }()
	select {
	case <-entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("acknowledgment did not reach final authority query")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	t.Cleanup(cancel)
	beforeOverlap := len(s.calls)
	overlapStarted = true
	go func() { overlapDone <- m.AddDirectory(ctx, "/other") }()
	select {
	case err := <-overlapDone:
		overlapJoined = true
		if !errors.Is(err, context.DeadlineExceeded) || len(s.calls) != beforeOverlap {
			t.Fatal("local overlap entered during final proof")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("local overlap ignored fixture watchdog")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		joined = true
		if result.err != nil || !result.status.Acknowledged || !r.processQuiescenceAcknowledged {
			t.Fatalf("fresh final proof did not commit under gate: %+v %v", result.status, result.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("final proof did not complete after release")
	}
}
