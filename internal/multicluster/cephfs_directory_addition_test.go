package multicluster

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// Production native command/parser orchestration, with only existing private
// original-template attestation replaced by the established test callback.
func newDirectoryAdditionFixture(t *testing.T) (*CephFSMirror, *directoryRemovalControlFake, *peerRemovalControlFake) {
	t.Helper()
	m, s, d, _, _ := newDirectoryRemovalFixture()
	m.peerGeneration = 1
	for _, daemon := range m.Daemons() {
		if err := m.RemoveDaemon(t.Context(), daemon.DaemonName); err != nil {
			t.Fatal(err)
		}
	}
	return m, s, d
}

func beginDirectoryAdditionFixture(t *testing.T, m *CephFSMirror, path string) (*CephFSMirrorDirectoryAddition, error) {
	t.Helper()
	return m.beginDirectoryAddition(t.Context(), path, peerRemovalClusterReader)
}

func TestCephFSDirectoryAdditionRetainsAppliedReplyLossAndStatusIsReadOnly(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	s.addError = errors.New("native response private-key-9523")
	s.hook = func(_ *directoryRemovalControlFake, args []string) {
		if len(args) == 6 && args[3] == "add" {
			r := m.directoryAddition
			if r == nil || !r.requestAttempted || r.registered || m.ownedDirectories["/added"] || m.directoryGenerations["/added"] != 0 {
				t.Fatal("native dispatch preceded immutable attempted intent")
			}
		}
	}
	r, err := beginDirectoryAdditionFixture(t, m, "/added/./")
	if r == nil || !errors.Is(err, s.addError) || strings.Contains(err.Error(), "9523") || s.adds != 1 || r.requestAcknowledged || r.registered || !slices.Contains(s.directories, "/added") {
		t.Fatalf("lost response erased or adopted intent: %v", err)
	}
	st, err := r.Status(t.Context())
	if err != nil || !st.PolicyPresent || st.Registered || s.adds != 1 || m.ownedDirectories["/added"] || slices.Contains(m.Directories, "/added") || m.directoryGenerations["/added"] != 0 {
		t.Fatalf("Status adopted intent: %+v %v", st, err)
	}
	s.addError = nil
	for i := 0; i < 3; i++ {
		same, err := beginDirectoryAdditionFixture(t, m, "/added")
		if same != r || err != nil || !r.registered || s.adds != 1 || m.directoryGenerations["/added"] != 1 || !m.ownedDirectories["/added"] || len(m.daemons) != 0 {
			t.Fatalf("reconcile failed or repeated add/generation: %v", err)
		}
	}
	if strings.Join(m.Directories, ",") != "/owned,/added" || r.baseGeneration != 0 || r.generation != 1 {
		t.Fatal("published duplicate path or rewrote base generation")
	}
	st, err = r.Status(t.Context())
	if err != nil || !st.PolicyPresent || !st.Registered || st.Directory != "/added" || st.PeerID != m.peerID || st.SourceFilesystemID != 1 || st.DestinationFilesystemID != 2 {
		t.Fatalf("registered identity/report: %+v %v", st, err)
	}
}

func TestCephFSDirectoryAdditionRetriesOnlyUncertainAbsentRequest(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncertain", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			s.applyAdd = false
			if !acknowledged {
				s.addError = peerRemovalTransport
			}
			r, err := beginDirectoryAdditionFixture(t, m, "/added")
			if r == nil || err == nil || r.registered || s.adds != 1 || r.requestAcknowledged != acknowledged {
				t.Fatalf("first pending request: %v", err)
			}
			st, err := r.Status(t.Context())
			if err != nil || st.PolicyPresent || st.Registered || s.adds != 1 {
				t.Fatal("absent Status replayed request")
			}
			s.addError, s.applyAdd = nil, true
			same, err := beginDirectoryAdditionFixture(t, m, "/added")
			if same != r {
				t.Fatal("retry replaced original intent")
			}
			if acknowledged {
				if err == nil || s.adds != 1 || r.registered {
					t.Fatal("acknowledged but invisible request was resent")
				}
				s.directories = append(s.directories, "/added")
				same, err = beginDirectoryAdditionFixture(t, m, "/added")
			}
			wantAdds := 2
			if acknowledged {
				wantAdds = 1
			}
			if err != nil || same != r || !r.registered || s.adds != wantAdds || m.directoryGenerations["/added"] != 1 {
				t.Fatalf("fresh intent reconciliation: %v", err)
			}
		})
	}
}

func TestCephFSDirectoryAdditionPreflightRejectsForeignOwnedAndMalformedPolicy(t *testing.T) {
	for _, fault := range []string{"owned", "pending-release", "overflow", "present", "ancestor", "descendant", "root", "nil", "object", "duplicate", "noncanonical", "trailing", "source-fs", "dest-pool", "mon-peer", "mgr-peer", "foreign-peer", "no-peer-generation", "no-private-template"} {
		t.Run(fault, func(t *testing.T) {
			m, s, d := newDirectoryAdditionFixture(t)
			switch fault {
			case "owned":
				m.ownedDirectories["/added"] = true
			case "pending-release":
				m.pendingDirectoryRelease = map[string]bool{"/added": true}
			case "overflow":
				m.directoryGenerations["/added"] = ^uint64(0)
			case "present":
				s.directories = append(s.directories, "/added")
			case "ancestor":
				s.directories = []string{"/added"}
			case "descendant":
				s.directories = []string{"/added/child"}
			case "root":
				s.directories = []string{"/"}
			case "nil":
				s.policyOverride = "null"
			case "object":
				s.policyOverride = "{}"
			case "duplicate":
				s.policyOverride = `["/other","/other"]`
			case "noncanonical":
				s.policyOverride = `["/other/../x"]`
			case "trailing":
				s.policyOverride = `[] {}`
			case "source-fs":
				s.id++
			case "dest-pool":
				d.pool++
			case "mon-peer":
				s.fsPeers = "{}"
			case "mgr-peer":
				s.managerPeers = "{}"
			case "foreign-peer":
				s.managerPeers = strings.ReplaceAll(s.managerPeers, cephFSObserverDraftPeer, peerRemovalNewPeer)
			case "no-peer-generation":
				m.peerGeneration = 0
			}
			path := "/added"
			if fault == "ancestor" {
				path = "/added/child"
			}
			var r *CephFSMirrorDirectoryAddition
			var err error
			if fault == "no-private-template" {
				r, err = m.BeginDirectoryAddition(t.Context(), path)
			} else {
				r, err = beginDirectoryAdditionFixture(t, m, path)
			}
			if r != nil || !errors.Is(err, errCephFSObservationGuard) || m.directoryAddition != nil || s.adds != 0 || m.directoryGenerations["/added"] == 1 {
				t.Fatalf("preflight admitted %s: %v", fault, err)
			}
		})
	}
}

func TestCephFSDirectoryAdditionDoesNotAdoptLegacyLostReply(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	s.addError = peerRemovalTransport
	if err := m.AddDirectory(t.Context(), "/added"); !errors.Is(err, peerRemovalTransport) || !slices.Contains(s.directories, "/added") || m.ownedDirectories["/added"] {
		t.Fatal("legacy setup did not retain its previous contract")
	}
	r, err := beginDirectoryAdditionFixture(t, m, "/added")
	if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.adds != 1 || m.directoryAddition != nil || m.ownedDirectories["/added"] {
		t.Fatal("retroactively adopted unrecorded legacy request")
	}
}

func TestCephFSDirectoryAdditionPendingOverlapIncludesRemovalRetryShortcuts(t *testing.T) {
	for _, operation := range []string{"legacy-add-same", "legacy-add-other", "typed-add-other", "daemon-add", "rebalance", "bootstrap", "peer-begin-retry", "peer-remove-retry", "directory-begin-retry", "directory-remove-retry"} {
		t.Run(operation, func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			s.addError = peerRemovalTransport
			r, err := beginDirectoryAdditionFixture(t, m, "/added")
			if r == nil || err == nil {
				t.Fatal("missing pending intent")
			}
			// Terminal receipts make only the pre-shortcut addition gate refuse.
			m.peerRemoval = &CephFSMirrorPeerRemoval{mirror: m, peerID: m.peerID, completed: true}
			m.directoryRemoval = &CephFSMirrorDirectoryRemoval{directory: "/owned", generation: 1, processQuiescenceAcknowledged: true}
			beforeCalls := len(s.calls)
			beforeAdds, beforeRemovals := s.adds, s.removals
			switch operation {
			case "legacy-add-same":
				err = m.AddDirectory(t.Context(), "/added")
			case "legacy-add-other":
				err = m.AddDirectory(t.Context(), "/other")
			case "typed-add-other":
				_, err = beginDirectoryAdditionFixture(t, m, "/other")
			case "daemon-add":
				_, err = m.AddDaemon(t.Context(), "new")
			case "rebalance":
				err = m.RebalanceDirectories(t.Context())
			case "bootstrap":
				_, err = m.RebootstrapPeer(t.Context())
			case "peer-begin-retry":
				_, err = m.BeginPeerRemoval(t.Context(), m.peerID)
			case "peer-remove-retry":
				err = m.RemovePeer(t.Context(), m.peerID)
			case "directory-begin-retry":
				_, err = m.BeginDirectoryRemoval(t.Context(), "/owned")
			case "directory-remove-retry":
				err = m.RemoveDirectory(t.Context(), "/owned")
			}
			if !errors.Is(err, errCephFSObservationGuard) || len(s.calls) != beforeCalls || s.adds != beforeAdds || s.removals != beforeRemovals || m.directoryAddition != r || r.registered {
				t.Fatalf("pending %s escaped guard or changed intent: %v", operation, err)
			}
		})
	}
}

func TestCephFSDirectoryAdditionOriginalOwnerAndGenerationGuards(t *testing.T) {
	for _, fault := range []string{"closed", "source-handle", "source-name", "source-client", "dest-name", "source-id", "dest-id", "source-pool-name", "source-pool", "dest-pool", "dest-client", "dest-site", "peer", "peer-generation", "pending-peer", "path-generation", "path-owned", "pending-release", "cluster", "native-fs", "native-peer"} {
		t.Run(fault, func(t *testing.T) {
			m, s, d := newDirectoryAdditionFixture(t)
			s.addError = peerRemovalTransport
			r, _ := beginDirectoryAdditionFixture(t, m, "/added")
			if r == nil {
				t.Fatal("missing intent")
			}
			switch fault {
			case "closed":
				m.closed = true
			case "source-handle":
				m.source = m.destination
			case "source-name":
				m.SourceFilesystem = "new"
			case "source-client":
				m.SourceClientEntity = "client.new"
			case "dest-name":
				m.DestinationFilesystem = "new"
			case "source-id":
				m.filesystemID++
			case "dest-id":
				m.destinationFilesystemID++
			case "source-pool-name":
				m.metadataPool = "new"
			case "source-pool":
				m.metadataPoolID++
			case "dest-pool":
				m.destinationMetadataPoolID++
			case "dest-client":
				m.DestinationClientEntity = "client.new"
			case "dest-site":
				m.destinationSite = "new"
			case "peer":
				m.peerID = peerRemovalNewPeer
			case "peer-generation":
				m.peerGeneration++
			case "pending-peer":
				m.pendingPeerImport = &cephFSPeerIdentity{}
			case "path-generation":
				m.directoryGenerations["/added"]++
			case "path-owned":
				m.ownedDirectories["/added"] = true
			case "pending-release":
				m.pendingDirectoryRelease = map[string]bool{"/added": true}
			case "cluster":
				r.original.readClusters = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalNewPeer, peerRemovalDestinationFSID}, nil
				}
			case "native-fs":
				d.id++
			case "native-peer":
				s.fsPeers = "{}"
			}
			st, err := r.Status(t.Context())
			if st.PolicyPresent || st.Registered || !errors.Is(err, errCephFSObservationGuard) || r.registered || s.adds != 1 {
				t.Fatalf("Status adopted changed %s: %+v %v", fault, st, err)
			}
			_, err = beginDirectoryAdditionFixture(t, m, "/added")
			if !errors.Is(err, errCephFSObservationGuard) || r.registered || s.adds != 1 {
				t.Fatalf("Begin adopted changed %s: %v", fault, err)
			}
		})
	}
}

func TestCephFSDirectoryAdditionFinalNativePathDriftNeverPublishes(t *testing.T) {
	for _, phase := range []string{"preflight", "publish", "overlap"} {
		t.Run(phase, func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			var r *CephFSMirrorDirectoryAddition
			if phase != "preflight" {
				s.addError = peerRemovalTransport
				r, _ = beginDirectoryAdditionFixture(t, m, "/added")
			}
			reads := 0
			s.peerRemovalControlFake.hook = func(_ *peerRemovalControlFake, args []string) {
				if slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fs", "snapshot", "mirror", "peer_list", "source"}) {
					reads++
					if reads == 3 {
						if phase == "preflight" {
							s.directories = append(s.directories, "/added")
						} else if phase == "publish" {
							s.directories = []string{"/owned"}
						} else {
							s.directories = append(s.directories, "/added/foreign")
						}
					}
				}
			}
			same, err := beginDirectoryAdditionFixture(t, m, "/added")
			if err == nil || m.ownedDirectories["/added"] || m.directoryGenerations["/added"] != 0 || slices.Contains(m.Directories, "/added") || (r != nil && same != r) {
				t.Fatalf("final native drift published %s: %v reads=%d", phase, err, reads)
			}
			if phase == "preflight" && (same != nil || m.directoryAddition != nil || s.adds != 0) {
				t.Fatal("foreign final-path drift created attempted intent")
			}
		})
	}
}

func TestCephFSDirectoryAdditionCompletedReceiptCannotResurrectPolicy(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	r, err := beginDirectoryAdditionFixture(t, m, "/added")
	if err != nil || !r.registered {
		t.Fatal(err)
	}
	s.directories = []string{"/owned"}
	if st, err := r.Status(t.Context()); st.Registered || st.PolicyPresent || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("registered receipt silently accepted vanished native policy")
	}
	if same, err := beginDirectoryAdditionFixture(t, m, "/added"); same != r || !errors.Is(err, errCephFSObservationGuard) || s.adds != 1 || m.directoryGenerations["/added"] != 1 {
		t.Fatal("completed receipt resurrected old native registration")
	}
}

func TestCephFSDirectoryAdditionSupersedesOldRemovalBeforeUncertainAdd(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	delete(m.ownedDirectories, "/owned")
	m.Directories, s.directories = nil, nil
	old := &CephFSMirrorDirectoryRemoval{directory: "/owned", generation: 1, processQuiescenceAcknowledged: true}
	m.directoryRemoval = old
	s.addError = peerRemovalTransport
	s.hook = func(_ *directoryRemovalControlFake, args []string) {
		if len(args) == 6 && args[3] == "add" && !old.superseded {
			t.Fatal("old removal remained usable at uncertain add dispatch")
		}
	}
	r, err := beginDirectoryAdditionFixture(t, m, "/owned")
	if r == nil || !errors.Is(err, peerRemovalTransport) || !old.superseded || m.directoryGenerations["/owned"] != 1 || m.ownedDirectories["/owned"] || r.baseGeneration != 1 {
		t.Fatal("uncertain re-add adopted generation or kept old removal")
	}
	if same, err := beginDirectoryAdditionFixture(t, m, "/owned"); same != r || err != nil || m.directoryGenerations["/owned"] != 2 || !m.ownedDirectories["/owned"] || old.completed {
		t.Fatal("fresh owned generation rewrote original removal disposition")
	}
}

func TestCephFSDirectoryAdditionSupersessionPrecedesLaterMutation(t *testing.T) {
	for _, operation := range []string{"legacy-remove-error", "legacy-add-error", "rebalance-remove-error", "rebalance-add-error", "typed-remove", "other-typed-add"} {
		t.Run(operation, func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			r, err := beginDirectoryAdditionFixture(t, m, "/added")
			if err != nil {
				t.Fatal(err)
			}
			beforeGeneration := m.directoryGenerations["/added"]
			switch operation {
			case "legacy-remove-error":
				s.removeError = peerRemovalTransport
				err = m.RemoveDirectory(t.Context(), "/added")
			case "legacy-add-error":
				s.addError = peerRemovalTransport
				err = m.AddDirectory(t.Context(), "/added")
			case "rebalance-remove-error", "rebalance-add-error":
				// Restore existing fake running topology, without real construction.
				_, _, _, a, b := newDirectoryRemovalFixture()
				m.daemons = []*CephFSMirrorDaemon{{Container: a, DaemonName: "a"}, {Container: b, DaemonName: "b"}}
				m.ownedDirectories = map[string]bool{"/added": true}
				if operation == "rebalance-remove-error" {
					s.removeError = peerRemovalTransport
				} else {
					s.directories = []string{"/owned"}
					s.addError = peerRemovalTransport
				}
				err = m.RebalanceDirectories(t.Context())
			case "typed-remove":
				// Use an actual newly registered owned generation with live witnesses.
				m, s, _, _, _ = newDirectoryRemovalFixture()
				m.peerGeneration = 1
				delete(m.ownedDirectories, "/owned")
				m.Directories, s.directories = nil, nil
				r, err = beginDirectoryAdditionFixture(t, m, "/owned")
				if err != nil {
					t.Fatal(err)
				}
				_, err = m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			case "other-typed-add":
				_, err = beginDirectoryAdditionFixture(t, m, "/other")
			}
			if !r.superseded || (strings.HasSuffix(operation, "error") && !errors.Is(err, peerRemovalTransport)) || (operation == "typed-remove" && err != nil) || (operation == "other-typed-add" && err != nil) {
				t.Fatalf("later %s retained old addition: %v", operation, err)
			}
			if operation != "typed-remove" && m.directoryGenerations["/added"] != beforeGeneration {
				t.Fatal("failed mutation or other path changed original generation")
			}
			before := len(s.calls)
			if _, err := r.Status(t.Context()); !errors.Is(err, errCephFSObservationGuard) || len(s.calls) != before {
				t.Fatal("superseded addition still queried native or adopted path")
			}
		})
	}
}

func TestCephFSDirectoryAdditionNilCanceledQueryAndFinalOwnerDrift(t *testing.T) {
	var mirror *CephFSMirror
	var receipt *CephFSMirrorDirectoryAddition
	if _, err := mirror.BeginDirectoryAddition(t.Context(), "/added"); !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("nil mirror")
	}
	if _, err := receipt.Status(t.Context()); !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("nil receipt")
	}
	for _, fault := range []string{"canceled", "query", "final-context", "final-owner"} {
		t.Run(fault, func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			s.addError = peerRemovalTransport
			r, _ := beginDirectoryAdditionFixture(t, m, "/added")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if fault == "canceled" {
				cancel()
			}
			if fault == "query" {
				s.peerRemovalControlFake.queryError = errors.Join(errors.New("secret-3812"), context.Canceled, context.DeadlineExceeded)
			}
			reads := 0
			s.hook = func(_ *directoryRemovalControlFake, args []string) {
				if slices.Equal(args, []string{"fs", "snapshot", "mirror", "ls", "source"}) {
					reads++
					if reads == 3 {
						if fault == "final-context" {
							cancel()
						}
						if fault == "final-owner" {
							m.peerGeneration++
						}
					}
				}
			}
			same, err := m.beginDirectoryAddition(ctx, "/added", peerRemovalClusterReader)
			if (same != r && fault != "canceled") || m.directoryAddition != r || err == nil || r.registered || m.ownedDirectories["/added"] || m.directoryGenerations["/added"] != 0 || strings.Contains(err.Error(), "3812") {
				t.Fatalf("fault adopted or lost intent %s: %v", fault, err)
			}
			if (fault == "query" && (!errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded))) || (fault == "final-context" && !errors.Is(err, context.Canceled)) {
				t.Fatal("canonical context causes lost")
			}
		})
	}
}

func TestCephFSDirectoryAdditionUnrelatedLegacyAddPreservesSelectedReceipt(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	r, err := beginDirectoryAdditionFixture(t, m, "/added")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddDirectory(t.Context(), "/unrelated"); err != nil {
		t.Fatal(err)
	}
	if st, err := r.Status(t.Context()); err != nil || !st.Registered || r.superseded || m.directoryGenerations["/added"] != 1 || s.adds != 2 {
		t.Fatalf("unrelated legacy add invalidated selected receipt: %+v %v", st, err)
	}
}

func TestCephFSDirectoryAdditionFreshQueryFailureKeepsPublishedOwnership(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	r, err := beginDirectoryAdditionFixture(t, m, "/added")
	if err != nil {
		t.Fatal(err)
	}
	s.peerRemovalControlFake.queryError = peerRemovalTransport
	st, err := r.Status(t.Context())
	if !errors.Is(err, peerRemovalTransport) || st.Registered || st.PolicyPresent || !r.registered || !m.ownedDirectories["/added"] || m.directoryGenerations["/added"] != 1 || m.guardDirectoryAdditionOverlap() != nil || s.adds != 1 {
		t.Fatal("fresh observation reused report or revoked historical publication")
	}
}

func TestCephFSDirectoryAdditionReadOnlyStatusHasBoundedDeadline(t *testing.T) {
	m, _, _ := newDirectoryAdditionFixture(t)
	r, err := beginDirectoryAdditionFixture(t, m, "/added")
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	r.original.readClusters = func(ctx context.Context) ([2]string, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 30*time.Second || time.Until(deadline) < 20*time.Second {
			t.Fatal("Status lacks default bounded deadline")
		}
		reads++
		return peerRemovalClusterReader(ctx)
	}
	if st, err := r.Status(t.Context()); err != nil || !st.Registered || reads < 3 {
		t.Fatalf("bounded Status failed: %+v %v", st, err)
	}
}

func TestCephFSDirectoryAdditionFinalProofRetainsFixtureGate(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	s.addError = peerRemovalTransport
	r, _ := beginDirectoryAdditionFixture(t, m, "/added")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var busyDone chan error
	released, joined, busyJoined := false, false, false
	t.Cleanup(func() {
		if !released {
			close(release)
			released = true
		}
		if !joined {
			select {
			case <-done:
				joined = true
			case <-time.After(500 * time.Millisecond):
				t.Error("final proof failed to join")
			}
		}
		if busyDone != nil && !busyJoined {
			select {
			case <-busyDone:
				busyJoined = true
			case <-time.After(500 * time.Millisecond):
				t.Error("overlap admission failed to join after unlock")
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	reads := 0
	s.hook = func(_ *directoryRemovalControlFake, args []string) {
		if slices.Equal(args, []string{"fs", "snapshot", "mirror", "ls", "source"}) {
			reads++
			if reads == 3 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}
	}
	go func() { _, err := m.beginDirectoryAddition(ctx, "/added", peerRemovalClusterReader); done <- err }()
	select {
	case <-entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fresh proof did not reach final native read")
	}
	busyCtx, busyCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer busyCancel()
	busyDone = make(chan error, 1)
	go func() { busyDone <- m.AddDirectory(busyCtx, "/unrelated") }()
	select {
	case err := <-busyDone:
		busyJoined = true
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("fixture gate released during final proof: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("overlap admission ignored watchdog")
	}
	if r.registered || m.ownedDirectories["/added"] || s.adds != 1 {
		t.Fatal("publication occurred before final proof returned")
	}
	close(release)
	released = true
	select {
	case err := <-done:
		joined = true
		if err != nil || !r.registered || !m.ownedDirectories["/added"] || s.adds != 1 {
			t.Fatalf("final proof did not publish under retained gate: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("final proof did not finish")
	}
}

func TestCephFSDirectoryAdditionNewTypedCycleOwnsExactlyOneFreshGeneration(t *testing.T) {
	m, s, _, a, b := newDirectoryRemovalFixture()
	m.peerGeneration = 1
	delete(m.ownedDirectories, "/owned")
	m.Directories, s.directories = nil, nil
	addition, err := beginDirectoryAdditionFixture(t, m, "/owned")
	if err != nil || addition.baseGeneration != 1 || addition.generation != 2 {
		t.Fatalf("first typed owned cycle: %v", err)
	}
	removal, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
	if err != nil || removal.generation != 2 || !addition.superseded {
		t.Fatalf("new removal failed to capture typed generation: %v", err)
	}
	a.stats, b.stats = `{}`, `{}`
	if st, err := removal.Status(t.Context()); err != nil || !st.Released || !removal.completed {
		t.Fatalf("new original cycle did not release: %+v %v", st, err)
	}
	newAddition, err := beginDirectoryAdditionFixture(t, m, "/owned")
	if err != nil || newAddition == addition || newAddition.baseGeneration != 2 || newAddition.generation != 3 || m.directoryGenerations["/owned"] != 3 || !removal.superseded || s.adds != 2 || s.removals != 1 {
		t.Fatalf("new typed re-add reused old cycle: %v", err)
	}
	for i := 0; i < 2; i++ {
		if same, err := beginDirectoryAdditionFixture(t, m, "/owned"); err != nil || same != newAddition || s.adds != 2 || m.directoryGenerations["/owned"] != 3 {
			t.Fatal("repeat advanced new generation again")
		}
	}
	before := len(s.calls)
	if _, err := addition.Status(t.Context()); !errors.Is(err, errCephFSObservationGuard) || len(s.calls) != before {
		t.Fatal("old addition acquired new generation")
	}
	if _, err := removal.Status(t.Context()); !errors.Is(err, errCephFSObservationGuard) || len(s.calls) != before {
		t.Fatal("old removal acquired new generation")
	}
}

func TestCephFSDirectoryAdditionPeerRemovalSupersedesBeforeUnappliedAttempt(t *testing.T) {
	for _, operation := range []string{"legacy", "typed"} {
		t.Run(operation, func(t *testing.T) {
			m, s, _, _, _ := newDirectoryRemovalFixture()
			m.peerGeneration = 1
			addition, err := beginDirectoryAdditionFixture(t, m, "/added")
			if err != nil {
				t.Fatal(err)
			}
			originalPeer, originalGeneration := m.peerID, m.peerGeneration
			originalMON, originalMGR := s.fsPeers, s.managerPeers
			s.peerRemovalControlFake.applyRemoval = false
			s.peerRemovalControlFake.removeError = peerRemovalTransport
			s.peerRemovalControlFake.hook = func(_ *peerRemovalControlFake, args []string) {
				if len(args) >= 9 && slices.Equal(args[len(args)-6:], []string{"fs", "snapshot", "mirror", "peer_remove", "source", originalPeer}) && !addition.superseded {
					t.Fatal("peer CLI dispatched before addition supersession")
				}
			}
			if operation == "legacy" {
				err = m.RemovePeer(t.Context(), originalPeer)
			} else {
				var receipt *CephFSMirrorPeerRemoval
				receipt, err = m.beginPeerRemoval(t.Context(), originalPeer, peerRemovalClusterReader)
				if receipt == nil {
					t.Fatal("typed failed request lost original intent")
				}
			}
			if !errors.Is(err, peerRemovalTransport) || !addition.superseded || m.peerID != originalPeer || m.peerGeneration != originalGeneration || s.fsPeers != originalMON || s.managerPeers != originalMGR || s.peerRemovalControlFake.removals != 1 || m.directoryGenerations["/added"] != 1 {
				t.Fatalf("unapplied %s peer removal retained addition authority or changed scope: %v", operation, err)
			}
			before := len(s.calls)
			if st, err := addition.Status(t.Context()); !errors.Is(err, errCephFSObservationGuard) || st.Registered || len(s.calls) != before {
				t.Fatal("old addition reused unchanged peer after attempted removal")
			}
		})
	}
}

func TestCephFSDirectoryAdditionStalePeerRemovalRetryPreservesNewAddition(t *testing.T) {
	for _, operation := range []string{"legacy", "typed"} {
		t.Run(operation, func(t *testing.T) {
			m, s, _ := newDirectoryAdditionFixture(t)
			addition, err := beginDirectoryAdditionFixture(t, m, "/added")
			if err != nil {
				t.Fatal(err)
			}
			old := &CephFSMirrorPeerRemoval{mirror: m, source: m.source, destination: m.destination, peerID: peerRemovalNewPeer, generation: 0, readClusters: peerRemovalClusterReader, cohort: []cephFSPeerRemovalWitness{{}}}
			m.peerRemoval = old
			before := len(s.calls)
			if operation == "legacy" {
				err = m.RemovePeer(t.Context(), old.peerID)
			} else {
				var same *CephFSMirrorPeerRemoval
				same, err = m.BeginPeerRemoval(t.Context(), old.peerID)
				if same != old {
					t.Fatal("stale retry replaced retained old receipt")
				}
			}
			if !errors.Is(err, errCephFSObservationGuard) || len(s.calls) != before || addition.superseded || !addition.registered || m.directoryGenerations["/added"] != 1 {
				t.Fatalf("stale %s retry invalidated unrelated new addition: %v", operation, err)
			}
			if st, err := addition.Status(t.Context()); err != nil || !st.Registered {
				t.Fatal("new addition lost authority after stale old-peer retry", err)
			}
		})
	}
}

func TestCephFSDirectoryAdditionBusyFixtureGateHasWatchdog(t *testing.T) {
	m, s, _ := newDirectoryAdditionFixture(t)
	m.mu.Lock()
	locked, joined := true, false
	done := make(chan error, 1)
	t.Cleanup(func() {
		if locked {
			m.mu.Unlock()
			locked = false
		}
		if !joined {
			select {
			case <-done:
				joined = true
			case <-time.After(500 * time.Millisecond):
				t.Error("addition failed to join after unlock")
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	t.Cleanup(cancel)
	go func() { _, err := m.beginDirectoryAddition(ctx, "/added", peerRemovalClusterReader); done <- err }()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("busy gate ignored context: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("busy addition ignored watchdog")
	}
	m.mu.Unlock()
	locked = false
	if s.adds != 0 || m.directoryAddition != nil {
		t.Fatal("busy admission dispatched mutation")
	}
}
