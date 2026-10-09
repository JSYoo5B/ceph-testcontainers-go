package multicluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
)

func TestMultiClusterWaitManagerRejectsLateReadyCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			want := error(context.Canceled)
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 10*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			b := &ceph.ManagerContainer{DaemonName: "b", Container: &cephFSManagerCandidate{id: "manager-b", running: true}}
			calls := 0
			ids, err := waitForCephFSManagerCandidates(ctx, time.Millisecond, func(context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error) {
				calls++
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
				return ceph.ManagerStatus{Available: true, ActiveName: "b", ActiveGID: 77}, []*ceph.ManagerContainer{b}, nil
			})
			if err == nil || !errors.Is(err, want) || len(ids) != 0 || calls != 1 {
				t.Fatalf("late manager response published success: ids=%v calls=%d err=%v", ids, calls, err)
			}
		})
	}
}

// A Deadline call after the main test holds the owner proves that the next
// public Status attempt has started, before its owner admission. Both public
// deadlines exceed this context, so their derived contexts retain this probe.
type multiClusterWaitAdmissionProbe struct {
	context.Context
	held   *atomic.Bool
	queued chan struct{}
	once   sync.Once
}

func (p *multiClusterWaitAdmissionProbe) Deadline() (time.Time, bool) {
	if p.held.Load() {
		p.once.Do(func() { close(p.queued) })
	}
	return p.Context.Deadline()
}

type multiClusterWaitRemovalResult struct {
	problem, state string
	policyRemoved  bool
	terminal       bool
	err            error
}

func TestMultiClusterWaitRemovalRetainsReportAcrossOwnerAdmissionCancel(t *testing.T) {
	for _, kind := range []string{"peer", "directory"} {
		for _, deadline := range []bool{false, true} {
			name := kind + "/cancel"
			if deadline {
				name = kind + "/deadline"
			}
			t.Run(name, func(t *testing.T) {
				first := make(chan struct{})
				var signal sync.Once
				var mirror *CephFSMirror
				var wait func(context.Context) multiClusterWaitRemovalResult
				if kind == "peer" {
					r, m, _, _, a, _ := beginPeerRemovalFixture(t)
					mirror = m
					a.queryError = peerRemovalTransport
					a.execHook = func(*peerRemovalDaemonFake, []string) { signal.Do(func() { close(first) }) }
					wait = func(ctx context.Context) multiClusterWaitRemovalResult {
						s, err := r.WaitDrained(ctx)
						return multiClusterWaitRemovalResult{s.Daemons["a"].Problem, s.Daemons["a"].State, s.PolicyRemoved, s.Drained, err}
					}
				} else {
					r, m, _, _, a, _ := beginDirectoryRemovalFixture(t)
					mirror = m
					a.statsError = peerRemovalTransport
					a.statsHook = func(*directoryRemovalDaemonFake) { signal.Do(func() { close(first) }) }
					wait = func(ctx context.Context) multiClusterWaitRemovalResult {
						s, err := r.WaitReleased(ctx)
						return multiClusterWaitRemovalResult{s.Daemons["a"].Problem, s.Daemons["a"].State, s.PolicyRemoved, s.Released, err}
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 1100*time.Millisecond)
				defer cancel()
				var held atomic.Bool
				probe := &multiClusterWaitAdmissionProbe{Context: ctx, held: &held, queued: make(chan struct{})}
				done, exited := make(chan multiClusterWaitRemovalResult, 1), make(chan struct{})
				unlock := func() {
					if held.Swap(false) {
						mirror.mu.Unlock()
					}
				}
				join := func() {
					select {
					case <-exited:
					case <-time.After(time.Second):
						t.Error("wait worker did not join after cancel and gate release")
					}
				}
				// Install cleanup before launch; only a successfully acquired owner
				// sets held. Late hooks signal only and never acquire an orphan gate.
				t.Cleanup(func() { cancel(); unlock(); join() })
				go func() { defer close(exited); done <- wait(probe) }()
				select {
				case <-first:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("wait never reached the original native failure")
				}
				gateCtx, gateCancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
				gateErr := lockRGWSyncObservation(gateCtx, &mirror.mu)
				gateCancel()
				if gateErr != nil {
					t.Fatal("wait held the owner across polls", gateErr)
				}
				held.Store(true)
				select {
				case <-probe.queued:
				case <-time.After(800 * time.Millisecond):
					t.Fatal("next public observation did not reach owner admission")
				}
				want := error(context.DeadlineExceeded)
				if !deadline {
					want = context.Canceled
					cancel()
				}
				var result multiClusterWaitRemovalResult
				select {
				case result = <-done:
				case <-time.After(time.Second):
					t.Fatal("queued observation ignored caller context")
				}
				unlock()
				join()
				if result.terminal || !result.policyRemoved || result.state != "unavailable" || result.problem == "" || !errors.Is(result.err, want) || !errors.Is(result.err, peerRemovalTransport) {
					t.Fatalf("unadmitted cancellation erased native report/cause: %+v", result)
				}
			})
		}
	}
}

func TestMultiClusterWaitLegacyRetainsNativeCauseAcrossContextOnlyAttempt(t *testing.T) {
	for _, kind := range []string{"image", "directory"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			native := errors.New("private-native-query-marker")
			calls := 0
			var err error
			if kind == "image" {
				previous := RBDMirrorImageStatus{SourceImageID: "source", GlobalID: "global", State: "down+unknown"}
				var status RBDMirrorImageStatus
				status, err = waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
					calls++
					if calls == 1 {
						return previous, rbdImageQueryError("read source image", native)
					}
					cancel()
					return RBDMirrorImageStatus{}, context.Canceled
				})
				if status.SourceImageID != previous.SourceImageID || status.State != previous.State || status.ReplayReady {
					t.Fatalf("image report changed: %+v", status)
				}
			} else {
				previous := CephFSMirrorDirectoryStatus{SourceFilesystemID: 1, DestinationFilesystemID: 2, PeerID: cephFSObserverDraftPeer, Directory: "/owned", State: "syncing"}
				var status CephFSMirrorDirectoryStatus
				status, err = waitCephFSObservedDirectory(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
					calls++
					if calls == 1 {
						return previous, cephFSObserveQuery("read directory", native)
					}
					cancel()
					return CephFSMirrorDirectoryStatus{}, context.Canceled
				}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
				if status.SourceFilesystemID != previous.SourceFilesystemID || status.State != previous.State || status.Ready {
					t.Fatalf("directory report changed: %+v", status)
				}
			}
			if calls != 2 || !errors.Is(err, context.Canceled) || !errors.Is(err, native) || strings.Contains(err.Error(), native.Error()) {
				t.Fatalf("context-only attempt erased/exposed native cause: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestMultiClusterWaitAdmittedReportReplacesOlderPartial(t *testing.T) {
	for _, kind := range []string{"peer", "directory"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			var err error
			var result multiClusterWaitRemovalResult
			if kind == "peer" {
				status, cause := waitCephFSPeerDrainWithAdmission(ctx, time.Millisecond, func(context.Context) (CephFSMirrorPeerRemovalStatus, bool, error) {
					calls++
					current := CephFSMirrorPeerRemovalStatus{PeerID: "original", Daemons: map[string]CephFSMirrorPeerRemovalDaemonStatus{"a": {State: "unobserved"}}}
					if calls == 1 {
						current.PolicyRemoved = true
						current.Daemons["a"] = CephFSMirrorPeerRemovalDaemonStatus{State: "unavailable", Problem: "old native problem"}
						return current, true, cephFSObserveQuery("native query", peerRemovalTransport)
					}
					cancel()
					// Admitted newer observations can have initial-looking fields,
					// e.g. a policy-presence read before context expiration.
					return current, true, context.Canceled
				})
				err = cause
				result = multiClusterWaitRemovalResult{status.Daemons["a"].Problem, status.Daemons["a"].State, status.PolicyRemoved, status.Drained, cause}
			} else {
				status, cause := waitCephFSDirectoryReleasedWithAdmission(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryRemovalStatus, bool, error) {
					calls++
					current := CephFSMirrorDirectoryRemovalStatus{Directory: "/owned", Daemons: map[string]CephFSMirrorDirectoryRemovalDaemonStatus{"a": {State: "unobserved"}}}
					if calls == 1 {
						current.PolicyRemoved = true
						current.Daemons["a"] = CephFSMirrorDirectoryRemovalDaemonStatus{State: "unavailable", Problem: "old native problem"}
						return current, true, cephFSObserveQuery("native query", peerRemovalTransport)
					}
					cancel()
					return current, true, context.Canceled
				})
				err = cause
				result = multiClusterWaitRemovalResult{status.Daemons["a"].Problem, status.Daemons["a"].State, status.PolicyRemoved, status.Released, cause}
			}
			if calls != 2 || result.problem != "" || result.state != "unobserved" || result.policyRemoved || result.terminal || !errors.Is(err, context.Canceled) || !errors.Is(err, peerRemovalTransport) {
				t.Fatalf("new admitted report was replaced by stale proof: %+v calls=%d", result, calls)
			}
		})
	}
}

func TestMultiClusterWaitLegacyUsesLatestIndependentCause(t *testing.T) {
	for _, kind := range []string{"image", "directory"} {
		for _, inner := range []bool{false, true} {
			name := kind + "/mixed-native-context"
			if inner {
				name = kind + "/inner-deadline"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				first, latest := errors.New("old-native-marker"), errors.New("latest-native-marker")
				calls := 0
				observe := func() (bool, error) {
					calls++
					if calls == 1 {
						return true, first
					}
					if inner && calls == 2 {
						if ctx.Err() != nil {
							t.Error("outer caller expired before inner attempt timeout")
						}
						return true, context.DeadlineExceeded
					}
					cancel()
					if inner {
						return false, context.Canceled
					}
					return true, errors.Join(latest, context.Canceled)
				}
				var err error
				if kind == "image" {
					_, err = waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
						present, cause := observe()
						status := RBDMirrorImageStatus{}
						if present {
							status.SourceImageID, status.GlobalID = "source", "global"
						}
						return status, rbdImageQueryError("native image query", cause)
					})
				} else {
					_, err = waitCephFSObservedDirectory(ctx, time.Millisecond, func(context.Context) (CephFSMirrorDirectoryStatus, error) {
						present, cause := observe()
						status := CephFSMirrorDirectoryStatus{}
						if present {
							status.SourceFilesystemID, status.DestinationFilesystemID, status.PeerID, status.Directory = 1, 2, "original", "/owned"
						}
						return status, cephFSObserveQuery("native directory query", cause)
					}, func(s CephFSMirrorDirectoryStatus) bool { return s.Ready })
				}
				wantCalls := 2
				want := latest
				if inner {
					wantCalls, want = 3, context.DeadlineExceeded
				}
				if calls != wantCalls || !errors.Is(err, context.Canceled) || !errors.Is(err, want) || errors.Is(err, first) || strings.Contains(err.Error(), "native-marker") {
					t.Fatalf("inner/mixed error was mistaken for caller-only failure: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}

func TestMultiClusterWaitScopedCauseRemainsCanonicalOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	private := errors.New("private-native-credential-marker")
	calls := 0
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) {
		calls++
		if calls == 1 {
			return RBDMirrorImageStatus{SourceImageID: "source", GlobalID: "global"}, scopedRBDImageError(ctx, rbdImageQueryError("native read", private))
		}
		cancel()
		return RBDMirrorImageStatus{}, scopedRBDImageError(ctx, context.Canceled)
	})
	err = scopedRBDImageError(ctx, err)
	if calls != 2 || status.SourceImageID != "source" || status.ReplayReady || !errors.Is(err, context.Canceled) || errors.Is(err, private) || strings.Contains(err.Error(), private.Error()) {
		t.Fatalf("scoped wait exposed a private cause or lost cancellation: %+v %v", status, err)
	}
}
