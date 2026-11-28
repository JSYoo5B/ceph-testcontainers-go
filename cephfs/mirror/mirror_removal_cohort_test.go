package mirror

import (
	"errors"
	"slices"
	"testing"
)

// removalCohortCase begins one removal of a two-daemon fixture and returns
// its cohort, its witness check, the source control fake and the first
// daemon fake.
func removalCohortCase(t *testing.T, removal string) (*cephFSRemovalCohort, cephFSWitnessCheck, *peerRemovalControlFake, *peerRemovalDaemonFake) {
	t.Helper()
	if removal == "peer" {
		r, _, s, _, a, _ := beginPeerRemovalFixture(t)
		return &r.cohort, r.drainCheck(), s, a
	}
	r, _, s, _, a, _ := beginDirectoryRemovalFixture(t)
	return &r.cohort, r.releaseCheck(), s.peerRemovalControlFake, a.peerRemovalDaemonFake
}

// TestRemovalCohortChecksWatchersBeforeAnyWitness requires both removals to
// refuse a foreign watcher before they read any daemon.
func TestRemovalCohortChecksWatchersBeforeAnyWitness(t *testing.T) {
	for _, removal := range []string{"peer", "directory"} {
		t.Run(removal, func(t *testing.T) {
			cohort, check, source, a := removalCohortCase(t, removal)
			source.watchers += "watcher=172.20.0.10:0/9012 client.4300 cookie=3\n"
			calls := len(a.calls)
			recorded := 0
			all, err, stopped := cohort.observe(t.Context(), check, "released", func(string, string, string) { recorded++ })
			if all || !stopped || !errors.Is(err, errCephFSObservationGuard) {
				t.Fatalf("foreign watcher: all=%v stopped=%v err=%v", all, stopped, err)
			}
			if recorded != 0 || len(a.calls) != calls {
				t.Fatalf("a daemon was read before the watcher check: recorded=%d calls=%d", recorded, len(a.calls)-calls)
			}
		})
	}
}

// TestRemovalWitnessConfirmationRefusesChangedStartAndSession runs the shared
// confirmation of both removals against a process whose start time changed
// and a session whose address changed, before and during the observation.
func TestRemovalWitnessConfirmationRefusesChangedStartAndSession(t *testing.T) {
	const later = "2026-10-07T02:00:00Z"
	faults := map[string]func(*peerRemovalDaemonFake){
		"start": func(a *peerRemovalDaemonFake) { a.state.StartedAt = later },
		"start-during": func(a *peerRemovalDaemonFake) {
			reads := 0
			a.stateHook = func(f *peerRemovalDaemonFake) {
				if reads++; reads == 2 {
					f.state.StartedAt = later
				}
			}
		},
		"session": func(a *peerRemovalDaemonFake) { a.address = "172.20.0.10:0/9012" },
		"session-during": func(a *peerRemovalDaemonFake) {
			a.execHook = func(f *peerRemovalDaemonFake, args []string) {
				if slices.Contains(args, "status") && !slices.Contains(args, "peer") {
					f.execHook = func(f *peerRemovalDaemonFake, args []string) {
						if slices.Contains(args, "status") && !slices.Contains(args, "peer") {
							f.address = "172.20.0.10:0/9012"
						}
					}
				}
			}
		},
	}
	for _, removal := range []string{"peer", "directory"} {
		for name, fault := range faults {
			t.Run(removal+"/"+name, func(t *testing.T) {
				cohort, check, _, a := removalCohortCase(t, removal)
				watchers, err := cohort.scope.watchers(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				w := &cohort.witnesses[0]
				if w.containerID != a.id {
					t.Fatalf("first witness is %s", w.containerID)
				}
				if state, err := cohort.confirmWitness(t.Context(), w, watchers, check); err != nil || state == "session-changed" {
					t.Fatalf("unchanged witness: %s %v", state, err)
				}
				fault(a)
				state, err := cohort.confirmWitness(t.Context(), w, watchers, check)
				if state != "session-changed" || !errors.Is(err, errCephFSObservationGuard) {
					t.Fatalf("%s: state=%s err=%v", name, state, err)
				}
			})
		}
	}
}
