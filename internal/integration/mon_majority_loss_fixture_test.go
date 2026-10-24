//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// majorityScript drives the mon_status responses of a fake five-MON cluster:
// every MON is in service, in an outage or in a scripted state, and reports an
// uptime that advances with the wall clock unless the script restarts it.
type majorityScript struct {
	f         *fakeMonCluster
	mu        sync.Mutex
	t0        time.Time
	mode      map[string]string
	quorum    []string
	election  int
	noUptime  map[string]bool
	restarted map[string]int64
}

func newMajorityScript() *majorityScript {
	f := newFakeMonCluster()
	f.members = []string{"a", "b", "c", "d", "e"}
	f.quorum = map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	s := &majorityScript{f: f, t0: time.Now(), mode: map[string]string{}, election: f.election, noUptime: map[string]bool{}, restarted: map[string]int64{}}
	s.quorum = slices.Clone(f.members)
	for _, name := range f.members {
		s.mode[name] = "peon"
		f.statusEdits[name] = s.edit(name)
	}
	s.mode["a"] = "leader"
	return s
}

// edit rewrites one MON's response to the scripted state.
func (s *majorityScript) edit(name string) func(map[string]any) {
	return func(v map[string]any) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.noUptime[name] {
			delete(v, "uptime")
		} else {
			v["uptime"] = float64(time.Since(s.t0).Milliseconds() + 600000 - s.restarted[name])
		}
		v["election_epoch"] = s.election
		switch mode := s.mode[name]; mode {
		case "leader", "peon":
			var ranks []int
			for rank, member := range s.f.members {
				if slices.Contains(s.quorum, member) {
					ranks = append(ranks, rank)
				}
			}
			v["state"], v["quorum"] = mode, ranks
		default:
			v["state"], v["quorum"] = mode, []int{}
		}
	}
}

// set puts the named MONs into a scripted state.
func (s *majorityScript) set(mode string, names ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		s.mode[name] = mode
	}
}

// alter rewrites a MON's scripted response further.
func (s *majorityScript) alter(name string, fn func(map[string]any)) {
	base := s.edit(name)
	s.f.statusEdits[name] = func(v map[string]any) { base(v); fn(v) }
}

func (s *majorityScript) outage() { s.set("probing", "a", "b", "c", "d", "e") }

func (s *majorityScript) recorder() *monReplacementRecorder {
	return newMonReplacementRecorder("majority", s.f, fakeMonReplacementBounds)
}

var fastMajorityWait = majorityWait{Window: 30 * time.Millisecond, Interval: 5 * time.Millisecond, Bound: 400 * time.Millisecond, MinRounds: 3}

func majorityBaselineOf(t *testing.T, s *majorityScript) (*monReplacementRecorder, *majorityBaseline) {
	t.Helper()
	rec := s.recorder()
	b, err := captureMajorityBaseline(t.Context(), rec, s.f, 5)
	if err != nil {
		t.Fatal(err)
	}
	return rec, b
}

func TestMajorityBaselineCapturesIdentityBeforeTheCut(t *testing.T) {
	s := newMajorityScript()
	_, b := majorityBaselineOf(t, s)
	if b.FSID != "fsid-1" || !slices.Equal(b.Members, []string{"a", "b", "c", "d", "e"}) || b.MonMapEpoch != 3 || b.ElectionEpoch != 10 || len(b.Mons) != 5 {
		t.Fatalf("baseline %+v", b)
	}
	for name, m := range b.Mons {
		if m.ContainerID != "cid-mon."+name+"-0123456789" || m.StartedAt != "t0" || m.UptimeMS < 600000 || m.UptimeAt.IsZero() || !strings.HasPrefix(m.Address, "v2:10.0.0.") {
			t.Fatalf("mon.%s %+v", name, m)
		}
	}
}

func TestMajorityBaselineRejectsAnUnformedOrUnattributedCluster(t *testing.T) {
	for name, prepare := range map[string]func(*majorityScript){
		"a monitor outside the quorum": func(s *majorityScript) { s.set("probing", "e"); s.f.quorum["e"] = false },
		"a monitor probing":            func(s *majorityScript) { s.set("probing", "e") },
		"a monitor with another name":  func(s *majorityScript) { s.alter("e", func(v map[string]any) { v["name"] = "x" }) },
		"a monitor with another monmap": func(s *majorityScript) {
			s.alter("c", func(v map[string]any) { v["monmap"].(map[string]any)["fsid"] = "fsid-2" })
		},
		"a monitor without an uptime":      func(s *majorityScript) { s.noUptime["d"] = true },
		"a monitor with another election":  func(s *majorityScript) { s.alter("b", func(v map[string]any) { v["election_epoch"] = 99 }) },
		"a monitor with another container": func(s *majorityScript) { s.f.restartOnStatus["mon.d"] = true },
		"a monitor without a container":    func(s *majorityScript) { s.f.partial["d"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			s := newMajorityScript()
			prepare(s)
			if b, err := captureMajorityBaseline(t.Context(), s.recorder(), s.f, 5); err == nil {
				t.Fatalf("accepted baseline %+v", b)
			}
		})
	}
	s := newMajorityScript()
	if _, err := captureMajorityBaseline(t.Context(), s.recorder(), s.f, 3); err == nil {
		t.Fatal("accepted five members as a baseline of three")
	}
}

// A loss is reported only after the stable window: rounds that still show a
// MON in service restart the count.
func TestMajorityLossWaitsForEveryMonitorToLeaveService(t *testing.T) {
	s := newMajorityScript()
	rec, b := majorityBaselineOf(t, s)
	flip := time.AfterFunc(60*time.Millisecond, s.outage)
	defer flip.Stop()
	wait := fastMajorityWait
	wait.Bound = 3 * time.Second
	rounds, err := waitMajorityState(t.Context(), rec, s.f, b, nil, wait, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := rounds[len(rounds)-1]
	if len(rounds) < 4 || len(evaluateMajorityRound(b, rounds[0], nil)) == 0 || len(evaluateMajorityRound(b, last, nil)) != 0 {
		t.Fatalf("%d rounds; first %s; last %s", len(rounds), rounds[0].summary(), last.summary())
	}
	streak := 0
	for i := len(rounds) - 1; i >= 0 && len(evaluateMajorityRound(b, rounds[i], nil)) == 0; i-- {
		streak++
	}
	if streak < 3 || last.At.Sub(rounds[len(rounds)-streak].At) < wait.Window {
		t.Fatalf("accepted a window of %d rounds spanning %v", streak, last.At.Sub(rounds[len(rounds)-streak].At))
	}
}

// Each variant leaves mon.c as something other than an attributed outage. No
// variant may satisfy the stable window, while the same harness accepts the
// plain outage.
func TestMajorityLossIsNotInferredFromOtherSamples(t *testing.T) {
	s := newMajorityScript()
	rec, b := majorityBaselineOf(t, s)
	s.outage()
	if _, err := waitMajorityState(t.Context(), rec, s.f, b, nil, fastMajorityWait, nil); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for name, prepare := range map[string]func(*majorityScript){
		"a monitor still leading": func(s *majorityScript) { s.set("leader", "c") },
		"a monitor still a peon":  func(s *majorityScript) { s.set("peon", "c") },
		"a probing monitor that lists a quorum": func(s *majorityScript) {
			s.alter("c", func(v map[string]any) { v["state"], v["quorum"] = "probing", []int{0, 1, 2} })
		},
		"a state outside the quorum search": func(s *majorityScript) { s.set("synchronizing", "c") },
		"a response with another name":      func(s *majorityScript) { s.alter("c", func(v map[string]any) { v["name"] = "x" }) },
		"a response with another fsid": func(s *majorityScript) {
			s.alter("c", func(v map[string]any) { v["monmap"].(map[string]any)["fsid"] = "fsid-2" })
		},
		"a container started again":   func(s *majorityScript) { s.f.started["mon.c"] = "t9" },
		"a container restarted while": func(s *majorityScript) { s.f.restartOnStatus["mon.c"] = true },
		"a monitor without a container": func(s *majorityScript) {
			s.f.partial["c"] = true
		},
		"a monitor that does not answer": func(s *majorityScript) { s.f.paused["mon.c"] = true },
		"another monmap epoch":           func(s *majorityScript) { s.f.epoch++ },
		"a flapping monitor": func(s *majorityScript) {
			var queries int
			s.alter("c", func(v map[string]any) {
				queries++
				if queries%2 == 0 {
					v["state"], v["quorum"] = "peon", []int{0, 1, 2}
				} else {
					v["state"], v["quorum"] = "probing", []int{}
				}
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newMajorityScript()
			rec, b := majorityBaselineOf(t, s)
			s.outage()
			prepare(s)
			wait := fastMajorityWait
			wait.Bound = 250 * time.Millisecond
			rounds, err := waitMajorityState(t.Context(), rec, s.f, b, nil, wait, nil)
			if err == nil {
				t.Fatalf("accepted a loss: %s", rounds[len(rounds)-1].summary())
			}
			// The window fails for the variant's own reason, not for another.
			var reasons []string
			for _, round := range rounds {
				reasons = append(reasons, evaluateMajorityRound(b, round, nil)...)
			}
			if joined := strings.Join(reasons, "|"); !strings.Contains(joined, "mon.c") && !strings.Contains(joined, "monmap epoch") {
				t.Fatalf("rejected for another reason: %v; %v", err, reasons)
			}
		})
	}
}

func TestMajorityQuorumNamesExactlyTheExpectedMembers(t *testing.T) {
	arrange := func(s *majorityScript) {
		s.set("probing", "a", "b")
		s.set("leader", "c")
		s.set("peon", "d", "e")
		s.quorum = []string{"c", "d", "e"}
		s.election = 12
	}
	want := []string{"c", "d", "e"}
	s := newMajorityScript()
	rec, b := majorityBaselineOf(t, s)
	arrange(s)
	if _, err := waitMajorityState(t.Context(), rec, s.f, b, want, fastMajorityWait, nil); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for name, prepare := range map[string]func(*majorityScript){
		"an outside monitor in service": func(s *majorityScript) { s.set("peon", "b") },
		"a member outside the quorum":   func(s *majorityScript) { s.set("probing", "e") },
		"two leaders":                   func(s *majorityScript) { s.set("leader", "d") },
		"no leader":                     func(s *majorityScript) { s.set("peon", "c") },
		"a larger quorum":               func(s *majorityScript) { s.quorum = []string{"b", "c", "d", "e"} },
		"a smaller quorum":              func(s *majorityScript) { s.quorum = []string{"c", "d"} },
		"an election epoch not advanced": func(s *majorityScript) {
			s.election = b.ElectionEpoch
		},
		"members in different epochs": func(s *majorityScript) {
			s.alter("e", func(v map[string]any) { v["election_epoch"] = 14 })
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newMajorityScript()
			rec, b := majorityBaselineOf(t, s)
			arrange(s)
			prepare(s)
			wait := fastMajorityWait
			wait.Bound = 250 * time.Millisecond
			_, err := waitMajorityState(t.Context(), rec, s.f, b, want, wait, nil)
			if err == nil {
				t.Fatal("accepted the quorum")
			}
			if !strings.Contains(err.Error(), "last round: ") || !(strings.Contains(err.Error(), "mon.") || strings.Contains(err.Error(), "leaders")) {
				t.Fatalf("rejected for another reason: %v", err)
			}
		})
	}
}

func TestMajorityContinuityDetectsARestartedProcess(t *testing.T) {
	s := newMajorityScript()
	rec, b := majorityBaselineOf(t, s)
	time.Sleep(20 * time.Millisecond)
	round := observeMajorityRound(t.Context(), rec, s.f, b)
	if problems := checkMajorityContinuity(b, round); len(problems) != 0 {
		t.Fatalf("continuous processes: %v", problems)
	}
	// The daemon restarted within the same container: its uptime starts over
	// although the container generation is unchanged.
	s.mu.Lock()
	s.restarted["d"] = 590000
	s.mu.Unlock()
	round = observeMajorityRound(t.Context(), rec, s.f, b)
	if problems := checkMajorityContinuity(b, round); len(problems) != 1 || !strings.Contains(problems[0], "mon.d") {
		t.Fatalf("restart not detected: %v", problems)
	}
	s.mu.Lock()
	s.restarted["d"], s.noUptime["e"] = 0, true
	s.mu.Unlock()
	round = observeMajorityRound(t.Context(), rec, s.f, b)
	if problems := checkMajorityContinuity(b, round); len(problems) != 1 || !strings.Contains(problems[0], "mon.e reports no complete uptime") {
		t.Fatalf("missing uptime accepted: %v", problems)
	}
}

func TestMajorityAddressesAreComparedWithTheBaseline(t *testing.T) {
	s := newMajorityScript()
	_, b := majorityBaselineOf(t, s)
	status, err := s.f.QuorumStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if problems := checkMajorityAddresses(b, status); len(problems) != 0 {
		t.Fatalf("unchanged monmap: %v", problems)
	}
	status.MonMap.Mons[1].PublicAddrs.Addrvec[0].Addr = "10.9.9.9:3300"
	status.MonMap.Mons = append(status.MonMap.Mons[:3], status.MonMap.Mons[4:]...)
	problems := checkMajorityAddresses(b, status)
	if len(problems) != 2 || !strings.Contains(strings.Join(problems, "|"), "mon.b address") || !strings.Contains(strings.Join(problems, "|"), "mon.d left the monmap") {
		t.Fatalf("changed monmap: %v", problems)
	}
}

// blockingQuorumCluster never answers a quorum query before its context ends,
// as a control CLI without a quorum does.
type blockingQuorumCluster struct{ *fakeMonCluster }

func (blockingQuorumCluster) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	<-ctx.Done()
	return ceph.QuorumStatus{}, ctx.Err()
}

// The management query during an outage is evidence only: a failure or a
// stall is retained within its bound, and a success is retained as well.
func TestMajorityManagementProbeIsBoundedEvidence(t *testing.T) {
	s := newMajorityScript()
	started := time.Now()
	blocked := probeMajorityManagement(t.Context(), blockingQuorumCluster{s.f}, 30*time.Millisecond)
	if !strings.Contains(blocked.Error, context.DeadlineExceeded.Error()) || blocked.Duration < 30*time.Millisecond || time.Since(started) > time.Second {
		t.Fatalf("blocked probe %+v after %v", blocked, time.Since(started))
	}
	failed := probeMajorityManagement(t.Context(), quorumlessMonCluster{s.f}, time.Second)
	if failed.Error != "quorum_status: timed out" || failed.Quorum != nil {
		t.Fatalf("failed probe %+v", failed)
	}
	passed := probeMajorityManagement(t.Context(), s.f, time.Second)
	if passed.Error != "" || len(passed.Quorum) != 5 {
		t.Fatalf("successful probe %+v", passed)
	}
}
