package expect

import (
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func receiverDaemon(cid, instance string) mirror.ReceiverDaemonStatus {
	return mirror.ReceiverDaemonStatus{ContainerID: cid, ClientName: "client.rbd-mirror." + cid, StartedAt: "2026-11-04T00:00:00Z",
		InstanceID: instance, LeaderInstanceID: "4101", PoolState: "running", Running: true, NamespaceDiscovered: true, Leader: instance == "4101"}
}

func readyReceivers() mirror.ReceiverStatus {
	return mirror.ReceiverStatus{
		Pool: "volumes", ExpectedDaemons: []string{"a", "b"},
		Daemons:          map[string]mirror.ReceiverDaemonStatus{"a": receiverDaemon("cid-a", "4101"), "b": receiverDaemon("cid-b", "4102")},
		LeaderDaemonName: "a", LeaderInstanceID: "4101", Instances: []string{"4101", "4102"},
		DaemonProblems: map[string]string{}, Ready: true,
	}
}

// electedSurvivor is the observation after leader a stopped: b leads, a stays
// in the cohort and the cohort is not ready.
func electedSurvivor() mirror.ReceiverStatus {
	s := readyReceivers()
	s.Daemons["a"] = mirror.ReceiverDaemonStatus{ContainerID: "cid-a", ClientName: "client.rbd-mirror.cid-a"}
	b := s.Daemons["b"]
	b.Leader, b.LeaderInstanceID = true, "4102"
	s.Daemons["b"] = b
	s.LeaderDaemonName, s.LeaderInstanceID, s.Instances = "b", "4102", []string{"4101", "4102"}
	s.DaemonProblems = map[string]string{"a": "receiver-not-running", "b": "receiver-membership-converging"}
	s.Ready = false
	return s
}

func TestReceiversMatchesReadyPinnedCohort(t *testing.T) {
	baseline := readyReceivers()
	e := Receivers{Select: []ReceiverDaemon{{Name: "a", Baseline: baseline.Daemons["a"], Running: true}, {Name: "b", Baseline: baseline.Daemons["b"], Running: true}}, Leader: "a", Ready: true, Exact: true}
	got, err := e.Evaluate(baseline)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 {
		t.Fatalf("baseline does not match itself: %+v %v", got, err)
	}
	if s := got.String(); s != "receivers [a b] leader=a ready exact in cohort [a b]: match" {
		t.Fatalf("summary: %q", s)
	}
}

func TestReceiversMatchesSurvivorElectionWithoutReadiness(t *testing.T) {
	e := Receivers{Select: []ReceiverDaemon{{Name: "b", Running: true}, {Name: "a"}}, Leader: "b"}
	got, err := e.Evaluate(electedSurvivor())
	if err != nil || !got.Matched {
		t.Fatalf("survivor election: %+v %v", got, err)
	}
	// The same election is not a ready cohort; Ready names the problems.
	got, err = Receivers{Select: []ReceiverDaemon{{Name: "b", Running: true}}, Leader: "b", Ready: true}.Evaluate(electedSurvivor())
	want := []ReceiverMismatch{
		{Reason: ReceiverMismatchProblem, Daemon: "a", Observed: "receiver-not-running"},
		{Reason: ReceiverMismatchProblem, Daemon: "b", Observed: "receiver-membership-converging"},
	}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("ready survivor: %+v %v", got, err)
	}
}

func TestReceiversConflictingLeaderViewsWithStoppedMember(t *testing.T) {
	for _, dissenter := range []string{"a", "b"} {
		t.Run(dissenter, func(t *testing.T) {
			status := readyReceivers()
			status.Ready = false
			status.ExpectedDaemons = append(status.ExpectedDaemons, "c")
			status.Daemons["c"] = mirror.ReceiverDaemonStatus{ContainerID: "cid-c"}
			member := status.Daemons[dissenter]
			member.LeaderInstanceID = "4102"
			status.Daemons[dissenter] = member
			expectation := Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}, {Name: "b", Running: true}, {Name: "c"}}, Leader: "a"}
			got, err := expectation.Evaluate(status)
			if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != ReceiverMismatchLeader {
				t.Fatalf("conflicting leader views: %+v, %v", got, err)
			}
			detail := got.Mismatches[0].Observed
			if !strings.HasPrefix(detail, `a (cohort leader instance="4101"`) ||
				!strings.Contains(detail, dissenter+" reports ") || !strings.Contains(detail, `leader_instance="4102"`) {
				t.Fatalf("missing conflicting leader views in %q", detail)
			}
		})
	}
}

func TestReceiversReportsEachReasonFromTheEvaluatedObservation(t *testing.T) {
	baseline := readyReceivers()
	observed := readyReceivers()
	observed.ExpectedDaemons = []string{"a", "b", "c", "d", "e"}
	observed.Daemons["a"] = receiverDaemon("cid-other", "4201")                                                                        // replaced
	observed.Daemons["b"] = mirror.ReceiverDaemonStatus{ContainerID: "cid-b", InstanceID: "4102", PoolState: "running", Running: true} // namespace undiscovered, same instance
	observed.Daemons["c"] = receiverDaemon("cid-c", "4103")                                                                            // wanted stopped
	observed.Daemons["e"] = receiverDaemon("cid-e", "4105")                                                                            // unselected
	observed.LeaderDaemonName, observed.LeaderInstanceID = "c", "4103"
	observed.ForeignInstances = []string{"9902", "9901"}
	observed.DaemonProblems = map[string]string{"b": "receiver-namespace-undiscovered"}
	observed.Ready = false
	e := Receivers{
		Select: []ReceiverDaemon{
			{Name: "a", Baseline: baseline.Daemons["a"], Running: true},
			{Name: "b", Baseline: baseline.Daemons["b"], Running: true, NewInstance: true},
			{Name: "c"},
			{Name: "d"},
			{Name: "f", Running: true},
		},
		Leader: "a", Exact: true,
	}
	got, err := e.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []ReceiverMismatch{
		{Reason: ReceiverMismatchReplaced, Daemon: "a", Want: "cid-a", Observed: "cid-other"},
		{Reason: ReceiverMismatchDiscovered, Daemon: "b", Want: "pool running, namespace discovered", Observed: "pool running, namespace discovered=false, instance 4102"},
		{Reason: ReceiverMismatchInstance, Daemon: "b", Want: "4102", Observed: "4102"},
		{Reason: ReceiverMismatchRunning, Daemon: "c", Want: "stopped", Observed: "running"},
		{Reason: ReceiverMismatchAbsent, Daemon: "d"},
		{Reason: ReceiverMismatchUnselected, Daemon: "e"},
		{Reason: ReceiverMismatchAbsent, Daemon: "f"},
		{Reason: ReceiverMismatchLeader, Want: "a", Observed: "c"},
		{Reason: ReceiverMismatchForeign, Observed: "9901"},
		{Reason: ReceiverMismatchForeign, Observed: "9902"},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"receivers [a b(new instance) c(stopped) d(stopped) f] leader=a exact in cohort [a b c d e]",
		"a replaced: want container cid-a, observed cid-other", "b discovered: want pool running, namespace discovered, observed pool running, namespace discovered=false, instance 4102",
		"b instance: want other than 4102, observed 4102", "c running: want stopped, observed running", "d absent", "e unselected"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	if !strings.HasSuffix(text, "; 2 more") {
		t.Fatalf("summary is not bounded: %q", text)
	}
}

func TestReceiversNewInstancePinsTheContainerAcrossARestart(t *testing.T) {
	baseline := readyReceivers()
	restarted := readyReceivers()
	restarted.Daemons["a"] = receiverDaemon("cid-a", "4301")
	restarted.Daemons["b"] = mirror.ReceiverDaemonStatus{ContainerID: "cid-b", ClientName: "client.rbd-mirror.cid-b", InstanceID: "4102", LeaderInstanceID: "4301", PoolState: "running", Running: true, NamespaceDiscovered: true}
	restarted.LeaderInstanceID, restarted.Instances = "4301", []string{"4102", "4301"}
	e := Receivers{Select: []ReceiverDaemon{{Name: "a", Baseline: baseline.Daemons["a"], Running: true, NewInstance: true}}, Ready: true}
	if got, err := e.Evaluate(restarted); err != nil || !got.Matched {
		t.Fatalf("same-container restart: %+v %v", got, err)
	}
	if got, err := e.Evaluate(baseline); err != nil || got.Matched || got.Mismatches[0].Reason != ReceiverMismatchInstance {
		t.Fatalf("an unchanged instance passed as new: %+v %v", got, err)
	}
	replaced := readyReceivers()
	replaced.Daemons["a"] = receiverDaemon("cid-new", "4301")
	got, err := e.Evaluate(replaced)
	if err != nil || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != ReceiverMismatchReplaced {
		t.Fatalf("a replacement container passed as a restart: %+v %v", got, err)
	}
	// Without a pinned container, a selection by name reads the new one.
	if got, err := (Receivers{Select: []ReceiverDaemon{{Name: "a", Baseline: mirror.ReceiverDaemonStatus{InstanceID: "4101"}, Running: true, NewInstance: true}}}).Evaluate(replaced); err != nil || !got.Matched {
		t.Fatalf("selection by name: %+v %v", got, err)
	}
}

func TestReceiversIgnoresUnselectedDaemonsUnlessExact(t *testing.T) {
	observed := readyReceivers()
	observed.ForeignInstances = nil
	e := Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}}}
	if got, err := e.Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("non-exact selection: %+v %v", got, err)
	}
	e.Exact = true
	got, err := e.Evaluate(observed)
	if err != nil || !slices.Equal(got.Mismatches, []ReceiverMismatch{{Reason: ReceiverMismatchUnselected, Daemon: "b"}}) {
		t.Fatalf("exact selection: %+v %v", got, err)
	}
	// Foreign instances count only for Exact or Ready.
	observed.Ready, observed.ForeignInstances = false, []string{"9901"}
	if got, err := (Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("foreign instance without Exact: %+v %v", got, err)
	}
}

func TestReceiversReadyWithoutExplanationReportsTheVerdict(t *testing.T) {
	observed := readyReceivers()
	observed.Ready = false
	got, err := Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}}, Ready: true}.Evaluate(observed)
	if err != nil || !slices.Equal(got.Mismatches, []ReceiverMismatch{{Reason: ReceiverMismatchReady}}) || !strings.HasSuffix(got.String(), "cohort not ready") {
		t.Fatalf("unexplained non-ready verdict: %+v %v", got, err)
	}
}

func TestReceiversEmptyObservationIsAMismatchNotAMatch(t *testing.T) {
	got, err := Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}}}.Evaluate(mirror.ReceiverStatus{})
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, []ReceiverMismatch{{Reason: ReceiverMismatchAbsent, Daemon: "a"}}) {
		t.Fatalf("empty observation: %+v %v", got, err)
	}
}

func TestReceiversRefusesInvalidExpectations(t *testing.T) {
	observed := readyReceivers()
	for name, e := range map[string]Receivers{
		"empty":                {Leader: "a"},
		"empty name":           {Select: []ReceiverDaemon{{Running: true}}},
		"duplicate":            {Select: []ReceiverDaemon{{Name: "a", Running: true}, {Name: "a"}}},
		"stopped leader":       {Select: []ReceiverDaemon{{Name: "a"}}, Leader: "a"},
		"unselected leader":    {Select: []ReceiverDaemon{{Name: "a", Running: true}}, Leader: "b"},
		"stopped new instance": {Select: []ReceiverDaemon{{Name: "a", Baseline: observed.Daemons["a"], NewInstance: true}}},
		"unpinned instance":    {Select: []ReceiverDaemon{{Name: "a", Running: true, NewInstance: true}}},
		"ready with stopped":   {Select: []ReceiverDaemon{{Name: "a", Running: true}, {Name: "b"}}, Ready: true},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Daemons != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestReceiversRefusesMalformedObservations(t *testing.T) {
	e := Receivers{Select: []ReceiverDaemon{{Name: "a", Running: true}}}
	for name, mutate := range map[string]func(*mirror.ReceiverStatus){
		"empty cohort name":      func(s *mirror.ReceiverStatus) { s.ExpectedDaemons = append(s.ExpectedDaemons, "") },
		"repeated cohort name":   func(s *mirror.ReceiverStatus) { s.ExpectedDaemons = append(s.ExpectedDaemons, "a") },
		"daemon outside cohort":  func(s *mirror.ReceiverStatus) { s.Daemons["z"] = receiverDaemon("cid-z", "4999") },
		"problem outside cohort": func(s *mirror.ReceiverStatus) { s.Ready, s.DaemonProblems["z"] = false, "receiver-not-running" },
		"leader outside cohort":  func(s *mirror.ReceiverStatus) { s.LeaderDaemonName = "z" },
		"ready without leader":   func(s *mirror.ReceiverStatus) { s.LeaderDaemonName = "" },
		"ready with foreign":     func(s *mirror.ReceiverStatus) { s.ForeignInstances = []string{"9901"} },
		"ready with problem":     func(s *mirror.ReceiverStatus) { s.DaemonProblems["b"] = "receiver-leader-converging" },
	} {
		observed := readyReceivers()
		mutate(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestReceiversEvaluationKeepsNoAliasOfCallerSlicesAndMaps(t *testing.T) {
	observed := readyReceivers()
	selection := []ReceiverDaemon{{Name: "a", Running: true}}
	got, err := Receivers{Select: selection}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	selection[0].Name = "z"
	observed.ExpectedDaemons[0], observed.Instances[0] = "z", "z"
	observed.Daemons["a"] = mirror.ReceiverDaemonStatus{}
	observed.DaemonProblems["a"] = "changed"
	if got.Expectation.Select[0].Name != "a" || got.Observed.ExpectedDaemons[0] != "a" || got.Observed.Instances[0] != "4101" || !got.Observed.Daemons["a"].Running || len(got.Observed.DaemonProblems) != 0 {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestReceiversEvaluationBoundsItsSummary(t *testing.T) {
	var selection []ReceiverDaemon
	for i := range 20 {
		selection = append(selection, ReceiverDaemon{Name: "d" + strconv.Itoa(10+i), Running: true})
	}
	got, err := Receivers{Select: selection}.Evaluate(mirror.ReceiverStatus{})
	if err != nil || len(got.Mismatches) != 20 {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "d17 absent; 12 more") || strings.Contains(text, "d18 absent") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
