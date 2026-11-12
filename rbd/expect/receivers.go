package expect

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"maps"
	"slices"
	"strings"
)

// ReceiverDaemon selects one owned receiver daemon of a Receivers
// expectation by its daemon name.
//
// Baseline is the daemon's status from an earlier mirror.ReceiverStatus
// observation, or the zero value. A non-empty Baseline.ContainerID pins the
// container, so a same-name replacement does not match. Baseline.InstanceID is
// read only with NewInstance; the other Baseline fields are not read.
//
// Running true requires a running receiving process that reports the selected
// pool as running, has discovered the namespace mapping and has a native
// instance ID; false requires a process that is not running. NewInstance also
// requires a native instance ID other than Baseline.InstanceID, as after a
// restart of the same container. None of these prove image replay.
type ReceiverDaemon struct {
	Name        string
	Baseline    mirror.ReceiverDaemonStatus
	Running     bool
	NewInstance bool
}

// Receivers states what a test expects of the owned receiver cohort in one
// mirror.ReceiverStatus observation. Select names the daemons it checks.
// Leader, when set, names the selected running daemon that must be the agreed
// owned leader: it must claim its own instance, and every selected running
// receiver must report that instance as leader. Ready requires the observation's own Ready verdict, and lists
// each daemon problem and foreign instance that prevents it; Ready false
// places no requirement on the verdict. Exact requires the observed cohort to
// be exactly the selected daemons with no foreign native instance.
//
// A match is a statement about the discovery and leader election that the
// observation reported: it does not prove that an image replays, nor that a
// write or a checkpoint arrived.
type Receivers struct {
	Select []ReceiverDaemon
	Leader string
	Ready  bool
	Exact  bool
}

// ReceiverMismatchReason names why a cohort does not satisfy a Receivers
// expectation.
type ReceiverMismatchReason string

// The reasons a ReceiversEvaluation reports.
const (
	// ReceiverMismatchAbsent: a selected daemon is not in the observed cohort.
	ReceiverMismatchAbsent ReceiverMismatchReason = "absent"
	// ReceiverMismatchReplaced: the daemon runs in another container than the
	// baseline pinned. Its other fields are not compared.
	ReceiverMismatchReplaced ReceiverMismatchReason = "replaced"
	// ReceiverMismatchRunning: the process runs although the expectation wants
	// it stopped, or the reverse.
	ReceiverMismatchRunning ReceiverMismatchReason = "running"
	// ReceiverMismatchDiscovered: a running process has not discovered the
	// selected pool and namespace mapping, or reports no instance.
	ReceiverMismatchDiscovered ReceiverMismatchReason = "discovered"
	// ReceiverMismatchInstance: a NewInstance selection still reports the
	// baseline's native instance.
	ReceiverMismatchInstance ReceiverMismatchReason = "instance"
	// ReceiverMismatchProblem: a Ready expectation found a daemon problem
	// code in the observation.
	ReceiverMismatchProblem ReceiverMismatchReason = "problem"
	// ReceiverMismatchUnselected: an Exact expectation found a daemon in the
	// observed cohort that the selection does not name.
	ReceiverMismatchUnselected ReceiverMismatchReason = "unselected"
	// ReceiverMismatchLeader: the agreed owned leader is not the expected one.
	ReceiverMismatchLeader ReceiverMismatchReason = "leader"
	// ReceiverMismatchForeign: a Ready or Exact expectation found a native
	// instance that the fixture does not own.
	ReceiverMismatchForeign ReceiverMismatchReason = "foreign"
	// ReceiverMismatchReady: a Ready expectation found the observation not
	// ready for no reason that another mismatch names.
	ReceiverMismatchReady ReceiverMismatchReason = "ready"
)

// ReceiverMismatch is one reason a cohort does not satisfy an expectation.
// Daemon is the daemon it concerns, empty for ReceiverMismatchLeader,
// ReceiverMismatchForeign and ReceiverMismatchReady. Want and Observed are
// the compared values as text: container IDs for ReceiverMismatchReplaced,
// "running" or "stopped" for ReceiverMismatchRunning, the pool state and
// namespace discovery for ReceiverMismatchDiscovered, instance IDs for
// ReceiverMismatchInstance and ReceiverMismatchForeign (Observed only), the
// problem code for ReceiverMismatchProblem (Observed only) and daemon names
// for ReceiverMismatchLeader. When the named leader matches but its election
// view conflicts with a selected receiver, Observed includes the conflicting views.
type ReceiverMismatch struct {
	Reason         ReceiverMismatchReason
	Daemon         string
	Want, Observed string
}

// String names the daemon, the reason and the evaluated values.
func (m ReceiverMismatch) String() string {
	switch m.Reason {
	case ReceiverMismatchAbsent:
		return m.Daemon + " absent"
	case ReceiverMismatchReplaced:
		return fmt.Sprintf("%s replaced: want container %s, observed %s", m.Daemon, m.Want, m.Observed)
	case ReceiverMismatchRunning, ReceiverMismatchDiscovered, ReceiverMismatchLeader:
		subject := m.Daemon
		if subject == "" {
			subject = "cohort"
		}
		return fmt.Sprintf("%s %s: want %s, observed %s", subject, m.Reason, m.Want, quoteEmpty(m.Observed))
	case ReceiverMismatchInstance:
		return fmt.Sprintf("%s instance: want other than %s, observed %s", m.Daemon, m.Want, m.Observed)
	case ReceiverMismatchProblem:
		return fmt.Sprintf("%s problem %s", m.Daemon, m.Observed)
	case ReceiverMismatchUnselected:
		return m.Daemon + " unselected"
	case ReceiverMismatchForeign:
		return "foreign instance " + m.Observed
	case ReceiverMismatchReady:
		return "cohort not ready"
	}
	return fmt.Sprintf("%s %s", m.Daemon, m.Reason)
}

// ReceiversEvaluation is the result of evaluating one observation.
// Expectation and Observed are copies of what was evaluated, so the
// diagnostics describe that observation even when the caller later reuses its
// slices and maps. Mismatches are ordered by selected or unselected daemon
// name, then in the order of the reason constants, followed by the leader,
// foreign instance (by ID) and ready mismatches; Matched means there are none.
type ReceiversEvaluation struct {
	Expectation Receivers
	Observed    mirror.ReceiverStatus
	Mismatches  []ReceiverMismatch
	Matched     bool
}

// evaluationShown bounds the mismatches that an evaluation's String lists.
const evaluationShown = 8

// String summarizes the expectation and at most eight mismatches.
func (e ReceiversEvaluation) String() string {
	var b strings.Builder
	b.WriteString("receivers [")
	for i, s := range e.Expectation.Select {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(s.Name)
		if !s.Running {
			b.WriteString("(stopped)")
		} else if s.NewInstance {
			b.WriteString("(new instance)")
		}
	}
	b.WriteString("]")
	if e.Expectation.Leader != "" {
		fmt.Fprintf(&b, " leader=%s", e.Expectation.Leader)
	}
	if e.Expectation.Ready {
		b.WriteString(" ready")
	}
	if e.Expectation.Exact {
		b.WriteString(" exact")
	}
	fmt.Fprintf(&b, " in cohort [%s]: ", strings.Join(e.Observed.ExpectedDaemons, " "))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == evaluationShown {
			fmt.Fprintf(&b, "; %d more", len(e.Mismatches)-i)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// Evaluate checks one mirror.ReceiverStatus observation (from ReceiverStatus,
// WaitReceiverReady or a receiver watch event) against the expectation. It
// reads only its arguments: no query, wait or retry, so a test composes it
// with its own polling or assertion helpers. A mismatch is a result; an error
// means no verdict. An empty or duplicate selection, an empty daemon name, a
// Leader that is not a running selection, a NewInstance selection that is not
// running or has no baseline instance, and a Ready expectation with a stopped
// selection are ceph.ErrInvalidRequest. An
// observation with an empty or repeated cohort name, a daemon, problem or
// leader outside its cohort, or a Ready verdict that its own leader, foreign
// instances or problems contradict is a malformed observation, never a
// mismatch. The result shares no slice or map with its arguments.
//
//purpose:observation
func (e Receivers) Evaluate(observed mirror.ReceiverStatus) (ReceiversEvaluation, error) {
	if err := e.validate(); err != nil {
		return ReceiversEvaluation{}, err
	}
	if err := validateReceiverObservation(observed); err != nil {
		return ReceiversEvaluation{}, err
	}
	result := ReceiversEvaluation{
		Expectation: Receivers{Select: slices.Clone(e.Select), Leader: e.Leader, Ready: e.Ready, Exact: e.Exact},
		Observed:    cloneReceiverStatus(observed),
	}
	byName := make(map[string][]ReceiverMismatch)
	selected := make(map[string]bool, len(e.Select))
	for _, s := range e.Select {
		selected[s.Name] = true
		byName[s.Name] = s.mismatches(observed)
	}
	for _, name := range observed.ExpectedDaemons {
		if e.Ready && observed.DaemonProblems[name] != "" {
			byName[name] = append(byName[name], ReceiverMismatch{Reason: ReceiverMismatchProblem, Daemon: name, Observed: observed.DaemonProblems[name]})
		}
		if e.Exact && !selected[name] {
			byName[name] = append(byName[name], ReceiverMismatch{Reason: ReceiverMismatchUnselected, Daemon: name})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		result.Mismatches = append(result.Mismatches, byName[name]...)
	}
	if e.Leader != "" {
		if leader := e.observedLeader(observed); leader != e.Leader {
			result.Mismatches = append(result.Mismatches, ReceiverMismatch{Reason: ReceiverMismatchLeader, Want: e.Leader, Observed: leader})
		}
	}
	if e.Ready || e.Exact {
		for _, id := range slices.Sorted(slices.Values(observed.ForeignInstances)) {
			result.Mismatches = append(result.Mismatches, ReceiverMismatch{Reason: ReceiverMismatchForeign, Observed: id})
		}
	}
	if e.Ready && !observed.Ready && !slices.ContainsFunc(result.Mismatches, func(m ReceiverMismatch) bool {
		return m.Reason == ReceiverMismatchProblem || m.Reason == ReceiverMismatchForeign
	}) {
		result.Mismatches = append(result.Mismatches, ReceiverMismatch{Reason: ReceiverMismatchReady})
	}
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e Receivers) observedLeader(observed mirror.ReceiverStatus) string {
	if observed.LeaderDaemonName != e.Leader {
		return observed.LeaderDaemonName
	}
	leader := observed.Daemons[e.Leader]
	var conflicts []string
	for _, selected := range e.Select {
		if !selected.Running {
			continue
		}
		member := observed.Daemons[selected.Name]
		isLeader := selected.Name == e.Leader
		if member.Leader != isLeader || member.LeaderInstanceID != leader.InstanceID ||
			(isLeader && (leader.InstanceID == "" || leader.InstanceID != observed.LeaderInstanceID)) {
			conflicts = append(conflicts, fmt.Sprintf("%s reports instance=%q leader=%t leader_instance=%q",
				selected.Name, member.InstanceID, member.Leader, member.LeaderInstanceID))
		}
	}
	if len(conflicts) == 0 {
		return e.Leader
	}
	slices.Sort(conflicts)
	return fmt.Sprintf("%s (cohort leader instance=%q, %s)", e.Leader, observed.LeaderInstanceID, strings.Join(conflicts, ", "))
}

func (e Receivers) validate() error {
	if len(e.Select) == 0 {
		return fmt.Errorf("%w: receiver expectation selects no daemon", ceph.ErrInvalidRequest)
	}
	running := make(map[string]bool, len(e.Select))
	for _, s := range e.Select {
		if s.Name == "" {
			return fmt.Errorf("%w: receiver expectation selects an empty daemon name", ceph.ErrInvalidRequest)
		}
		if _, dup := running[s.Name]; dup {
			return fmt.Errorf("%w: receiver expectation selects %s twice", ceph.ErrInvalidRequest, s.Name)
		}
		if s.NewInstance && (!s.Running || s.Baseline.InstanceID == "") {
			return fmt.Errorf("%w: receiver expectation wants a new instance of %s without a running selection and a baseline instance", ceph.ErrInvalidRequest, s.Name)
		}
		if e.Ready && !s.Running {
			return fmt.Errorf("%w: receiver expectation wants a ready cohort with %s stopped", ceph.ErrInvalidRequest, s.Name)
		}
		running[s.Name] = s.Running
	}
	if e.Leader != "" && !running[e.Leader] {
		return fmt.Errorf("%w: receiver expectation wants leader %s that is not a running selection", ceph.ErrInvalidRequest, e.Leader)
	}
	return nil
}

func validateReceiverObservation(observed mirror.ReceiverStatus) error {
	cohort := make(map[string]bool, len(observed.ExpectedDaemons))
	for _, name := range observed.ExpectedDaemons {
		if name == "" || cohort[name] {
			return fmt.Errorf("malformed receiver observation: cohort name %q is empty or repeated", name)
		}
		cohort[name] = true
	}
	for _, names := range [][]string{slices.Collect(maps.Keys(observed.Daemons)), slices.Collect(maps.Keys(observed.DaemonProblems))} {
		for _, name := range names {
			if !cohort[name] {
				return fmt.Errorf("malformed receiver observation: %s reported outside the cohort", name)
			}
		}
	}
	if observed.LeaderDaemonName != "" && !cohort[observed.LeaderDaemonName] {
		return fmt.Errorf("malformed receiver observation: leader %s is outside the cohort", observed.LeaderDaemonName)
	}
	if observed.Ready && (observed.LeaderDaemonName == "" || len(observed.ForeignInstances) != 0 || len(observed.DaemonProblems) != 0) {
		return fmt.Errorf("malformed receiver observation: ready without an owned leader or with foreign instances or problems")
	}
	return nil
}

// mismatches lists the mismatches of one selected daemon in reason order.
func (s ReceiverDaemon) mismatches(observed mirror.ReceiverStatus) []ReceiverMismatch {
	daemon, present := observed.Daemons[s.Name]
	if !present || !slices.Contains(observed.ExpectedDaemons, s.Name) {
		return []ReceiverMismatch{{Reason: ReceiverMismatchAbsent, Daemon: s.Name}}
	}
	if s.Baseline.ContainerID != "" && daemon.ContainerID != s.Baseline.ContainerID {
		return []ReceiverMismatch{{Reason: ReceiverMismatchReplaced, Daemon: s.Name, Want: s.Baseline.ContainerID, Observed: daemon.ContainerID}}
	}
	if daemon.Running != s.Running {
		return []ReceiverMismatch{{Reason: ReceiverMismatchRunning, Daemon: s.Name, Want: runningText(s.Running), Observed: runningText(daemon.Running)}}
	}
	if !s.Running {
		return nil
	}
	var result []ReceiverMismatch
	if daemon.PoolState != "running" || !daemon.NamespaceDiscovered || daemon.InstanceID == "" {
		result = append(result, ReceiverMismatch{Reason: ReceiverMismatchDiscovered, Daemon: s.Name, Want: "pool running, namespace discovered",
			Observed: fmt.Sprintf("pool %s, namespace discovered=%t, instance %s", quoteEmpty(daemon.PoolState), daemon.NamespaceDiscovered, quoteEmpty(daemon.InstanceID))})
	}
	if s.NewInstance && daemon.InstanceID == s.Baseline.InstanceID {
		result = append(result, ReceiverMismatch{Reason: ReceiverMismatchInstance, Daemon: s.Name, Want: s.Baseline.InstanceID, Observed: daemon.InstanceID})
	}
	return result
}

func runningText(running bool) string {
	if running {
		return "running"
	}
	return "stopped"
}

func quoteEmpty(s string) string {
	if s == "" {
		return `""`
	}
	return s
}

func cloneReceiverStatus(s mirror.ReceiverStatus) mirror.ReceiverStatus {
	s.ExpectedDaemons = slices.Clone(s.ExpectedDaemons)
	s.Daemons = maps.Clone(s.Daemons)
	s.Instances = slices.Clone(s.Instances)
	s.ForeignInstances = slices.Clone(s.ForeignInstances)
	s.DaemonProblems = maps.Clone(s.DaemonProblems)
	return s
}
