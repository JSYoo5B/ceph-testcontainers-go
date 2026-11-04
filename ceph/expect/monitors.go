package expect

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Monitors states the monmap membership and quorum a test expects in one
// ceph.Observer.QuorumStatus observation. Every set field is a condition and
// a zero field is not checked; at least one must be set.
//
// FSID pins the cluster: a QuorumStatus read from another cluster, such as a
// rebuilt one, does not match. Members is the exact set of MON names the
// monmap must list and MemberCount the number of members it must list, for a
// test whose members get new names. Quorum is the exact set of MON names the
// quorum must hold and QuorumSize the number it must hold. Names compare as
// sets, so their order is irrelevant.
//
// A match proves only what that one quorum_status answer reported: it does
// not prove that a member outside the quorum is stopped, that one inside it
// accepts new sessions, or which MON leads. Unlike ceph.Observer.WaitForQuorum,
// which accepts any majority of the current monmap, an expectation names the
// members and the quorum that the test requires.
type Monitors struct {
	FSID        string
	Members     []string
	MemberCount int
	Quorum      []string
	QuorumSize  int
}

// MonitorMismatchReason names why an observation does not satisfy a Monitors
// expectation.
type MonitorMismatchReason string

// The reasons a MonitorEvaluation reports.
const (
	// MonitorMismatchFSID: the observation is of a cluster with another FSID.
	MonitorMismatchFSID MonitorMismatchReason = "fsid"
	// MonitorMismatchMemberCount: the monmap lists another number of members.
	MonitorMismatchMemberCount MonitorMismatchReason = "member-count"
	// MonitorMismatchQuorumSize: the quorum holds another number of members.
	MonitorMismatchQuorumSize MonitorMismatchReason = "quorum-size"
	// MonitorMismatchAbsent: a name of Members or Quorum is not in the monmap.
	// Its quorum membership is not reported separately.
	MonitorMismatchAbsent MonitorMismatchReason = "absent"
	// MonitorMismatchUnexpected: the monmap lists a name that Members does not.
	MonitorMismatchUnexpected MonitorMismatchReason = "unexpected"
	// MonitorMismatchOutOfQuorum: a name of Quorum is in the monmap but not in
	// the quorum.
	MonitorMismatchOutOfQuorum MonitorMismatchReason = "out-of-quorum"
	// MonitorMismatchInQuorum: the quorum holds a name that Quorum does not.
	MonitorMismatchInQuorum MonitorMismatchReason = "in-quorum"
)

// MonitorMismatch is one reason an observation does not satisfy a Monitors
// expectation. Name is the MON for the per-member reasons and empty for the
// others. FSID and ObservedFSID are the pinned and the observed FSID for
// MonitorMismatchFSID. Count and ObservedCount are the expected and the
// observed number for MonitorMismatchMemberCount and
// MonitorMismatchQuorumSize.
type MonitorMismatch struct {
	Reason               MonitorMismatchReason
	Name                 string
	FSID, ObservedFSID   string
	Count, ObservedCount int
}

// String names the reason and the evaluated values.
func (m MonitorMismatch) String() string {
	switch m.Reason {
	case MonitorMismatchFSID:
		return fmt.Sprintf("fsid: want %s, observed %s", m.FSID, m.ObservedFSID)
	case MonitorMismatchMemberCount:
		return fmt.Sprintf("members: want %d, observed %d", m.Count, m.ObservedCount)
	case MonitorMismatchQuorumSize:
		return fmt.Sprintf("quorum: want %d, observed %d", m.Count, m.ObservedCount)
	case MonitorMismatchAbsent:
		return fmt.Sprintf("mon.%s absent", m.Name)
	case MonitorMismatchUnexpected:
		return fmt.Sprintf("mon.%s unexpected", m.Name)
	case MonitorMismatchOutOfQuorum:
		return fmt.Sprintf("mon.%s out of quorum", m.Name)
	case MonitorMismatchInQuorum:
		return fmt.Sprintf("mon.%s in quorum", m.Name)
	}
	if m.Name != "" {
		return fmt.Sprintf("mon.%s %s", m.Name, m.Reason)
	}
	return string(m.Reason)
}

// MonitorEvaluation is the result of evaluating one observation. Expectation
// and Observed are copies of what was evaluated, so the diagnostics describe
// that observation even when the caller later reuses its slices. Mismatches
// list the reasons without a MON name first, in the order of the reason
// constants, then the per-member reasons ordered by name and then in the
// order of the reason constants; Matched means there are none.
type MonitorEvaluation struct {
	Expectation Monitors
	Observed    ceph.QuorumStatus
	Mismatches  []MonitorMismatch
	Matched     bool
}

// monitorEvaluationShown bounds the mismatches that String lists.
const monitorEvaluationShown = 8

// String summarizes the expectation, the observed membership and at most
// eight mismatches.
func (e MonitorEvaluation) String() string {
	var b strings.Builder
	b.WriteString("mons want")
	if e.Expectation.FSID != "" {
		fmt.Fprintf(&b, " fsid %s", e.Expectation.FSID)
	}
	if len(e.Expectation.Members) > 0 {
		fmt.Fprintf(&b, " members [%s]", strings.Join(e.Expectation.Members, " "))
	}
	if e.Expectation.MemberCount > 0 {
		fmt.Fprintf(&b, " members=%d", e.Expectation.MemberCount)
	}
	if len(e.Expectation.Quorum) > 0 {
		fmt.Fprintf(&b, " quorum [%s]", strings.Join(e.Expectation.Quorum, " "))
	}
	if e.Expectation.QuorumSize > 0 {
		fmt.Fprintf(&b, " quorum=%d", e.Expectation.QuorumSize)
	}
	members := make([]string, len(e.Observed.MonMap.Mons))
	for i, mon := range e.Observed.MonMap.Mons {
		members[i] = mon.Name
	}
	fmt.Fprintf(&b, " in monmap [%s] quorum [%s]: ", strings.Join(members, " "), strings.Join(e.Observed.QuorumNames, " "))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == monitorEvaluationShown {
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

// Evaluate checks one successful ceph.Observer.QuorumStatus observation
// against the expectation. It reads only its arguments: no query, wait or
// retry, so a test composes it with its own polling or assertion helpers. A
// mismatch is a result; an error means no verdict.
//
// An expectation that sets no field, a negative count, an empty or repeated
// name, an unparsable FSID, a Quorum name outside Members, a count that
// contradicts its name list, or a quorum that is not a majority of the
// expected members is ceph.ErrInvalidRequest: no observation could match it.
// An observation with no monmap member, an empty or repeated name, an empty
// quorum, a quorum name outside the monmap or an unparsable FSID is a
// malformed observation, never a mismatch. The result shares no slice with
// its arguments.
//
//purpose:observation
func (e Monitors) Evaluate(observed ceph.QuorumStatus) (MonitorEvaluation, error) {
	if err := e.validate(); err != nil {
		return MonitorEvaluation{}, err
	}
	monmap, quorum, err := monitorObservation(observed)
	if err != nil {
		return MonitorEvaluation{}, err
	}
	result := MonitorEvaluation{
		Expectation: Monitors{FSID: e.FSID, Members: slices.Clone(e.Members), MemberCount: e.MemberCount,
			Quorum: slices.Clone(e.Quorum), QuorumSize: e.QuorumSize},
		Observed: cloneQuorumStatus(observed),
	}
	if e.FSID != "" && uuid.MustParse(e.FSID) != uuid.MustParse(observed.MonMap.FSID) {
		result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchFSID, FSID: e.FSID, ObservedFSID: observed.MonMap.FSID})
	}
	if e.MemberCount > 0 && len(monmap) != e.MemberCount {
		result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchMemberCount, Count: e.MemberCount, ObservedCount: len(monmap)})
	}
	if e.QuorumSize > 0 && len(quorum) != e.QuorumSize {
		result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchQuorumSize, Count: e.QuorumSize, ObservedCount: len(quorum)})
	}
	named := len(result.Mismatches)
	absent := make(map[string]bool)
	for _, name := range slices.Concat(e.Members, e.Quorum) {
		if !monmap[name] && !absent[name] {
			absent[name] = true
			result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchAbsent, Name: name})
		}
	}
	if len(e.Members) > 0 {
		for _, mon := range observed.MonMap.Mons {
			if !slices.Contains(e.Members, mon.Name) {
				result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchUnexpected, Name: mon.Name})
			}
		}
	}
	if len(e.Quorum) > 0 {
		for _, name := range e.Quorum {
			if monmap[name] && !quorum[name] {
				result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchOutOfQuorum, Name: name})
			}
		}
		for _, name := range observed.QuorumNames {
			if !slices.Contains(e.Quorum, name) {
				result.Mismatches = append(result.Mismatches, MonitorMismatch{Reason: MonitorMismatchInQuorum, Name: name})
			}
		}
	}
	slices.SortStableFunc(result.Mismatches[named:], func(a, b MonitorMismatch) int { return strings.Compare(a.Name, b.Name) })
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

// validate refuses an expectation that no observation could satisfy.
func (e Monitors) validate() error {
	if e.FSID == "" && len(e.Members) == 0 && e.MemberCount == 0 && len(e.Quorum) == 0 && e.QuorumSize == 0 {
		return fmt.Errorf("%w: MON expectation states no condition", ceph.ErrInvalidRequest)
	}
	if e.FSID != "" {
		if parsed, err := uuid.Parse(e.FSID); err != nil || parsed == uuid.Nil {
			return fmt.Errorf("%w: MON expectation pins an invalid FSID %q", ceph.ErrInvalidRequest, e.FSID)
		}
	}
	if e.MemberCount < 0 || e.QuorumSize < 0 {
		return fmt.Errorf("%w: MON expectation has a negative count", ceph.ErrInvalidRequest)
	}
	if err := uniqueNames("MON", "members", e.Members); err != nil {
		return err
	}
	if err := uniqueNames("MON", "quorum", e.Quorum); err != nil {
		return err
	}
	if len(e.Members) > 0 && e.MemberCount > 0 && len(e.Members) != e.MemberCount {
		return fmt.Errorf("%w: MON expectation names %d members but counts %d", ceph.ErrInvalidRequest, len(e.Members), e.MemberCount)
	}
	if len(e.Quorum) > 0 && e.QuorumSize > 0 && len(e.Quorum) != e.QuorumSize {
		return fmt.Errorf("%w: MON expectation names %d quorum members but counts %d", ceph.ErrInvalidRequest, len(e.Quorum), e.QuorumSize)
	}
	if len(e.Members) > 0 {
		for _, name := range e.Quorum {
			if !slices.Contains(e.Members, name) {
				return fmt.Errorf("%w: MON expectation puts mon.%s in the quorum but not in the members", ceph.ErrInvalidRequest, name)
			}
		}
	}
	members, size := max(len(e.Members), e.MemberCount), max(len(e.Quorum), e.QuorumSize)
	if members > 0 && size > 0 && (size > members || 2*size <= members) {
		return fmt.Errorf("%w: MON expectation wants a quorum of %d among %d members, which is not a majority", ceph.ErrInvalidRequest, size, members)
	}
	return nil
}

// uniqueNames refuses an empty or repeated daemon name in one name list of
// an expectation.
func uniqueNames(daemon, what string, names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("%w: %s expectation has an empty name in %s", ceph.ErrInvalidRequest, daemon, what)
		}
		if seen[name] {
			return fmt.Errorf("%w: %s expectation names %s twice in %s", ceph.ErrInvalidRequest, daemon, name, what)
		}
		seen[name] = true
	}
	return nil
}

// monitorObservation checks the observation and returns its monmap and
// quorum names as sets.
func monitorObservation(observed ceph.QuorumStatus) (monmap, quorum map[string]bool, err error) {
	if parsed, err := uuid.Parse(observed.MonMap.FSID); err != nil || parsed == uuid.Nil {
		return nil, nil, fmt.Errorf("malformed quorum observation: no valid FSID %q", observed.MonMap.FSID)
	}
	if len(observed.MonMap.Mons) == 0 {
		return nil, nil, fmt.Errorf("malformed quorum observation: the monmap lists no member")
	}
	monmap = make(map[string]bool, len(observed.MonMap.Mons))
	for _, mon := range observed.MonMap.Mons {
		if mon.Name == "" || monmap[mon.Name] {
			return nil, nil, fmt.Errorf("malformed quorum observation: monmap member %q is empty or repeated", mon.Name)
		}
		monmap[mon.Name] = true
	}
	if len(observed.QuorumNames) == 0 {
		return nil, nil, fmt.Errorf("malformed quorum observation: the quorum is empty")
	}
	quorum = make(map[string]bool, len(observed.QuorumNames))
	for _, name := range observed.QuorumNames {
		if !monmap[name] || quorum[name] {
			return nil, nil, fmt.Errorf("malformed quorum observation: quorum member %q is outside the monmap or repeated", name)
		}
		quorum[name] = true
	}
	return monmap, quorum, nil
}

// cloneQuorumStatus copies every slice of a QuorumStatus.
func cloneQuorumStatus(observed ceph.QuorumStatus) ceph.QuorumStatus {
	clone := observed
	clone.QuorumNames = slices.Clone(observed.QuorumNames)
	clone.MonMap.Mons = slices.Clone(observed.MonMap.Mons)
	for i := range clone.MonMap.Mons {
		clone.MonMap.Mons[i].PublicAddrs.Addrvec = slices.Clone(observed.MonMap.Mons[i].PublicAddrs.Addrvec)
	}
	return clone
}
