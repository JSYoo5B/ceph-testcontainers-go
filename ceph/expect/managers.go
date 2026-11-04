package expect

import (
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Managers states the MGRMap ownership a test expects in one
// ceph.Observer.ManagerStatus observation, such as the status of a
// ceph.ManagerWatchEvent.
//
// Available is always compared: the zero value expects an MGRMap with no
// available MGR. Active, when set, is the name the active MGR must have, and
// ActiveGID, when set, pins its registration: a GID from an earlier
// observation does not match an MGR that re-registered under the same name.
// FormerGID, when set, is the GID of an earlier active MGR that the current
// active one must not carry, which states a failover or a restart of the
// active MGR; it also requires an active MGR. Standbys, when not nil, is the
// exact set of standby names, so an empty non-nil slice requires that there
// is no standby. StandbyCount, when positive, is the number of standbys, for
// a test that does not know their names.
//
// A match proves only what that one MGRMap reported: it does not prove that
// a module serves requests, that a standby process runs, or that a stopped
// MGR has exited. Unlike ceph.Observer.WaitForClean, which requires only an
// available MGR, an expectation names the MGR registrations the test
// requires.
type Managers struct {
	Available    bool
	Active       string
	ActiveGID    uint64
	FormerGID    uint64
	Standbys     []string
	StandbyCount int
}

// ManagerMismatchReason names why an observation does not satisfy a Managers
// expectation.
type ManagerMismatchReason string

// The reasons a ManagerEvaluation reports.
const (
	// ManagerMismatchAvailable: the reported availability differs from the
	// expectation.
	ManagerMismatchAvailable ManagerMismatchReason = "available"
	// ManagerMismatchActive: the active MGR has another name, or there is
	// none.
	ManagerMismatchActive ManagerMismatchReason = "active"
	// ManagerMismatchActiveGID: the active MGR carries another GID than the
	// pinned one, or there is none.
	ManagerMismatchActiveGID ManagerMismatchReason = "active-gid"
	// ManagerMismatchFormerGID: the active MGR still carries the former GID,
	// or there is none.
	ManagerMismatchFormerGID ManagerMismatchReason = "former-gid"
	// ManagerMismatchStandbyCount: the MGRMap lists another number of
	// standbys.
	ManagerMismatchStandbyCount ManagerMismatchReason = "standby-count"
	// ManagerMismatchStandbyAbsent: a name of Standbys is not a standby.
	ManagerMismatchStandbyAbsent ManagerMismatchReason = "standby-absent"
	// ManagerMismatchStandbyUnexpected: a standby that Standbys does not name.
	ManagerMismatchStandbyUnexpected ManagerMismatchReason = "standby-unexpected"
)

// ManagerMismatch is one reason an observation does not satisfy a Managers
// expectation. The expected side is Name for ManagerMismatchActive and the
// standby reasons, GID for ManagerMismatchActiveGID and
// ManagerMismatchFormerGID, and Count for ManagerMismatchStandbyCount. The
// observed side is ObservedAvailable for ManagerMismatchAvailable,
// ObservedName for ManagerMismatchActive (empty without an active MGR),
// ObservedGID for the GID reasons (zero without an active MGR) and for
// ManagerMismatchStandbyUnexpected (that standby's GID), and ObservedCount
// for ManagerMismatchStandbyCount.
type ManagerMismatch struct {
	Reason            ManagerMismatchReason
	Name              string
	GID               uint64
	Count             int
	ObservedAvailable bool
	ObservedName      string
	ObservedGID       uint64
	ObservedCount     int
}

// String names the reason and the evaluated values.
func (m ManagerMismatch) String() string {
	switch m.Reason {
	case ManagerMismatchAvailable:
		return fmt.Sprintf("available: want %t, observed %t", !m.ObservedAvailable, m.ObservedAvailable)
	case ManagerMismatchActive:
		return fmt.Sprintf("active: want mgr.%s, observed %s", m.Name, managerName(m.ObservedName))
	case ManagerMismatchActiveGID:
		return fmt.Sprintf("active gid: want %d, observed %d", m.GID, m.ObservedGID)
	case ManagerMismatchFormerGID:
		return fmt.Sprintf("active gid: want other than %d, observed %d", m.GID, m.ObservedGID)
	case ManagerMismatchStandbyCount:
		return fmt.Sprintf("standbys: want %d, observed %d", m.Count, m.ObservedCount)
	case ManagerMismatchStandbyAbsent:
		return fmt.Sprintf("mgr.%s not a standby", m.Name)
	case ManagerMismatchStandbyUnexpected:
		return fmt.Sprintf("mgr.%s unexpected standby: gid %d", m.Name, m.ObservedGID)
	}
	if m.Name != "" {
		return fmt.Sprintf("mgr.%s %s", m.Name, m.Reason)
	}
	return string(m.Reason)
}

// managerName names an MGR in a diagnostic, or says that there is none.
func managerName(name string) string {
	if name == "" {
		return "none"
	}
	return "mgr." + name
}

// ManagerEvaluation is the result of evaluating one observation. Expectation
// and Observed are copies of what was evaluated, so the diagnostics describe
// that observation even when the caller later reuses its slices. Mismatches
// list the reasons about availability, the active MGR and the standby count
// first, in the order of the reason constants, then the per-standby reasons
// ordered by name and then in the order of the reason constants; Matched
// means there are none.
type ManagerEvaluation struct {
	Expectation Managers
	Observed    ceph.ManagerStatus
	Mismatches  []ManagerMismatch
	Matched     bool
}

// managerEvaluationShown bounds the mismatches that String lists.
const managerEvaluationShown = 8

// String summarizes the expectation, the observed MGRMap and at most eight
// mismatches.
func (e ManagerEvaluation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "mgrs want available=%t", e.Expectation.Available)
	if e.Expectation.Active != "" {
		fmt.Fprintf(&b, " active mgr.%s", e.Expectation.Active)
	}
	if e.Expectation.ActiveGID != 0 {
		fmt.Fprintf(&b, " gid %d", e.Expectation.ActiveGID)
	}
	if e.Expectation.FormerGID != 0 {
		fmt.Fprintf(&b, " gid other than %d", e.Expectation.FormerGID)
	}
	if e.Expectation.Standbys != nil {
		fmt.Fprintf(&b, " standbys [%s]", strings.Join(e.Expectation.Standbys, " "))
	}
	if e.Expectation.StandbyCount > 0 {
		fmt.Fprintf(&b, " standbys=%d", e.Expectation.StandbyCount)
	}
	standbys := make([]string, len(e.Observed.Standbys))
	for i, standby := range e.Observed.Standbys {
		standbys[i] = standby.Name
	}
	fmt.Fprintf(&b, " in available=%t active %s gid %d standbys [%s]: ", e.Observed.Available, managerName(e.Observed.ActiveName), e.Observed.ActiveGID, strings.Join(standbys, " "))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == managerEvaluationShown {
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

// Evaluate checks one successful ceph.Observer.ManagerStatus observation
// against the expectation. It reads only its arguments: no query, wait or
// retry, so a test composes it with its own polling or assertion helpers. A
// mismatch is a result; an error means no verdict.
//
// A negative StandbyCount, a StandbyCount that contradicts Standbys, an empty
// or repeated standby name, or a FormerGID equal to ActiveGID is
// ceph.ErrInvalidRequest. An observation whose active
// name and GID disagree about whether there is an active MGR, that is
// available without one, or that lists a standby with no name or GID or
// with a GID that another registration carries is a malformed
// observation, never a mismatch. The result shares no slice with its
// arguments.
//
//purpose:observation
func (e Managers) Evaluate(observed ceph.ManagerStatus) (ManagerEvaluation, error) {
	if err := e.validate(); err != nil {
		return ManagerEvaluation{}, err
	}
	standbys, err := managerObservation(observed)
	if err != nil {
		return ManagerEvaluation{}, err
	}
	result := ManagerEvaluation{
		Expectation: Managers{Available: e.Available, Active: e.Active, ActiveGID: e.ActiveGID, FormerGID: e.FormerGID,
			Standbys: slices.Clone(e.Standbys), StandbyCount: e.StandbyCount},
		Observed: observed,
	}
	result.Observed.Standbys = slices.Clone(observed.Standbys)
	if observed.Available != e.Available {
		result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchAvailable, ObservedAvailable: observed.Available})
	}
	if e.Active != "" && observed.ActiveName != e.Active {
		result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchActive, Name: e.Active, ObservedName: observed.ActiveName})
	}
	if e.ActiveGID != 0 && observed.ActiveGID != e.ActiveGID {
		result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchActiveGID, GID: e.ActiveGID, ObservedGID: observed.ActiveGID})
	}
	if e.FormerGID != 0 && (observed.ActiveGID == 0 || observed.ActiveGID == e.FormerGID) {
		result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchFormerGID, GID: e.FormerGID, ObservedGID: observed.ActiveGID})
	}
	if e.StandbyCount > 0 && len(observed.Standbys) != e.StandbyCount {
		result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchStandbyCount, Count: e.StandbyCount, ObservedCount: len(observed.Standbys)})
	}
	named := len(result.Mismatches)
	if e.Standbys != nil {
		for _, name := range e.Standbys {
			if !standbys[name] {
				result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchStandbyAbsent, Name: name})
			}
		}
		for _, standby := range observed.Standbys {
			if !slices.Contains(e.Standbys, standby.Name) {
				result.Mismatches = append(result.Mismatches, ManagerMismatch{Reason: ManagerMismatchStandbyUnexpected, Name: standby.Name, ObservedGID: standby.GID})
			}
		}
	}
	slices.SortStableFunc(result.Mismatches[named:], func(a, b ManagerMismatch) int { return strings.Compare(a.Name, b.Name) })
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

// validate refuses an expectation that no observation could satisfy.
func (e Managers) validate() error {
	if e.StandbyCount < 0 {
		return fmt.Errorf("%w: MGR expectation has a negative standby count", ceph.ErrInvalidRequest)
	}
	if err := uniqueNames("MGR", "standbys", e.Standbys); err != nil {
		return err
	}
	if e.Standbys != nil && e.StandbyCount > 0 && len(e.Standbys) != e.StandbyCount {
		return fmt.Errorf("%w: MGR expectation names %d standbys but counts %d", ceph.ErrInvalidRequest, len(e.Standbys), e.StandbyCount)
	}
	if e.FormerGID != 0 && e.FormerGID == e.ActiveGID {
		return fmt.Errorf("%w: MGR expectation pins active GID %d that it also names the former GID", ceph.ErrInvalidRequest, e.ActiveGID)
	}
	return nil
}

// managerObservation checks the observation and returns the set of its
// standby names. A name may repeat, or be the active one, while a restarted
// daemon re-registers; a GID never does.
func managerObservation(observed ceph.ManagerStatus) (map[string]bool, error) {
	if (observed.ActiveName == "") != (observed.ActiveGID == 0) {
		return nil, fmt.Errorf("malformed manager observation: active name %q with GID %d", observed.ActiveName, observed.ActiveGID)
	}
	if observed.Available && observed.ActiveGID == 0 {
		return nil, fmt.Errorf("malformed manager observation: available without an active MGR")
	}
	standbys := make(map[string]bool, len(observed.Standbys))
	gids := map[uint64]bool{observed.ActiveGID: observed.ActiveGID != 0}
	for _, standby := range observed.Standbys {
		if standby.Name == "" || standby.GID == 0 {
			return nil, fmt.Errorf("malformed manager observation: standby %q with GID %d", standby.Name, standby.GID)
		}
		if gids[standby.GID] {
			return nil, fmt.Errorf("malformed manager observation: standby mgr.%s repeats GID %d", standby.Name, standby.GID)
		}
		standbys[standby.Name], gids[standby.GID] = true, true
	}
	return standbys, nil
}
