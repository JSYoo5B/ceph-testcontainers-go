package expect

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/google/uuid"
)

// HealthMuteState states whether a required health check must be muted.
type HealthMuteState string

// The mute states a HealthCheck can require.
const (
	// HealthMuteAny does not compare the check's Muted flag.
	HealthMuteAny HealthMuteState = ""
	// HealthMuted requires the check to be reported as muted.
	HealthMuted HealthMuteState = "muted"
	// HealthUnmuted requires the check to be reported as not muted, so it
	// counts towards the overall status.
	HealthUnmuted HealthMuteState = "unmuted"
)

// HealthCheck is one health check that a Health expectation requires. Code
// is the native, case-sensitive check code such as "OSD_DOWN". Severity,
// when set, is the native severity the check must report (HEALTH_OK,
// HEALTH_WARN or HEALTH_ERR). Muted states the reported mute flag. Count,
// when nonzero, is the native summary count the check must report; a count
// of zero cannot be required. The free-form summary and detail messages are
// never compared: they carry pool names, paths and other application data.
type HealthCheck struct {
	Code     string
	Severity string
	Muted    HealthMuteState
	Count    int64
}

// Health states the health checks and stored mutes a test expects in one
// ceph.Observer.HealthDetails observation. FSID, when set, pins the cluster,
// so an observation of another cluster does not match. Status, when set, is
// the overall status the observation must report; Ceph derives it from the
// worst unmuted check, so HEALTH_OK can accompany muted warnings. Require
// lists checks that must be present and Forbid codes that must be absent.
// Mutes lists codes that must have a stored mute and NoMutes codes that must
// not; a stored sticky mute can outlive its check. Exact also requires no
// check outside Require and no stored mute outside Mutes.
//
// A match proves only what this one observation reported. Ceph raises many
// checks after a delay and clears them at a later monitor tick, so a missing
// check is not proof that the condition behind it is gone, and a present
// check says nothing about the client data path. Unlike the readiness waits
// of package ceph, such as ceph.Observer.WaitForClean, an expectation never
// polls and names each check that differs.
type Health struct {
	FSID    string
	Status  string
	Require []HealthCheck
	Forbid  []string
	Mutes   []string
	NoMutes []string
	Exact   bool
}

// HealthMismatchReason names why a health observation does not satisfy a
// Health expectation.
type HealthMismatchReason string

// The reasons a HealthEvaluation reports.
const (
	// HealthMismatchFSID: the observation belongs to another cluster.
	HealthMismatchFSID HealthMismatchReason = "fsid"
	// HealthMismatchStatus: the overall status differs.
	HealthMismatchStatus HealthMismatchReason = "status"
	// HealthMismatchAbsent: a required check is not reported. Its severity,
	// mute flag and count are not compared.
	HealthMismatchAbsent HealthMismatchReason = "absent"
	// HealthMismatchSeverity: a required check reports another severity.
	HealthMismatchSeverity HealthMismatchReason = "severity"
	// HealthMismatchMuted: a required check reports another mute flag.
	HealthMismatchMuted HealthMismatchReason = "muted"
	// HealthMismatchCount: a required check reports another summary count.
	HealthMismatchCount HealthMismatchReason = "count"
	// HealthMismatchPresent: a forbidden check is reported.
	HealthMismatchPresent HealthMismatchReason = "present"
	// HealthMismatchUnexpected: an Exact expectation found a check that
	// neither Require nor Forbid names (a forbidden check is
	// HealthMismatchPresent).
	HealthMismatchUnexpected HealthMismatchReason = "unexpected"
	// HealthMismatchMuteAbsent: a code listed in Mutes has no stored mute.
	HealthMismatchMuteAbsent HealthMismatchReason = "mute absent"
	// HealthMismatchMutePresent: a code listed in NoMutes has a stored mute.
	HealthMismatchMutePresent HealthMismatchReason = "mute present"
	// HealthMismatchMuteUnexpected: an Exact expectation found a stored mute
	// that neither Mutes nor NoMutes names (a mute listed in NoMutes is
	// HealthMismatchMutePresent).
	HealthMismatchMuteUnexpected HealthMismatchReason = "mute unexpected"
)

// healthReasonRank orders the reasons of one code as the constants above.
var healthReasonRank = map[HealthMismatchReason]int{
	HealthMismatchFSID: 0, HealthMismatchStatus: 1, HealthMismatchAbsent: 2, HealthMismatchSeverity: 3,
	HealthMismatchMuted: 4, HealthMismatchCount: 5, HealthMismatchPresent: 6, HealthMismatchUnexpected: 7,
	HealthMismatchMuteAbsent: 8, HealthMismatchMutePresent: 9, HealthMismatchMuteUnexpected: 10,
}

// HealthMismatch is one reason a health observation does not satisfy an
// expectation. Code is the check or mute code, empty for HealthMismatchFSID
// and HealthMismatchStatus. Want and Observed are the compared values as
// text, both empty where the reason alone says it: Want is empty for
// HealthMismatchPresent, HealthMismatchUnexpected, HealthMismatchMutePresent
// and HealthMismatchMuteUnexpected, whose Observed describes what was
// reported, and Observed is empty for HealthMismatchAbsent and
// HealthMismatchMuteAbsent.
type HealthMismatch struct {
	Reason         HealthMismatchReason
	Code           string
	Want, Observed string
}

// String names the code, the reason and the evaluated values.
func (m HealthMismatch) String() string {
	switch m.Reason {
	case HealthMismatchFSID, HealthMismatchStatus:
		return fmt.Sprintf("%s: want %s, observed %s", m.Reason, m.Want, m.Observed)
	case HealthMismatchAbsent, HealthMismatchMuteAbsent:
		return fmt.Sprintf("%s %s", m.Code, m.Reason)
	case HealthMismatchSeverity, HealthMismatchMuted, HealthMismatchCount:
		return fmt.Sprintf("%s %s: want %s, observed %s", m.Code, m.Reason, m.Want, m.Observed)
	case HealthMismatchPresent, HealthMismatchUnexpected, HealthMismatchMutePresent, HealthMismatchMuteUnexpected:
		return fmt.Sprintf("%s %s: %s", m.Code, m.Reason, m.Observed)
	}
	return fmt.Sprintf("%s %s", m.Code, m.Reason)
}

// HealthEvaluation is the result of evaluating one health observation.
// Expectation and Observed are deep copies of what was evaluated, so the
// diagnostics describe that observation even when the caller later reuses
// its slices and maps. Mismatches list HealthMismatchFSID and
// HealthMismatchStatus first, then the codes in ascending order, each code's
// reasons in the order of the reason constants; Matched means there are none.
type HealthEvaluation struct {
	Expectation Health
	Observed    ceph.HealthSnapshot
	Mismatches  []HealthMismatch
	Matched     bool
}

// healthEvaluationShown bounds the mismatches that String lists.
const healthEvaluationShown = 8

// String summarizes the expectation and at most eight mismatches.
func (e HealthEvaluation) String() string {
	var b strings.Builder
	b.WriteString("health want")
	x := e.Expectation
	if x.Status != "" {
		fmt.Fprintf(&b, " status=%s", x.Status)
	}
	if len(x.Require) > 0 {
		codes := make([]string, len(x.Require))
		for i, c := range x.Require {
			codes[i] = c.Code
		}
		fmt.Fprintf(&b, " require=[%s]", strings.Join(codes, " "))
	}
	for _, list := range []struct {
		name  string
		codes []string
	}{{"forbid", x.Forbid}, {"mutes", x.Mutes}, {"nomutes", x.NoMutes}} {
		if len(list.codes) > 0 {
			fmt.Fprintf(&b, " %s=[%s]", list.name, strings.Join(list.codes, " "))
		}
	}
	if x.Exact {
		b.WriteString(" exact")
	}
	fmt.Fprintf(&b, " in %s with %d checks, %d mutes: ", e.Observed.Status, len(e.Observed.Checks), len(e.Observed.Mutes))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == healthEvaluationShown {
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

// Evaluate checks one successful ceph.Observer.HealthDetails observation
// against the expectation. It reads only its arguments: no query, wait or
// retry, so a test composes it with its own polling or assertion helpers. A
// mismatch is a result; an error means no verdict. An expectation that
// states nothing, an invalid FSID, status, severity or mute state, an empty
// or repeated code, and a contradiction (a code both required and forbidden,
// both in Mutes and NoMutes, a muted required check without a stored mute or
// an unmuted one with a stored mute, or an unmuted required severity worse
// than Status) are ceph.ErrInvalidRequest. An observation with an invalid
// status or severity, an empty check code, an empty or repeated mute code,
// or an invalid FSID when the expectation pins one is a malformed
// observation, never a mismatch. The result shares no slice or map with its
// arguments.
//
//purpose:observation
func (e Health) Evaluate(observed ceph.HealthSnapshot) (HealthEvaluation, error) {
	if err := e.validate(); err != nil {
		return HealthEvaluation{}, err
	}
	if _, ok := healthRank(observed.Status); !ok {
		return HealthEvaluation{}, fmt.Errorf("malformed health observation: invalid status %q", observed.Status)
	}
	for code, check := range observed.Checks {
		if code == "" {
			return HealthEvaluation{}, fmt.Errorf("malformed health observation: a check has no code")
		}
		if _, ok := healthRank(check.Severity); !ok {
			return HealthEvaluation{}, fmt.Errorf("malformed health observation: check %s has invalid severity %q", code, check.Severity)
		}
	}
	mutes := make(map[string]ceph.HealthMute, len(observed.Mutes))
	for _, m := range observed.Mutes {
		if m.Code == "" {
			return HealthEvaluation{}, fmt.Errorf("malformed health observation: a mute has no code")
		}
		if _, dup := mutes[m.Code]; dup {
			return HealthEvaluation{}, fmt.Errorf("malformed health observation: mute %s reported twice", m.Code)
		}
		mutes[m.Code] = m
	}
	otherCluster, err := fsidDiffers(e.FSID, observed.FSID)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("malformed health observation: %w", err)
	}

	result := HealthEvaluation{Expectation: e.clone(), Observed: cloneHealth(observed)}
	add := func(reason HealthMismatchReason, code, want, got string) {
		result.Mismatches = append(result.Mismatches, HealthMismatch{Reason: reason, Code: code, Want: want, Observed: got})
	}
	if otherCluster {
		add(HealthMismatchFSID, "", e.FSID, observed.FSID)
	}
	if e.Status != "" && observed.Status != e.Status {
		add(HealthMismatchStatus, "", e.Status, observed.Status)
	}
	named := make(map[string]bool, len(e.Require)+len(e.Forbid))
	for _, code := range e.Forbid {
		named[code] = true
	}
	for _, want := range e.Require {
		named[want.Code] = true
		got, present := observed.Checks[want.Code]
		if !present {
			add(HealthMismatchAbsent, want.Code, "", "")
			continue
		}
		if want.Severity != "" && got.Severity != want.Severity {
			add(HealthMismatchSeverity, want.Code, want.Severity, got.Severity)
		}
		if want.Muted != HealthMuteAny && got.Muted != (want.Muted == HealthMuted) {
			add(HealthMismatchMuted, want.Code, string(want.Muted), healthMuteState(got.Muted))
		}
		if want.Count != 0 && got.Count != want.Count {
			add(HealthMismatchCount, want.Code, strconv.FormatInt(want.Count, 10), strconv.FormatInt(got.Count, 10))
		}
	}
	for _, code := range e.Forbid {
		if got, present := observed.Checks[code]; present {
			add(HealthMismatchPresent, code, "", healthCheckText(got))
		}
	}
	if e.Exact {
		for code, got := range observed.Checks {
			if !named[code] {
				add(HealthMismatchUnexpected, code, "", healthCheckText(got))
			}
		}
	}
	for _, code := range e.Mutes {
		if _, present := mutes[code]; !present {
			add(HealthMismatchMuteAbsent, code, "", "")
		}
	}
	for _, code := range e.NoMutes {
		if got, present := mutes[code]; present {
			add(HealthMismatchMutePresent, code, "", healthMuteText(got))
		}
	}
	if e.Exact {
		for code, got := range mutes {
			if !slices.Contains(e.Mutes, code) && !slices.Contains(e.NoMutes, code) {
				add(HealthMismatchMuteUnexpected, code, "", healthMuteText(got))
			}
		}
	}
	slices.SortStableFunc(result.Mismatches, func(a, b HealthMismatch) int {
		if a.Code != b.Code {
			return strings.Compare(a.Code, b.Code)
		}
		return healthReasonRank[a.Reason] - healthReasonRank[b.Reason]
	})
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e Health) validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: health expectation "+format, append([]any{ceph.ErrInvalidRequest}, args...)...)
	}
	if e.FSID == "" && e.Status == "" && len(e.Require) == 0 && len(e.Forbid) == 0 && len(e.Mutes) == 0 && len(e.NoMutes) == 0 && !e.Exact {
		return invalid("states nothing")
	}
	if e.FSID != "" {
		if !validFSID(e.FSID) {
			return invalid("pins an invalid FSID")
		}
	}
	status := -1
	if e.Status != "" {
		rank, ok := healthRank(e.Status)
		if !ok {
			return invalid("wants invalid status %q", e.Status)
		}
		status = rank
	}
	codes := func(name string, list []string) (map[string]bool, error) {
		seen := make(map[string]bool, len(list))
		for _, code := range list {
			if code == "" {
				return nil, invalid("lists an empty %s code", name)
			}
			if seen[code] {
				return nil, invalid("lists %s code %s twice", name, code)
			}
			seen[code] = true
		}
		return seen, nil
	}
	forbid, err := codes("forbidden", e.Forbid)
	if err != nil {
		return err
	}
	mutes, err := codes("mute", e.Mutes)
	if err != nil {
		return err
	}
	noMutes, err := codes("no-mute", e.NoMutes)
	if err != nil {
		return err
	}
	for code := range mutes {
		if noMutes[code] {
			return invalid("both wants and forbids a mute of %s", code)
		}
	}
	required := make(map[string]bool, len(e.Require))
	for _, c := range e.Require {
		switch {
		case c.Code == "":
			return invalid("requires a check without a code")
		case required[c.Code]:
			return invalid("requires check %s twice", c.Code)
		case forbid[c.Code]:
			return invalid("both requires and forbids check %s", c.Code)
		}
		required[c.Code] = true
		severity := -1
		if c.Severity != "" {
			rank, ok := healthRank(c.Severity)
			if !ok {
				return invalid("wants invalid severity %q of %s", c.Severity, c.Code)
			}
			severity = rank
		}
		switch c.Muted {
		case HealthMuteAny:
		case HealthMuted:
			if noMutes[c.Code] {
				return invalid("wants %s muted without a stored mute", c.Code)
			}
		case HealthUnmuted:
			if mutes[c.Code] {
				return invalid("wants %s unmuted with a stored mute", c.Code)
			}
			if status >= 0 && severity > status {
				return invalid("wants unmuted %s %s under status %s", c.Code, c.Severity, e.Status)
			}
		default:
			return invalid("wants invalid mute state %q of %s", c.Muted, c.Code)
		}
	}
	return nil
}

func (e Health) clone() Health {
	e.Require, e.Forbid = slices.Clone(e.Require), slices.Clone(e.Forbid)
	e.Mutes, e.NoMutes = slices.Clone(e.Mutes), slices.Clone(e.NoMutes)
	return e
}

func cloneHealth(s ceph.HealthSnapshot) ceph.HealthSnapshot {
	if s.Checks != nil {
		checks := maps.Clone(s.Checks)
		for code, check := range checks {
			check.Details = slices.Clone(check.Details)
			checks[code] = check
		}
		s.Checks = checks
	}
	s.Mutes = slices.Clone(s.Mutes)
	return s
}

// fsidDiffers reports whether a pinned FSID, valid by validation, names
// another cluster than the observed one. Nothing is pinned when want is
// empty; an observed FSID that is not a UUID is an error.
func fsidDiffers(want, observed string) (bool, error) {
	if want == "" {
		return false, nil
	}
	if !validFSID(observed) {
		return false, fmt.Errorf("no valid FSID")
	}
	return uuid.MustParse(observed) != uuid.MustParse(want), nil
}

// validFSID reports whether s is a non-nil UUID, as every native FSID is.
func validFSID(s string) bool {
	parsed, err := uuid.Parse(s)
	return err == nil && parsed != uuid.Nil
}

func healthRank(value string) (int, bool) {
	switch value {
	case "HEALTH_OK":
		return 0, true
	case "HEALTH_WARN":
		return 1, true
	case "HEALTH_ERR":
		return 2, true
	}
	return 0, false
}

func healthMuteState(muted bool) string {
	if muted {
		return string(HealthMuted)
	}
	return string(HealthUnmuted)
}

func healthCheckText(c ceph.HealthCheck) string {
	return fmt.Sprintf("%s count=%d %s", c.Severity, c.Count, healthMuteState(c.Muted))
}

func healthMuteText(m ceph.HealthMute) string {
	switch {
	case m.Sticky:
		return "sticky"
	case m.ExpiresAt != "":
		return "expires " + m.ExpiresAt
	}
	return "until cleared"
}
