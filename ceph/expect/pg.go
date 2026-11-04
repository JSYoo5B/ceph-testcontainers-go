package expect

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// PGs states the placement groups a test expects in one ceph.Pool.PGs
// observation of one pool.
//
// FSID, when set, pins the cluster. Only Name and ID of Pool are read: Name,
// when set, must be the observed pool name, and ID, when nonzero, pins the
// native pool ID (Ceph numbers pools from 1), so a pool that was removed and
// recreated under the same name does not match. A PoolState from an earlier
// observation, such as the PoolAfter of a baseline, pins both.
//
// Select names the PGs to check; when it is empty every reported PG is
// checked. Count, when nonzero, is the exact number of PGs the pool must
// report. An expectation selects no PG unless it sets Count or Select, so an
// empty report never matches by having nothing to check.
//
// Each checked PG must satisfy every condition that is set:
//   - State, when set, is the exact native state string, such as
//     "active+clean"; "active+clean+scrubbing" does not match it.
//   - Require and Forbid list native state tokens (the parts of the state
//     between "+") the PG must and must not report, such as "active" or
//     "degraded". A token matches only whole: "backfill" does not match
//     "backfill_wait".
//   - Up and Acting, when non-nil, are the exact ordered vectors the PG must
//     report, including missing EC shard slots (2147483647); an empty
//     non-nil vector requires an empty report.
//   - ActingSize, when nonzero, is the length of the acting vector.
//   - InUp and InActing list OSD IDs the vector must contain; NotInUp and
//     NotInActing OSD IDs it must not contain. NotInActing can list
//     2147483647 to forbid a missing EC shard slot.
//   - ActingPrimary, when non-empty, lists the reported acting primaries
//     that are acceptable; -1 is the native value for no primary.
//
// A match proves only what this one observation reported: the PG reports
// arrive from the MGR asynchronously, are not an atomic placement snapshot
// and are no read-after-write barrier, and the native PGReady flag is not
// compared. Unlike ceph.Observer.WaitForClean, which waits for the whole
// cluster to report clean, an expectation checks one pool's PGs once and
// names each PG and condition that differs.
type PGs struct {
	FSID                                 string
	Pool                                 ceph.PoolState
	Select                               []ceph.PGID
	Count                                int
	State                                string
	Require, Forbid                      []string
	Up, Acting                           []int
	ActingSize                           int
	InUp, NotInUp, InActing, NotInActing []int
	ActingPrimary                        []int
}

// PGMismatchReason names why a pool PG observation does not satisfy a PGs
// expectation.
type PGMismatchReason string

// The reasons a PGEvaluation reports.
const (
	// PGMismatchFSID: the observation belongs to another cluster.
	PGMismatchFSID PGMismatchReason = "fsid"
	// PGMismatchPool: the observation reports another pool name, or the
	// pinned name now carries another native pool ID.
	PGMismatchPool PGMismatchReason = "pool"
	// PGMismatchCount: the pool reports another number of PGs.
	PGMismatchCount PGMismatchReason = "count"
	// PGMismatchAbsent: a selected PG is not reported. Its conditions are not
	// compared.
	PGMismatchAbsent PGMismatchReason = "absent"
	// PGMismatchState: the PG reports another exact state.
	PGMismatchState PGMismatchReason = "state"
	// PGMismatchRequired: the PG state lacks a required token.
	PGMismatchRequired PGMismatchReason = "required"
	// PGMismatchForbidden: the PG state carries a forbidden token.
	PGMismatchForbidden PGMismatchReason = "forbidden"
	// PGMismatchUp: the PG reports another up vector.
	PGMismatchUp PGMismatchReason = "up"
	// PGMismatchActing: the PG reports another acting vector.
	PGMismatchActing PGMismatchReason = "acting"
	// PGMismatchActingSize: the acting vector has another length.
	PGMismatchActingSize PGMismatchReason = "acting size"
	// PGMismatchUpMissing: the up vector lacks an OSD of InUp.
	PGMismatchUpMissing PGMismatchReason = "up missing"
	// PGMismatchUpPresent: the up vector contains an OSD of NotInUp.
	PGMismatchUpPresent PGMismatchReason = "up present"
	// PGMismatchActingMissing: the acting vector lacks an OSD of InActing.
	PGMismatchActingMissing PGMismatchReason = "acting missing"
	// PGMismatchActingPresent: the acting vector contains an OSD of
	// NotInActing.
	PGMismatchActingPresent PGMismatchReason = "acting present"
	// PGMismatchPrimary: the acting primary is not one of ActingPrimary.
	PGMismatchPrimary PGMismatchReason = "primary"
)

// PGMismatch is one reason a pool PG observation does not satisfy an
// expectation. PGID names the PG, empty for PGMismatchFSID, PGMismatchPool
// and PGMismatchCount. Want and Observed are the compared values as text:
// the token for PGMismatchRequired and PGMismatchForbidden and the OSD ID for
// the missing and present reasons in Want, with the reported state or vector
// in Observed. Both are empty for PGMismatchAbsent.
type PGMismatch struct {
	Reason         PGMismatchReason
	PGID           ceph.PGID
	Want, Observed string
}

// String names the PG, the reason and the evaluated values.
func (m PGMismatch) String() string {
	switch m.Reason {
	case PGMismatchFSID, PGMismatchPool, PGMismatchCount:
		return fmt.Sprintf("%s: want %s, observed %s", m.Reason, m.Want, m.Observed)
	case PGMismatchAbsent:
		return fmt.Sprintf("pg %s absent", m.PGID)
	case PGMismatchRequired, PGMismatchForbidden:
		return fmt.Sprintf("pg %s %s %s: observed %s", m.PGID, m.Reason, m.Want, m.Observed)
	case PGMismatchUpMissing, PGMismatchUpPresent, PGMismatchActingMissing, PGMismatchActingPresent:
		return fmt.Sprintf("pg %s %s osd.%s: observed %s", m.PGID, m.Reason, m.Want, m.Observed)
	case PGMismatchPrimary:
		return fmt.Sprintf("pg %s primary: want one of %s, observed %s", m.PGID, m.Want, m.Observed)
	}
	return fmt.Sprintf("pg %s %s: want %s, observed %s", m.PGID, m.Reason, m.Want, m.Observed)
}

// PGEvaluation is the result of evaluating one pool PG observation.
// Expectation and Observed are deep copies of what was evaluated, so the
// diagnostics describe that observation even when the caller later reuses
// its slices. Mismatches list PGMismatchFSID, PGMismatchPool and
// PGMismatchCount first, then the PGs by numeric PG number, each PG's reasons
// in the order of the reason constants and, within one reason, in the order
// of the expectation's list; Matched means there are none.
type PGEvaluation struct {
	Expectation PGs
	Observed    ceph.PoolPGSnapshot
	Mismatches  []PGMismatch
	Matched     bool
}

// pgEvaluationShown bounds the mismatches that String lists.
const pgEvaluationShown = 8

// String summarizes the expectation and at most eight mismatches.
func (e PGEvaluation) String() string {
	x := e.Expectation
	var b strings.Builder
	b.WriteString("pgs")
	if x.Pool.Name != "" || x.Pool.ID != 0 {
		fmt.Fprintf(&b, " of pool %s", poolText(x.Pool))
	}
	if len(x.Select) > 0 {
		ids := make([]string, len(x.Select))
		for i, id := range x.Select {
			ids[i] = string(id)
		}
		fmt.Fprintf(&b, " [%s]", strings.Join(ids, " "))
	}
	b.WriteString(" want")
	if x.Count != 0 {
		fmt.Fprintf(&b, " count=%d", x.Count)
	}
	if x.State != "" {
		fmt.Fprintf(&b, " state=%s", x.State)
	}
	for _, t := range []struct {
		name   string
		tokens []string
	}{{"require", x.Require}, {"forbid", x.Forbid}} {
		if len(t.tokens) > 0 {
			fmt.Fprintf(&b, " %s=[%s]", t.name, strings.Join(t.tokens, " "))
		}
	}
	for _, v := range []struct {
		name string
		ids  []int
	}{{"up", x.Up}, {"acting", x.Acting}} {
		if v.ids != nil {
			fmt.Fprintf(&b, " %s=%s", v.name, osdsText(v.ids))
		}
	}
	if x.ActingSize != 0 {
		fmt.Fprintf(&b, " acting-size=%d", x.ActingSize)
	}
	for _, v := range []struct {
		name string
		ids  []int
	}{{"in-up", x.InUp}, {"not-in-up", x.NotInUp}, {"in-acting", x.InActing}, {"not-in-acting", x.NotInActing}, {"primary", x.ActingPrimary}} {
		if len(v.ids) > 0 {
			fmt.Fprintf(&b, " %s=%s", v.name, osdsText(v.ids))
		}
	}
	fmt.Fprintf(&b, " in %d observed: ", len(e.Observed.PGs))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == pgEvaluationShown {
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

// Evaluate checks one successful ceph.Pool.PGs observation against the
// expectation. It reads only its arguments: no query, wait or retry, so a
// test composes it with its own polling or assertion helpers. A mismatch is
// a result; an error means no verdict. An expectation that selects no PG
// (neither Count nor Select), a negative count, size, pool ID or OSD ID, an
// invalid FSID, PGID or state token, a repeated entry, a selected PG of
// another pinned pool, and a contradiction (more selected PGs than Count, a
// token both required and forbidden or disagreeing with State, an OSD both
// wanted and forbidden or disagreeing with Up or Acting, an ActingSize other
// than the length of Acting) are ceph.ErrInvalidRequest. An observation whose
// two pool reads disagree on name or ID, a PG of another pool or with an
// invalid or repeated PGID, an empty state, a negative OSD in a vector, a
// primary below -1, or an invalid FSID when the expectation pins one is a
// malformed observation, never a mismatch. The result shares no slice with
// its arguments.
//
//purpose:observation
func (e PGs) Evaluate(observed ceph.PoolPGSnapshot) (PGEvaluation, error) {
	if err := e.validate(); err != nil {
		return PGEvaluation{}, err
	}
	byID, err := observedPGs(observed)
	if err != nil {
		return PGEvaluation{}, err
	}
	otherCluster, err := fsidDiffers(e.FSID, observed.FSID)
	if err != nil {
		return PGEvaluation{}, fmt.Errorf("malformed pool PG observation: %w", err)
	}

	result := PGEvaluation{Expectation: e.clone(), Observed: clonePGs(observed)}
	add := func(reason PGMismatchReason, id ceph.PGID, want, got string) {
		result.Mismatches = append(result.Mismatches, PGMismatch{Reason: reason, PGID: id, Want: want, Observed: got})
	}
	if otherCluster {
		add(PGMismatchFSID, "", e.FSID, observed.FSID)
	}
	if pool := observed.PoolAfter; (e.Pool.Name != "" && pool.Name != e.Pool.Name) || (e.Pool.ID != 0 && pool.ID != e.Pool.ID) {
		add(PGMismatchPool, "", poolText(e.Pool), poolText(ceph.PoolState{Name: pool.Name, ID: pool.ID}))
	}
	if e.Count != 0 && len(observed.PGs) != e.Count {
		add(PGMismatchCount, "", strconv.Itoa(e.Count), strconv.Itoa(len(observed.PGs)))
	}
	checked := slices.Clone(e.Select)
	if len(checked) == 0 {
		for _, pg := range observed.PGs {
			checked = append(checked, pg.PGID)
		}
	}
	slices.SortFunc(checked, comparePGIDs)
	for _, id := range checked {
		pg, present := byID[id]
		if !present {
			add(PGMismatchAbsent, id, "", "")
			continue
		}
		e.evaluatePG(pg, add)
	}
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

// evaluatePG appends the mismatches of one reported PG in reason order.
func (e PGs) evaluatePG(pg ceph.PGState, add func(PGMismatchReason, ceph.PGID, string, string)) {
	id := pg.PGID
	if e.State != "" && pg.State != e.State {
		add(PGMismatchState, id, e.State, pg.State)
	}
	tokens := strings.Split(pg.State, "+")
	for _, t := range e.Require {
		if !slices.Contains(tokens, t) {
			add(PGMismatchRequired, id, t, pg.State)
		}
	}
	for _, t := range e.Forbid {
		if slices.Contains(tokens, t) {
			add(PGMismatchForbidden, id, t, pg.State)
		}
	}
	if e.Up != nil && !slices.Equal(pg.Up, e.Up) {
		add(PGMismatchUp, id, osdsText(e.Up), osdsText(pg.Up))
	}
	if e.Acting != nil && !slices.Equal(pg.Acting, e.Acting) {
		add(PGMismatchActing, id, osdsText(e.Acting), osdsText(pg.Acting))
	}
	if e.ActingSize != 0 && len(pg.Acting) != e.ActingSize {
		add(PGMismatchActingSize, id, strconv.Itoa(e.ActingSize), osdsText(pg.Acting))
	}
	for _, m := range []struct {
		reason  PGMismatchReason
		ids     []int
		vector  []int
		present bool
	}{
		{PGMismatchUpMissing, e.InUp, pg.Up, false},
		{PGMismatchUpPresent, e.NotInUp, pg.Up, true},
		{PGMismatchActingMissing, e.InActing, pg.Acting, false},
		{PGMismatchActingPresent, e.NotInActing, pg.Acting, true},
	} {
		for _, osd := range m.ids {
			if slices.Contains(m.vector, osd) == m.present {
				add(m.reason, id, strconv.Itoa(osd), osdsText(m.vector))
			}
		}
	}
	if len(e.ActingPrimary) > 0 && !slices.Contains(e.ActingPrimary, pg.ActingPrimary) {
		add(PGMismatchPrimary, id, osdsText(e.ActingPrimary), strconv.Itoa(pg.ActingPrimary))
	}
}

func (e PGs) validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: PG expectation "+format, append([]any{ceph.ErrInvalidRequest}, args...)...)
	}
	switch {
	case e.Count == 0 && len(e.Select) == 0:
		return invalid("selects no PG: set Count or Select")
	case e.Count < 0:
		return invalid("wants a negative count")
	case e.Count != 0 && len(e.Select) > e.Count:
		return invalid("selects %d PGs of %d", len(e.Select), e.Count)
	case e.ActingSize < 0:
		return invalid("wants a negative acting size")
	case e.Pool.ID < 0:
		return invalid("pins a negative pool ID")
	}
	if e.FSID != "" {
		if !validFSID(e.FSID) {
			return invalid("pins an invalid FSID")
		}
	}
	seen := make(map[ceph.PGID]bool, len(e.Select))
	for _, id := range e.Select {
		if _, err := ceph.ParsePGID(string(id)); err != nil {
			return invalid("selects invalid PGID %q", id)
		}
		if seen[id] {
			return invalid("selects pg %s twice", id)
		}
		seen[id] = true
		if pool, _, _ := strings.Cut(string(id), "."); e.Pool.ID != 0 && pool != strconv.FormatInt(e.Pool.ID, 10) {
			return invalid("selects pg %s outside pool %d", id, e.Pool.ID)
		}
	}
	var stateTokens []string
	if e.State != "" {
		stateTokens = strings.Split(e.State, "+")
		if slices.Contains(stateTokens, "") {
			return invalid("wants invalid state %q", e.State)
		}
	}
	if _, err := pgTokens("required", e.Require); err != nil {
		return invalid("%v", err)
	}
	forbidden, err := pgTokens("forbidden", e.Forbid)
	if err != nil {
		return invalid("%v", err)
	}
	for _, t := range e.Require {
		if forbidden[t] {
			return invalid("both requires and forbids state %s", t)
		}
		if stateTokens != nil && !slices.Contains(stateTokens, t) {
			return invalid("requires state %s outside %s", t, e.State)
		}
	}
	for t := range forbidden {
		if slices.Contains(stateTokens, t) {
			return invalid("forbids state %s of %s", t, e.State)
		}
	}
	for _, v := range []struct {
		name string
		ids  []int
	}{{"up", e.Up}, {"acting", e.Acting}} {
		if slices.ContainsFunc(v.ids, func(osd int) bool { return osd < 0 }) {
			return invalid("wants a negative OSD in %s", v.name)
		}
	}
	if e.Acting != nil && e.ActingSize != 0 && len(e.Acting) != e.ActingSize {
		return invalid("wants acting %s of size %d", osdsText(e.Acting), e.ActingSize)
	}
	for _, pair := range []struct {
		name      string
		in, notIn []int
		exact     []int
	}{{"up", e.InUp, e.NotInUp, e.Up}, {"acting", e.InActing, e.NotInActing, e.Acting}} {
		in, err := pgOSDs("in "+pair.name, pair.in, 0)
		if err != nil {
			return invalid("%v", err)
		}
		notIn, err := pgOSDs("not in "+pair.name, pair.notIn, 0)
		if err != nil {
			return invalid("%v", err)
		}
		for osd := range in {
			if notIn[osd] {
				return invalid("both wants and forbids osd.%d in %s", osd, pair.name)
			}
			if pair.exact != nil && !slices.Contains(pair.exact, osd) {
				return invalid("wants osd.%d in %s outside %s", osd, pair.name, osdsText(pair.exact))
			}
		}
		for osd := range notIn {
			if slices.Contains(pair.exact, osd) {
				return invalid("forbids osd.%d in %s of %s", osd, pair.name, osdsText(pair.exact))
			}
		}
	}
	if _, err := pgOSDs("primary", e.ActingPrimary, -1); err != nil {
		return invalid("%v", err)
	}
	return nil
}

// pgTokens validates a list of state tokens.
func pgTokens(name string, tokens []string) (map[string]bool, error) {
	seen := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		if t == "" || strings.Contains(t, "+") {
			return nil, fmt.Errorf("lists invalid %s state token %q", name, t)
		}
		if seen[t] {
			return nil, fmt.Errorf("lists %s state %s twice", name, t)
		}
		seen[t] = true
	}
	return seen, nil
}

// pgOSDs validates a list of OSD IDs no smaller than least.
func pgOSDs(name string, ids []int, least int) (map[int]bool, error) {
	seen := make(map[int]bool, len(ids))
	for _, osd := range ids {
		if osd < least {
			return nil, fmt.Errorf("lists invalid OSD %d %s", osd, name)
		}
		if seen[osd] {
			return nil, fmt.Errorf("lists osd.%d %s twice", osd, name)
		}
		seen[osd] = true
	}
	return seen, nil
}

// observedPGs indexes the PGs of a well-formed observation.
func observedPGs(s ceph.PoolPGSnapshot) (map[ceph.PGID]ceph.PGState, error) {
	malformed := func(format string, args ...any) (map[ceph.PGID]ceph.PGState, error) {
		return nil, fmt.Errorf("malformed pool PG observation: "+format, args...)
	}
	if s.PoolBefore.Name == "" || s.PoolBefore.Name != s.PoolAfter.Name || s.PoolBefore.ID != s.PoolAfter.ID || s.PoolAfter.ID < 0 {
		return malformed("the pool reads disagree or name no pool")
	}
	prefix := strconv.FormatInt(s.PoolAfter.ID, 10) + "."
	byID := make(map[ceph.PGID]ceph.PGState, len(s.PGs))
	for _, pg := range s.PGs {
		if _, err := ceph.ParsePGID(string(pg.PGID)); err != nil || !strings.HasPrefix(string(pg.PGID), prefix) {
			return malformed("pg %q is not a PG of pool %d", pg.PGID, s.PoolAfter.ID)
		}
		if _, dup := byID[pg.PGID]; dup {
			return malformed("pg %s reported twice", pg.PGID)
		}
		if pg.State == "" {
			return malformed("pg %s has no state", pg.PGID)
		}
		negative := func(osd int) bool { return osd < 0 }
		if slices.ContainsFunc(pg.Up, negative) || slices.ContainsFunc(pg.Acting, negative) || pg.UpPrimary < -1 || pg.ActingPrimary < -1 {
			return malformed("pg %s reports an invalid OSD", pg.PGID)
		}
		byID[pg.PGID] = pg
	}
	return byID, nil
}

func (e PGs) clone() PGs {
	e.Select = slices.Clone(e.Select)
	e.Require, e.Forbid = slices.Clone(e.Require), slices.Clone(e.Forbid)
	e.Up, e.Acting = slices.Clone(e.Up), slices.Clone(e.Acting)
	e.InUp, e.NotInUp = slices.Clone(e.InUp), slices.Clone(e.NotInUp)
	e.InActing, e.NotInActing = slices.Clone(e.InActing), slices.Clone(e.NotInActing)
	e.ActingPrimary = slices.Clone(e.ActingPrimary)
	return e
}

func clonePGs(s ceph.PoolPGSnapshot) ceph.PoolPGSnapshot {
	s.PGs = slices.Clone(s.PGs)
	for i := range s.PGs {
		s.PGs[i].Up, s.PGs[i].Acting = slices.Clone(s.PGs[i].Up), slices.Clone(s.PGs[i].Acting)
	}
	return s
}

// comparePGIDs orders valid PGIDs by pool, then by numeric PG number.
func comparePGIDs(a, b ceph.PGID) int {
	split := func(id ceph.PGID) (uint64, uint64) {
		pool, seed, _ := strings.Cut(string(id), ".")
		p, _ := strconv.ParseUint(pool, 10, 64)
		s, _ := strconv.ParseUint(seed, 16, 64)
		return p, s
	}
	ap, as := split(a)
	bp, bs := split(b)
	return cmp.Or(cmp.Compare(ap, bp), cmp.Compare(as, bs), strings.Compare(string(a), string(b)))
}

func poolText(p ceph.PoolState) string {
	name, id := p.Name, strconv.FormatInt(p.ID, 10)
	if name == "" {
		name = "*"
	}
	if p.ID == 0 {
		id = "*"
	}
	return name + "/" + id
}

func osdsText(ids []int) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return "[" + strings.Join(parts, " ") + "]"
}
