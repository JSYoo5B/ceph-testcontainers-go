package expect

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// MDSs states the MDS topology a test expects of one filesystem in one
// cephfs.Filesystem.MDSStatus observation, or the Status of one
// cephfs.MDSWatchEvent. Every field is optional, but an expectation must
// state at least one condition.
//
// FilesystemID, when positive, pins the native filesystem ID (fscid), so a
// filesystem recreated under the same name does not match. MaxMDS, when
// positive, is the max_mds the MDSMap must report.
//
// Ranks are the ranks that must be up:active, each with the holder it
// states: a Name or GID pins the daemon, and NotGID requires a holder other
// than that baseline GID, which is how a takeover or a restarted daemon
// shows. Owned additionally requires every holder in Ranks to be one of this
// fixture's daemons. AllRanksActive requires every rank below the observed
// MaxMDS to be up:active, and Exact requires no up:active rank outside
// Ranks.
//
// Standby and StandbyReplay, when non-nil, state the fixture's ordinary and
// replay standbys of this filesystem: the exact count and daemons that must
// be among them.
//
// A match proves only what that one FSMap reported. A rank in replay,
// reconnect or rejoin during a takeover is not up:active, so it does not
// match until the takeover completes; up:active is not proof that clients
// reconnected or that the daemon serves I/O. Standbys of other filesystems
// and external standbys are never in the observation. Unlike
// cephfs.Filesystem.WaitReady, which waits for the configured counts, an
// expectation checks the identities a test names in one captured
// observation and explains which rank or standby differs.
type MDSs struct {
	FilesystemID   int64
	MaxMDS         int
	Ranks          []MDSRank
	Owned          bool
	AllRanksActive bool
	Exact          bool
	Standby        *MDSStandbys
	StandbyReplay  *MDSStandbys
}

// MDSRank states one rank that must be up:active and, optionally, its
// holder. Name and GID, when set, must equal the holder's. NotGID, when set,
// must differ from the holder's GID: a baseline GID that a takeover or a
// restart replaces. A rank with none of them only has to be up:active.
type MDSRank struct {
	Rank   int
	Name   string
	GID    uint64
	NotGID uint64
}

// MDSStandbys states one standby list of an MDSs expectation. Count is the
// exact number of entries the list must hold, and Daemons are the daemons
// that must be among them, so Count may not be smaller than len(Daemons). A
// zero MDSStandbys requires an empty list.
type MDSStandbys struct {
	Count   int
	Daemons []MDSDaemon
}

// MDSDaemon names one expected MDS daemon. Name, when set, selects the entry
// of that name; otherwise GID selects it, so one of them is required to
// select a standby. GID, when set, must equal the selected entry's GID and
// NotGID, when set, must differ from it. In an MDSMismatch it is the
// expectation of that rank or standby.
type MDSDaemon struct {
	Name   string
	GID    uint64
	NotGID uint64
}

// MDSList names the list of a cephfs.FilesystemStatus a mismatch refers to.
type MDSList string

// The lists of a cephfs.FilesystemStatus.
const (
	// MDSListActive is FilesystemStatus.Active, the up:active ranks.
	MDSListActive MDSList = "active"
	// MDSListStandby is FilesystemStatus.Standby, the owned ordinary
	// standbys.
	MDSListStandby MDSList = "standby"
	// MDSListStandbyReplay is FilesystemStatus.StandbyReplay, the owned
	// journal followers.
	MDSListStandbyReplay MDSList = "standby-replay"
)

// MDSMismatchReason names why an observation does not satisfy an MDSs
// expectation.
type MDSMismatchReason string

// The reasons an MDSEvaluation reports.
const (
	// MDSMismatchFilesystem: the filesystem ID differs from the pinned one.
	MDSMismatchFilesystem MDSMismatchReason = "filesystem"
	// MDSMismatchMaxMDS: max_mds differs from the expectation.
	MDSMismatchMaxMDS MDSMismatchReason = "max_mds"
	// MDSMismatchInactive: an expected rank has no up:active holder.
	MDSMismatchInactive MDSMismatchReason = "inactive"
	// MDSMismatchName: the holder of a rank has another name.
	MDSMismatchName MDSMismatchReason = "name"
	// MDSMismatchGID: the rank holder or the standby has another GID than
	// the pinned one, so it is another daemon or the daemon restarted.
	MDSMismatchGID MDSMismatchReason = "gid"
	// MDSMismatchUnchanged: the rank holder or the standby still has the GID
	// it must no longer have.
	MDSMismatchUnchanged MDSMismatchReason = "unchanged"
	// MDSMismatchUnowned: Owned is set and the holder of a rank is not one of
	// this fixture's daemons.
	MDSMismatchUnowned MDSMismatchReason = "unowned"
	// MDSMismatchUnselected: an Exact expectation found an up:active rank
	// that Ranks does not name.
	MDSMismatchUnselected MDSMismatchReason = "unselected"
	// MDSMismatchCount: a standby list has another number of entries.
	MDSMismatchCount MDSMismatchReason = "count"
	// MDSMismatchAbsent: an expected standby is not in its list.
	MDSMismatchAbsent MDSMismatchReason = "absent"
)

// MDSMismatch is one reason an observation does not satisfy an MDSs
// expectation. List is the list it refers to, empty for
// MDSMismatchFilesystem and MDSMismatchMaxMDS. Rank is the active rank for
// MDSListActive and zero otherwise. Want is the expected daemon of that rank
// or standby, zero for MDSMismatchUnselected and for a rank expected only
// to be up:active. Observed is the evaluated rank holder or standby entry,
// zero when there is none. WantValue and ObservedValue are the compared
// numbers of MDSMismatchFilesystem, MDSMismatchMaxMDS and MDSMismatchCount,
// zero otherwise.
type MDSMismatch struct {
	Reason                   MDSMismatchReason
	List                     MDSList
	Rank                     int
	Want                     MDSDaemon
	Observed                 cephfs.MDSStatus
	WantValue, ObservedValue int64
}

// String names the rank or standby, the reason and the evaluated values.
func (m MDSMismatch) String() string {
	var subject string
	switch {
	case m.List == "":
	case m.List == MDSListActive:
		subject = "rank " + strconv.Itoa(m.Rank)
	case m.Want.Name != "":
		subject = string(m.List) + " mds." + m.Want.Name
	case m.Want.GID != 0:
		subject = string(m.List) + " gid " + strconv.FormatUint(m.Want.GID, 10)
	default:
		subject = string(m.List)
	}
	switch m.Reason {
	case MDSMismatchFilesystem:
		return fmt.Sprintf("filesystem id: want %d, observed %d", m.WantValue, m.ObservedValue)
	case MDSMismatchMaxMDS:
		return fmt.Sprintf("max_mds: want %d, observed %d", m.WantValue, m.ObservedValue)
	case MDSMismatchInactive, MDSMismatchAbsent:
		return subject + " " + string(m.Reason)
	case MDSMismatchName:
		return fmt.Sprintf("%s name: want mds.%s, observed mds.%s", subject, m.Want.Name, m.Observed.Name)
	case MDSMismatchGID:
		return fmt.Sprintf("%s gid: want %d, observed mds.%s gid %d", subject, m.Want.GID, m.Observed.Name, m.Observed.GID)
	case MDSMismatchUnchanged:
		return fmt.Sprintf("%s unchanged: mds.%s still has gid %d", subject, m.Observed.Name, m.Observed.GID)
	case MDSMismatchUnowned, MDSMismatchUnselected:
		return fmt.Sprintf("%s %s: mds.%s gid %d owned=%t", subject, m.Reason, m.Observed.Name, m.Observed.GID, m.Observed.Owned)
	case MDSMismatchCount:
		return fmt.Sprintf("%s count: want %d, observed %d", subject, m.WantValue, m.ObservedValue)
	}
	return strings.TrimSpace(subject + " " + string(m.Reason))
}

// MDSEvaluation is the result of evaluating one observation. Expectation and
// Observed are copies of what was evaluated, so the diagnostics describe that
// observation even when the caller later reuses its slices. Mismatches are
// ordered filesystem ID, max_mds, the active ranks by rank, the standbys and
// then the replay standbys; within one rank in the order of the reason
// constants, and within one standby list the count before the expected
// daemons in their order. Matched means there are none.
type MDSEvaluation struct {
	Expectation MDSs
	Observed    cephfs.FilesystemStatus
	Mismatches  []MDSMismatch
	Matched     bool
}

// mdsEvaluationShown bounds the mismatches that String lists.
const mdsEvaluationShown = 8

// String summarizes the expectation, the observation and at most eight
// mismatches.
func (e MDSEvaluation) String() string {
	var b strings.Builder
	b.WriteString("mds want")
	x := e.Expectation
	if x.FilesystemID > 0 {
		fmt.Fprintf(&b, " fs=%d", x.FilesystemID)
	}
	if x.MaxMDS > 0 {
		fmt.Fprintf(&b, " max_mds=%d", x.MaxMDS)
	}
	if len(x.Ranks) > 0 {
		ranks := make([]string, len(x.Ranks))
		for i, r := range x.Ranks {
			ranks[i] = strconv.Itoa(r.Rank)
		}
		fmt.Fprintf(&b, " ranks [%s]", strings.Join(ranks, " "))
	}
	for _, flag := range []struct {
		set  bool
		name string
	}{{x.Owned, "owned"}, {x.AllRanksActive, "all-active"}, {x.Exact, "exact"}} {
		if flag.set {
			b.WriteString(" " + flag.name)
		}
	}
	if x.Standby != nil {
		fmt.Fprintf(&b, " standby=%d", x.Standby.Count)
	}
	if x.StandbyReplay != nil {
		fmt.Fprintf(&b, " standby-replay=%d", x.StandbyReplay.Count)
	}
	o := e.Observed
	fmt.Fprintf(&b, " in %q fs=%d max_mds=%d active=%d standby=%d standby-replay=%d: ",
		o.FilesystemName, o.FilesystemID, o.MaxMDS, len(o.Active), len(o.Standby), len(o.StandbyReplay))
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == mdsEvaluationShown {
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

// Evaluate checks one successful MDSStatus observation against the
// expectation. It reads only its arguments: no query, wait or retry, so a
// test composes it with its own polling or assertion helpers. A mismatch is
// a result; an error means no verdict.
//
// An expectation that states nothing, has a negative FilesystemID, MaxMDS,
// rank or standby count, names a rank twice or at or above its own MaxMDS,
// sets Owned without Ranks, pins a GID equal to its NotGID, selects a
// standby by neither name nor GID, expects more standbys than its Count, or
// names one daemon or GID in two places is ceph.ErrInvalidRequest. An
// observation with a negative filesystem ID, a max_mds below 1 (such as the
// zero Status of a cephfs.MDSWatchEvent gap), an entry without a
// name or GID or in a state its list does not hold, an unowned standby, a
// repeated active rank, or a name or GID in two entries is a malformed
// observation, never a mismatch. The result shares no slice with its
// arguments.
//
//purpose:observation
func (e MDSs) Evaluate(observed cephfs.FilesystemStatus) (MDSEvaluation, error) {
	if err := e.validate(); err != nil {
		return MDSEvaluation{}, err
	}
	if err := validateMDSObservation(observed); err != nil {
		return MDSEvaluation{}, err
	}
	result := MDSEvaluation{Expectation: e.clone(), Observed: cloneFilesystemStatus(observed)}
	if e.FilesystemID > 0 && observed.FilesystemID != e.FilesystemID {
		result.Mismatches = append(result.Mismatches, MDSMismatch{Reason: MDSMismatchFilesystem, WantValue: e.FilesystemID, ObservedValue: observed.FilesystemID})
	}
	if e.MaxMDS > 0 && observed.MaxMDS != e.MaxMDS {
		result.Mismatches = append(result.Mismatches, MDSMismatch{Reason: MDSMismatchMaxMDS, WantValue: int64(e.MaxMDS), ObservedValue: int64(observed.MaxMDS)})
	}
	result.Mismatches = append(result.Mismatches, e.rankMismatches(observed.Active, observed.MaxMDS)...)
	result.Mismatches = append(result.Mismatches, standbyMismatches(MDSListStandby, e.Standby, observed.Standby)...)
	result.Mismatches = append(result.Mismatches, standbyMismatches(MDSListStandbyReplay, e.StandbyReplay, observed.StandbyReplay)...)
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

// rankMismatches compares the active list, ordered by rank; maxMDS is the
// observed max_mds.
func (e MDSs) rankMismatches(active []cephfs.MDSStatus, maxMDS int) []MDSMismatch {
	byRank := make(map[int]cephfs.MDSStatus, len(active))
	for _, holder := range active {
		byRank[holder.Rank] = holder
	}
	selected := make(map[int]bool, len(e.Ranks))
	var out []MDSMismatch
	for _, r := range e.Ranks {
		selected[r.Rank] = true
		want := MDSDaemon{Name: r.Name, GID: r.GID, NotGID: r.NotGID}
		holder, ok := byRank[r.Rank]
		if !ok {
			out = append(out, MDSMismatch{Reason: MDSMismatchInactive, List: MDSListActive, Rank: r.Rank, Want: want})
			continue
		}
		mismatch := func(reason MDSMismatchReason) {
			out = append(out, MDSMismatch{Reason: reason, List: MDSListActive, Rank: r.Rank, Want: want, Observed: holder})
		}
		if r.Name != "" && holder.Name != r.Name {
			mismatch(MDSMismatchName)
		}
		if r.GID != 0 && holder.GID != r.GID {
			mismatch(MDSMismatchGID)
		}
		if r.NotGID != 0 && holder.GID == r.NotGID {
			mismatch(MDSMismatchUnchanged)
		}
		if e.Owned && !holder.Owned {
			mismatch(MDSMismatchUnowned)
		}
	}
	if e.AllRanksActive {
		for rank := range maxMDS {
			if _, ok := byRank[rank]; !ok && !selected[rank] {
				out = append(out, MDSMismatch{Reason: MDSMismatchInactive, List: MDSListActive, Rank: rank})
			}
		}
	}
	if e.Exact {
		for _, holder := range active {
			if !selected[holder.Rank] {
				out = append(out, MDSMismatch{Reason: MDSMismatchUnselected, List: MDSListActive, Rank: holder.Rank, Observed: holder})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b MDSMismatch) int { return cmp.Compare(a.Rank, b.Rank) })
	return out
}

// standbyMismatches compares one standby list; want nil compares nothing.
func standbyMismatches(list MDSList, want *MDSStandbys, observed []cephfs.MDSStatus) []MDSMismatch {
	if want == nil {
		return nil
	}
	var out []MDSMismatch
	if len(observed) != want.Count {
		out = append(out, MDSMismatch{Reason: MDSMismatchCount, List: list, WantValue: int64(want.Count), ObservedValue: int64(len(observed))})
	}
	for _, daemon := range want.Daemons {
		i := slices.IndexFunc(observed, func(s cephfs.MDSStatus) bool {
			if daemon.Name != "" {
				return s.Name == daemon.Name
			}
			return s.GID == daemon.GID
		})
		if i < 0 {
			out = append(out, MDSMismatch{Reason: MDSMismatchAbsent, List: list, Want: daemon})
			continue
		}
		entry := observed[i]
		if daemon.GID != 0 && entry.GID != daemon.GID {
			out = append(out, MDSMismatch{Reason: MDSMismatchGID, List: list, Want: daemon, Observed: entry})
		}
		if daemon.NotGID != 0 && entry.GID == daemon.NotGID {
			out = append(out, MDSMismatch{Reason: MDSMismatchUnchanged, List: list, Want: daemon, Observed: entry})
		}
	}
	return out
}

// validate refuses an expectation that has no verdict.
func (e MDSs) validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: MDS expectation "+format, append([]any{ceph.ErrInvalidRequest}, args...)...)
	}
	if e.Owned && len(e.Ranks) == 0 {
		return invalid("sets Owned without Ranks")
	}
	if e.FilesystemID == 0 && e.MaxMDS == 0 && len(e.Ranks) == 0 && !e.AllRanksActive && !e.Exact && e.Standby == nil && e.StandbyReplay == nil {
		return invalid("states no condition")
	}
	if e.FilesystemID < 0 || e.MaxMDS < 0 {
		return invalid("has a negative filesystem ID %d or max_mds %d", e.FilesystemID, e.MaxMDS)
	}
	names, gids := map[string]bool{}, map[uint64]bool{}
	claim := func(where string, d MDSDaemon) error {
		if d.GID != 0 && d.GID == d.NotGID {
			return invalid("%s pins gid %d that it also excludes", where, d.GID)
		}
		if d.Name != "" {
			if names[d.Name] {
				return invalid("names mds.%s twice", d.Name)
			}
			names[d.Name] = true
		}
		if d.GID != 0 {
			if gids[d.GID] {
				return invalid("pins gid %d twice", d.GID)
			}
			gids[d.GID] = true
		}
		return nil
	}
	ranks := make(map[int]bool, len(e.Ranks))
	for _, r := range e.Ranks {
		where := "rank " + strconv.Itoa(r.Rank)
		switch {
		case r.Rank < 0:
			return invalid("selects negative %s", where)
		case ranks[r.Rank]:
			return invalid("selects %s twice", where)
		case e.MaxMDS > 0 && r.Rank >= e.MaxMDS:
			return invalid("selects %s at or above its max_mds %d", where, e.MaxMDS)
		}
		ranks[r.Rank] = true
		if err := claim(where, MDSDaemon{Name: r.Name, GID: r.GID, NotGID: r.NotGID}); err != nil {
			return err
		}
	}
	for _, list := range []struct {
		name MDSList
		want *MDSStandbys
	}{{MDSListStandby, e.Standby}, {MDSListStandbyReplay, e.StandbyReplay}} {
		if list.want == nil {
			continue
		}
		if list.want.Count < 0 || list.want.Count < len(list.want.Daemons) {
			return invalid("expects %d %s daemons in a count of %d", len(list.want.Daemons), list.name, list.want.Count)
		}
		for _, d := range list.want.Daemons {
			if d.Name == "" && d.GID == 0 {
				return invalid("selects a %s daemon by neither name nor gid", list.name)
			}
			if err := claim(string(list.name)+" daemon", d); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateMDSObservation refuses a status that MDSStatus cannot report.
func validateMDSObservation(s cephfs.FilesystemStatus) error {
	if s.FilesystemID < 0 || s.MaxMDS < 1 {
		return fmt.Errorf("malformed MDS observation: filesystem ID %d, max_mds %d", s.FilesystemID, s.MaxMDS)
	}
	names, gids := map[string]bool{}, map[uint64]bool{}
	ranks := map[int]bool{}
	for _, list := range []struct {
		name    MDSList
		state   string
		entries []cephfs.MDSStatus
	}{{MDSListActive, "up:active", s.Active}, {MDSListStandby, "up:standby", s.Standby}, {MDSListStandbyReplay, "up:standby-replay", s.StandbyReplay}} {
		for _, entry := range list.entries {
			switch {
			case entry.Name == "" || entry.GID == 0:
				return fmt.Errorf("malformed MDS observation: %s entry without a name or gid: %+v", list.name, entry)
			case entry.State != list.state:
				return fmt.Errorf("malformed MDS observation: %s mds.%s in state %q", list.name, entry.Name, entry.State)
			case list.name != MDSListActive && !entry.Owned:
				return fmt.Errorf("malformed MDS observation: unowned %s mds.%s", list.name, entry.Name)
			case names[entry.Name] || gids[entry.GID]:
				return fmt.Errorf("malformed MDS observation: mds.%s or gid %d reported twice", entry.Name, entry.GID)
			}
			names[entry.Name], gids[entry.GID] = true, true
			if list.name != MDSListActive {
				continue
			}
			if entry.Rank < 0 || ranks[entry.Rank] {
				return fmt.Errorf("malformed MDS observation: active rank %d negative or reported twice", entry.Rank)
			}
			ranks[entry.Rank] = true
		}
	}
	return nil
}

// clone returns e with its own slices.
func (e MDSs) clone() MDSs {
	e.Ranks = slices.Clone(e.Ranks)
	for _, list := range []**MDSStandbys{&e.Standby, &e.StandbyReplay} {
		if *list != nil {
			*list = &MDSStandbys{Count: (*list).Count, Daemons: slices.Clone((*list).Daemons)}
		}
	}
	return e
}

// cloneFilesystemStatus returns s with its own lists.
func cloneFilesystemStatus(s cephfs.FilesystemStatus) cephfs.FilesystemStatus {
	s.Active = slices.Clone(s.Active)
	s.Standby = slices.Clone(s.Standby)
	s.StandbyReplay = slices.Clone(s.StandbyReplay)
	return s
}
