//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file holds the Docker-free part of the MDS generation and rank binding
// recipe: strict readers for the FSMap dump, the daemon "status" answer and the
// "session ls" / "client ls" answer, plus a judge that decides whether the
// session evidence of a window belongs to one rank held by one daemon
// generation. It does not run a cluster and it does not read the client
// metrics section; the caller passes the rank it used to select metrics rows.
//
// Source basis (Tentacle 20.2.4, 7f793731):
//
//   - Session::dump (mds/SessionMap.cc) writes no rank. A session list is
//     attributed only by the daemon that answered it. MDSRankDispatcher::
//     dump_sessions writes a bare array of session objects, skips non-client
//     entities, and "session ls" and "client ls" are the same command.
//   - The target of "ceph tell mds.<spec>" goes through Client::resolve_mds
//     (client/Client.cc). A spec is tried first as a role ("<fs>:<rank>", or a
//     bare rank of the default filesystem), then as a decimal GID, then "*"
//     (every MDS daemon, several answers), then as a daemon name. A bare
//     integer is therefore ambiguous and "*" is never a single rank.
//   - MDSDaemon::dump_status ("status") reports whoami (the rank, -1 without
//     one), id (the daemon's global ID, which is the GID in the FSMap), state,
//     mdsmap_epoch and, when it holds a rank, fs_name. A GID is assigned by the
//     monitors per daemon boot, so a different GID is a different daemon
//     generation even under the same name and rank.
//   - MDSMap::dump and MDSMap::mds_info_t::dump (mds/MDSMap.cc) write max_mds,
//     the "in" rank list, "up" as mds_<rank> to GID, failed and damaged rank
//     lists and "info" as gid_<GID> objects. A standby-replay daemon carries
//     the rank it follows in its info entry but is not the "up" daemon of that
//     rank.

// mdsDaemonInfo is one entry of an MDSMap "info" section.
type mdsDaemonInfo struct {
	GID   uint64
	Name  string
	Rank  int64
	State string
	Laggy bool
}

// mdsMapSnapshot is the part of one filesystem's MDSMap the judge needs.
type mdsMapSnapshot struct {
	FS      string
	Epoch   uint64
	MaxMDS  int64
	In      []int64
	Up      map[int64]uint64
	Failed  []int64
	Damaged []int64
	Info    map[uint64]mdsDaemonInfo
}

type rankJSON = map[string]json.RawMessage

func rankObject(raw json.RawMessage, what string) (rankJSON, error) {
	var out rankJSON
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("%s is not a JSON object (%v)", what, err)
	}
	return out, nil
}

func rankField(obj rankJSON, name string) (json.RawMessage, error) {
	raw, ok := obj[name]
	if !ok {
		return nil, fmt.Errorf("field %q is missing", name)
	}
	return raw, nil
}

func rankUint(obj rankJSON, name string) (uint64, error) {
	raw, err := rankField(obj, name)
	if err != nil {
		return 0, err
	}
	v, perr := strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64)
	if perr != nil {
		return 0, fmt.Errorf("field %q is not an unsigned integer: %.64s", name, raw)
	}
	return v, nil
}

func rankInt(obj rankJSON, name string) (int64, error) {
	raw, err := rankField(obj, name)
	if err != nil {
		return 0, err
	}
	return rankIntLiteral(raw, name)
}

func rankIntLiteral(raw json.RawMessage, what string) (int64, error) {
	v, err := strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is not a signed integer: %.64s", what, raw)
	}
	return v, nil
}

func rankString(obj rankJSON, name string) (string, error) {
	raw, err := rankField(obj, name)
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || bytes.TrimSpace(raw)[0] != '"' {
		return "", fmt.Errorf("field %q is not a string: %.64s", name, raw)
	}
	return s, nil
}

func rankIntList(obj rankJSON, name string) ([]int64, error) {
	raw, err := rankField(obj, name)
	if err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || items == nil {
		return nil, fmt.Errorf("field %q is not an array (%v)", name, err)
	}
	out := make([]int64, 0, len(items))
	for i, item := range items {
		v, err := rankIntLiteral(item, fmt.Sprintf("%s[%d]", name, i))
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// parseMDSMapSnapshot reads the output of "ceph fs dump --format json" for the
// one filesystem named fsName. Other filesystems are only checked for a name,
// so a filesystem that cannot be attributed is an error and not "unrelated".
// Zero or two filesystems with the name, a missing or mistyped field, an info
// key that disagrees with its embedded GID, or an "up" key that is not
// mds_<rank> is an error. Nothing absent decodes to an empty value.
func parseMDSMapSnapshot(dump []byte, fsName string) (mdsMapSnapshot, error) {
	var out mdsMapSnapshot
	top, err := rankObject(dump, "fs dump")
	if err != nil {
		return out, err
	}
	rawFS, err := rankField(top, "filesystems")
	if err != nil {
		return out, err
	}
	var filesystems []json.RawMessage
	if err := json.Unmarshal(rawFS, &filesystems); err != nil || filesystems == nil {
		return out, fmt.Errorf("filesystems is not an array (%v)", err)
	}
	var chosen rankJSON
	for i, rawEntry := range filesystems {
		entry, err := rankObject(rawEntry, fmt.Sprintf("filesystem %d", i))
		if err != nil {
			return out, err
		}
		rawMap, err := rankField(entry, "mdsmap")
		if err != nil {
			return out, fmt.Errorf("filesystem %d: %w", i, err)
		}
		mdsmap, err := rankObject(rawMap, fmt.Sprintf("filesystem %d mdsmap", i))
		if err != nil {
			return out, err
		}
		name, err := rankString(mdsmap, "fs_name")
		if err != nil {
			return out, fmt.Errorf("filesystem %d: %w", i, err)
		}
		if name != fsName {
			continue
		}
		if chosen != nil {
			return out, fmt.Errorf("filesystem %q appears twice", fsName)
		}
		chosen = mdsmap
	}
	if chosen == nil {
		return out, fmt.Errorf("filesystem %q is not in the dump", fsName)
	}
	out.FS = fsName
	if out.Epoch, err = rankUint(chosen, "epoch"); err != nil {
		return out, err
	}
	if out.MaxMDS, err = rankInt(chosen, "max_mds"); err != nil {
		return out, err
	}
	if out.In, err = rankIntList(chosen, "in"); err != nil {
		return out, err
	}
	if out.Failed, err = rankIntList(chosen, "failed"); err != nil {
		return out, err
	}
	if out.Damaged, err = rankIntList(chosen, "damaged"); err != nil {
		return out, err
	}
	rawUp, err := rankField(chosen, "up")
	if err != nil {
		return out, err
	}
	up, err := rankObject(rawUp, "up")
	if err != nil {
		return out, err
	}
	out.Up = map[int64]uint64{}
	for key, raw := range up {
		rank, perr := strconv.ParseInt(strings.TrimPrefix(key, "mds_"), 10, 64)
		if !strings.HasPrefix(key, "mds_") || perr != nil || rank < 0 {
			return out, fmt.Errorf("up key %q is not mds_<rank>", key)
		}
		gid, perr := strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64)
		if perr != nil {
			return out, fmt.Errorf("up %q is not an unsigned GID: %.64s", key, raw)
		}
		out.Up[rank] = gid
	}
	rawInfo, err := rankField(chosen, "info")
	if err != nil {
		return out, err
	}
	info, err := rankObject(rawInfo, "info")
	if err != nil {
		return out, err
	}
	out.Info = map[uint64]mdsDaemonInfo{}
	for key, raw := range info {
		entry, err := rankObject(raw, "info "+key)
		if err != nil {
			return out, err
		}
		var d mdsDaemonInfo
		if d.GID, err = rankUint(entry, "gid"); err != nil {
			return out, fmt.Errorf("info %s: %w", key, err)
		}
		if key != fmt.Sprintf("gid_%d", d.GID) {
			return out, fmt.Errorf("info key %q disagrees with gid %d", key, d.GID)
		}
		if d.Name, err = rankString(entry, "name"); err != nil {
			return out, fmt.Errorf("info %s: %w", key, err)
		}
		if d.Rank, err = rankInt(entry, "rank"); err != nil {
			return out, fmt.Errorf("info %s: %w", key, err)
		}
		if d.State, err = rankString(entry, "state"); err != nil {
			return out, fmt.Errorf("info %s: %w", key, err)
		}
		// laggy_since is written only while the daemon is laggy.
		_, d.Laggy = entry["laggy_since"]
		out.Info[d.GID] = d
	}
	return out, nil
}

// rankSelectionKind classifies one MDSMap snapshot for single-rank use.
type rankSelectionKind int

const (
	// rankSelectionIneligible is the default: the map cannot name one active
	// rank, for a reason other than having several ranks.
	rankSelectionIneligible rankSelectionKind = iota
	// rankSelectionOne means exactly rank 0, held by one active daemon.
	rankSelectionOne
	// rankSelectionMultiRank means the map has or is about to have more than one rank.
	rankSelectionMultiRank
)

type rankSelection struct {
	Kind   rankSelectionKind
	Reason string
	Rank   int64
	GID    uint64
	Name   string
}

// selectSingleRank decides whether the snapshot is a one-rank filesystem whose
// rank 0 is held by one active, non-laggy daemon. Several ranks are detected
// from max_mds, the "in" list and the "up" map, so a scale-up in progress is
// multi-rank even while only one rank is up. The daemon is the one in "up",
// never one found by scanning info for a rank: a standby-replay daemon carries
// its followed rank there. Another non-standby-replay daemon claiming the rank
// makes the snapshot ineligible.
func selectSingleRank(m mdsMapSnapshot) rankSelection {
	if m.MaxMDS != 1 || len(m.In) > 1 || len(m.Up) > 1 {
		return rankSelection{Kind: rankSelectionMultiRank, Reason: fmt.Sprintf("max_mds %d, in %v, up %d rank(s)", m.MaxMDS, m.In, len(m.Up))}
	}
	no := func(format string, args ...any) rankSelection {
		return rankSelection{Reason: fmt.Sprintf(format, args...)}
	}
	if len(m.In) != 1 || m.In[0] != 0 {
		return no("in set %v is not exactly rank 0", m.In)
	}
	if len(m.Failed) != 0 || len(m.Damaged) != 0 {
		return no("failed %v or damaged %v ranks are present", m.Failed, m.Damaged)
	}
	gid, ok := m.Up[0]
	if !ok || gid == 0 {
		return no("rank 0 has no up daemon")
	}
	d, ok := m.Info[gid]
	switch {
	case !ok:
		return no("up gid %d has no info entry", gid)
	case d.Rank != 0:
		return no("up gid %d reports rank %d", gid, d.Rank)
	case d.State != "up:active":
		return no("rank 0 daemon is %s", d.State)
	case d.Laggy:
		return no("rank 0 daemon is laggy")
	}
	for other, o := range m.Info {
		if other != gid && o.Rank == 0 && o.State != "up:standby-replay" {
			return no("gid %d also claims rank 0 as %s", other, o.State)
		}
	}
	return rankSelection{Kind: rankSelectionOne, Rank: 0, GID: gid, Name: d.Name}
}

// mdsDaemonStatus is the identity part of a daemon "status" answer.
type mdsDaemonStatus struct {
	FS       string
	Whoami   int64
	ID       uint64
	State    string
	MapEpoch uint64
}

// parseMDSDaemonStatus reads "tell mds.<spec> status". A daemon without a rank
// does not write fs_name, so its answer is an error here rather than a status
// with an empty filesystem.
func parseMDSDaemonStatus(data []byte) (mdsDaemonStatus, error) {
	var s mdsDaemonStatus
	obj, err := rankObject(data, "status")
	if err != nil {
		return s, err
	}
	if s.FS, err = rankString(obj, "fs_name"); err != nil {
		return s, err
	}
	if s.Whoami, err = rankInt(obj, "whoami"); err != nil {
		return s, err
	}
	if s.ID, err = rankUint(obj, "id"); err != nil {
		return s, err
	}
	if s.State, err = rankString(obj, "state"); err != nil {
		return s, err
	}
	if s.MapEpoch, err = rankUint(obj, "mdsmap_epoch"); err != nil {
		return s, err
	}
	if s.FS == "" || s.State == "" {
		return s, fmt.Errorf("status has an empty fs_name or state")
	}
	return s, nil
}

// rankSessionList is one parsed session list: client ID to session state.
type rankSessionList struct{ States map[uint64]string }

// parseRankSessionList reads "session ls" / "client ls". The document must be
// an array, an empty array is a real empty list, and every session, including
// those of unselected clients, needs an id, a client entity whose number equals
// the id, and a nonempty state. A repeated id is an error.
func parseRankSessionList(data []byte) (rankSessionList, error) {
	out := rankSessionList{States: map[uint64]string{}}
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil || rows == nil {
		return out, fmt.Errorf("session list is not an array (%v)", err)
	}
	for i, raw := range rows {
		row, err := rankObject(raw, fmt.Sprintf("session %d", i))
		if err != nil {
			return out, err
		}
		id, err := rankUint(row, "id")
		if err != nil || id == 0 {
			return out, fmt.Errorf("session %d has no nonzero id (%v)", i, err)
		}
		rawEntity, err := rankField(row, "entity")
		if err != nil {
			return out, fmt.Errorf("session %d: %w", i, err)
		}
		entity, err := rankObject(rawEntity, "entity")
		if err != nil {
			return out, fmt.Errorf("session %d: %w", i, err)
		}
		rawName, err := rankField(entity, "name")
		if err != nil {
			return out, fmt.Errorf("session %d: %w", i, err)
		}
		name, err := rankObject(rawName, "entity name")
		if err != nil {
			return out, fmt.Errorf("session %d: %w", i, err)
		}
		typ, err := rankString(name, "type")
		if err != nil || typ != "client" {
			return out, fmt.Errorf("session %d entity type is not client (%v)", i, err)
		}
		num, err := rankUint(name, "num")
		if err != nil || num != id {
			return out, fmt.Errorf("session %d entity number does not match id %d (%v)", i, id, err)
		}
		state, err := rankString(row, "state")
		if err != nil || state == "" {
			return out, fmt.Errorf("session %d has no state (%v)", i, err)
		}
		if _, dup := out.States[id]; dup {
			return out, fmt.Errorf("session id %d appears twice", id)
		}
		out.States[id] = state
	}
	return out, nil
}

// Probes are timed. Start and End bracket the query, because the daemon samples
// its answer somewhere inside that interval.
type rankMapProbe struct {
	Start, End time.Time
	Map        mdsMapSnapshot
}

type rankStatusProbe struct {
	Start, End time.Time
	Target     string
	Status     mdsDaemonStatus
}

type rankSessionProbe struct {
	Start, End time.Time
	Target     string
	Command    string
	List       rankSessionList
}

// rankBindingEvidence is everything the judge needs. Probes outside
// [WindowStart, Deadline] are not evidence, in either direction. MetricsRank is
// the rank the caller used to select client metrics rows.
type rankBindingEvidence struct {
	FS                    string
	Clients               []uint64
	MetricsRank           int64
	WindowStart, Deadline time.Time
	Maps                  []rankMapProbe
	Statuses              []rankStatusProbe
	Sessions              []rankSessionProbe
}

type rankBindingVerdict int

const (
	// rankBindingInconclusive is the default, so a forgotten branch cannot pass.
	rankBindingInconclusive rankBindingVerdict = iota
	// rankBindingBound means every selected client has an open session in a
	// list answered by the one daemon generation holding the one rank.
	rankBindingBound
	// rankBindingRejected means the evidence is positively from several
	// ranks, several daemon generations, or another rank than claimed.
	rankBindingRejected
)

type rankBindingOutcome struct {
	Verdict rankBindingVerdict
	Reason  string
	Rank    int64
	GID     uint64
	Name    string
}

func rankInconclusive(format string, args ...any) rankBindingOutcome {
	return rankBindingOutcome{Reason: fmt.Sprintf(format, args...)}
}

func rankRejected(format string, args ...any) rankBindingOutcome {
	return rankBindingOutcome{Verdict: rankBindingRejected, Reason: fmt.Sprintf(format, args...)}
}

type rankTargetKind int

const (
	rankTargetUnusable rankTargetKind = iota // not a single, recognised target
	rankTargetBound                          // names the bound rank or daemon
	rankTargetOther                          // names another rank, filesystem or daemon
	rankTargetMany                           // addresses several daemons
)

// classifyRankTarget interprets the argument given to "ceph tell" the way
// Client::resolve_mds does. Only a role of this filesystem at the bound rank
// or the bound daemon's name is a bound target. A bare integer could be a rank
// of the default filesystem or a GID and is unusable. "*" reaches every daemon.
func classifyRankTarget(target, fs string, sel rankSelection, m mdsMapSnapshot) (rankTargetKind, string) {
	spec, ok := strings.CutPrefix(target, "mds.")
	switch {
	case !ok || spec == "":
		return rankTargetUnusable, fmt.Sprintf("target %q is not mds.<spec>", target)
	case spec == "*":
		return rankTargetMany, "target addresses every MDS daemon"
	}
	if i := strings.LastIndex(spec, ":"); i >= 0 {
		rank, err := strconv.ParseInt(spec[i+1:], 10, 64)
		if err != nil || rank < 0 || spec[:i] == "" {
			return rankTargetUnusable, fmt.Sprintf("target %q is not <fs>:<rank>", target)
		}
		if spec[:i] != fs || rank != sel.Rank {
			return rankTargetOther, fmt.Sprintf("target %q is not rank %d of %q", target, sel.Rank, fs)
		}
		return rankTargetBound, ""
	}
	if _, err := strconv.ParseUint(spec, 10, 64); err == nil {
		return rankTargetUnusable, fmt.Sprintf("target %q is a bare integer, a rank of the default filesystem or a GID", target)
	}
	if spec == sel.Name {
		return rankTargetBound, ""
	}
	for _, d := range m.Info {
		if d.Name == spec {
			return rankTargetOther, fmt.Sprintf("target %q is another daemon", target)
		}
	}
	return rankTargetUnusable, fmt.Sprintf("target %q names no daemon of the map", target)
}

func probeInWindow(start, end, from, to time.Time) bool {
	return !start.Before(from) && !end.After(to)
}

// judgeRankBinding applies the binding policy:
//
//   - Admission: a filesystem name, distinct nonzero client IDs, a nonnegative
//     metrics rank and an ordered nonzero window. A probe with a missing or
//     inverted interval, or probes of one kind out of start order, make the
//     whole evidence inconclusive.
//   - Eligibility: only probes whose whole interval lies in the window count.
//     At least two maps, two statuses and one list are needed so a list can
//     be bracketed from both sides; that count is checked after the rejections.
//   - Rejections come before inconclusive findings: a snapshot with several
//     ranks, two different up GIDs across snapshots (mixed generation), a
//     status whose whoami or id differs from the bound rank and GID or that
//     names another filesystem, a target for another rank, filesystem or
//     daemon (or for every daemon), and a metrics rank other than the bound one.
//   - Inconclusive: an ineligible snapshot, a status that is not up:active, a
//     decreasing map epoch, an unusable target, an unknown command, no
//     bracketed list, or a selected client without an open session in a
//     bracketed list. An empty list or an absent client is never evidence of
//     anything.
//   - A list is bracketed when a map and a status ended before it started and
//     a map and a status started after it ended. A GID is unique per daemon
//     boot, so an unchanged GID across a bracket is one daemon generation.
func judgeRankBinding(e rankBindingEvidence) rankBindingOutcome {
	seen := map[uint64]bool{}
	for _, c := range e.Clients {
		if c == 0 || seen[c] {
			return rankInconclusive("client IDs are empty, zero or repeated")
		}
		seen[c] = true
	}
	switch {
	case e.FS == "":
		return rankInconclusive("no filesystem name")
	case len(e.Clients) == 0:
		return rankInconclusive("no selected clients")
	case e.MetricsRank < 0:
		return rankInconclusive("metrics rank %d is not a rank", e.MetricsRank)
	case e.WindowStart.IsZero() || e.Deadline.IsZero() || e.Deadline.Before(e.WindowStart):
		return rankInconclusive("window times are missing or out of order")
	}
	var prev time.Time
	check := func(kind string, i int, start, end time.Time) error {
		if start.IsZero() || end.Before(start) || start.Before(prev) {
			return fmt.Errorf("%s probe %d has missing or out-of-order times", kind, i)
		}
		prev = start
		return nil
	}
	prev = time.Time{}
	var maps []rankMapProbe
	for i, p := range e.Maps {
		if err := check("map", i, p.Start, p.End); err != nil {
			return rankInconclusive("%v", err)
		}
		if probeInWindow(p.Start, p.End, e.WindowStart, e.Deadline) {
			maps = append(maps, p)
		}
	}
	prev = time.Time{}
	var statuses []rankStatusProbe
	for i, p := range e.Statuses {
		if err := check("status", i, p.Start, p.End); err != nil {
			return rankInconclusive("%v", err)
		}
		if probeInWindow(p.Start, p.End, e.WindowStart, e.Deadline) {
			statuses = append(statuses, p)
		}
	}
	prev = time.Time{}
	var lists []rankSessionProbe
	for i, p := range e.Sessions {
		if err := check("session", i, p.Start, p.End); err != nil {
			return rankInconclusive("%v", err)
		}
		if probeInWindow(p.Start, p.End, e.WindowStart, e.Deadline) {
			lists = append(lists, p)
		}
	}
	if len(maps) == 0 {
		return rankInconclusive("no map inside the window")
	}

	// Rejections first: a positive sign of several ranks or generations
	// outranks any single missing piece.
	sels := make([]rankSelection, len(maps))
	for i, p := range maps {
		if p.Map.FS != e.FS {
			return rankRejected("map %d is for filesystem %q", i, p.Map.FS)
		}
		sels[i] = selectSingleRank(p.Map)
		if sels[i].Kind == rankSelectionMultiRank {
			return rankRejected("map %d: %s", i, sels[i].Reason)
		}
	}
	// Mixed generations show even in a snapshot that is otherwise unusable,
	// for instance a replacement daemon that is still replaying.
	var gen uint64
	for _, p := range maps {
		if gid := p.Map.Up[0]; gid != 0 {
			if gen != 0 && gid != gen {
				return rankRejected("rank 0 daemon changed from gid %d to gid %d across maps", gen, gid)
			}
			gen = gid
		}
	}
	var bound *rankSelection
	for i := range sels {
		if sels[i].Kind != rankSelectionOne {
			return rankInconclusive("map %d is not a single active rank: %s", i, sels[i].Reason)
		}
		bound = &sels[i]
	}
	for i := 1; i < len(maps); i++ {
		if maps[i].Map.Epoch < maps[i-1].Map.Epoch {
			return rankInconclusive("map epoch decreased from %d to %d", maps[i-1].Map.Epoch, maps[i].Map.Epoch)
		}
	}
	if e.MetricsRank != bound.Rank {
		return rankRejected("metrics rows were selected for rank %d but the bound rank is %d", e.MetricsRank, bound.Rank)
	}
	lastMap := maps[len(maps)-1].Map

	for i, s := range statuses {
		kind, why := classifyRankTarget(s.Target, e.FS, *bound, lastMap)
		switch kind {
		case rankTargetMany, rankTargetOther:
			return rankRejected("status %d: %s", i, why)
		case rankTargetUnusable:
			return rankInconclusive("status %d: %s", i, why)
		}
		switch {
		case s.Status.FS != e.FS:
			return rankRejected("status %d answers for filesystem %q", i, s.Status.FS)
		case s.Status.Whoami != bound.Rank:
			return rankRejected("status %d reports rank %d, not %d", i, s.Status.Whoami, bound.Rank)
		case s.Status.ID != bound.GID:
			return rankRejected("status %d reports daemon gid %d, not %d", i, s.Status.ID, bound.GID)
		}
	}
	for i, l := range lists {
		kind, why := classifyRankTarget(l.Target, e.FS, *bound, lastMap)
		switch kind {
		case rankTargetMany, rankTargetOther:
			return rankRejected("session list %d: %s", i, why)
		case rankTargetUnusable:
			return rankInconclusive("session list %d: %s", i, why)
		}
	}
	if len(maps) < 2 || len(statuses) < 2 || len(lists) < 1 {
		return rankInconclusive("need two maps, two statuses and one session list inside the window (have %d, %d, %d)", len(maps), len(statuses), len(lists))
	}
	for i, s := range statuses {
		if s.Status.State != "up:active" {
			return rankInconclusive("status %d reports %s", i, s.Status.State)
		}
		if i > 0 && s.Status.MapEpoch < statuses[i-1].Status.MapEpoch {
			return rankInconclusive("daemon map epoch decreased from %d to %d", statuses[i-1].Status.MapEpoch, s.Status.MapEpoch)
		}
	}

	opened := map[uint64]bool{}
	other := map[uint64]string{}
	bracketed := 0
	for i, l := range lists {
		if l.Command != "session ls" && l.Command != "client ls" {
			return rankInconclusive("session list %d was produced by %q", i, l.Command)
		}
		if !bracketedBy(l, maps, statuses) {
			continue
		}
		bracketed++
		for _, c := range e.Clients {
			if state, ok := l.List.States[c]; ok {
				if state == "open" {
					opened[c] = true
				} else {
					other[c] = state
				}
			}
		}
	}
	if bracketed == 0 {
		return rankInconclusive("no session list is bracketed by maps and statuses on both sides")
	}
	for _, c := range e.Clients {
		if !opened[c] {
			if state, ok := other[c]; ok {
				return rankInconclusive("client.%d is %s, not open, in every bracketed list", c, state)
			}
			return rankInconclusive("client.%d is absent from every bracketed list", c)
		}
	}
	return rankBindingOutcome{Verdict: rankBindingBound, Reason: "selected clients are open on the one rank held by one daemon generation", Rank: bound.Rank, GID: bound.GID, Name: bound.Name}
}

// bracketedBy reports whether a map and a status ended before the list began
// and a map and a status began after it ended.
func bracketedBy(l rankSessionProbe, maps []rankMapProbe, statuses []rankStatusProbe) bool {
	var mapBefore, mapAfter, statusBefore, statusAfter bool
	for _, m := range maps {
		mapBefore = mapBefore || !m.End.After(l.Start)
		mapAfter = mapAfter || !m.Start.Before(l.End)
	}
	for _, s := range statuses {
		statusBefore = statusBefore || !s.End.After(l.Start)
		statusAfter = statusAfter || !s.Start.Before(l.End)
	}
	return mapBefore && mapAfter && statusBefore && statusAfter
}
