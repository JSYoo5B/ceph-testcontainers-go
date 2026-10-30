//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// The functions below turn the JSON that real Ceph commands print into the
// inputs of the private upgrade sequence plan (upgradeSettled), without Ceph
// or Docker. Each shape was read from the pinned Ceph source (Tentacle 20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964):
//
//   - "ceph versions": Monitor::handle_command, "versions" (src/mon/Monitor.cc);
//   - "ceph mon dump": MonmapMonitor, MonMap::dump (src/mon/MonmapMonitor.cc,
//     src/mon/MonMap.cc). min_mon_release is an unsigned integer
//     (ceph_release_t: squid is 19, tentacle is 20) next to
//     min_mon_release_name, and the "quorum" array holds monitor RANKS;
//   - "ceph quorum_status": Monitor::_quorum_status (src/mon/Monitor.cc). Its
//     "quorum" holds ranks and "quorum_names" holds names;
//   - "ceph osd stat": OSDMap::print_summary (src/osd/OSDMap.cc), a flat object
//     with num_osds, num_up_osds and num_in_osds, so "up and in" is not one
//     number;
//   - "ceph mgr stat": MgrMonitor (src/mon/MgrMonitor.cc), which names the
//     active manager but only COUNTS standbys. The standby names come from
//     "ceph mgr dump" (MgrMap::dump, src/mon/MgrMap.h);
//   - "ceph pg stat": PGMapDigest::print_oneline_summary (src/mon/PGMap.cc),
//     with state names from pg_state_string (src/osd/osd_types.cc);
//   - "ceph pg dump_stuck": PGMap::dump_stuck (src/mon/PGMap.cc), a bare JSON
//     array. It only lists PGs stuck for longer than mon_pg_stuck_threshold,
//     so an empty array is never evidence that PGs are settled by itself.
//
// The Formatter writes a top-level section without its name
// (JSONFormatter::print_name returns when its stack is empty), so each
// document is a bare object or array. The CLI prints human notes such as
// "dumped monmap epoch N" on stderr, so these parsers take stdout only and
// reject anything that is not exactly one JSON value.
//
// Every parser is strict. A document that is empty, not JSON, has trailing
// data or a duplicate object key, and a required field that is missing, null
// or of the wrong type, is an error and never decoded into an empty value that
// could count as evidence. Cross-field disagreements (a quorum rank without a
// monitor, counts that do not add up) are errors for the same reason.
// The tests use synthetic outputs; they executed no Ceph or Docker.

var (
	upgradeObsUUID  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	upgradeObsUint  = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	upgradeObsPGID  = regexp.MustCompile(`^[0-9]+\.[0-9a-f]+$`)
	upgradeObsToken = map[string]bool{"active": true, "clean": true, "scrubbing": true, "deep": true}
)

// upgradeObsCheck verifies that data is exactly one JSON value without
// duplicate keys at any depth; encoding/json keeps the last of two equal keys
// silently.
func upgradeObsCheck(data []byte, what string) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("%s: no output", what)
	}
	if err := strictjson.Check(data, strictjson.Options{}); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

type upgradeObsObject map[string]json.RawMessage

func upgradeObsObjectOf(data []byte, what string) (upgradeObsObject, error) {
	if err := upgradeObsCheck(data, what); err != nil {
		return nil, err
	}
	return upgradeObsObjectRaw(data, what)
}

func upgradeObsObjectRaw(raw json.RawMessage, what string) (upgradeObsObject, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("%s is not a JSON object", what)
	}
	return obj, nil
}

// field returns the raw value of a required key. Missing and null are errors.
func (o upgradeObsObject) field(key, what string) (json.RawMessage, error) {
	raw, ok := o[key]
	if !ok {
		return nil, fmt.Errorf("%s has no %q", what, key)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s %q is null", what, key)
	}
	return raw, nil
}

// uint returns a required non-negative integer. A string, a fraction or an
// exponent is a wrong type.
func (o upgradeObsObject) uint(key, what string) (int, error) {
	raw, err := o.field(key, what)
	if err != nil {
		return 0, err
	}
	text := string(bytes.TrimSpace(raw))
	n, convErr := strconv.Atoi(text)
	if !upgradeObsUint.MatchString(text) || convErr != nil {
		return 0, fmt.Errorf("%s %q is %s, want a non-negative integer", what, key, text)
	}
	return n, nil
}

func (o upgradeObsObject) str(key, what string) (string, error) {
	raw, err := o.field(key, what)
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("%s %q is not a string", what, key)
	}
	return s, nil
}

func (o upgradeObsObject) boolean(key, what string) (bool, error) {
	raw, err := o.field(key, what)
	if err != nil {
		return false, err
	}
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("%s %q is not a boolean", what, key)
}

// array returns the elements of a required array. An empty array is a real,
// present value; a missing, null or non-array value is an error.
func (o upgradeObsObject) array(key, what string) ([]json.RawMessage, error) {
	raw, err := o.field(key, what)
	if err != nil {
		return nil, err
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil || elems == nil {
		return nil, fmt.Errorf("%s %q is not an array", what, key)
	}
	return elems, nil
}

func upgradeObsInt(raw json.RawMessage, what string) (int, error) {
	text := string(bytes.TrimSpace(raw))
	n, err := strconv.Atoi(text)
	if !upgradeObsUint.MatchString(text) || err != nil {
		return 0, fmt.Errorf("%s is %s, want a non-negative integer", what, text)
	}
	return n, nil
}

// ---- ceph versions --------------------------------------------------------

// parseUpgradeVersionsStrict rejects duplicate keys before the existing
// "ceph versions" decoder runs. That decoder already treats a missing or null
// "mon"/"overall", non-object roles, bad counts and an "overall" that
// disagrees with the roles as errors; it cannot see a repeated key because
// encoding/json keeps the last one.
func parseUpgradeVersionsStrict(data []byte) (upgradeInventory, error) {
	if err := upgradeObsCheck(data, "ceph versions"); err != nil {
		return nil, err
	}
	return parseUpgradeInventory(data)
}

// ---- monmap, mon dump, quorum_status ---------------------------------------

type upgradeMonEntry struct {
	Rank int
	Name string
}

// upgradeMonmap is MonMap::dump restricted to the fields the plan judges.
type upgradeMonmap struct {
	Epoch             int
	FSID              string
	MinMonRelease     int
	MinMonReleaseName string
	Mons              []upgradeMonEntry // ordered by rank, ranks 0..n-1
}

func (m upgradeMonmap) names() []string {
	out := make([]string, 0, len(m.Mons))
	for _, e := range m.Mons {
		out = append(out, e.Name)
	}
	return out
}

func (m upgradeMonmap) nameOf(rank int) (string, bool) {
	for _, e := range m.Mons {
		if e.Rank == rank {
			return e.Name, true
		}
	}
	return "", false
}

func parseUpgradeMonmapObject(obj upgradeObsObject, what string) (upgradeMonmap, error) {
	var m upgradeMonmap
	var err error
	if m.Epoch, err = obj.uint("epoch", what); err != nil {
		return m, err
	}
	if m.Epoch == 0 {
		return m, fmt.Errorf("%s epoch is 0", what)
	}
	if m.FSID, err = obj.str("fsid", what); err != nil {
		return m, err
	}
	if !upgradeObsUUID.MatchString(m.FSID) {
		return m, fmt.Errorf("%s fsid %q is not a UUID", what, m.FSID)
	}
	if m.MinMonRelease, err = obj.uint("min_mon_release", what); err != nil {
		return m, err
	}
	if m.MinMonReleaseName, err = obj.str("min_mon_release_name", what); err != nil {
		return m, err
	}
	if want, known := upgradeReleaseNames[m.MinMonRelease]; known && want != m.MinMonReleaseName {
		return m, fmt.Errorf("%s min_mon_release %d is named %q, want %q", what, m.MinMonRelease, m.MinMonReleaseName, want)
	}
	elems, err := obj.array("mons", what)
	if err != nil {
		return m, err
	}
	if len(elems) == 0 {
		return m, fmt.Errorf("%s has no monitors", what)
	}
	names := map[string]bool{}
	for i, raw := range elems {
		mon, err := upgradeObsObjectRaw(raw, fmt.Sprintf("%s mons[%d]", what, i))
		if err != nil {
			return m, err
		}
		rank, err := mon.uint("rank", fmt.Sprintf("%s mons[%d]", what, i))
		if err != nil {
			return m, err
		}
		name, err := mon.str("name", fmt.Sprintf("%s mons[%d]", what, i))
		if err != nil {
			return m, err
		}
		switch {
		case name == "":
			return m, fmt.Errorf("%s mons[%d] has an empty name", what, i)
		case names[name]:
			return m, fmt.Errorf("%s lists monitor %q twice", what, name)
		case rank != i:
			return m, fmt.Errorf("%s mons[%d] has rank %d, want %d", what, i, rank, i)
		}
		names[name] = true
		m.Mons = append(m.Mons, upgradeMonEntry{Rank: rank, Name: name})
	}
	return m, nil
}

// ranksToNames resolves a rank array against the monmap. Every rank must be
// an integer, unique and present in the map.
func (m upgradeMonmap) ranksToNames(elems []json.RawMessage, what string) ([]string, error) {
	names := make([]string, 0, len(elems))
	seen := map[int]bool{}
	for i, raw := range elems {
		rank, err := upgradeObsInt(raw, fmt.Sprintf("%s[%d]", what, i))
		if err != nil {
			return nil, err
		}
		if seen[rank] {
			return nil, fmt.Errorf("%s lists rank %d twice", what, rank)
		}
		seen[rank] = true
		name, ok := m.nameOf(rank)
		if !ok {
			return nil, fmt.Errorf("%s rank %d is not in the monmap", what, rank)
		}
		names = append(names, name)
	}
	return names, nil
}

// upgradeMonDump is "ceph mon dump" with the quorum ranks resolved to names.
type upgradeMonDump struct {
	upgradeMonmap
	QuorumNames []string
}

func parseUpgradeMonDump(data []byte) (upgradeMonDump, error) {
	const what = "mon dump"
	var d upgradeMonDump
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return d, err
	}
	if d.upgradeMonmap, err = parseUpgradeMonmapObject(obj, what); err != nil {
		return d, err
	}
	quorum, err := obj.array("quorum", what)
	if err != nil {
		return d, err
	}
	if d.QuorumNames, err = d.ranksToNames(quorum, what+" quorum"); err != nil {
		return d, err
	}
	return d, nil
}

// upgradeQuorumStatus is "ceph quorum_status".
type upgradeQuorumStatus struct {
	ElectionEpoch int
	Names         []string // quorum_names
	Leader        string   // empty exactly when there is no quorum
	Monmap        upgradeMonmap
}

func parseUpgradeQuorumStatus(data []byte) (upgradeQuorumStatus, error) {
	const what = "quorum_status"
	var q upgradeQuorumStatus
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return q, err
	}
	if q.ElectionEpoch, err = obj.uint("election_epoch", what); err != nil {
		return q, err
	}
	rawMap, err := obj.field("monmap", what)
	if err != nil {
		return q, err
	}
	mapObj, err := upgradeObsObjectRaw(rawMap, what+" monmap")
	if err != nil {
		return q, err
	}
	if q.Monmap, err = parseUpgradeMonmapObject(mapObj, what+" monmap"); err != nil {
		return q, err
	}
	ranks, err := obj.array("quorum", what)
	if err != nil {
		return q, err
	}
	fromRanks, err := q.Monmap.ranksToNames(ranks, what+" quorum")
	if err != nil {
		return q, err
	}
	rawNames, err := obj.array("quorum_names", what)
	if err != nil {
		return q, err
	}
	for i, raw := range rawNames {
		var name string
		if err := json.Unmarshal(raw, &name); err != nil || name == "" {
			return q, fmt.Errorf("%s quorum_names[%d] is not a non-empty string", what, i)
		}
		q.Names = append(q.Names, name)
	}
	if !upgradeObsSameSet(fromRanks, q.Names) {
		return q, fmt.Errorf("%s quorum ranks %v and quorum_names %v disagree", what, fromRanks, q.Names)
	}
	if q.Leader, err = obj.str("quorum_leader_name", what); err != nil {
		return q, err
	}
	_, hasAge := obj["quorum_age"]
	if len(q.Names) == 0 {
		if q.Leader != "" || hasAge {
			return q, fmt.Errorf("%s has a leader or quorum_age without a quorum", what)
		}
		return q, nil
	}
	if !upgradeObsContains(q.Names, q.Leader) {
		return q, fmt.Errorf("%s leader %q is not in quorum %v", what, q.Leader, q.Names)
	}
	if _, err := obj.uint("quorum_age", what); err != nil {
		return q, err
	}
	return q, nil
}

// ---- osd stat ---------------------------------------------------------------

// upgradeOSDStat is "ceph osd stat".
type upgradeOSDStat struct {
	Epoch, NumOSDs, NumUp, NumIn, NumRemapped int
}

func parseUpgradeOSDStat(data []byte) (upgradeOSDStat, error) {
	const what = "osd stat"
	var s upgradeOSDStat
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return s, err
	}
	for _, f := range []struct {
		key string
		dst *int
	}{
		{"epoch", &s.Epoch}, {"num_osds", &s.NumOSDs}, {"num_up_osds", &s.NumUp},
		{"num_in_osds", &s.NumIn}, {"num_remapped_pgs", &s.NumRemapped},
		// The timestamps are required fields of the same dump; they carry no
		// judgement here, but a document without them is not an osd stat.
		{"osd_up_since", new(int)}, {"osd_in_since", new(int)},
	} {
		if *f.dst, err = obj.uint(f.key, what); err != nil {
			return s, err
		}
	}
	if s.Epoch == 0 {
		return s, fmt.Errorf("%s epoch is 0", what)
	}
	if s.NumUp > s.NumOSDs || s.NumIn > s.NumOSDs {
		return s, fmt.Errorf("%s has %d up and %d in of %d OSDs", what, s.NumUp, s.NumIn, s.NumOSDs)
	}
	return s, nil
}

// upIn returns the number of OSDs that are up and in only when that number is
// known exactly: every OSD in the map is both. The stat has separate counts,
// so any other combination leaves the intersection unknown (nil).
func (s upgradeOSDStat) upIn() *int {
	if s.NumUp == s.NumOSDs && s.NumIn == s.NumOSDs && s.NumOSDs > 0 {
		return intFact(s.NumOSDs)
	}
	return nil
}

// ---- mgr stat, mgr dump -----------------------------------------------------

// upgradeMgrStat is "ceph mgr stat".
type upgradeMgrStat struct {
	Epoch      int
	Available  bool
	ActiveName string
	NumStandby int
}

func parseUpgradeMgrStat(data []byte) (upgradeMgrStat, error) {
	const what = "mgr stat"
	var s upgradeMgrStat
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return s, err
	}
	if s.Epoch, err = obj.uint("epoch", what); err != nil {
		return s, err
	}
	if s.Available, err = obj.boolean("available", what); err != nil {
		return s, err
	}
	if s.ActiveName, err = obj.str("active_name", what); err != nil {
		return s, err
	}
	if s.NumStandby, err = obj.uint("num_standby", what); err != nil {
		return s, err
	}
	if s.Available && s.ActiveName == "" {
		return s, fmt.Errorf("%s is available without an active manager", what)
	}
	return s, nil
}

// upgradeMgrDump is the part of "ceph mgr dump" that names the standbys.
type upgradeMgrDump struct {
	Epoch      int
	Available  bool
	ActiveName string
	Standbys   []string
}

func parseUpgradeMgrDump(data []byte) (upgradeMgrDump, error) {
	const what = "mgr dump"
	var d upgradeMgrDump
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return d, err
	}
	if d.Epoch, err = obj.uint("epoch", what); err != nil {
		return d, err
	}
	if d.Available, err = obj.boolean("available", what); err != nil {
		return d, err
	}
	if d.ActiveName, err = obj.str("active_name", what); err != nil {
		return d, err
	}
	elems, err := obj.array("standbys", what)
	if err != nil {
		return d, err
	}
	seen := map[string]bool{}
	gids := map[int]bool{}
	for i, raw := range elems {
		where := fmt.Sprintf("%s standbys[%d]", what, i)
		sb, err := upgradeObsObjectRaw(raw, where)
		if err != nil {
			return d, err
		}
		name, err := sb.str("name", where)
		if err != nil {
			return d, err
		}
		gid, err := sb.uint("gid", where)
		if err != nil {
			return d, err
		}
		switch {
		case name == "":
			return d, fmt.Errorf("%s has an empty name", where)
		case seen[name] || name == d.ActiveName:
			return d, fmt.Errorf("%s lists manager %q twice", what, name)
		case gids[gid]:
			return d, fmt.Errorf("%s lists gid %d twice", what, gid)
		}
		seen[name], gids[gid] = true, true
		d.Standbys = append(d.Standbys, name)
	}
	if d.Available && d.ActiveName == "" {
		return d, fmt.Errorf("%s is available without an active manager", what)
	}
	return d, nil
}

// upgradeMgrView joins stat and dump. They are two commands, so they must
// describe the same map; a disagreement means one of them is stale and the
// pair is not evidence.
func upgradeMgrView(stat upgradeMgrStat, dump upgradeMgrDump) (upgradeMgrDump, error) {
	switch {
	case stat.Epoch != dump.Epoch:
		return upgradeMgrDump{}, fmt.Errorf("mgr stat epoch %d and mgr dump epoch %d differ", stat.Epoch, dump.Epoch)
	case stat.Available != dump.Available || stat.ActiveName != dump.ActiveName:
		return upgradeMgrDump{}, errors.New("mgr stat and mgr dump disagree about the active manager")
	case stat.NumStandby != len(dump.Standbys):
		return upgradeMgrDump{}, fmt.Errorf("mgr stat counts %d standbys, mgr dump lists %d", stat.NumStandby, len(dump.Standbys))
	}
	return dump, nil
}

// ---- pg stat, pg dump_stuck -------------------------------------------------

// upgradePGStat is "ceph pg stat".
type upgradePGStat struct {
	NumPGs  int
	ByState map[string]int
}

func parseUpgradePGStat(data []byte) (upgradePGStat, error) {
	const what = "pg stat"
	s := upgradePGStat{ByState: map[string]int{}}
	obj, err := upgradeObsObjectOf(data, what)
	if err != nil {
		return s, err
	}
	if s.NumPGs, err = obj.uint("num_pgs", what); err != nil {
		return s, err
	}
	elems, err := obj.array("num_pg_by_state", what)
	if err != nil {
		return s, err
	}
	sum := 0
	for i, raw := range elems {
		where := fmt.Sprintf("%s num_pg_by_state[%d]", what, i)
		st, err := upgradeObsObjectRaw(raw, where)
		if err != nil {
			return s, err
		}
		name, err := st.str("name", where)
		if err != nil {
			return s, err
		}
		num, err := st.uint("num", where)
		if err != nil {
			return s, err
		}
		switch {
		case name == "":
			return s, fmt.Errorf("%s has an empty state name", where)
		case num == 0:
			return s, fmt.Errorf("%s state %q has no PGs", where, name)
		}
		if _, dup := s.ByState[name]; dup {
			return s, fmt.Errorf("%s lists state %q twice", what, name)
		}
		s.ByState[name] = num
		sum += num
	}
	if sum != s.NumPGs {
		return s, fmt.Errorf("%s states add up to %d PGs, num_pgs is %d", what, sum, s.NumPGs)
	}
	return s, nil
}

// settled judges the PG states. Yes needs at least one PG and every state to
// be active+clean, optionally with scrubbing or deep: a scrub does not make a
// clean PG unsettled. Any other token (peering, degraded, undersized,
// remapped, backfill*, recover*, stale, down, incomplete, creating, unknown
// and so on, including a token this code does not know) is No. No PGs at all
// is unknown: the manager has not reported any, which is not a settled pool.
func (s upgradePGStat) settled() upgradeTri {
	if s.NumPGs == 0 {
		return upgradeUnknown
	}
	for state := range s.ByState {
		tokens := strings.Split(state, "+")
		have := map[string]bool{}
		for _, t := range tokens {
			if !upgradeObsToken[t] {
				return upgradeNo
			}
			have[t] = true
		}
		if !have["active"] || !have["clean"] {
			return upgradeNo
		}
	}
	return upgradeYes
}

// parseUpgradePGStuck decodes "ceph pg dump_stuck -f json" into the stuck PG
// IDs. The document is a bare array, so a null, object or string is an error
// and an empty array is the one valid "no PG is stuck past the threshold".
func parseUpgradePGStuck(data []byte) ([]string, error) {
	const what = "pg dump_stuck"
	if err := upgradeObsCheck(data, what); err != nil {
		return nil, err
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(data, &elems); err != nil || elems == nil {
		return nil, fmt.Errorf("%s is not a JSON array", what)
	}
	ids := []string{}
	seen := map[string]bool{}
	for i, raw := range elems {
		where := fmt.Sprintf("%s[%d]", what, i)
		pg, err := upgradeObsObjectRaw(raw, where)
		if err != nil {
			return nil, err
		}
		id, err := pg.str("pgid", where)
		if err != nil {
			return nil, err
		}
		if !upgradeObsPGID.MatchString(id) {
			return nil, fmt.Errorf("%s pgid %q is not a PG ID", where, id)
		}
		if seen[id] {
			return nil, fmt.Errorf("%s lists PG %s twice", what, id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, nil
}

// upgradePGsSettled combines both commands. The stuck list covers only PGs
// older than the threshold, so it can only veto: a PG that just became
// unclean is in neither list but is visible in pg stat. Any stuck PG is No
// even when pg stat looked clean (the two were sampled separately).
func upgradePGsSettled(stat upgradePGStat, stuck []string) upgradeTri {
	if len(stuck) > 0 {
		return upgradeNo
	}
	return stat.settled()
}

// ---- assembly ---------------------------------------------------------------

// upgradeObservationOutputs are the stdout bytes of one checkpoint's commands.
// Every command is required; a nil or empty output is "not collected".
type upgradeObservationOutputs struct {
	Versions, MonDump, QuorumStatus, OSDStat, MgrStat, MgrDump, PGStat, PGStuck []byte
}

// upgradeSettledFromOutputs builds the plan's settled evidence for the
// transition of daemon. Any command that cannot be parsed, or any
// disagreement between commands, becomes ObservationErr and no other field is
// filled, so a partial observation never reaches the plan as evidence.
func upgradeSettledFromOutputs(daemon string, o upgradeObservationOutputs) upgradeSettled {
	var errs []string
	note := func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	inv, err := parseUpgradeVersionsStrict(o.Versions)
	note(err)
	mon, err := parseUpgradeMonDump(o.MonDump)
	note(err)
	quorum, err := parseUpgradeQuorumStatus(o.QuorumStatus)
	note(err)
	osd, err := parseUpgradeOSDStat(o.OSDStat)
	note(err)
	mgrStat, err := parseUpgradeMgrStat(o.MgrStat)
	note(err)
	mgrDump, err := parseUpgradeMgrDump(o.MgrDump)
	note(err)
	pgStat, err := parseUpgradePGStat(o.PGStat)
	note(err)
	stuck, err := parseUpgradePGStuck(o.PGStuck)
	note(err)
	var mgr upgradeMgrDump
	if len(errs) == 0 {
		if mgr, err = upgradeMgrView(mgrStat, mgrDump); err != nil {
			note(err)
		}
		note(upgradeMonObservationsAgree(mon, quorum))
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return upgradeSettled{Daemon: daemon, ObservationErr: errors.New("settled observation unusable: " + strings.Join(errs, "; "))}
	}
	ev := upgradeSettled{
		Daemon:        daemon,
		Inventory:     inv,
		Quorum:        quorum.Names,
		OSDsUpIn:      osd.upIn(),
		PGsSettled:    upgradePGsSettled(pgStat, stuck),
		MinMonRelease: intFact(mon.MinMonRelease),
	}
	// An unavailable manager map has no usable active name for the plan.
	if mgr.Available {
		ev.MgrActive = mgr.ActiveName
		ev.MgrStandbys = mgr.Standbys
	}
	return ev
}

func upgradeObsSameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	return sameStrings(a, b)
}

func upgradeObsContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// upgradeMonObservationsAgree requires the two MON observations to describe
// the same map, release and quorum. The adapter consumes the release from the
// mon dump and the quorum from quorum_status, so any fact one of them states
// that the other contradicts makes the pair unusable.
func upgradeMonObservationsAgree(mon upgradeMonDump, q upgradeQuorumStatus) error {
	var diffs []string
	m := q.Monmap
	if mon.Epoch != m.Epoch {
		diffs = append(diffs, fmt.Sprintf("map epoch %d vs %d", mon.Epoch, m.Epoch))
	}
	if mon.FSID != m.FSID {
		diffs = append(diffs, "fsid")
	}
	if mon.MinMonRelease != m.MinMonRelease || mon.MinMonReleaseName != m.MinMonReleaseName {
		diffs = append(diffs, fmt.Sprintf("min_mon_release %d (%s) vs %d (%s)", mon.MinMonRelease, mon.MinMonReleaseName, m.MinMonRelease, m.MinMonReleaseName))
	}
	if !reflect.DeepEqual(mon.Mons, m.Mons) {
		diffs = append(diffs, fmt.Sprintf("rank to name mapping %v vs %v", mon.Mons, m.Mons))
	}
	if !upgradeObsSameSet(mon.QuorumNames, q.Names) {
		diffs = append(diffs, fmt.Sprintf("quorum %v vs %v", mon.QuorumNames, q.Names))
	}
	if len(diffs) > 0 {
		return errors.New("mon dump and quorum_status contradict each other: " + strings.Join(diffs, "; "))
	}
	return nil
}
