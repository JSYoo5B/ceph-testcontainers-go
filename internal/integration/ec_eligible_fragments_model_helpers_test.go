//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// This file is the observation model of the erasure-coded "eligible fragment"
// witness. It reads three primary-side reports of one PG and joins them with
// the client's own receipt:
//
//   - "pg query": the PG state, the up and acting vectors (a position is a
//     fragment index, CRUSH_ITEM_NONE marks a hole), the primary's own info
//     and one info per peer, named "osd(shard)".
//   - "pg log": the primary's log. Its entries name the object, the version a
//     shard must have reached and, for a client write, the writer's identity.
//   - "pg list_unfound": the recovery map's size and the objects whose
//     sources cannot reconstruct them.
//
// None of them is a per-object inventory of fragments. A peer's info carries
// PG-wide positions, so the join below is deliberately conservative: it counts
// a fragment as eligible only when the shard's last_complete has reached the
// version that the object requires, and it never turns a missing report into
// a statement about a fragment. Pinned Tentacle (7f793731f1b3) dumps these
// fields in PeeringState::dump_peering_state, pg_info_t::dump,
// pg_log_entry_t::dump and PrimaryLogPG::do_command; the minimum a decoder
// needs, ECRecPred, deduplicates locations by shard index, which is why the
// model counts distinct indices and not OSDs.

// ecFragmentNone is CRUSH_ITEM_NONE, a hole in an acting vector.
const ecFragmentNone = 2147483647

type ecPGInfo struct {
	PGID         string            `json:"pgid"`
	LastUpdate   string            `json:"last_update"`
	LastComplete string            `json:"last_complete"`
	LastBackfill string            `json:"last_backfill"`
	PartialLast  []json.RawMessage `json:"partial_writes_last_complete"`
	Stats        struct {
		StatSum struct {
			Recovered int64 `json:"num_objects_recovered"`
		} `json:"stat_sum"`
	} `json:"stats"`
}

type ecPeerInfo struct {
	Peer string `json:"peer"`
	ecPGInfo
}

// ecPGReport is the part of "pg query" that the witness reads.
type ecPGReport struct {
	State  string       `json:"state"`
	Epoch  uint64       `json:"epoch"`
	Up     []int        `json:"up"`
	Acting []int        `json:"acting"`
	Info   ecPGInfo     `json:"info"`
	Peers  []ecPeerInfo `json:"peer_info"`
}

var (
	ecShardSuffix = regexp.MustCompile(`s(\d+)$`)
	ecPeerName    = regexp.MustCompile(`^(\d+)\((\d+)\)$`)
	ecReqidClient = regexp.MustCompile(`^client\.(\d+)\.\d+:\d+$`)
)

func parseECPGReport(data []byte) (ecPGReport, error) {
	var report ecPGReport
	if err := json.Unmarshal(data, &report); err != nil {
		return report, fmt.Errorf("pg query: %w", err)
	}
	if report.State == "" || report.Epoch == 0 || len(report.Acting) == 0 {
		return report, fmt.Errorf("pg query without state, epoch or acting vector: %.256s", data)
	}
	infos := []ecPGInfo{report.Info}
	for _, peer := range report.Peers {
		infos = append(infos, peer.ecPGInfo)
	}
	for _, info := range infos {
		if !ecShardSuffix.MatchString(info.PGID) {
			return report, fmt.Errorf("pg query info without an erasure-coded shard: %q", info.PGID)
		}
		for _, position := range []string{info.LastUpdate, info.LastComplete} {
			if _, err := parseEversion(position); err != nil {
				return report, err
			}
		}
		if info.LastBackfill == "" {
			return report, fmt.Errorf("pg query info %q without last_backfill", info.PGID)
		}
	}
	return report, nil
}

// ecLogEntry is one entry of the primary's PG log.
type ecLogEntry struct {
	Op      string `json:"op"`
	Object  string `json:"object"`
	Version string `json:"version"`
	Prior   string `json:"prior_version"`
	Reqid   string `json:"reqid"`
}

// ecPGLog is "pg log". Head and Tail bound what the log can still say.
type ecPGLog struct {
	Head    string       `json:"head"`
	Tail    string       `json:"tail"`
	Entries []ecLogEntry `json:"log"`
}

// parseECPGLog accepts the log at the top level or under the section names
// that the dump code opens, because a formatter drops the name of its first
// section.
func parseECPGLog(data []byte) (ecPGLog, error) {
	var wrapped struct {
		ecPGLog
		PGLog *ecPGLog `json:"pg_log_t"`
		OpLog *struct {
			PGLog *ecPGLog `json:"pg_log_t"`
		} `json:"op_log"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return ecPGLog{}, fmt.Errorf("pg log: %w", err)
	}
	log := wrapped.ecPGLog
	switch {
	case wrapped.PGLog != nil:
		log = *wrapped.PGLog
	case wrapped.OpLog != nil && wrapped.OpLog.PGLog != nil:
		log = *wrapped.OpLog.PGLog
	}
	for _, position := range []string{log.Head, log.Tail} {
		if _, err := parseEversion(position); err != nil {
			return log, fmt.Errorf("pg log: %w", err)
		}
	}
	return log, nil
}

type ecUnfoundObject struct {
	Oid struct {
		Oid    string      `json:"oid"`
		Snapid json.Number `json:"snapid"`
	} `json:"oid"`
}

// ecUnfoundReport is "pg list_unfound". NumMissing counts the whole recovery map,
// while Objects holds only the entries that are unfound, one page at a time.
type ecUnfoundReport struct {
	NumMissing int               `json:"num_missing"`
	NumUnfound int               `json:"num_unfound"`
	More       bool              `json:"more"`
	Objects    []ecUnfoundObject `json:"objects"`
}

func parseECUnfound(data []byte) (ecUnfoundReport, error) {
	var wrapped struct {
		ecUnfoundReport
		Missing *ecUnfoundReport `json:"missing"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return ecUnfoundReport{}, fmt.Errorf("pg list_unfound: %w", err)
	}
	report := wrapped.ecUnfoundReport
	if wrapped.Missing != nil {
		report = *wrapped.Missing
	}
	if report.Objects == nil {
		return report, fmt.Errorf("pg list_unfound without an objects array: %.256s", data)
	}
	return report, nil
}

// ecObjectID is a hobject_t as the log prints it: pool, bit-reversed hash,
// namespace, key, name and snapshot ("head", "snapdir" or hexadecimal).
type ecObjectID struct {
	Pool              int
	Hash, Nspace, Key string
	Name, Snap        string
	hexSnap           uint64
	hasSnapID         bool
}

func ecUnescape(text string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] != '%' {
			out.WriteByte(text[i])
			continue
		}
		if i+3 > len(text) {
			return "", fmt.Errorf("truncated escape in %q", text)
		}
		value, err := strconv.ParseUint(text[i+1:i+3], 16, 8)
		if err != nil {
			return "", fmt.Errorf("escape in %q: %w", text, err)
		}
		out.WriteByte(byte(value))
		i += 2
	}
	return out.String(), nil
}

func parseECObjectID(text string) (ecObjectID, error) {
	parts := strings.Split(text, ":")
	if len(parts) != 6 {
		return ecObjectID{}, fmt.Errorf("object %q is not pool:hash:namespace:key:name:snap", text)
	}
	pool, err := strconv.Atoi(parts[0])
	if err != nil {
		return ecObjectID{}, fmt.Errorf("object %q: %w", text, err)
	}
	id := ecObjectID{Pool: pool, Hash: parts[1], Snap: parts[5]}
	for target, source := range map[*string]string{&id.Nspace: parts[2], &id.Key: parts[3], &id.Name: parts[4]} {
		if *target, err = ecUnescape(source); err != nil {
			return ecObjectID{}, err
		}
	}
	if id.Snap != "head" && id.Snap != "snapdir" {
		if id.hexSnap, err = strconv.ParseUint(id.Snap, 16, 64); err != nil {
			return ecObjectID{}, fmt.Errorf("object %q snapshot: %w", text, err)
		}
		id.hasSnapID = true
	}
	return id, nil
}

// ecTarget names the object whose recovery is judged: a head, or the clone of
// one self-managed snapshot (Snap is the client's snapshot ID).
type ecTarget struct {
	Name string
	Snap uint64 // zero selects the head
}

func (target ecTarget) String() string {
	if target.Snap == 0 {
		return target.Name + "@head"
	}
	return fmt.Sprintf("%s@%x", target.Name, target.Snap)
}

func (target ecTarget) matches(id ecObjectID) bool {
	if id.Name != target.Name || id.Nspace != "" {
		return false
	}
	if target.Snap == 0 {
		return id.Snap == "head"
	}
	return id.hasSnapID && id.hexSnap == target.Snap
}

// ecRequiredVersion joins the log with the identity of the client that wrote
// the object. The head requires the version of its last log entry, which must
// be a modify by that client. A clone requires the version of its clone
// entry, which the OSD logs in the same operation just before the writer's
// head modify (the clone entry carries no client identity itself).
func ecRequiredVersion(log ecPGLog, target ecTarget, writer uint64) (eversion, error) {
	last := -1
	for index, entry := range log.Entries {
		id, err := parseECObjectID(entry.Object)
		if err != nil {
			continue
		}
		if target.matches(id) {
			last = index
		}
	}
	if last < 0 {
		return eversion{}, fmt.Errorf("no entry for %v in a log from %s to %s", target, log.Tail, log.Head)
	}
	entry := log.Entries[last]
	version, err := parseEversion(entry.Version)
	if err != nil {
		return eversion{}, err
	}
	modifiedBy := func(e ecLogEntry) error {
		match := ecReqidClient.FindStringSubmatch(e.Reqid)
		if e.Op != "modify" || match == nil || match[1] != strconv.FormatUint(writer, 10) {
			return fmt.Errorf("entry %s %s %s of %v is not a modify by client.%d (request %q)", e.Version, e.Op, e.Object, target, writer, e.Reqid)
		}
		return nil
	}
	if target.Snap == 0 {
		return version, modifiedBy(entry)
	}
	if entry.Op != "clone" {
		return eversion{}, fmt.Errorf("last entry %s of %v is a %s, not a clone", entry.Version, target, entry.Op)
	}
	if last+1 >= len(log.Entries) {
		return eversion{}, fmt.Errorf("clone entry %s of %v is the last log entry", entry.Version, target)
	}
	next := log.Entries[last+1]
	nextVersion, err := parseEversion(next.Version)
	if err != nil {
		return eversion{}, err
	}
	if err := modifiedBy(next); err != nil {
		return eversion{}, err
	}
	if nextVersion.Epoch != version.Epoch || nextVersion.Version != version.Version+1 {
		return eversion{}, fmt.Errorf("clone %s of %v is not followed directly by the writer's modify (%s)", entry.Version, target, next.Version)
	}
	return version, nil
}

// ecObservation is one consistent look at a PG: the PG query before and after
// the log and unfound reads, so that a change of epoch is visible.
type ecObservation struct {
	Before, After ecPGReport
	Log           ecPGLog
	Unfound       ecUnfoundReport
	Primary       int
}

// ecVerdict is the outcome for one object. The kinds stay apart on purpose.
type ecVerdict string

const (
	// ecSufficient: at least K distinct fragment indices are proven eligible.
	ecSufficient ecVerdict = "sufficient-eligible-fragments"
	// ecInsufficient: even counting every lagging fragment that is still
	// present, fewer than K distinct indices could supply the version.
	ecInsufficient ecVerdict = "insufficient-eligible-fragments"
	// ecUnfoundObserved: the primary lists this very object as unfound.
	ecUnfoundObserved ecVerdict = "unfound-observed"
	// ecPeeringBlocked: the PG is not active, so no per-object statement is made.
	ecPeeringBlocked ecVerdict = "peering-blocked"
	// ecIncomplete: the observation cannot decide, or lagging fragments may
	// or may not have caught up.
	ecIncomplete ecVerdict = "incomplete-observation"
)

// Status of one fragment index for one object.
const (
	ecEligible     = "eligible"
	ecBehind       = "behind-required-version"
	ecUnbackfilled = "not-backfilled"
	// ecCursor: a finite last_backfill cursor. MissingLoc rejects a source only
	// when the object is at or beyond the cursor, and the model does not compare
	// the cursor with the object, so the fragment is neither proven nor excluded.
	ecCursor     = "backfill-cursor-unresolved"
	ecUnobserved = "unobserved"
)

type ecFragment struct {
	Index  int
	OSD    int
	Status string
}

// ecClassification is the receipt of one object: the required version, every
// fragment index and the verdict.
type ecClassification struct {
	Target    ecTarget
	Need      eversion
	Verdict   ecVerdict
	Fragments []ecFragment
	Detail    string
}

func (c ecClassification) indices(status string) []int {
	var out []int
	for _, fragment := range c.Fragments {
		if fragment.Status == status {
			out = append(out, fragment.Index)
		}
	}
	return out
}

// classifyECObject decides one object of a PG with k data fragments from one
// observation. need is the version that the object requires (ecRequiredVersion).
func classifyECObject(obs ecObservation, k int, target ecTarget, need eversion, needErr error) ecClassification {
	result := ecClassification{Target: target, Need: need}
	incomplete := func(format string, args ...any) ecClassification {
		result.Verdict, result.Detail = ecIncomplete, fmt.Sprintf(format, args...)
		return result
	}
	before, after := obs.Before, obs.After
	if before.Epoch != after.Epoch || before.State != after.State || !slices.Equal(before.Acting, after.Acting) {
		return incomplete("the PG moved while it was observed: epoch %d to %d, %s to %s, acting %v to %v",
			before.Epoch, after.Epoch, before.State, after.State, before.Acting, after.Acting)
	}
	states := strings.Split(before.State, "+")
	if !slices.Contains(states, "active") || slices.ContainsFunc(states, func(s string) bool {
		return s == "peering" || s == "down" || s == "incomplete" || s == "stale" || s == "unknown"
	}) {
		result.Verdict, result.Detail = ecPeeringBlocked, "PG state "+before.State
		return result
	}
	acting := before.Acting
	if k < 1 || len(acting) <= k {
		return incomplete("acting vector %v does not fit k=%d with parity", acting, k)
	}
	if needErr != nil {
		return incomplete("required version: %v", needErr)
	}
	// Every info must say which fragment index it is, consistently with the
	// acting vector. A shifted or compacted vector would silently move
	// fragments between indices.
	index := func(pgid string) int {
		n, _ := strconv.Atoi(ecShardSuffix.FindStringSubmatch(pgid)[1])
		return n
	}
	type location struct {
		osd  int
		info ecPGInfo
	}
	locations := map[int][]location{}
	primaryIndex := index(before.Info.PGID)
	if primaryIndex >= len(acting) || acting[primaryIndex] != obs.Primary {
		return incomplete("the primary osd.%d reports fragment %d, but the acting vector %v has osd.%d there", obs.Primary, primaryIndex, acting, valueAt(acting, primaryIndex))
	}
	locations[primaryIndex] = append(locations[primaryIndex], location{obs.Primary, before.Info})
	for _, peer := range before.Peers {
		match := ecPeerName.FindStringSubmatch(peer.Peer)
		if match == nil {
			return incomplete("peer %q is not osd(shard)", peer.Peer)
		}
		osd, _ := strconv.Atoi(match[1])
		shard, _ := strconv.Atoi(match[2])
		if index(peer.PGID) != shard {
			return incomplete("peer %q reports pgid %q", peer.Peer, peer.PGID)
		}
		if at := slices.Index(acting, osd); at >= 0 && at != shard {
			return incomplete("peer %q is at acting position %d", peer.Peer, at)
		}
		if shard >= len(acting) || acting[shard] != osd {
			continue // A stray copy outside the acting set supplies nothing here.
		}
		locations[shard] = append(locations[shard], location{osd, peer.ecPGInfo})
	}
	for _, listed := range locations {
		for _, l := range listed {
			if len(l.info.PartialLast) != 0 {
				return incomplete("osd.%d reports partial_writes_last_complete, which last_complete does not describe", l.osd)
			}
		}
	}
	if obs.Unfound.More {
		// A page of the list does not establish that this object is absent.
		result.Detail = "unfound list is paged, so absence is not claimed; "
	} else if obs.Unfound.NumUnfound != len(obs.Unfound.Objects) {
		return incomplete("list_unfound counts %d unfound objects but lists %d", obs.Unfound.NumUnfound, len(obs.Unfound.Objects))
	}
	for _, object := range obs.Unfound.Objects {
		snap, err := object.Oid.Snapid.Int64()
		head := err == nil && snap == -2 || object.Oid.Snapid.String() == "18446744073709551614"
		if object.Oid.Oid == target.Name && (target.Snap == 0 && head || target.Snap != 0 && err == nil && uint64(snap) == target.Snap) {
			result.Verdict, result.Detail = ecUnfoundObserved, result.Detail+"listed by list_unfound"
			return result
		}
	}
	// The primary does not advance a peer's last_complete when recovery
	// finishes (observed natively: active+clean with a peer still at the
	// pre-failure last_complete), so a lagging last_complete is only
	// evidence of a missing object while the PG is not clean.
	clean := slices.Contains(states, "clean")
	candidates := 0
	for fragment := range acting {
		entry := ecFragment{Index: fragment, OSD: -1, Status: ecUnobserved}
		if acting[fragment] != ecFragmentNone {
			entry.OSD = acting[fragment]
		}
		for _, l := range locations[fragment] {
			lastComplete, _ := parseEversion(l.info.LastComplete)
			lastUpdate, _ := parseEversion(l.info.LastUpdate)
			status := ecEligible
			switch {
			case l.info.LastBackfill == "MIN":
				status = ecUnbackfilled
			case l.info.LastBackfill != "MAX":
				status = ecCursor
			case lastUpdate.less(need):
				status = ecBehind
			case lastComplete.less(need) && !clean:
				status = ecBehind
			}
			if status == ecEligible || entry.Status != ecEligible {
				entry.OSD, entry.Status = l.osd, status
			}
		}
		if entry.Status == ecBehind || entry.Status == ecCursor {
			candidates++
		}
		result.Fragments = append(result.Fragments, entry)
	}
	eligible := len(result.indices(ecEligible))
	switch {
	case eligible >= k:
		result.Verdict = ecSufficient
	case eligible+candidates >= k:
		return incomplete("%d eligible and %d lagging or partially backfilled fragments; whether they can supply the object is not observed", eligible, candidates)
	default:
		result.Verdict = ecInsufficient
	}
	return result
}

func valueAt(values []int, index int) int {
	if index < 0 || index >= len(values) {
		return -1
	}
	return values[index]
}
