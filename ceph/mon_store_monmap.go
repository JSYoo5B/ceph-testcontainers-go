package ceph

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"strconv"
)

// This file is the live monmap adapter behind the monStoreMonmap port of
// mon_store_inspect.go. It reads two native documents and decodes them
// strictly:
//
//   - "ceph mon dump --format json": MonmapMonitor::preprocess_command and
//     MonMap::dump (src/mon/MonmapMonitor.cc, src/mon/MonMap.cc). The document
//     is the monmap object (epoch, fsid, mons[]{rank, name, public_addrs}) with
//     a "quorum" array of monitor RANKS next to it. The CLI prints "dumped
//     monmap epoch N" on stderr, so only stdout is read.
//   - "ceph quorum_status --format json": Monitor::_quorum_status
//     (src/mon/Monitor.cc): election_epoch, "quorum" (ranks), "quorum_names",
//     quorum_leader_name, quorum_age (only with a quorum) and the same monmap
//     object.
//
// Both were read at the pinned commit 7f793731f1b39eb4f465e960113d2363c311b964
// (Tentacle 20.2.4); the recorded mon dump fixtures under
// ceph/testdata/daemon-addresses carry the same shapes. An address vector
// entry is entity_addr_t::dump (type, addr, nonce) inside entity_addrvec_t::dump
// (src/msg/msg_types.cc).
//
// Two documents are asked for so that the quorum fact does not rest on one
// answer: the member is "in quorum" or "out of quorum" only when both agree
// with each other on the epoch, the FSID, the members, every address vector
// and the quorum set. A disagreement, for example an election between the two
// commands, is an error, never a guess. The adapter has no production caller
// and no public name.

const (
	monStoreMonmapTimeoutSeconds = "5"
	// monStoreMonmapOutputLimit is the most output of one command the runner
	// may hand back; the runner enforces it, the decoder refuses more.
	monStoreMonmapOutputLimit = 1 << 20
)

// monStoreCLI runs one command against the live cluster and returns its
// standard output only. A non-zero exit, a timeout and truncated output are
// errors. Implementations enforce monStoreMonmapOutputLimit.
type monStoreCLI func(ctx context.Context, argv ...string) ([]byte, error)

// monStoreMonmapCLI implements monStoreMonmap on top of the two commands.
type monStoreMonmapCLI struct {
	run monStoreCLI
}

// monStoreMonDumpArgv and monStoreQuorumStatusArgv are the commands. They name
// the JSON format explicitly: the default is plain text.
func monStoreMonDumpArgv() []string {
	return []string{"ceph", "--connect-timeout", monStoreMonmapTimeoutSeconds, "mon", "dump", "--format", "json"}
}

func monStoreQuorumStatusArgv() []string {
	return []string{"ceph", "--connect-timeout", monStoreMonmapTimeoutSeconds, "quorum_status", "--format", "json"}
}

// Member reads both documents and decodes the named member. The two commands
// run one after the other, mon dump first; a failure of the first does not
// start the second.
func (m monStoreMonmapCLI) Member(ctx context.Context, mon string) (*monStoreMember, error) {
	if m.run == nil {
		return nil, errors.New("the monmap view has no command runner")
	}
	monDump, err := m.run(ctx, monStoreMonDumpArgv()...)
	if err != nil {
		return nil, fmt.Errorf("mon dump: %w", err)
	}
	quorumStatus, err := m.run(ctx, monStoreQuorumStatusArgv()...)
	if err != nil {
		return nil, fmt.Errorf("quorum_status: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decodeMonStoreMember(monDump, quorumStatus, mon)
}

// monStoreMapMember is one decoded monmap entry.
type monStoreMapMember struct {
	rank      int
	name      string
	addresses []string
}

// monStoreMap is the part of MonMap::dump the adapter judges.
type monStoreMap struct {
	epoch   uint64
	fsid    string
	members []monStoreMapMember
}

func (m monStoreMap) nameOf(rank int) (string, bool) {
	if rank < 0 || rank >= len(m.members) {
		return "", false
	}
	return m.members[rank].name, true
}

// monStoreAddress decodes one entity_addr_t dump into "type:addr/nonce". The
// type must be v1 or v2 (a member's public address is never "any"), addr an IP
// literal with a port in 1..65535 that is not the unspecified address, and the
// nonce an unsigned 32-bit integer.
func monStoreAddress(raw monStoreJSONObject, what string) (string, error) {
	typ, err := raw.str("type", what)
	if err != nil {
		return "", err
	}
	addr, err := raw.str("addr", what)
	if err != nil {
		return "", err
	}
	nonce, err := raw.uint64("nonce", what)
	if err != nil {
		return "", err
	}
	ap, err := netip.ParseAddrPort(addr)
	switch {
	case typ != "v1" && typ != "v2":
		return "", fmt.Errorf("%s has type %q, want v1 or v2", what, typ)
	case err != nil || ap.Port() == 0 || ap.Addr().IsUnspecified():
		return "", fmt.Errorf("%s has an address that is not a usable IP and port", what)
	case nonce > math.MaxUint32:
		return "", fmt.Errorf("%s has a nonce beyond 32 bits", what)
	}
	return typ + ":" + addr + "/" + strconv.FormatUint(nonce, 10), nil
}

// parseMonStoreMap decodes the monmap object: epoch (at least 1), fsid
// (canonical UUID) and a non-empty mons array in rank order with unique
// non-empty names and a non-empty, duplicate-free address vector each.
func parseMonStoreMap(obj monStoreJSONObject, what string) (monStoreMap, error) {
	var m monStoreMap
	var err error
	if m.epoch, err = obj.uint64("epoch", what); err != nil {
		return m, err
	}
	if m.epoch == 0 {
		return m, fmt.Errorf("%s epoch is 0", what)
	}
	if m.fsid, err = obj.str("fsid", what); err != nil {
		return m, err
	}
	if !monStoreCanonicalUUID(m.fsid) {
		return m, fmt.Errorf("%s fsid is not a canonical UUID", what)
	}
	mons, err := obj.array("mons", what)
	if err != nil {
		return m, err
	}
	if len(mons) == 0 {
		return m, fmt.Errorf("%s lists no monitors", what)
	}
	names := map[string]bool{}
	for i, raw := range mons {
		item := fmt.Sprintf("%s mons[%d]", what, i)
		mon, err := monStoreObjectOf(raw, item)
		if err != nil {
			return m, err
		}
		rank, err := mon.uint64("rank", item)
		if err != nil {
			return m, err
		}
		name, err := mon.str("name", item)
		if err != nil {
			return m, err
		}
		switch {
		case name == "":
			return m, fmt.Errorf("%s has an empty name", item)
		case names[name]:
			return m, fmt.Errorf("%s lists a monitor twice", what)
		case rank != uint64(i):
			return m, fmt.Errorf("%s has rank %d, want %d", item, rank, i)
		}
		names[name] = true
		public, err := mon.object("public_addrs", item)
		if err != nil {
			return m, err
		}
		vector, err := public.array("addrvec", item+" public_addrs")
		if err != nil {
			return m, err
		}
		if len(vector) == 0 {
			return m, fmt.Errorf("%s has an empty address vector", item)
		}
		member := monStoreMapMember{rank: i, name: name}
		for j, entry := range vector {
			where := fmt.Sprintf("%s address %d", item, j)
			addr, err := monStoreObjectOf(entry, where)
			if err != nil {
				return m, err
			}
			text, err := monStoreAddress(addr, where)
			if err != nil {
				return m, err
			}
			if slices.Contains(member.addresses, text) {
				return m, fmt.Errorf("%s repeats an address", item)
			}
			member.addresses = append(member.addresses, text)
		}
		m.members = append(m.members, member)
	}
	return m, nil
}

// monStoreRanksToNames resolves a rank array against the map. The array must
// be non-empty (the command was answered by a member of a quorum), hold unique
// integers and name only ranks of the map.
func monStoreRanksToNames(m monStoreMap, elems []string, what string) ([]string, error) {
	if len(elems) == 0 {
		return nil, fmt.Errorf("%s is empty: an answering cluster has a quorum", what)
	}
	var names []string
	seen := map[uint64]bool{}
	for i, text := range elems {
		rank, err := monStoreUintOf([]byte(text), fmt.Sprintf("%s[%d]", what, i))
		if err != nil {
			return nil, err
		}
		if seen[rank] {
			return nil, fmt.Errorf("%s lists rank %d twice", what, rank)
		}
		seen[rank] = true
		name, ok := m.nameOf(int(min(rank, math.MaxInt32)))
		if !ok {
			return nil, fmt.Errorf("%s rank %d is not in the monmap", what, rank)
		}
		names = append(names, name)
	}
	return names, nil
}

func monStoreArrayText(o monStoreJSONObject, name, what string) ([]string, error) {
	elems, err := o.array(name, what)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(elems))
	for i, e := range elems {
		out[i] = string(e)
	}
	return out, nil
}

// parseMonStoreMonDump decodes "ceph mon dump" and resolves its quorum ranks
// to member names.
func parseMonStoreMonDump(data []byte) (monStoreMap, []string, error) {
	const what = "mon dump"
	obj, err := monStoreObjectOf(data, what)
	if err != nil {
		return monStoreMap{}, nil, err
	}
	m, err := parseMonStoreMap(obj, what)
	if err != nil {
		return m, nil, err
	}
	ranks, err := monStoreArrayText(obj, "quorum", what)
	if err != nil {
		return m, nil, err
	}
	quorum, err := monStoreRanksToNames(m, ranks, what+" quorum")
	return m, quorum, err
}

// parseMonStoreQuorumStatus decodes "ceph quorum_status": the embedded monmap,
// the quorum as ranks and as names (which must name the same members), a leader
// that is in the quorum and the age that exists exactly when there is a quorum.
func parseMonStoreQuorumStatus(data []byte) (monStoreMap, []string, error) {
	const what = "quorum_status"
	obj, err := monStoreObjectOf(data, what)
	if err != nil {
		return monStoreMap{}, nil, err
	}
	if _, err := obj.uint64("election_epoch", what); err != nil {
		return monStoreMap{}, nil, err
	}
	mapObj, err := obj.object("monmap", what)
	if err != nil {
		return monStoreMap{}, nil, err
	}
	m, err := parseMonStoreMap(mapObj, what+" monmap")
	if err != nil {
		return m, nil, err
	}
	ranks, err := monStoreArrayText(obj, "quorum", what)
	if err != nil {
		return m, nil, err
	}
	fromRanks, err := monStoreRanksToNames(m, ranks, what+" quorum")
	if err != nil {
		return m, nil, err
	}
	rawNames, err := obj.array("quorum_names", what)
	if err != nil {
		return m, nil, err
	}
	var names []string
	for i, raw := range rawNames {
		probe := monStoreJSONObject{"v": raw}
		name, err := probe.str("v", fmt.Sprintf("%s quorum_names[%d]", what, i))
		if err != nil || name == "" {
			return m, nil, fmt.Errorf("%s quorum_names[%d] is not a non-empty string", what, i)
		}
		names = append(names, name)
	}
	if !monStoreSameSet(fromRanks, names) {
		return m, nil, fmt.Errorf("%s quorum ranks and quorum_names disagree", what)
	}
	leader, err := obj.str("quorum_leader_name", what)
	if err != nil {
		return m, nil, err
	}
	if !slices.Contains(names, leader) {
		return m, nil, fmt.Errorf("%s leader is not in the quorum", what)
	}
	if _, err := obj.uint64("quorum_age", what); err != nil {
		return m, nil, err
	}
	return m, names, nil
}

func monStoreSameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// decodeMonStoreMember decodes both documents, requires them to agree and
// returns the named member, or nil with a nil error when the name is not in the
// map. The quorum fact is in when the name is in the quorum and out otherwise;
// it is never unknown here because every ambiguity is an error.
func decodeMonStoreMember(monDump, quorumStatus []byte, mon string) (*monStoreMember, error) {
	if len(monDump) > monStoreMonmapOutputLimit || len(quorumStatus) > monStoreMonmapOutputLimit {
		return nil, errors.New("monmap output is beyond the limit")
	}
	dumpMap, dumpQuorum, err := parseMonStoreMonDump(monDump)
	if err != nil {
		return nil, err
	}
	statusMap, statusQuorum, err := parseMonStoreQuorumStatus(quorumStatus)
	if err != nil {
		return nil, err
	}
	if dumpMap.epoch != statusMap.epoch || dumpMap.fsid != statusMap.fsid || len(dumpMap.members) != len(statusMap.members) {
		return nil, errors.New("mon dump and quorum_status describe different monmaps")
	}
	for i, a := range dumpMap.members {
		b := statusMap.members[i]
		if a.name != b.name || !slices.Equal(a.addresses, b.addresses) {
			return nil, errors.New("mon dump and quorum_status disagree on a member")
		}
	}
	if !monStoreSameSet(dumpQuorum, statusQuorum) {
		return nil, errors.New("mon dump and quorum_status disagree on the quorum")
	}
	for _, member := range dumpMap.members {
		if member.name != mon {
			continue
		}
		state := monStoreQuorumOut
		if slices.Contains(dumpQuorum, mon) {
			state = monStoreQuorumIn
		}
		return &monStoreMember{name: mon, fsid: dumpMap.fsid, addresses: slices.Clone(member.addresses), quorum: state}, nil
	}
	return nil, nil
}
