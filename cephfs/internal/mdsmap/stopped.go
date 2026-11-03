package mdsmap

import (
	"errors"
	"slices"
	"strconv"
)

// StoppedExpectation is every value the stopped-MDS decoder compares the
// native global map with; the decoder keeps no reference to their source.
type StoppedExpectation struct {
	// TargetName is the stopped daemon's immutable name, which must be absent.
	TargetName string
	// Filesystem is the configured name of the owned filesystem.
	Filesystem string
	// Native is the identity captured at creation; it must not be nil.
	Native *Native
	// Pools is the native pool identity read for this decode.
	Pools *Pools
	// ActiveMDS, StandbyMDS and StandbyReplay are the configured policy.
	ActiveMDS, StandbyMDS int
	StandbyReplay         bool
}

// StoppedState is the accepted result of decoding the global map.
type StoppedState struct {
	Scope *Scope
	// Registered are the names of the owned filesystem's assigned members and
	// of the standbys that can join it.
	Registered map[string]bool
}

// errStoppedTarget refuses a map that still names the stopped daemon.
var errStoppedTarget = errors.New("original stopped MDS name is still registered")

// DecodeStopped strictly decodes one `fs dump` document and compares it
// with want. It is a function of its arguments: it takes no lock, reads no
// filesystem record, roster or container and starts no command, so it can run
// with the owner lock held or not. Docker verification of the surviving
// daemons is not part of it. It refuses up:oneshot-replay. The checks run in
// one pass: each filesystem's membership and, for the owned one, its policy and
// survivors; then the standbys and the attached pools.
func DecodeStopped(data []byte, want StoppedExpectation) (*StoppedState, error) {
	var native globalMap
	if DecodeJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return nil, errors.New("decode stopped MDS global map")
	}
	s := newSeen()
	var scope *Scope
	active := map[int]string{}
	registered := map[string]bool{}
	for _, entry := range *native.Filesystems {
		if err := checkStoppedMDSMembership(s, entry, want); err != nil {
			return nil, err
		}
		if *entry.Map.Name != want.Filesystem {
			continue
		}
		var err error
		if scope, err = stoppedMDSScope(*entry.ID, entry.Map, want); err != nil {
			return nil, err
		}
		if err := collectStoppedMDSSurvivors(*entry.ID, entry.Map, scope, active, registered); err != nil {
			return nil, err
		}
	}
	if scope == nil || scope.Active < 1 || len(active) != scope.Active {
		return nil, errors.New("stopped MDS filesystem has no complete active survivor set")
	}
	if err := collectStoppedMDSStandbys(s, *native.Standbys, want, registered); err != nil {
		return nil, err
	}
	for _, id := range scope.DataPools {
		name, ok := want.Pools.Names[id]
		if !ok {
			return nil, errors.New("stopped MDS attached data pool is unavailable")
		}
		scope.PoolNames = append(scope.PoolNames, name)
	}
	return &StoppedState{Scope: scope, Registered: registered}, nil
}

// checkStoppedMDSMembership checks one filesystem entry: a unique identity,
// no row for the stopped target, every assigned GID with a complete info row
// and every info row holding its up slot. A missing sibling row cannot prove
// the target name absent.
func checkStoppedMDSMembership(s seen, entry filesystemEntry, want StoppedExpectation) error {
	if !admitIdentity(s, entry) || entry.Map.Up == nil {
		return errors.New("stopped MDS filesystem entries are incomplete or duplicated")
	}
	switch admitRanks(s, entry.Map, want.TargetName) {
	case faultNone:
		return nil
	case faultForbidden:
		return errStoppedTarget
	case faultRow, faultShape:
		return errors.New("stopped MDS global daemon identity is present or malformed")
	case faultUp:
		return errors.New("global stopped MDS assigned membership is incomplete")
	default:
		return errors.New("global stopped MDS info membership is contradictory")
	}
}

// stoppedMDSScope checks that the owned filesystem keeps its creation identity,
// configured policy and settled ranks, and returns the removal scope.
func stoppedMDSScope(id int64, m *mapFields, want StoppedExpectation) (*Scope, error) {
	if id != want.Native.ID || m.Metadata == nil || *m.Metadata != want.Native.MetadataPool || m.Data == nil || !slices.Equal(*m.Data, want.Pools.DataPools) || m.Max == nil || *m.Max != want.ActiveMDS || m.Wanted == nil || *m.Wanted != want.StandbyMDS || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay != want.StandbyReplay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 || len(*m.In) != *m.Max || len(*m.Up) != *m.Max {
		return nil, errors.New("original stopped MDS filesystem policy or settled ranks changed")
	}
	scope := &Scope{DataPools: slices.Clone(*m.Data), Active: *m.Max, Standby: *m.Wanted, Joinable: *m.Flags.Joinable, Replay: *m.Flags.Replay, Protected: *m.Flags.Refuse}
	seenIn := map[int]bool{}
	for _, rank := range *m.In {
		if rank < 0 || rank >= *m.Max || seenIn[rank] {
			return nil, errors.New("stopped MDS active rank set is malformed")
		}
		seenIn[rank] = true
	}
	return scope, nil
}

// collectStoppedMDSSurvivors checks the owned filesystem's assigned members:
// each is settled, its own and not laggy, active members hold distinct ranks
// within max_mds and replay members exist only under the replay policy. It
// records them in active and registered.
func collectStoppedMDSSurvivors(id int64, m *mapFields, scope *Scope, active map[int]string, registered map[string]bool) error {
	for _, row := range *m.Info {
		if *row.Join != id || (*row.State != "up:active" && *row.State != "up:standby-replay") || len(row.Laggy) != 0 || registered[*row.Name] {
			return errors.New("stopped MDS target has unsettled or foreign assigned members")
		}
		registered[*row.Name] = true
		if *row.State == "up:active" {
			if *row.Rank >= *m.Max || active[*row.Rank] != "" || (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
				return errors.New("stopped MDS active rank membership is contradictory")
			}
			active[*row.Rank] = *row.Name
		} else if !scope.Replay || *row.Rank >= *m.Max {
			return errors.New("stopped MDS replay membership is contradictory")
		}
	}
	return nil
}

// collectStoppedMDSStandbys checks the global standby list and records the
// standbys that can join the owned filesystem in registered.
func collectStoppedMDSStandbys(s seen, standbys []Row, want StoppedExpectation, registered map[string]bool) error {
	for _, row := range standbys {
		switch admitRow(s, row, want.TargetName) {
		case faultNone:
		case faultForbidden:
			return errStoppedTarget
		default:
			return errors.New("stopped MDS global standby identity is present or malformed")
		}
		if *row.Rank != -1 || *row.State != "up:standby" {
			return errors.New("stopped MDS global standby identity is present or malformed")
		}
		if *row.Join == want.Native.ID {
			if len(row.Laggy) != 0 {
				return errors.New("owned MDS standby is laggy or has malformed laggy state")
			}
			registered[*row.Name] = true
		}
	}
	return nil
}
