package mdsmap

import (
	"errors"
	"slices"
)

// ReplacementExpectation is every value the replacement decoder compares
// the native global map with; the decoder keeps no reference to their source.
type ReplacementExpectation struct {
	// OldName is the stopped daemon's immutable name, which must be absent.
	OldName string
	// Filesystem is the configured name of the owned filesystem.
	Filesystem string
	// ReservedName is the name reserved for the replacement daemon.
	ReservedName string
	// WorkerExists reports whether the replacement daemon was already created.
	WorkerExists bool
	// Native is the identity captured at creation; it must not be nil.
	Native *Native
	// Pools is the native pool identity read for this decode.
	Pools *Pools
}

// ReplacementState is the accepted result of decoding the global map.
type ReplacementState struct {
	Scope *Scope
	// Ready reports that the exact new member is up:active.
	Ready bool
}

// DecodeReplacement strictly decodes one `fs dump` document and compares
// it with want. It is a function of its arguments: it takes no lock, reads no
// filesystem record, roster or container and starts no command, so it can run
// with the topology lock held or not. It refuses up:oneshot-replay. The checks
// run in one pass: each filesystem's membership, then the owned filesystem's
// policy and rank, then the standbys.
func DecodeReplacement(data []byte, want ReplacementExpectation) (*ReplacementState, error) {
	var native globalMap
	if DecodeJSON(data, &native) != nil || native.Filesystems == nil || native.Standbys == nil {
		return nil, errors.New("MDS replacement map is incomplete or malformed")
	}
	s := newSeen()
	var scope *Scope
	ready := false
	for _, entry := range *native.Filesystems {
		if err := checkReplacementMDSMembership(s, entry, want); err != nil {
			return nil, err
		}
		if *entry.Map.Name != want.Filesystem {
			continue
		}
		var err error
		if scope, err = replacementMDSScope(*entry.ID, entry.Map, want); err != nil {
			return nil, err
		}
		if ready, err = replacementMDSRankReady(entry.Map, want); err != nil {
			return nil, err
		}
	}
	if scope == nil {
		return nil, errors.New("original MDS replacement filesystem is absent")
	}
	reservedStandby, err := checkReplacementMDSStandbys(s, *native.Standbys, want)
	if err != nil {
		return nil, err
	}
	return &ReplacementState{Scope: scope, Ready: ready && !reservedStandby}, nil
}

// replacementRowError is the refusal of a daemon row that admitRow refused.
func replacementRowError(f fault) error {
	if f == faultForbidden {
		return errStoppedTarget
	}
	return errors.New("MDS replacement global daemon map is contradictory")
}

// checkReplacementMDSMembership checks one filesystem entry: a unique
// identity, every rank row matching its up slot and the up map naming only
// such rows. A foreign filesystem may not hold the reserved name.
func checkReplacementMDSMembership(s seen, entry filesystemEntry, want ReplacementExpectation) error {
	if !admitIdentity(s, entry) || entry.Map.Up == nil {
		return errors.New("MDS replacement global filesystem map is incomplete")
	}
	m := entry.Map
	switch f := admitRanks(s, m, want.OldName); f {
	case faultNone:
	case faultForbidden, faultRow:
		return replacementRowError(f)
	case faultUp:
		return errors.New("MDS replacement global up map has no matching daemon")
	default:
		return errors.New("MDS replacement global rank membership is malformed")
	}
	if *m.Name != want.Filesystem {
		for _, row := range *m.Info {
			if *row.Name == want.ReservedName {
				return errors.New("reserved MDS replacement name belongs to another filesystem")
			}
		}
	}
	return nil
}

// replacementMDSScope checks that the owned filesystem keeps the policy and
// pools it was created with and returns the removal scope it implies.
func replacementMDSScope(id int64, m *mapFields, want ReplacementExpectation) (*Scope, error) {
	if id != want.Native.ID || m.Metadata == nil || *m.Metadata != want.Native.MetadataPool || m.Data == nil || !slices.Equal(*m.Data, want.Pools.DataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || !slices.Equal(*m.In, []int{0}) || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Damaged)+len(*m.Stopped) != 0 {
		return nil, errors.New("original MDS replacement filesystem policy changed")
	}
	scope := &Scope{Active: 1, Standby: 0, Joinable: true, Protected: true, DataPools: slices.Clone(want.Pools.DataPools)}
	seenPools := map[int64]bool{}
	for _, id := range want.Pools.DataPools {
		if id < 0 || seenPools[id] {
			return nil, errors.New("MDS replacement data attachments are invalid or duplicated")
		}
		seenPools[id] = true
		name, ok := want.Pools.Names[id]
		if !ok {
			return nil, errors.New("MDS replacement attached pool is unavailable")
		}
		scope.PoolNames = append(scope.PoolNames, name)
	}
	return scope, nil
}

// replacementMDSRankReady checks rank zero of the owned filesystem: failed
// without a worker before creation, still failed while the new service has not
// registered, and otherwise held by exactly the reserved daemon in a recovery
// state. It reports whether that daemon is up:active.
func replacementMDSRankReady(m *mapFields, want ReplacementExpectation) (bool, error) {
	if !want.WorkerExists {
		if len(*m.Info) != 0 || len(*m.Up) != 0 || !slices.Equal(*m.Failed, []int{0}) {
			return false, errors.New("original MDS replacement requires failed rank zero without a worker")
		}
		return false, nil
	}
	if len(*m.Info) == 0 && len(*m.Up) == 0 && slices.Equal(*m.Failed, []int{0}) {
		// Original newly-created service has not yet registered.
		return false, nil
	}
	if len(*m.Info) != 1 || len(*m.Up) != 1 || len(*m.Failed) != 0 {
		return false, errors.New("MDS replacement rank is not the exact new member")
	}
	ready := false
	for _, row := range *m.Info {
		if *row.Name != want.ReservedName || *row.Rank != 0 || *row.Join != want.Native.ID || len(row.Laggy) != 0 || !slices.Contains([]string{"up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active"}, *row.State) {
			return false, errors.New("MDS replacement native rank identity or recovery state changed")
		}
		ready = *row.State == "up:active"
	}
	return ready, nil
}

// checkReplacementMDSStandbys checks the global standby list: every standby is
// a well-formed ordinary standby, none can take the failed rank, and the
// reserved name appears only as the new worker. It reports whether the reserved
// daemon is still a standby, which means the replacement is not ready.
func checkReplacementMDSStandbys(s seen, standbys []Row, want ReplacementExpectation) (bool, error) {
	reserved := false
	for _, row := range standbys {
		if f := admitRow(s, row, want.OldName); f != faultNone {
			return false, replacementRowError(f)
		}
		if *row.Rank != -1 || *row.State != "up:standby" {
			return false, errors.New("MDS replacement global standby map is malformed")
		}
		if *row.Name == want.ReservedName {
			if !want.WorkerExists || *row.Join != want.Native.ID || len(row.Laggy) != 0 {
				return false, errors.New("reserved MDS replacement name is already registered")
			}
			reserved = true
		} else if *row.Join == -1 || *row.Join == want.Native.ID || !s.fsIDs[*row.Join] {
			return false, errors.New("foreign standby can replace the failed original MDS rank")
		}
	}
	return reserved, nil
}
