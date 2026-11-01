package mdsmap

import (
	"errors"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// ReplacementExpectation is every value the replacement decoder compares
// the native global map with. The caller reads them from the filesystem, the
// stopped target's identity record and the replacement record under the
// topology lock; the decoder keeps no reference to any of them.
type ReplacementExpectation struct {
	// oldName is the stopped daemon's immutable name, which must be absent.
	OldName string
	// filesystem is the configured name of the owned filesystem.
	Filesystem string
	// reservedName is the name reserved for the replacement daemon.
	ReservedName string
	// workerExists reports whether the replacement daemon was already created.
	WorkerExists bool
	// Native is the identity captured at creation; it must not be nil.
	Native *Native
	// Pools is the native pool identity read for this decode.
	Pools *Pools
}

// ReplacementState is the accepted result of decoding the global map.
type ReplacementState struct {
	Scope *Scope
	// ready reports that the exact new member is up:active.
	Ready bool
}

type replacementMDSMap struct {
	Standbys    *[]StoppedRow               `json:"standbys"`
	Filesystems *[]replacementMDSFilesystem `json:"filesystems"`
}

// replacementMDSFilesystem is one filesystem entry of the global map.
type replacementMDSFilesystem struct {
	ID  *int64                `json:"id"`
	Map *replacementMDSFields `json:"mdsmap"`
}

// replacementMDSFields is the mdsmap of one filesystem entry.
type replacementMDSFields struct {
	Name                         *string  `json:"fs_name"`
	Max                          *int     `json:"max_mds"`
	Wanted                       *int     `json:"standby_count_wanted"`
	Metadata                     *int64   `json:"metadata_pool"`
	Data                         *[]int64 `json:"data_pools"`
	In, Failed, Damaged, Stopped *[]int
	Up                           *map[string]uint64     `json:"up"`
	Info                         *map[string]StoppedRow `json:"info"`
	Flags                        *struct {
		Joinable *bool `json:"joinable"`
		Replay   *bool `json:"allow_standby_replay"`
		Refuse   *bool `json:"refuse_standby_for_another_fs"`
	} `json:"flags_state"`
}

// replacementMDSSeen is every daemon and filesystem identity the decoder has
// met so far in the global map; each one may appear once.
type replacementMDSSeen struct {
	names   map[string]bool
	gids    map[uint64]bool
	fsNames map[string]bool
	fsIDs   map[int64]bool
}

// DecodeReplacement strictly decodes one `fs dump` document and compares
// it with want. It is a function of its arguments: it takes no lock, reads no
// filesystem record, roster or container and starts no command, so it can run
// with the topology lock held or not. It refuses up:oneshot-replay. The checks
// run in one pass: each filesystem's membership, then the owned filesystem's
// policy and rank, then the standbys.
func DecodeReplacement(data []byte, want ReplacementExpectation) (*ReplacementState, error) {
	var native replacementMDSMap
	if decodeJSON(data, &native) != nil || native.Filesystems == nil || native.Standbys == nil {
		return nil, errors.New("MDS replacement map is incomplete or malformed")
	}
	seen := replacementMDSSeen{names: map[string]bool{}, gids: map[uint64]bool{}, fsNames: map[string]bool{}, fsIDs: map[int64]bool{}}
	var scope *Scope
	ready := false
	for _, entry := range *native.Filesystems {
		if err := checkReplacementMDSMembership(entry, want, seen); err != nil {
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
	reservedStandby, err := checkReplacementMDSStandbys(*native.Standbys, want, seen)
	if err != nil {
		return nil, err
	}
	return &ReplacementState{Scope: scope, Ready: ready && !reservedStandby}, nil
}

// admitReplacementMDSRow refuses a daemon row that names the stopped original,
// is incomplete or in an unaccepted state, or repeats a name or GID already
// seen, and records it otherwise.
func admitReplacementMDSRow(row StoppedRow, want ReplacementExpectation, seen replacementMDSSeen) error {
	if row.Name != nil && *row.Name == want.OldName {
		return errors.New("original stopped MDS name is still registered")
	}
	if row.Name == nil || !naming.PoolResource.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil || row.Join == nil || *row.Join < -1 || row.State == nil || !StateAccepted(*row.State) || seen.names[*row.Name] || seen.gids[*row.GID] {
		return errors.New("MDS replacement global daemon map is contradictory")
	}
	seen.names[*row.Name], seen.gids[*row.GID] = true, true
	return nil
}

// checkReplacementMDSMembership checks one filesystem entry: a unique
// identity, every rank row matching its up slot and the up map naming only
// such rows. A foreign filesystem may not hold the reserved name.
func checkReplacementMDSMembership(entry replacementMDSFilesystem, want ReplacementExpectation, seen replacementMDSSeen) error {
	m := entry.Map
	if entry.ID == nil || *entry.ID < 0 || seen.fsIDs[*entry.ID] || m == nil || m.Name == nil || *m.Name == "" || seen.fsNames[*m.Name] || m.Info == nil || m.Up == nil {
		return errors.New("MDS replacement global filesystem map is incomplete")
	}
	seen.fsIDs[*entry.ID], seen.fsNames[*m.Name] = true, true
	for key, row := range *m.Info {
		if err := admitReplacementMDSRow(row, want, seen); err != nil {
			return err
		}
		if key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" || *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
			return errors.New("MDS replacement global rank membership is malformed")
		}
		if *m.Name != want.Filesystem && *row.Name == want.ReservedName {
			return errors.New("reserved MDS replacement name belongs to another filesystem")
		}
	}
	for key, gid := range *m.Up {
		row, ok := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
		if !ok || gid == 0 || row.Rank == nil || key != "mds_"+strconv.Itoa(*row.Rank) || row.State == nil || *row.State == "up:standby-replay" {
			return errors.New("MDS replacement global up map has no matching daemon")
		}
	}
	return nil
}

// replacementMDSScope checks that the owned filesystem keeps the policy and
// pools it was created with and returns the removal scope it implies.
func replacementMDSScope(id int64, m *replacementMDSFields, want ReplacementExpectation) (*Scope, error) {
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
func replacementMDSRankReady(m *replacementMDSFields, want ReplacementExpectation) (bool, error) {
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
func checkReplacementMDSStandbys(standbys []StoppedRow, want ReplacementExpectation, seen replacementMDSSeen) (bool, error) {
	reserved := false
	for _, row := range standbys {
		if err := admitReplacementMDSRow(row, want, seen); err != nil {
			return false, err
		}
		if *row.Rank != -1 || *row.State != "up:standby" {
			return false, errors.New("MDS replacement global standby map is malformed")
		}
		if *row.Name == want.ReservedName {
			if !want.WorkerExists || *row.Join != want.Native.ID || len(row.Laggy) != 0 {
				return false, errors.New("reserved MDS replacement name is already registered")
			}
			reserved = true
		} else if *row.Join == -1 || *row.Join == want.Native.ID || !seen.fsIDs[*row.Join] {
			return false, errors.New("foreign standby can replace the failed original MDS rank")
		}
	}
	return reserved, nil
}
