package cephfs

import (
	"errors"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// replacementMDSMapExpectation is every value the replacement decoder compares
// the native global map with. The caller reads them from the filesystem, the
// stopped target's identity record and the replacement record under the
// topology lock; the decoder keeps no reference to any of them.
type replacementMDSMapExpectation struct {
	// oldName is the stopped daemon's immutable name, which must be absent.
	oldName string
	// filesystem is the configured name of the owned filesystem.
	filesystem string
	// reservedName is the name reserved for the replacement daemon.
	reservedName string
	// workerExists reports whether the replacement daemon was already created.
	workerExists bool
	// native is the identity captured at creation. It is dereferenced at the
	// same points the combined reader did, so a missing identity behaves as
	// before.
	native *cephFSNativeIdentity
	// pools is the native pool identity read for this decode.
	pools *cephFSNativePools
}

// replacementMDSMapState is the accepted result of decoding the global map.
type replacementMDSMapState struct {
	scope *cephFSMDSRemovalScope
	// ready reports that the exact new member is up:active.
	ready bool
}

type replacementMDSMap struct {
	Standbys    *[]stoppedMDSRow `json:"standbys"`
	Filesystems *[]struct {
		ID  *int64 `json:"id"`
		Map *struct {
			Name                         *string  `json:"fs_name"`
			Max                          *int     `json:"max_mds"`
			Wanted                       *int     `json:"standby_count_wanted"`
			Metadata                     *int64   `json:"metadata_pool"`
			Data                         *[]int64 `json:"data_pools"`
			In, Failed, Damaged, Stopped *[]int
			Up                           *map[string]uint64        `json:"up"`
			Info                         *map[string]stoppedMDSRow `json:"info"`
			Flags                        *struct {
				Joinable *bool `json:"joinable"`
				Replay   *bool `json:"allow_standby_replay"`
				Refuse   *bool `json:"refuse_standby_for_another_fs"`
			} `json:"flags_state"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

// decodeMDSReplacementMap strictly decodes one `fs dump` document and compares
// it with want. It is a function of its arguments: it takes no lock, reads no
// Filesystem, roster or container and starts no command, so it can run with the
// topology lock held or not. Every guard below was moved unchanged from
// readMDSReplacementScope, including the refusal of up:oneshot-replay.
func decodeMDSReplacementMap(data []byte, want replacementMDSMapExpectation) (*replacementMDSMapState, error) {
	var native replacementMDSMap
	if coldMDSJSON(data, &native) != nil || native.Filesystems == nil || native.Standbys == nil {
		return nil, errors.New("MDS replacement map is incomplete or malformed")
	}
	names, gids, fsNames, fsIDs := map[string]bool{}, map[uint64]bool{}, map[string]bool{}, map[int64]bool{}
	validRow := func(row stoppedMDSRow) error {
		if row.Name != nil && *row.Name == want.oldName {
			return errors.New("original stopped MDS name is still registered")
		}
		if row.Name == nil || !naming.PoolResource.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil || row.Join == nil || *row.Join < -1 || row.State == nil || !cephFSMDSRowStateAccepted(*row.State) || names[*row.Name] || gids[*row.GID] {
			return errors.New("MDS replacement global daemon map is contradictory")
		}
		names[*row.Name], gids[*row.GID] = true, true
		return nil
	}
	var scope *cephFSMDSRemovalScope
	ready := false
	for _, entry := range *native.Filesystems {
		m := entry.Map
		if entry.ID == nil || *entry.ID < 0 || fsIDs[*entry.ID] || m == nil || m.Name == nil || *m.Name == "" || fsNames[*m.Name] || m.Info == nil || m.Up == nil {
			return nil, errors.New("MDS replacement global filesystem map is incomplete")
		}
		fsIDs[*entry.ID], fsNames[*m.Name] = true, true
		for key, row := range *m.Info {
			if err := validRow(row); err != nil {
				return nil, err
			}
			if key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" || *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
				return nil, errors.New("MDS replacement global rank membership is malformed")
			}
			if *m.Name != want.filesystem && *row.Name == want.reservedName {
				return nil, errors.New("reserved MDS replacement name belongs to another filesystem")
			}
		}
		for key, gid := range *m.Up {
			row, ok := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
			if !ok || gid == 0 || row.Rank == nil || key != "mds_"+strconv.Itoa(*row.Rank) || row.State == nil || *row.State == "up:standby-replay" {
				return nil, errors.New("MDS replacement global up map has no matching daemon")
			}
		}
		if *m.Name != want.filesystem {
			continue
		}
		if *entry.ID != want.native.id || m.Metadata == nil || *m.Metadata != want.native.metadataPool || m.Data == nil || !slices.Equal(*m.Data, want.pools.dataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || !slices.Equal(*m.In, []int{0}) || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Damaged)+len(*m.Stopped) != 0 {
			return nil, errors.New("original MDS replacement filesystem policy changed")
		}
		scope = &cephFSMDSRemovalScope{active: 1, standby: 0, joinable: true, protected: true, dataPools: slices.Clone(want.pools.dataPools)}
		seenPools := map[int64]bool{}
		for _, id := range want.pools.dataPools {
			if id < 0 || seenPools[id] {
				return nil, errors.New("MDS replacement data attachments are invalid or duplicated")
			}
			seenPools[id] = true
			pool, err := want.pools.poolByID(id)
			if err != nil {
				return nil, errors.New("MDS replacement attached pool is unavailable")
			}
			scope.poolNames = append(scope.poolNames, pool.Name)
		}
		if !want.workerExists {
			if len(*m.Info) != 0 || len(*m.Up) != 0 || !slices.Equal(*m.Failed, []int{0}) {
				return nil, errors.New("original MDS replacement requires failed rank zero without a worker")
			}
		} else if len(*m.Info) == 0 && len(*m.Up) == 0 && slices.Equal(*m.Failed, []int{0}) {
			// Original newly-created service has not yet registered.
		} else {
			if len(*m.Info) != 1 || len(*m.Up) != 1 || len(*m.Failed) != 0 {
				return nil, errors.New("MDS replacement rank is not the exact new member")
			}
			for _, row := range *m.Info {
				if *row.Name != want.reservedName || *row.Rank != 0 || *row.Join != want.native.id || len(row.Laggy) != 0 || !slices.Contains([]string{"up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active"}, *row.State) {
					return nil, errors.New("MDS replacement native rank identity or recovery state changed")
				}
				ready = *row.State == "up:active"
			}
		}
	}
	if scope == nil {
		return nil, errors.New("original MDS replacement filesystem is absent")
	}
	for _, row := range *native.Standbys {
		if err := validRow(row); err != nil {
			return nil, err
		}
		if *row.Rank != -1 || *row.State != "up:standby" {
			return nil, errors.New("MDS replacement global standby map is malformed")
		}
		if *row.Name == want.reservedName {
			if !want.workerExists || *row.Join != want.native.id || len(row.Laggy) != 0 {
				return nil, errors.New("reserved MDS replacement name is already registered")
			}
			ready = false
		} else if *row.Join == -1 || *row.Join == want.native.id || !fsIDs[*row.Join] {
			return nil, errors.New("foreign standby can replace the failed original MDS rank")
		}
	}
	return &replacementMDSMapState{scope: scope, ready: ready}, nil
}
