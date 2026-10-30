package cephfs

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// stoppedMDSMapExpectation is every value the stopped-MDS decoder compares the
// native global map with. The caller reads them from the filesystem and the
// target's identity record under the owner lock; the decoder keeps no reference
// to either.
type stoppedMDSMapExpectation struct {
	// targetName is the stopped daemon's immutable name, which must be absent.
	targetName string
	// filesystem is the configured name of the owned filesystem.
	filesystem string
	// native is the identity captured at creation. It is dereferenced at the
	// same points the combined decoder did, so a missing identity behaves as
	// before.
	native *cephFSNativeIdentity
	// pools is the native pool identity read for this decode.
	pools *cephFSNativePools
	// activeMDS, standbyMDS and standbyReplay are the configured policy.
	activeMDS, standbyMDS int
	standbyReplay         bool
}

// stoppedMDSMapState is the accepted result of decoding the global map.
type stoppedMDSMapState struct {
	scope *cephFSMDSRemovalScope
	// registered are the names of the owned filesystem's assigned members and
	// of the standbys that can join it.
	registered map[string]bool
}

// stoppedMDSMap is the global map of `fs dump`.
type stoppedMDSMap struct {
	Standbys    *[]stoppedMDSRow        `json:"standbys"`
	Filesystems *[]stoppedMDSFilesystem `json:"filesystems"`
}

// stoppedMDSFilesystem is one filesystem entry of the global map.
type stoppedMDSFilesystem struct {
	ID  *int64            `json:"id"`
	Map *stoppedMDSFields `json:"mdsmap"`
}

// stoppedMDSFields is the mdsmap of one filesystem entry.
type stoppedMDSFields struct {
	Name     *string                   `json:"fs_name"`
	Max      *int                      `json:"max_mds"`
	Wanted   *int                      `json:"standby_count_wanted"`
	Metadata *int64                    `json:"metadata_pool"`
	Data     *[]int64                  `json:"data_pools"`
	Info     *map[string]stoppedMDSRow `json:"info"`
	Up       *map[string]uint64        `json:"up"`
	In       *[]int                    `json:"in"`
	Failed   *[]int                    `json:"failed"`
	Damaged  *[]int                    `json:"damaged"`
	Stopped  *[]int                    `json:"stopped"`
	Flags    *struct {
		Joinable *bool `json:"joinable"`
		Replay   *bool `json:"allow_standby_replay"`
		Refuse   *bool `json:"refuse_standby_for_another_fs"`
	} `json:"flags_state"`
}

// stoppedMDSSeen is every daemon and filesystem identity the decoder has met
// so far in the global map; each one may appear once.
type stoppedMDSSeen struct {
	rows    map[string]stoppedMDSRow
	gids    map[uint64]bool
	fsNames map[string]bool
	fsIDs   map[int64]bool
}

// decodeStoppedMDSMap strictly decodes one `fs dump` document and compares it
// with want. It is a function of its arguments: it takes no lock, reads no
// Filesystem, roster or container and starts no command, so it can run with
// the owner lock held or not. Docker verification of the surviving daemons is
// not part of it (verifyStoppedMDSSurvivors). Every guard below was moved
// unchanged from decodeStoppedMDSScope, including the refusal of
// up:oneshot-replay. The checks run in the order of the original single pass:
// each filesystem's membership and, for the owned one, its policy and
// survivors; then the standbys and the attached pools.
func decodeStoppedMDSMap(data []byte, want stoppedMDSMapExpectation) (*stoppedMDSMapState, error) {
	var native stoppedMDSMap
	if coldMDSJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return nil, errors.New("decode stopped MDS global map")
	}
	seen := stoppedMDSSeen{rows: map[string]stoppedMDSRow{}, gids: map[uint64]bool{}, fsNames: map[string]bool{}, fsIDs: map[int64]bool{}}
	var scope *cephFSMDSRemovalScope
	active := map[int]string{}
	registered := map[string]bool{}
	for _, entry := range *native.Filesystems {
		if err := checkStoppedMDSMembership(entry, want, seen); err != nil {
			return nil, err
		}
		if *entry.Map.Name != want.filesystem {
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
	if scope == nil || scope.active < 1 || len(active) != scope.active {
		return nil, errors.New("stopped MDS filesystem has no complete active survivor set")
	}
	if err := collectStoppedMDSStandbys(*native.Standbys, want, seen, registered); err != nil {
		return nil, err
	}
	for _, id := range scope.dataPools {
		pool, err := want.pools.poolByID(id)
		if err != nil {
			return nil, errors.New("stopped MDS attached data pool is unavailable")
		}
		scope.poolNames = append(scope.poolNames, pool.Name)
	}
	return &stoppedMDSMapState{scope: scope, registered: registered}, nil
}

// admitStoppedMDSRow reports whether row is complete, in an accepted state,
// not the stopped target and not a repeat of a name or GID already seen, and
// records it when it is.
func admitStoppedMDSRow(row stoppedMDSRow, want stoppedMDSMapExpectation, seen stoppedMDSSeen) bool {
	if row.Name == nil || !naming.PoolResource.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.State == nil || !cephFSMDSRowStateAccepted(*row.State) || row.Rank == nil || row.Join == nil || *row.Join < -1 || seen.gids[*row.GID] || seen.rows[*row.Name].Name != nil || *row.Name == want.targetName {
		return false
	}
	seen.gids[*row.GID], seen.rows[*row.Name] = true, row
	return true
}

// checkStoppedMDSMembership checks one filesystem entry: a unique identity,
// no row for the stopped target, every assigned GID with a complete info row
// and every info row holding its up slot.
func checkStoppedMDSMembership(entry stoppedMDSFilesystem, want stoppedMDSMapExpectation, seen stoppedMDSSeen) error {
	m := entry.Map
	if entry.ID == nil || *entry.ID < 0 || m == nil || m.Name == nil || *m.Name == "" || seen.fsIDs[*entry.ID] || seen.fsNames[*m.Name] || m.Info == nil || m.Up == nil {
		return errors.New("stopped MDS filesystem entries are incomplete or duplicated")
	}
	seen.fsIDs[*entry.ID], seen.fsNames[*m.Name] = true, true
	for key, row := range *m.Info {
		if row.Name != nil && *row.Name == want.targetName {
			return errors.New("original stopped MDS name is still registered")
		}
		if !admitStoppedMDSRow(row, want, seen) || key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:standby" || *row.State == "up:boot" {
			return errors.New("stopped MDS global daemon identity is present or malformed")
		}
	}
	// Every globally assigned GID must have a corresponding complete info
	// row. A missing sibling row cannot prove the target name absent.
	for key, gid := range *m.Up {
		text, ok := strings.CutPrefix(key, "mds_")
		rank, err := strconv.Atoi(text)
		row, found := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
		if !ok || err != nil || rank < 0 || text != strconv.Itoa(rank) || gid == 0 || !found || *row.GID != gid || *row.Rank != rank || *row.State == "up:standby-replay" {
			return errors.New("global stopped MDS assigned membership is incomplete")
		}
	}
	for _, row := range *m.Info {
		if *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
			return errors.New("global stopped MDS info membership is contradictory")
		}
	}
	return nil
}

// stoppedMDSScope checks that the owned filesystem keeps its creation identity,
// configured policy and settled ranks, and returns the removal scope.
func stoppedMDSScope(id int64, m *stoppedMDSFields, want stoppedMDSMapExpectation) (*cephFSMDSRemovalScope, error) {
	if id != want.native.id || m.Metadata == nil || *m.Metadata != want.native.metadataPool || m.Data == nil || !slices.Equal(*m.Data, want.pools.dataPools) || m.Max == nil || *m.Max != want.activeMDS || m.Wanted == nil || *m.Wanted != want.standbyMDS || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay != want.standbyReplay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 || len(*m.In) != *m.Max || len(*m.Up) != *m.Max {
		return nil, errors.New("original stopped MDS filesystem policy or settled ranks changed")
	}
	scope := &cephFSMDSRemovalScope{dataPools: slices.Clone(*m.Data), active: *m.Max, standby: *m.Wanted, joinable: *m.Flags.Joinable, replay: *m.Flags.Replay, protected: *m.Flags.Refuse}
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
func collectStoppedMDSSurvivors(id int64, m *stoppedMDSFields, scope *cephFSMDSRemovalScope, active map[int]string, registered map[string]bool) error {
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
		} else if !scope.replay || *row.Rank >= *m.Max {
			return errors.New("stopped MDS replay membership is contradictory")
		}
	}
	return nil
}

// collectStoppedMDSStandbys checks the global standby list and records the
// standbys that can join the owned filesystem in registered.
func collectStoppedMDSStandbys(standbys []stoppedMDSRow, want stoppedMDSMapExpectation, seen stoppedMDSSeen, registered map[string]bool) error {
	for _, row := range standbys {
		if row.Name != nil && *row.Name == want.targetName {
			return errors.New("original stopped MDS name is still registered")
		}
		if !admitStoppedMDSRow(row, want, seen) || *row.Rank != -1 || *row.State != "up:standby" {
			return errors.New("stopped MDS global standby identity is present or malformed")
		}
		if *row.Join == want.native.id {
			if len(row.Laggy) != 0 {
				return errors.New("owned MDS standby is laggy or has malformed laggy state")
			}
			registered[*row.Name] = true
		}
	}
	return nil
}
