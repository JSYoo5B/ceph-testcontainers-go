package mdsmap

import (
	"errors"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// ColdExpectation is every value the cold-start decoder compares the
// native global map with. verifyColdMDS reads them from the filesystem and the
// confirmed cold capability; the decoder keeps no reference to either.
type ColdExpectation struct {
	// filesystem is the configured name of the owned filesystem.
	Filesystem string
	// reservedName is the name the first MDS would take, which must be absent.
	ReservedName string
	// Native is the identity captured at creation; it must not be nil.
	Native *Native
	// DataPools are the data pool IDs attached when the filesystem was created.
	DataPools []int64
}

// DecodeCold strictly decodes one `fs dump` document and checks that the
// owned filesystem still has no rank, no worker and no assigned member and that
// no standby can join it. It is a function of its arguments: it takes no lock,
// reads no filesystem record, roster or container and starts no command. It
// refuses up:oneshot-replay.
func DecodeCold(data []byte, want ColdExpectation) error {
	var native struct {
		Standbys    *[]Row `json:"standbys"`
		Filesystems *[]struct {
			ID  *int64 `json:"id"`
			Map *struct {
				Name     *string            `json:"fs_name"`
				Max      *int               `json:"max_mds"`
				Wanted   *int               `json:"standby_count_wanted"`
				Metadata *int64             `json:"metadata_pool"`
				Data     *[]int64           `json:"data_pools"`
				Info     *map[string]Row    `json:"info"`
				Up       *map[string]uint64 `json:"up"`
				In       *[]int             `json:"in"`
				Failed   *[]int             `json:"failed"`
				Damaged  *[]int             `json:"damaged"`
				Stopped  *[]int             `json:"stopped"`
				Flags    *struct {
					Joinable *bool `json:"joinable"`
					Replay   *bool `json:"allow_standby_replay"`
					Refuse   *bool `json:"refuse_standby_for_another_fs"`
				} `json:"flags_state"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if decodeJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return errors.New("decode cold MDS map")
	}
	gids, names := map[uint64]bool{}, map[string]bool{}
	rowValid := func(row Row) bool {
		if row.Name == nil || !naming.PoolResource.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil || row.State == nil || !StateAccepted(*row.State) || row.Join == nil || *row.Join < -1 || gids[*row.GID] || names[*row.Name] || *row.Name == want.ReservedName {
			return false
		}
		gids[*row.GID], names[*row.Name] = true, true
		return true
	}
	found := 0
	fsNames, fsIDs := map[string]bool{}, map[int64]bool{}
	for _, entry := range *native.Filesystems {
		if entry.ID == nil || *entry.ID < 0 || entry.Map == nil || entry.Map.Name == nil || *entry.Map.Name == "" || fsIDs[*entry.ID] || fsNames[*entry.Map.Name] || entry.Map.Info == nil {
			return errors.New("cold filesystem map entries are incomplete or duplicated")
		}
		fsIDs[*entry.ID], fsNames[*entry.Map.Name] = true, true
		if *entry.Map.Name != want.Filesystem {
			for _, row := range *entry.Map.Info {
				if !rowValid(row) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" {
					return errors.New("foreign MDS map is incomplete or conflicts with initial identity")
				}
			}
			continue
		}
		found++
		m := entry.Map
		if *entry.ID != want.Native.ID || m.Metadata == nil || *m.Metadata != want.Native.MetadataPool || m.Data == nil || !slices.Equal(*m.Data, want.DataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse {
			return errors.New("original cold filesystem policy or identity changed")
		}
		if m.Up == nil || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Info) != 0 || len(*m.Up) != 0 || len(*m.In) != 0 || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 {
			return errors.New("cold filesystem already has MDS rank or worker state")
		}
	}
	if found != 1 {
		return errors.New("original cold filesystem map is absent")
	}
	for _, row := range *native.Standbys {
		if !rowValid(row) || *row.Rank != -1 || *row.State != "up:standby" {
			return errors.New("native standby map is incomplete or conflicting")
		}
		if *row.Join == -1 || *row.Join == want.Native.ID || !fsIDs[*row.Join] {
			return errors.New("foreign standby can join the cold filesystem")
		}
	}
	return nil
}
