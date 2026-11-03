package mdsmap

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// Native is the identity captured when the filesystem was created: its
// native ID and metadata pool.
type Native struct {
	ID, MetadataPool int64
}

// Pools is the native pool identity read for one decode: the attached data
// pool IDs and the names of the cluster's pools by ID.
type Pools struct {
	DataPools []int64
	Names     map[int64]string
}

// Scope is what a stopped-MDS or replacement decode establishes about the
// owned filesystem: its attached data pools and their names, the configured
// active and standby counts and the joinable, standby-replay and refuse
// flags.
type Scope struct {
	DataPools                   []int64
	PoolNames                   []string
	Active, Standby             int
	Joinable, Replay, Protected bool
}

// Row is one daemon row of the map, with its laggy mark.
type Row struct {
	Name  *string         `json:"name"`
	GID   *uint64         `json:"gid"`
	Rank  *int            `json:"rank"`
	State *string         `json:"state"`
	Join  *int64          `json:"join_fscid"`
	Laggy json.RawMessage `json:"laggy_since"`
}

// globalMap is the document of `fs dump`.
type globalMap struct {
	Standbys    *[]Row             `json:"standbys"`
	Filesystems *[]filesystemEntry `json:"filesystems"`
}

// filesystemEntry is one filesystem entry of the global map.
type filesystemEntry struct {
	ID  *int64     `json:"id"`
	Map *mapFields `json:"mdsmap"`
}

// mapFields is the mdsmap of one filesystem entry.
type mapFields struct {
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
}

// seen is every daemon and filesystem identity a decoder has met so far in
// the global map; each one may appear once.
type seen struct {
	names   map[string]bool
	gids    map[uint64]bool
	fsNames map[string]bool
	fsIDs   map[int64]bool
}

func newSeen() seen {
	return seen{names: map[string]bool{}, gids: map[uint64]bool{}, fsNames: map[string]bool{}, fsIDs: map[int64]bool{}}
}

// fault is what one shared row or membership check refused; each decoder
// reports it with its own text.
type fault int

const (
	faultNone fault = iota
	// faultForbidden is a row with the name the decode requires absent.
	faultForbidden
	// faultRow is a row that is incomplete, in an unaccepted state or a
	// repeat of a name or GID already seen.
	faultRow
	// faultShape is a filesystem row not under its gid_<GID> key, without a
	// rank or in a standby state.
	faultShape
	// faultUp is an up slot that names no row of its rank, or a
	// standby-replay row.
	faultUp
	// faultSlot is a row other than a standby-replay one that does not hold
	// its up slot.
	faultSlot
)

// admitRow checks one daemon row: complete, in an accepted state, not named
// forbidden and not a repeat of a name or GID already seen. It records an
// admitted row.
func admitRow(s seen, row Row, forbidden string) fault {
	if row.Name != nil && *row.Name == forbidden {
		return faultForbidden
	}
	if row.Name == nil || !naming.PoolResource.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil ||
		row.State == nil || !StateAccepted(*row.State) || row.Join == nil || *row.Join < -1 || s.names[*row.Name] || s.gids[*row.GID] {
		return faultRow
	}
	s.names[*row.Name], s.gids[*row.GID] = true, true
	return faultNone
}

// admitIdentity checks one filesystem entry's identity and records it: a
// non-negative ID and a name, both unique, and an info map.
func admitIdentity(s seen, entry filesystemEntry) bool {
	m := entry.Map
	if entry.ID == nil || *entry.ID < 0 || m == nil || m.Name == nil || *m.Name == "" || s.fsIDs[*entry.ID] || s.fsNames[*m.Name] || m.Info == nil {
		return false
	}
	s.fsIDs[*entry.ID], s.fsNames[*m.Name] = true, true
	return true
}

// admitRanks checks the rank membership of one filesystem whose identity was
// admitted: every info row admitted under its gid_<GID> key with a rank and
// not in a standby state, every up slot naming such a row of its rank that
// is not standby-replay, and every other row holding its up slot. A nil up
// map is faultUp.
func admitRanks(s seen, m *mapFields, forbidden string) fault {
	if m.Up == nil {
		return faultUp
	}
	for key, row := range *m.Info {
		if f := admitRow(s, row, forbidden); f != faultNone {
			return f
		}
		if key != "gid_"+strconv.FormatUint(*row.GID, 10) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" {
			return faultShape
		}
	}
	for key, gid := range *m.Up {
		text, ok := strings.CutPrefix(key, "mds_")
		rank, err := strconv.Atoi(text)
		row, found := (*m.Info)["gid_"+strconv.FormatUint(gid, 10)]
		if !ok || err != nil || rank < 0 || text != strconv.Itoa(rank) || gid == 0 || !found || *row.Rank != rank || *row.State == "up:standby-replay" {
			return faultUp
		}
	}
	for _, row := range *m.Info {
		if *row.State != "up:standby-replay" && (*m.Up)["mds_"+strconv.Itoa(*row.Rank)] != *row.GID {
			return faultSlot
		}
	}
	return faultNone
}

// DecodeJSON strictly decodes one native document: it rejects duplicate
// fields, aliases of known fields, nesting beyond the shared limit and
// trailing documents. It never returns the raw decode error, since native
// auth input can contain credentials.
func DecodeJSON(data []byte, value any) error {
	if strictjson.Decode(data, value, strictjson.Options{MaxBytes: 4 << 20}) != nil {
		return errors.New("invalid native JSON")
	}
	return nil
}

// IdentityPresent strictly decodes one `auth ls` document and reports
// whether it lists entity. Every listed entity name must be present, trimmed
// and unique.
func IdentityPresent(data []byte, entity string) (bool, error) {
	var auth struct {
		Entities *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if DecodeJSON(data, &auth) != nil || auth.Entities == nil {
		return false, errors.New("decode the native identity names")
	}
	names := map[string]bool{}
	for _, entry := range *auth.Entities {
		if entry.Entity == nil || *entry.Entity == "" || strings.TrimSpace(*entry.Entity) != *entry.Entity || names[*entry.Entity] {
			return false, errors.New("native identity names are incomplete or duplicated")
		}
		names[*entry.Entity] = true
	}
	return names[entity], nil
}
