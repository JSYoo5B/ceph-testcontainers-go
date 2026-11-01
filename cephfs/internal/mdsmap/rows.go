package mdsmap

import (
	"encoding/json"
	"errors"

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

// Row is one daemon row of the map.
type Row struct {
	Name  *string `json:"name"`
	GID   *uint64 `json:"gid"`
	Rank  *int    `json:"rank"`
	State *string `json:"state"`
	Join  *int64  `json:"join_fscid"`
}

// StoppedRow is a daemon row with its laggy mark.
type StoppedRow struct {
	Row
	Laggy json.RawMessage `json:"laggy_since"`
}

// decodeJSON strictly decodes one document: it rejects duplicate fields,
// aliases of known fields and trailing documents. It never returns the raw
// decode error, since native auth input can contain credentials.
func decodeJSON(data []byte, value any) error {
	if strictjson.Decode(data, value, strictjson.Options{MaxBytes: 4 << 20}) != nil {
		return errors.New("invalid cold native JSON")
	}
	return nil
}
