package mirror

import (
	"encoding/json"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// cephFSPeerStatusDirectories returns the directory entries of one reply of
// `fs mirror peer status <fs>@<id> <peer>` as the flat object that Squid and
// Tentacle print, {directory: entry}. Umbrella 21.2.0 nests the same entry
// (state, last_synced_snap, snaps_synced and the other counters) as
// {"metrics": {directory: {"peer": {peer: entry}}}}. A directory can never be
// named "metrics", because mirrored directories are absolute paths, so the
// two shapes are told apart without guessing.
//
// The nested shape is read strictly: "metrics" must be the only top-level
// key, "peer" the only key of a directory, and the queried peer the only peer
// of a directory. Anything else is a guard error rather than a directory that
// silently reads as absent. Duplicate keys are refused at any depth in both
// shapes, before map decoding could hide one.
func cephFSPeerStatusDirectories(data []byte, peerID string) ([]byte, error) {
	if err := strictjson.Check(data, strictjson.Options{}); errors.Is(err, strictjson.ErrDuplicateKey) {
		return nil, cephFSObserveGuard("CephFS directory peer status has a duplicate key")
	} else if err != nil {
		return nil, cephFSObserveGuard("decode CephFS directory peer status object")
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(data, &top) != nil || top == nil {
		return nil, cephFSObserveGuard("decode CephFS directory peer status map")
	}
	metrics, nested := top["metrics"]
	if !nested {
		return data, nil
	}
	if len(top) != 1 {
		return nil, cephFSObserveGuard("CephFS directory peer status mixes metrics with other keys")
	}
	var directories map[string]json.RawMessage
	if json.Unmarshal(metrics, &directories) != nil || directories == nil {
		return nil, cephFSObserveGuard("decode CephFS directory peer status metrics")
	}
	flat := make(map[string]json.RawMessage, len(directories))
	for directory, raw := range directories {
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil || len(fields) != 1 || fields["peer"] == nil {
			return nil, cephFSObserveGuard("decode CephFS directory peer status metrics entry")
		}
		var peers map[string]json.RawMessage
		if json.Unmarshal(fields["peer"], &peers) != nil || len(peers) != 1 || peers[peerID] == nil {
			return nil, cephFSObserveGuard("CephFS directory peer status names another peer")
		}
		flat[directory] = peers[peerID]
	}
	out, err := json.Marshal(flat)
	if err != nil {
		return nil, cephFSObserveGuard("encode CephFS directory peer status entries")
	}
	return out, nil
}
