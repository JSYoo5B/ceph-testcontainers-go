//go:build all || (integration && multicluster)

package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// CephFSPeerStatusDirectories reads one reply of `fs mirror peer status
// <fs>@<id> <peer>` as the per-directory entries {directory: entry}, for test
// oracles that read the daemon independently of the fixture. Squid and
// Tentacle print that flat object. Umbrella 21.2.0 nests each entry as
// {"metrics": {directory: {"peer": {peer: entry}}}}; the nested shape must
// name only the queried peer. Duplicate keys are refused at any depth before
// map decoding, which keeps only the last one, could hide a directory.
func CephFSPeerStatusDirectories(data []byte, peerID string) (map[string]json.RawMessage, error) {
	if err := strictjson.Check(data, strictjson.Options{}); err != nil {
		return nil, errors.Join(fmt.Errorf("peer status is not strict JSON: %s", data), err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil || top == nil {
		return nil, fmt.Errorf("peer status is not an object: %s", data)
	}
	metrics, nested := top["metrics"]
	if !nested {
		return top, nil
	}
	var directories map[string]struct {
		Peer map[string]json.RawMessage `json:"peer"`
	}
	if len(top) != 1 || json.Unmarshal(metrics, &directories) != nil || directories == nil {
		return nil, fmt.Errorf("nested peer status is malformed: %s", data)
	}
	entries := make(map[string]json.RawMessage, len(directories))
	for directory, value := range directories {
		entry, ok := value.Peer[peerID]
		if !ok || len(value.Peer) != 1 {
			return nil, fmt.Errorf("nested peer status of %s names other peers than %s: %s", directory, peerID, data)
		}
		entries[directory] = entry
	}
	return entries, nil
}

// cephfsByteUnits are the binary units of Ceph's byte_u_t, which Umbrella
// uses for sync_bytes.
var cephfsByteUnits = map[string]float64{"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}

var cephfsByteText = regexp.MustCompile(`\A([0-9]+)(?:\.([0-9]+))? (B|KiB|MiB|GiB|TiB)\z`)

// CephFSSyncBytesContains reports whether the sync_bytes value of a peer
// status snapshot can be want. Squid and Tentacle print the exact count as a
// number. Umbrella prints a rounded text such as "128.09 KiB" or "50 KiB";
// it can be want when want rounds to the same digits.
func CephFSSyncBytesContains(raw json.RawMessage, want uint64) (bool, error) {
	var exact uint64
	if json.Unmarshal(raw, &exact) == nil {
		return exact == want, nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return false, fmt.Errorf("sync_bytes %s is neither a count nor a byte text", raw)
	}
	match := cephfsByteText.FindStringSubmatch(text)
	if match == nil {
		return false, fmt.Errorf("sync_bytes %q is not a byte text", text)
	}
	value, err := strconv.ParseFloat(match[1]+"."+match[2]+"0", 64)
	if err != nil {
		return false, errors.Join(fmt.Errorf("sync_bytes %q", text), err)
	}
	unit := cephfsByteUnits[match[3]]
	half := 0.5 * math.Pow(10, -float64(len(match[2])))
	return math.Abs(float64(want)/unit-value) <= half, nil
}
