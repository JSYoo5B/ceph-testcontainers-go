//go:build all || (integration && multicluster)

package cephfs_test

import (
	"encoding/json"
	"fmt"

	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// cephFSMirrorReplay is the independent native peer-status evidence for
// /federation, including the spelling that supplied the current snapshot.
type cephFSMirrorReplay struct {
	State         string
	Current, Last cephfsmirror.Snapshot
	CurrentField  string
	LastBytes     json.RawMessage
	Synced        uint64
}

func parseCephFSMirrorPeerReplay(out []byte, peerID string) (cephFSMirrorReplay, error) {
	type snapshot struct {
		ID    *uint64         `json:"id"`
		Name  *string         `json:"name"`
		Bytes json.RawMessage `json:"sync_bytes"`
	}
	decode := func(raw json.RawMessage) (snapshot, error) {
		var value snapshot
		if err := json.Unmarshal(raw, &value); err != nil || value.ID == nil || *value.ID == 0 || value.Name == nil || *value.Name == "" {
			return value, fmt.Errorf("invalid native snapshot identity: %s", raw)
		}
		return value, nil
	}
	directories, err := harness.CephFSPeerStatusDirectories(out, peerID)
	if err != nil {
		return cephFSMirrorReplay{}, err
	}
	var entry struct {
		State        string          `json:"state"`
		Current      json.RawMessage `json:"current_syncing_snap"`
		SquidCurrent json.RawMessage `json:"current_sycning_snap"`
		Last         json.RawMessage `json:"last_synced_snap"`
		Synced       *uint64         `json:"snaps_synced"`
	}
	raw, ok := directories["/federation"]
	if !ok || json.Unmarshal(raw, &entry) != nil || entry.State == "" || entry.Synced == nil {
		return cephFSMirrorReplay{}, fmt.Errorf("peer status has no synced /federation: %s", out)
	}
	last, err := decode(entry.Last)
	if err != nil {
		return cephFSMirrorReplay{}, err
	}
	replay := cephFSMirrorReplay{State: entry.State, Last: cephfsmirror.Snapshot{ID: *last.ID, Name: *last.Name}, LastBytes: last.Bytes, Synced: *entry.Synced}
	for _, field := range []struct {
		name string
		raw  json.RawMessage
	}{
		{"current_syncing_snap", entry.Current}, {"current_sycning_snap", entry.SquidCurrent},
	} {
		if len(field.raw) == 0 {
			continue
		}
		current, err := decode(field.raw)
		if err != nil {
			return cephFSMirrorReplay{}, err
		}
		identity := cephfsmirror.Snapshot{ID: *current.ID, Name: *current.Name}
		if replay.CurrentField != "" && replay.Current != identity {
			return cephFSMirrorReplay{}, fmt.Errorf("conflicting current snapshot identities: %s", raw)
		}
		replay.Current, replay.CurrentField = identity, field.name
	}
	return replay, nil
}
