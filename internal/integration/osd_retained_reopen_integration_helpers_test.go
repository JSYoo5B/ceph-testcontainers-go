//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// osdAddrs are an OSD's messenger address vectors from the OSD map, each
// entry as type:addr/nonce. Public carries client and MON traffic; Cluster
// carries replication and heartbeats between OSDs.
type osdAddrs struct {
	Public, Cluster []string
}

// osdMessengerAddrs reads osd.id's address vectors from the OSD map.
func osdMessengerAddrs(t *testing.T, ctx context.Context, cluster *ceph.Container, id int) osdAddrs {
	t.Helper()
	addrs, err := parseOSDMessengerAddrs(harness.MustCeph(t, ctx, cluster, "osd", "dump", "--format", "json"), id)
	if err != nil {
		t.Fatal(err)
	}
	return addrs
}

// parseOSDMessengerAddrs reads osd.id's public and cluster address vectors
// from `osd dump --format json`. Each vector must be present and nonempty: a
// comparison made with one of them missing would say nothing about that
// vector's reuse.
func parseOSDMessengerAddrs(data []byte, id int) (osdAddrs, error) {
	type vector struct {
		Addrs []struct {
			Type  string `json:"type"`
			Addr  string `json:"addr"`
			Nonce uint64 `json:"nonce"`
		} `json:"addrvec"`
	}
	var dump struct {
		OSDs []struct {
			ID      *int    `json:"osd"`
			Public  *vector `json:"public_addrs"`
			Cluster *vector `json:"cluster_addrs"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return osdAddrs{}, fmt.Errorf("decode the OSD map: %w", err)
	}
	for _, o := range dump.OSDs {
		if o.ID == nil || *o.ID != id {
			continue
		}
		var out osdAddrs
		for name, v := range map[string]struct {
			vector *vector
			into   *[]string
		}{"public_addrs": {o.Public, &out.Public}, "cluster_addrs": {o.Cluster, &out.Cluster}} {
			if v.vector == nil || len(v.vector.Addrs) == 0 {
				return osdAddrs{}, fmt.Errorf("osd.%d has no %s in the OSD map", id, name)
			}
			for _, a := range v.vector.Addrs {
				if a.Type == "" || a.Addr == "" {
					return osdAddrs{}, fmt.Errorf("osd.%d %s entry %+v lacks a type or address", id, name, a)
				}
				*v.into = append(*v.into, fmt.Sprintf("%s:%s/%d", a.Type, a.Addr, a.Nonce))
			}
		}
		return out, nil
	}
	return osdAddrs{}, fmt.Errorf("osd.%d is not in the OSD map", id)
}

// reusedOSDMessengerAddrs returns the addresses of after, on either vector,
// that before already had on either vector.
func reusedOSDMessengerAddrs(before, after osdAddrs) []string {
	old := slices.Concat(before.Public, before.Cluster)
	var reused []string
	for _, addr := range slices.Concat(after.Public, after.Cluster) {
		if slices.Contains(old, addr) && !slices.Contains(reused, addr) {
			reused = append(reused, addr)
		}
	}
	return reused
}
