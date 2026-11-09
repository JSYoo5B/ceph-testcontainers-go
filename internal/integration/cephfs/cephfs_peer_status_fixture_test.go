//go:build all || (integration && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"encoding/json"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The oracles read both native shapes of `fs mirror peer status`: the flat
// object of 20.2.4 and the nested one of 21.2.0, both as printed after one
// synced snapshot of /federation.
func TestCephFSPeerStatusDirectoriesReadsTheNativeShapes(t *testing.T) {
	const peer = "eabf8839-65e7-4ca6-8aac-44e6f99aba63"
	for name, data := range map[string]string{
		"tentacle": `{"/federation":{"state":"idle","last_synced_snap":{"id":2,"name":"backup-1","sync_duration":0,"sync_time_stamp":"1928633.881435s","sync_bytes":131165},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}`,
		"umbrella": `{"metrics":{"/federation":{"peer":{"` + peer + `":{"state":"idle","last_synced_snap":{"id":2,"name":"backup-1","crawl_duration":"0s","datasync_queue_wait_duration":"0s","sync_duration":"0s","sync_time_stamp":"2026-11-09T00:45:35.026520+0000","sync_bytes":"128.09 KiB","sync_files":6},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}}}}`,
	} {
		entries, err := harness.CephFSPeerStatusDirectories([]byte(data), peer)
		if err != nil || len(entries) != 1 {
			t.Fatalf("%s: %v %v", name, entries, err)
		}
		var entry struct {
			State string `json:"state"`
			Last  struct {
				ID    uint64          `json:"id"`
				Bytes json.RawMessage `json:"sync_bytes"`
			} `json:"last_synced_snap"`
		}
		if err := json.Unmarshal(entries["/federation"], &entry); err != nil || entry.State != "idle" || entry.Last.ID != 2 {
			t.Fatalf("%s: %+v %v", name, entry, err)
		}
		// 131165 bytes, which 21.2.0 prints as 128.09 KiB.
		if ok, err := harness.CephFSSyncBytesContains(entry.Last.Bytes, 131165); !ok || err != nil {
			t.Fatalf("%s: sync_bytes %s does not hold 131165: %v", name, entry.Last.Bytes, err)
		}
		if ok, _ := harness.CephFSSyncBytesContains(entry.Last.Bytes, 131200); ok {
			t.Fatalf("%s: sync_bytes %s holds 131200", name, entry.Last.Bytes)
		}
	}
	for _, data := range []string{`{"metrics":{"/f":{"peer":{"other":{}}}}}`, `{"metrics":{},"/f":{}}`, `{"metrics":[]}`, `null`} {
		if entries, err := harness.CephFSPeerStatusDirectories([]byte(data), peer); err == nil {
			t.Errorf("%s: accepted as %v", data, entries)
		}
	}
	for _, raw := range []string{`"50 KiB"`, `51200`} {
		if ok, err := harness.CephFSSyncBytesContains(json.RawMessage(raw), 50*1024); !ok || err != nil {
			t.Errorf("%s does not hold 51200: %v", raw, err)
		}
	}
	for _, raw := range []string{`"50"`, `"50 kB"`, `true`, `"-1 B"`} {
		if _, err := harness.CephFSSyncBytesContains(json.RawMessage(raw), 1); err == nil {
			t.Errorf("%s accepted as a byte count", raw)
		}
	}
}
