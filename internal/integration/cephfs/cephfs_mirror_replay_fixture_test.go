//go:build all || (integration && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"fmt"
	"testing"

	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
)

func TestCephFSMirrorPathReplaySnapshotIdentity(t *testing.T) {
	const last = `"state":"syncing","last_synced_snap":{"id":1,"name":"backup-1","sync_bytes":51},"snaps_synced":1`
	for _, field := range []string{"current_syncing_snap", "current_sycning_snap"} {
		t.Run(field, func(t *testing.T) {
			for _, identity := range []string{`null`, `{}`, `{"id":2}`, `{"name":"backup-2"}`, `{"id":0,"name":"backup-2"}`, `{"id":2,"name":""}`, `{"id":"2","name":"backup-2"}`} {
				if _, err := parseCephFSMirrorPeerReplay([]byte(fmt.Sprintf(`{"/federation":{%s,%q:%s}}`, last, field, identity)), "peer"); err == nil {
					t.Fatalf("accepted malformed current identity %s", identity)
				}
			}
			replay, err := parseCephFSMirrorPeerReplay([]byte(fmt.Sprintf(`{"/federation":{%s,%q:{"id":2,"name":"backup-2"}}}`, last, field)), "peer")
			if err != nil || replay.Current != (cephfsmirror.Snapshot{ID: 2, Name: "backup-2"}) || replay.CurrentField != field || replay.Last != (cephfsmirror.Snapshot{ID: 1, Name: "backup-1"}) {
				t.Fatalf("native spelling lost identity: %+v %v", replay, err)
			}
		})
	}
	if _, err := parseCephFSMirrorPeerReplay([]byte(fmt.Sprintf(`{"/federation":{%s,"current_syncing_snap":{"id":2,"name":"backup-2"},"current_sycning_snap":{"id":3,"name":"different"}}}`, last)), "peer"); err == nil {
		t.Fatal("accepted conflicting native spellings")
	}
	replay, err := parseCephFSMirrorPeerReplay([]byte(fmt.Sprintf(`{"/federation":{%s}}`, last)), "peer")
	if err != nil || replay.CurrentField != "" || replay.Current != (cephfsmirror.Snapshot{}) {
		t.Fatalf("missing identity was fabricated: %+v %v", replay, err)
	}
}
