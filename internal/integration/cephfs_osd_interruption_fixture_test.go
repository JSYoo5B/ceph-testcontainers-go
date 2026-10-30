//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// objecter_requests dumps saved from TestCephFSOSDInterruption on
// quay.io/ceph/ceph:v20.2.4 while osd.0 was paused. Only the fields that the
// parser reads, and the pool-less ops lists, are kept; the values are native.
const (
	// The writer's fsync: the tail's write to the file's first object in the
	// data pool (3).
	clientObjecterSample = `{"ops": [{"tid": 4, "pg": "3.f0b56f30", "osd": 0, "object_id": "10000000000.00000000", "object_locator": "@3", "age": 5.964090412, "attempts": 1, "osd_ops": ["write 28~32 in=32b"]}], "linger_ops": [], "pool_ops": [], "pool_stat_ops": [], "statfs_ops": [], "command_ops": []}`
	// The MDS: a journal write and its open file table in the metadata pool
	// (2).
	mdsObjecterSample = `{"ops": [{"tid": 47, "pg": "2.6e5f474", "osd": 0, "object_id": "200.00000001", "object_locator": "@2", "age": 6.0800921719999996, "attempts": 1, "osd_ops": ["write 7254~1726 [fadvise_dontneed] in=1726b"]}, {"tid": 48, "pg": "2.4b2c82a6", "osd": 0, "object_id": "mds0_openfiles.0", "object_locator": "@2", "age": 5.7260868049999996, "attempts": 1, "osd_ops": ["omap-set-header in=42b", "omap-set-vals in=65b"]}], "linger_ops": [], "pool_ops": [], "pool_stat_ops": [], "statfs_ops": [], "command_ops": []}`
)

func TestParseObjecterOps(t *testing.T) {
	client, err := harness.ParseObjecterOps([]byte(clientObjecterSample))
	if err != nil {
		t.Fatal(err)
	}
	writes := harness.ObjecterOpsOn(client, 3, "10000000000.00000000")
	if len(writes) != 1 || writes[0].TID != 4 || writes[0].OSD != 0 || writes[0].Age < 5 || writes[0].OSDOps[0] != "write 28~32 in=32b" {
		t.Fatalf("client writes %+v", writes)
	}
	if other := harness.ObjecterOpsOn(client, 2, ""); len(other) != 0 {
		t.Fatalf("client ops in pool 2: %+v", other)
	}
	mds, err := harness.ParseObjecterOps([]byte(mdsObjecterSample))
	if err != nil {
		t.Fatal(err)
	}
	if journal := harness.ObjecterOpsOn(mds, 2, "200."); len(journal) != 1 || journal[0].Object != "200.00000001" {
		t.Fatalf("journal ops %+v", journal)
	}
	for _, locator := range []struct {
		locator string
		pool    int64
		ok      bool
	}{{"@3", 3, true}, {"@12;ns", 12, true}, {"@4:key", 4, true}, {"3", 0, false}, {"@x", 0, false}} {
		if pool, ok := (harness.ObjecterOp{Locator: locator.locator}).Pool(); pool != locator.pool || ok != locator.ok {
			t.Errorf("pool of %q = %d, %t", locator.locator, pool, ok)
		}
	}
	for _, bad := range []string{`{}`, `{"ops": null}`, `[]`} {
		if _, err := harness.ParseObjecterOps([]byte(bad)); err == nil {
			t.Errorf("parseObjecterOps(%s) accepted it", bad)
		}
	}
}
