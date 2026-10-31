//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"strings"
	"testing"
)

func logicalLedger(outcomes ...string) []attachMutation {
	ledger := make([]attachMutation, len(outcomes))
	for i, outcome := range outcomes {
		ledger[i] = attachMutation{Key: "w-" + string(rune('a'+i)), Outcome: outcome, SHA256: strings.Repeat("0", 64)}
	}
	return ledger
}

var logicalAllocatorLog = []string{
	"bluestore(/var/lib/ceph/osd/ceph-0) _do_alloc_write failed to allocate 0x400000 allocated 0x 143000 min_alloc_size 0x1000 available 0x 0",
	"bluestore(/var/lib/ceph/osd/ceph-0) _do_write _do_alloc_write failed with (28) No space left on device",
	"bluestore(/var/lib/ceph/osd/ceph-0) ENOSPC from bluestore, misconfigured cluster",
}

func logicalCase(change func(*logicalPhase)) *logicalPhase {
	p := &logicalPhase{Ledger: logicalLedger(mutAcked, mutAcked, mutUnknown, mutUnknown), Checks: []string{"OSD_FULL", "POOL_FULL"},
		Running: true, FSAvail: 800 << 20, BlockSize: 256 << 20, BlockAllocated: 233 << 20}
	if change != nil {
		change(p)
	}
	return p
}

func TestJudgeLogicalPhase(t *testing.T) {
	aborted := func(p *logicalPhase) { p.Running, p.Log, p.Checks = false, logicalAllocatorLog, nil }
	for _, tc := range []struct {
		name   string
		phase  *logicalPhase
		want   string
		failed bool
	}{
		{"held by the full ratio", logicalCase(nil), logicalPolicyBlocked, false},
		{"allocator abort", logicalCase(aborted), logicalAllocatorAborted, false},
		{"allocator abort while the map was already full", logicalCase(func(p *logicalPhase) { aborted(p); p.Checks = []string{"OSD_FULL"} }), logicalAllocatorAborted, false},
		{"everything acknowledged", logicalCase(func(p *logicalPhase) { p.Ledger = logicalLedger(mutAcked, mutAcked) }), logicalUnreached, false},
		{"filesystem nearly full", logicalCase(func(p *logicalPhase) { aborted(p); p.FSAvail = 8 << 20 }), logicalFilesystemFirst, false},
		{"stopped with no evidence", logicalCase(func(p *logicalPhase) { p.Checks = []string{"POOL_NO_REDUNDANCY"} }), logicalOther, false},
		// A partial log is not an allocator refusal.
		{"one marker only", logicalCase(func(p *logicalPhase) { aborted(p); p.Log = logicalAllocatorLog[:1] }), "", true},
		{"allocator refused, OSD running", logicalCase(func(p *logicalPhase) { p.Log = logicalAllocatorLog }), "", true},
		{"exit without allocator markers", logicalCase(func(p *logicalPhase) { p.Running = false }), "", true},
		{"acknowledged after the block", logicalCase(func(p *logicalPhase) { p.Ledger = logicalLedger(mutAcked, mutUnknown, mutAcked) }), "", true},
		{"refused outright under the ratio", logicalCase(func(p *logicalPhase) { p.Ledger = logicalLedger(mutAcked, mutFailed) }), "", true},
		{"block allocated beyond its length", logicalCase(func(p *logicalPhase) { p.BlockAllocated = p.BlockSize + 1 }), "", true},
		{"nothing written", logicalCase(func(p *logicalPhase) { p.Ledger = nil }), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeLogicalPhase(tc.phase)
			if tc.failed != (err != nil) || got != tc.want {
				t.Fatalf("judgeLogicalPhase = %q, %v; want %q, error %t", got, err, tc.want, tc.failed)
			}
		})
	}
	if _, err := judgeLogicalPhase(nil); err == nil {
		t.Fatal("a nil phase was judged")
	}
}

func TestParseHealthChecks(t *testing.T) {
	got, err := parseHealthChecks([]byte(`{"status":"HEALTH_ERR","checks":{"POOL_FULL":{},"OSD_FULL":{}},"mutes":[]}` + "\n"))
	if err != nil || strings.Join(got, ",") != "OSD_FULL,POOL_FULL" {
		t.Fatalf("parseHealthChecks = %v, %v", got, err)
	}
	for _, bad := range []string{`{"status":"HEALTH_OK"}`, `{"checks":{}}`, `{"status":"HEALTH_OK","checks":{}}}`, `{"status":"HEALTH_OK","checks":{},"other":1}`} {
		if _, err := parseHealthChecks([]byte(bad)); err == nil {
			t.Errorf("parseHealthChecks accepted %s", bad)
		}
	}
}

func TestJudgeDBCapacity(t *testing.T) {
	const db, wal = int64(96 << 20), int64(64 << 20)
	before := bluefsUsage{DBTotal: db, DBUsed: 30 << 20, WALTotal: wal, WALUsed: 10 << 20}
	spilled := bluefsUsage{DBTotal: db, DBUsed: 86 << 20, WALTotal: wal, WALUsed: 40 << 20, SlowUsed: 88 << 20}
	spill := []string{"BLUEFS_SPILLOVER"}
	for _, tc := range []struct {
		name          string
		before, after bluefsUsage
		checks        []string
		acked         int
		running       bool
		want          string
		failed        bool
	}{
		{"spillover", before, spilled, spill, 10, true, dbSpilled, false},
		{"room left", before, bluefsUsage{DBTotal: db, DBUsed: 50 << 20, WALTotal: wal}, nil, 10, true, dbNotFilled, false},
		{"slow use with room on the DB", before, bluefsUsage{DBTotal: db, DBUsed: 40 << 20, WALTotal: wal, SlowUsed: 8 << 20}, nil, 10, true, dbUnexplained, false},
		{"full DB without the check", before, spilled, nil, 10, true, "", true},
		{"check without slow use", before, bluefsUsage{DBTotal: db, DBUsed: 90 << 20, WALTotal: wal}, spill, 10, true, "", true},
		{"no separate DB device", bluefsUsage{WALTotal: wal}, bluefsUsage{WALTotal: wal, SlowUsed: 88 << 20}, spill, 10, true, "", true},
		{"a write was lost", before, spilled, spill, 9, true, "", true},
		{"the OSD stopped", before, spilled, spill, 10, false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeDBCapacity(db, wal, tc.before, tc.after, tc.checks, tc.acked, 10, tc.running)
			if tc.failed != (err != nil) || got != tc.want {
				t.Fatalf("judgeDBCapacity = %q, %v; want %q, error %t", got, err, tc.want, tc.failed)
			}
		})
	}
	u, err := parseBlueFSUsage([]byte(`{"bluefs":{"db_total_bytes":1,"db_used_bytes":2,"wal_total_bytes":3,"wal_used_bytes":4,"slow_used_bytes":5,"other":6}}`))
	if err != nil || u != (bluefsUsage{1, 2, 3, 4, 5}) {
		t.Fatalf("parseBlueFSUsage = %+v, %v", u, err)
	}
	if _, err := parseBlueFSUsage([]byte(`{"bluefs":{"db_total_bytes":1,"db_used_bytes":2,"wal_total_bytes":3,"wal_used_bytes":4}}`)); err == nil {
		t.Fatal("a dump without slow_used_bytes was accepted")
	}
}

func TestMonStoreDomain(t *testing.T) {
	entry := func(errno, dev int, id, fstype string) string {
		return fmt.Sprintf(`{"errno":%d,"symlink":false,"regular":false,"ino":2,"dev":%d,"size":4096,"allocated":4096,"mount":{"id":%q,"mountpoint":"/","fstype":%q}}`, errno, dev, id, fstype)
	}
	for _, tc := range []struct {
		name, store, dir string
		want             string
		failed           bool
	}{
		{"overlay root", entry(0, 67, "0:67", "overlay"), entry(0, 67, "0:67", "overlay"), monStoreEngineDisk, false},
		{"a mounted volume", entry(0, 80, "0:80", "ext4"), entry(0, 80, "0:80", "ext4"), monStoreOther, false},
		{"store.db elsewhere", entry(0, 80, "0:80", "tmpfs"), entry(0, 67, "0:67", "overlay"), "", true},
		{"store.db missing", entry(2, 0, "0:0", ""), entry(0, 67, "0:67", "overlay"), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := parseBlockBacking([]byte(`{"version":1,"block":` + tc.store + `,"scratch":` + tc.dir + `}`))
			if err != nil {
				t.Fatal(err)
			}
			got, err := monStoreDomain(r)
			if tc.failed != (err != nil) || got != tc.want {
				t.Fatalf("monStoreDomain = %q, %v; want %q, error %t", got, err, tc.want, tc.failed)
			}
		})
	}
}
