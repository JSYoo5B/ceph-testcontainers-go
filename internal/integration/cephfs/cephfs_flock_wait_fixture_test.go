//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// flockOps parses a dump_ops_in_flight holding one setfilelock of client 4321
// with tid 7 and the given rule, type, wait flag, inode and flag point.
func flockOps(t *testing.T, rule, kind, wait int, inode uint64, flag string) []harness.MDSRequestOp {
	t.Helper()
	raw := fmt.Sprintf(`{"ops":[{"description":"client_request(client.4321:7 setfilelock rule %d, type %d, owner 1, pid 9, start 0, length 0, wait %d #0x%x 2026-11-01T00:00:00.000000+0000 caller_uid=0, caller_gid=0{})","duration":0.01,
"type_data":{"flag_point":%q,"reqid":{"entity":{"type":"client","num":4321},"tid":7},"events":[{"event":"initiated"},{"event":%q}]}}]}`, rule, kind, wait, inode, flag, flag)
	ops, err := harness.ParseMDSRequestOps([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func TestFlockWaitRequiresTheWaitingStage(t *testing.T) {
	w := flockWait{client: 4321, rule: 2, inode: 0x10000000001}
	// The sampled stage of the failure in issue 378 is not waiting yet; the
	// next sample is.
	sequence := [][]harness.MDSRequestOp{
		nil,
		flockOps(t, 2, 2, 1, w.inode, "acquired locks"),
		flockOps(t, 2, 2, 1, w.inode, flockWaitingStage),
	}
	var ready int
	for i, ops := range sequence {
		if _, _, waiting := w.find(ops); waiting {
			ready = i
			break
		}
	}
	if ready != 2 {
		t.Fatalf("the poll became ready at sample %d, want 2 (the waiting stage)", ready)
	}
	seen, found, waiting := w.find(sequence[1])
	if !found || waiting || seen.flagPoint != "acquired locks" || seen.tid != 7 {
		t.Fatalf("the earlier stage: %+v found=%t waiting=%t", seen, found, waiting)
	}
}

func TestFlockWaitNeverAdmitsAnotherRequest(t *testing.T) {
	w := flockWait{client: 4321, rule: 2, inode: 0x10000000001}
	for name, ops := range map[string][]harness.MDSRequestOp{
		"fcntl rule":   flockOps(t, 1, 2, 1, w.inode, flockWaitingStage),
		"shared lock":  flockOps(t, 2, 1, 1, w.inode, flockWaitingStage),
		"non-blocking": flockOps(t, 2, 2, 0, w.inode, flockWaitingStage),
		"other inode":  flockOps(t, 2, 2, 1, w.inode+1, flockWaitingStage),
	} {
		seen, found, waiting := w.find(ops)
		if found || waiting {
			t.Errorf("%s satisfied the wait: %+v", name, seen)
		}
		if seen.tid != 7 {
			t.Errorf("%s: the mismatching request is not kept for the timeout report: %+v", name, seen)
		}
	}
	other := flockWait{client: 1234, rule: 2, inode: w.inode}
	if _, found, waiting := other.find(flockOps(t, 2, 2, 1, w.inode, flockWaitingStage)); found || waiting {
		t.Error("another client's request satisfied the wait")
	}
	// A request that stays at an earlier stage never becomes ready.
	for range 3 {
		if _, _, waiting := w.find(flockOps(t, 2, 2, 1, w.inode, "acquired locks")); waiting {
			t.Fatal("a request that never reached the waiting stage was ready")
		}
	}
}
