//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// "get subtrees" of both ranks saved from TestCephFSLiveRankMigration on
// quay.io/ceph/ceph:v20.2.4, rank 0 (mds.a) first. Only the fields that
// MDSSubtree reads are kept; the values are native.
const (
	// /stream pinned to rank 0.
	subtreesPinnedZeroRank0 = `[{"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds0"}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": 0, "dir": {"path": "/stream"}}]`
	subtreesPinnedZeroRank1 = `[{"is_auth": true, "auth_first": 1, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds1"}}, {"is_auth": false, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}]`
	// The pin is 1, and rank 0 still has the subtree.
	subtreesPendingRank0 = `[{"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds0"}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": 1, "dir": {"path": "/stream"}}]`
	// Rank 1 has the subtree.
	subtreesMovedRank0 = `[{"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds0"}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}, {"is_auth": false, "auth_first": 1, "auth_second": -2, "export_pin": 1, "dir": {"path": "/stream"}}]`
	subtreesMovedRank1 = `[{"is_auth": true, "auth_first": 1, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds1"}}, {"is_auth": false, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}, {"is_auth": true, "auth_first": 1, "auth_second": -2, "export_pin": 1, "dir": {"path": "/stream"}}]`
	// With the pin back at 0, rank 0 first merged /stream into the root
	// subtree, so neither rank listed it, and split it out again later.
	subtreesMergedRank0 = `[{"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": "~mds0"}}, {"is_auth": true, "auth_first": 0, "auth_second": -2, "export_pin": -1, "dir": {"path": ""}}]`
)

func parsedSubtrees(t *testing.T, data string) []harness.MDSSubtree {
	t.Helper()
	subtrees, err := harness.ParseMDSSubtrees([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return subtrees
}

func TestParseMDSSubtrees(t *testing.T) {
	listed := parsedSubtrees(t, subtreesMovedRank1)
	wrapped := parsedSubtrees(t, `{"subtrees": `+subtreesMovedRank1+`}`)
	if len(listed) != 3 || len(wrapped) != 3 || listed[2] != wrapped[2] {
		t.Fatalf("listed %+v, wrapped %+v", listed, wrapped)
	}
	if got := listed[2]; got.Dir.Path != "/stream" || !got.IsAuth || got.AuthFirst != 1 || got.AuthSecond != -2 || got.ExportPin != 1 {
		t.Fatalf("subtree = %+v", got)
	}
	if _, err := harness.ParseMDSSubtrees([]byte(`{"subtrees": 1}`)); err == nil {
		t.Fatal("a malformed list was accepted")
	}
}

func TestSoleSubtreeAuthority(t *testing.T) {
	// ambiguous is rank 1's moved view with a second authority, as during an
	// import; both pairs rank 0's pending view, still authoritative, with
	// rank 1's moved one. Both are constructed from the native views.
	ambiguous := strings.Replace(subtreesMovedRank1, `"auth_first": 1, "auth_second": -2, "export_pin": 1`, `"auth_first": 1, "auth_second": 0, "export_pin": 1`, 1)
	for name, test := range map[string]struct {
		rank0, rank1 string
		rank         int
		want         string
	}{
		"pinned to rank 0":           {subtreesPinnedZeroRank0, subtreesPinnedZeroRank1, 0, ""},
		"pinned to rank 0, not 1":    {subtreesPinnedZeroRank0, subtreesPinnedZeroRank1, 1, "claimed by [rank0(auth=0,-2 export_pin=0)]"},
		"pin 1 before the export":    {subtreesPendingRank0, subtreesPinnedZeroRank1, 1, "claimed by [rank0(auth=0,-2 export_pin=1)]"},
		"pin 1 is not a rank 0 pin":  {subtreesPendingRank0, subtreesPinnedZeroRank1, 0, "claimed by [rank0(auth=0,-2 export_pin=1)]"},
		"moved to rank 1":            {subtreesMovedRank0, subtreesMovedRank1, 1, ""},
		"merged into the root":       {subtreesMergedRank0, subtreesPinnedZeroRank1, 0, "claimed by [], want rank0 alone"},
		"claimed by both ranks":      {subtreesPendingRank0, subtreesMovedRank1, 1, "claimed by [rank0(auth=0,-2 export_pin=1) rank1(auth=1,-2 export_pin=1)]"},
		"ambiguous during an import": {subtreesMovedRank0, ambiguous, 1, "claimed by [rank1(auth=1,0 export_pin=1)]"},
	} {
		views := map[int][]harness.MDSSubtree{0: parsedSubtrees(t, test.rank0), 1: parsedSubtrees(t, test.rank1)}
		err := harness.SoleSubtreeAuthority(views, "/stream", test.rank)
		if test.want == "" {
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: error %v, want %q", name, err, test.want)
		}
	}
}

func TestCheckStreamProgress(t *testing.T) {
	baseline := harness.CephFSActorReceipt{Token: "run", Phase: 1, PID: 53, Client: 4271, Inode: 1099511627777}
	last := harness.CephFSStreamProgress{Token: "run", PID: 53, Client: 4271, Inode: 1099511627777, Count: 7}
	for name, test := range map[string]struct {
		edit func(*harness.CephFSStreamProgress)
		want string
	}{
		"more records":    {func(p *harness.CephFSStreamProgress) { p.Count = 73 }, ""},
		"same count":      {func(*harness.CephFSStreamProgress) {}, ""},
		"fewer records":   {func(p *harness.CephFSStreamProgress) { p.Count = 6 }, "went back from 7 to 6"},
		"another run":     {func(p *harness.CephFSStreamProgress) { p.Token = "other" }, "does not belong"},
		"another process": {func(p *harness.CephFSStreamProgress) { p.PID = 54 }, "does not belong"},
		"another client":  {func(p *harness.CephFSStreamProgress) { p.Client = 4272 }, "does not belong"},
		"another inode":   {func(p *harness.CephFSStreamProgress) { p.Inode++ }, "does not belong"},
	} {
		next := last
		test.edit(&next)
		err := harness.CheckStreamProgress("run", baseline, last, next)
		if test.want == "" {
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: error %v, want %q", name, err, test.want)
		}
	}
}

func TestStreamRecords(t *testing.T) {
	if got := hex.EncodeToString(harness.StreamRecords([]byte{0xab}, 2)); got != "ab"+"0000000000000001"+"fffffffffffffffe"+"0000000000000002"+"fffffffffffffffd" {
		t.Fatalf("records = %s", got)
	}
	// The SHA-256 of the 266 base bytes and 93 records that the actor wrote
	// and read back in the native run above.
	base := make([]byte, 0, 266)
	for value := range 256 {
		base = append(base, byte(value))
	}
	base = append(base, 0x00, 0xff, 0x7f, 0x80, '\r', '\n', 0x1b, 0x00, 'c', 'f')
	records := harness.StreamRecords(base, 93)
	if sum := sha256.Sum256(records); len(records) != 266+16*93 || hex.EncodeToString(sum[:]) != "d89d3052cecce6094b7b3abb510b57908c7a733c5e1ed31a349d7f76567991be" {
		t.Fatalf("%d bytes with sha256 %x", len(records), sum)
	}
}
