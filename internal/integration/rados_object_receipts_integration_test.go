//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The RADOS object operations that python-rados and the rados CLI of the
// official image reach, each with a receipt: xattr listing and removal, OMAP
// listings, paging, header, removal and clear, rollback to a self-managed
// snapshot, allocation hints and fadvise flags, pool and cluster statistics
// against known data, and PG object enumeration with its cursors,
// continuation handles, namespaces and filter. A second client reads every
// result back.
func TestRADOSObjectReceipts(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(1))
	const objects, stats, enumerated, namespace = "tc-objects", "tc-objstats", "tc-enumeration", "receipts"
	for _, pool := range []struct {
		name string
		pgs  int
	}{{objects, 1}, {stats, 1}, {enumerated, 4}} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool.name, PGNum: pool.pgs, Replicas: 1, MinSize: 1, Application: "rados"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	var xattr struct {
		GetxattrRemoved        int      `json:"getxattr_removed"`
		RmxattrNeverSet        int      `json:"rmxattr_never_set"`
		RmxattrNeverSetVersion []uint64 `json:"rmxattr_never_set_version"`
		GetxattrsMissingObject int      `json:"getxattrs_missing_object"`
		RmxattrMissingObject   int      `json:"rmxattr_missing_object"`
		GetxattrMissingObject  int      `json:"getxattr_missing_object"`
		Listed                 map[string]string
		AfterCompound          map[string]string `json:"after_compound"`
	}
	receipt := radosObjectReceiptPhase(t, ctx, cluster, client, &xattr, "xattr", objects, namespace)
	// getxattr of a removed name is ENODATA; rmxattr of a name that was never
	// set is a write that succeeds; a missing object is ENOENT for each.
	if xattr.GetxattrRemoved != 61 || xattr.RmxattrNeverSet != 0 || len(xattr.RmxattrNeverSetVersion) != 2 ||
		xattr.RmxattrNeverSetVersion[1] <= xattr.RmxattrNeverSetVersion[0] ||
		xattr.GetxattrsMissingObject != 2 || xattr.RmxattrMissingObject != 2 || xattr.GetxattrMissingObject != 2 ||
		len(xattr.Listed) != 4 || xattr.Listed["empty"] != "" || xattr.AfterCompound["added"] != "ff00" {
		t.Fatalf("xattr receipt: %s", receipt)
	}
	t.Logf("RADOS_XATTR_RECEIPT %s", receipt)

	var omap struct {
		PagesOf7         []int    `json:"pages_of_7"`
		ValuePagesOf7    []int    `json:"value_pages_of_7"`
		CappedPagesOf100 []int    `json:"capped_pages_of_100"`
		AfterRemovals    []string `json:"after_removals"`
		AfterClear       int      `json:"after_clear"`
		HeaderAfterClear string   `json:"header_after_clear"`
		ByKeysMissing    int      `json:"by_keys_missing_object"`
		KeysMissing      int      `json:"keys_missing_object"`
	}
	receipt = radosObjectReceiptPhase(t, ctx, cluster, client, &omap, "omap", objects, namespace)
	// 25 keys in pages of 7, and in pages capped at 10 by the OSD although
	// the client asked for 100; each listing ends with an empty page.
	if !slices.Equal(omap.PagesOf7, []int{7, 7, 7, 4, 0}) || !slices.Equal(omap.ValuePagesOf7, omap.PagesOf7) ||
		!slices.Equal(omap.CappedPagesOf100, []int{10, 10, 5, 0}) || len(omap.AfterRemovals) != 25-2-5 ||
		slices.Contains(omap.AfterRemovals, "k03") || slices.Contains(omap.AfterRemovals, "k12") ||
		omap.AfterClear != 0 || omap.HeaderAfterClear != "" || omap.ByKeysMissing != 2 || omap.KeysMissing != 2 {
		t.Fatalf("omap receipt: %s", receipt)
	}
	t.Logf("RADOS_OMAP_RECEIPT %s", receipt)

	var rollback struct {
		Snap     uint64            `json:"snap"`
		Versions map[string]uint64 `json:"versions"`
		Clones   [][]any           `json:"clones"`
		Born     string            `json:"born_after_snapshot"`
	}
	receipt = radosObjectReceiptPhase(t, ctx, cluster, client, &rollback, "rollback", objects, namespace)
	v := rollback.Versions
	if v["snapped"] == 0 || v["snapped"] >= v["changed"] || v["changed"] >= v["rollback"] || v["rollback"] >= v["next"] ||
		len(rollback.Clones) != 2 || rollback.Born != "deleted" {
		t.Fatalf("rollback receipt: %s", receipt)
	}
	t.Logf("RADOS_ROLLBACK_RECEIPT %s", receipt)

	type hintedObject struct {
		Size       int            `json:"size"`
		ObjectInfo map[string]any `json:"object_info"`
	}
	var hints struct {
		Existing       hintedObject `json:"alloc_hint_existing"`
		Missing        hintedObject `json:"alloc_hint_missing"`
		CmpextMatch    int          `json:"cmpext_match"`
		CmpextMismatch int          `json:"cmpext_mismatch"`
		MaxErrno       int          `json:"max_errno"`
		Copied         []bool       `json:"copy_from_data_xattrs_omap"`
	}
	receipt = radosObjectReceiptPhase(t, ctx, cluster, client, &hints, "hints", objects, namespace)
	if hints.Existing.Size != 8192 || hints.Missing.Size != 0 || hints.CmpextMatch != 0 ||
		hints.CmpextMismatch != hints.MaxErrno+9 || !slices.Equal(hints.Copied, []bool{true, true, true}) {
		t.Fatalf("hints receipt: %s", receipt)
	}
	// The object info is read back where the image can decode it; the script
	// has then checked the hinted sizes.
	if reason, ok := hints.Existing.ObjectInfo["unavailable"]; ok {
		t.Logf("allocation hints are not read back from the object info: %v", reason)
	} else if hints.Existing.ObjectInfo["expected_object_size"] != float64(4194304) {
		t.Fatalf("hints receipt: %s", receipt)
	}
	t.Logf("RADOS_HINTS_RECEIPT %s", receipt)

	pool, err := poolHandle(t, ctx, cluster, stats).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type statsView struct {
		Pool   map[string]int64 `json:"pool"`
		CephDF map[string]int64 `json:"ceph_df"`
	}
	var statistics struct {
		Baseline statsView `json:"baseline"`
		Written  statsView `json:"written"`
		Removed  statsView `json:"removed"`
	}
	receipt = radosObjectReceiptPhase(t, ctx, cluster, client, &statistics, "stats", stats, namespace, strconv.Itoa(pool.Size))
	// Six objects of 4, 8, ... 24 KiB, then three of them removed: the pool's
	// object count and stored bytes follow exactly.
	for _, step := range []struct {
		view           statsView
		objects, bytes int64
	}{{statistics.Baseline, 0, 0}, {statistics.Written, 6, 4096 * 21}, {statistics.Removed, 3, 4096 * 12}} {
		if step.view.Pool["num_objects"] != step.objects || step.view.CephDF["stored"] != step.bytes {
			t.Fatalf("stats receipt: %s", receipt)
		}
	}
	t.Logf("RADOS_STATS_RECEIPT %s", receipt)

	var enumeration radosEnumerationReceipt
	receipt = radosEnumerationPhase(t, ctx, cluster, client, &enumeration, enumerated)
	if problem := enumeration.problem(); problem != "" {
		t.Fatalf("enumeration receipt: %s: %s", problem, receipt)
	}
	t.Logf("RADOS_ENUMERATION_RECEIPT %s", receipt)
}
