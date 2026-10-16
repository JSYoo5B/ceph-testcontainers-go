//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// rgwRadosReceipt is what TestRGWRadosReceipt logs.
type rgwRadosReceipt struct {
	DataPool  string         `json:"data_pool"`
	IndexPool string         `json:"index_pool"`
	Bucket    rgwBucketStats `json:"bucket"`
	SmallHead string         `json:"small_head"`
	SmallSHA  string         `json:"small_sha256"`
	IndexOID  string         `json:"index_oid"`
	// LargeIndexOID is the shard that held the large key before DELETE;
	// LargeKeyAfter lists the shards that still held it afterwards.
	LargeIndexOID string   `json:"large_index_oid"`
	LargeKeyAfter []string `json:"large_key_after_delete"`
	LargeHead     string   `json:"large_head"`
	LargeTails    []string `json:"large_tails"`
	LargeSizes    []int64  `json:"large_sizes"`
	GCAfterDel    []string `json:"gc_after_delete"`
}

// TestRGWRadosReceipt follows S3 requests to the RADOS objects that the
// gateway writes for them, read with the rados and radosgw-admin tools rather
// than through the gateway. A small object is one head object named
// "<marker>_<key>" in the data pool with the same bytes, and its key is in an
// omap of the bucket's index object. A 6 MiB object is a head object and tail
// stripes ("<marker>__shadow_.<prefix>_<n>") whose sizes add up to the
// object; it is the bucket's only object with tails.
// Its key is located in exactly one index shard before DELETE; deleting it
// removes the head and that key at once and leaves the tails to the garbage
// collector, which lists them until "gc process" removes them.
func TestRGWRadosReceipt(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	cluster, err := rgw.Run(ctx, image, append(roles,
		ceph.WithMonitorCount(1), ceph.WithManagerCount(1), ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1))...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	gateway := cluster.Gateways()[0]
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := s3HTTPClient{endpoint: endpoint, accessKey: gateway.AccessKey, secretKey: gateway.SecretKey, region: gateway.Region,
		http: &http.Client{Timeout: 60 * time.Second}}
	control := cluster.Container.Container
	rados := func(args ...string) string {
		t.Helper()
		return execOutput(t, ctx, control, append([]string{"rados"}, args...)...)
	}
	lines := func(out string) []string {
		var result []string
		for _, line := range strings.Split(out, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				result = append(result, line)
			}
		}
		return result
	}
	var receipt rgwRadosReceipt
	// indexShards reads every index object's omap keys, so a key's shard is
	// located, not assumed.
	indexShards := func() map[string][]string {
		t.Helper()
		shards := map[string][]string{}
		for _, oid := range lines(rados("-p", receipt.IndexPool, "ls")) {
			if strings.HasPrefix(oid, ".dir."+receipt.Bucket.ID+".") {
				shards[oid] = lines(rados("-p", receipt.IndexPool, "listomapkeys", oid))
			}
		}
		return shards
	}

	client.request(t, ctx, http.MethodPut, "/tc-rados", nil, http.StatusOK)
	small := make([]byte, 1031)
	for i := range small {
		small[i] = byte(i*13 + 7)
	}
	client.request(t, ctx, http.MethodPut, "/tc-rados/small", small, http.StatusOK)
	pools, err := cluster.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range pools {
		switch {
		case strings.HasSuffix(pool.Name, ".rgw.buckets.data"):
			receipt.DataPool = pool.Name
		case strings.HasSuffix(pool.Name, ".rgw.buckets.index"):
			receipt.IndexPool = pool.Name
		}
	}
	if receipt.DataPool == "" || receipt.IndexPool == "" {
		t.Fatalf("no bucket data or index pool: %+v", pools)
	}
	out, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "tc-rados")
	if err != nil {
		t.Fatal("bucket stats:", err)
	}
	if receipt.Bucket, err = parseRGWBucketStats(out); err != nil || receipt.Bucket.Bucket != "tc-rados" {
		t.Fatalf("bucket stats: %+v %v: %.512s", receipt.Bucket, err, out)
	}
	marker := receipt.Bucket.Marker

	// The small object is one head object with the same bytes.
	head, tails := rgwObjectParts(lines(rados("-p", receipt.DataPool, "ls")), marker, "small")
	if head == "" || len(tails) != 0 {
		t.Fatalf("small object: head %q, tails %v", head, tails)
	}
	receipt.SmallHead = head
	sum := strings.Fields(execOutput(t, ctx, control, "sh", "-c", `rados -p "$1" get "$2" /tmp/rgw-head && sha256sum /tmp/rgw-head && rm -f /tmp/rgw-head`, "sh", receipt.DataPool, head))
	want := sha256.Sum256(small)
	if len(sum) == 0 || sum[0] != hex.EncodeToString(want[:]) {
		t.Fatalf("head object %s has sha256 %v, want %x", head, sum, want)
	}
	receipt.SmallSHA = sum[0]
	// The key is in the omap of exactly one of the bucket's index shards.
	if receipt.IndexOID, err = rgwIndexShardOf(indexShards(), receipt.Bucket.ID, "small"); err != nil {
		t.Fatal(err)
	}

	// A 6 MiB object is a head object and tail stripes.
	large := bytes.Repeat([]byte("0123456789abcdef"), 6<<20/16)
	client.request(t, ctx, http.MethodPut, "/tc-rados/large", large, http.StatusOK)
	listing := lines(rados("-p", receipt.DataPool, "ls"))
	head, tails = rgwObjectParts(listing, marker, "large")
	if head == "" || len(tails) == 0 {
		t.Fatalf("large object: head %q, tails %v in %v", head, tails, listing)
	}
	receipt.LargeHead, receipt.LargeTails = head, tails
	var total int64
	for _, oid := range append([]string{head}, tails...) {
		size, err := parseRADOSStatSize(rados("-p", receipt.DataPool, "stat", oid))
		if err != nil {
			t.Fatal(err)
		}
		receipt.LargeSizes = append(receipt.LargeSizes, size)
		total += size
	}
	if total != int64(len(large)) {
		t.Fatalf("head and tails of the large object hold %d bytes, want %d: %v %v", total, len(large), append([]string{head}, tails...), receipt.LargeSizes)
	}
	if read := client.request(t, ctx, http.MethodGet, "/tc-rados/large", nil, http.StatusOK); !bytes.Equal(read, large) {
		t.Fatal("the large object reads back different bytes")
	}

	// Its key is located before DELETE, so the deletion below is a change
	// from present to absent, not a check of a key that was never there.
	if receipt.LargeIndexOID, err = rgwIndexShardOf(indexShards(), receipt.Bucket.ID, "large"); err != nil {
		t.Fatal(err)
	}

	// Deleting it removes the head and the key at once; the tails wait for
	// the garbage collector.
	client.request(t, ctx, http.MethodDelete, "/tc-rados/large", nil, http.StatusNoContent)
	head, remaining := rgwObjectParts(lines(rados("-p", receipt.DataPool, "ls")), marker, "large")
	if head != "" {
		t.Fatalf("head %s outlived the delete", head)
	}
	after := indexShards()
	if _, present := after[receipt.LargeIndexOID]; !present {
		t.Fatalf("index shard %s that held the key is gone", receipt.LargeIndexOID)
	}
	receipt.LargeKeyAfter = []string{}
	for oid, keys := range after {
		if slices.Contains(keys, "large") {
			receipt.LargeKeyAfter = append(receipt.LargeKeyAfter, oid)
		}
	}
	if len(receipt.LargeKeyAfter) != 0 || !slices.Contains(after[receipt.IndexOID], "small") {
		t.Fatalf("after DELETE the key large is in %v (it was in %s), and small in %s holds %v",
			receipt.LargeKeyAfter, receipt.LargeIndexOID, receipt.IndexOID, after[receipt.IndexOID])
	}
	out, err = gateway.Admin(ctx, "gc", "list", "--include-all")
	if err != nil {
		t.Fatal("gc list:", err)
	}
	if receipt.GCAfterDel, err = rgwGCOIDs(out); err != nil {
		t.Fatalf("gc list: %v: %.1024s", err, out)
	}
	for _, tail := range remaining {
		if !slices.Contains(receipt.GCAfterDel, tail) {
			t.Fatalf("tail %s is neither removed nor queued for gc: %v", tail, receipt.GCAfterDel)
		}
	}
	if len(remaining) == 0 {
		t.Fatalf("the tails %v were removed at once, not left to gc", tails)
	}
	if _, err := gateway.Admin(ctx, "gc", "process", "--include-all"); err != nil {
		t.Fatal("gc process:", err)
	}
	if _, left := rgwObjectParts(lines(rados("-p", receipt.DataPool, "ls")), marker, "large"); len(left) != 0 {
		t.Fatalf("tails %v outlived gc process", left)
	}
	if out, err := gateway.Admin(ctx, "gc", "list", "--include-all"); err != nil {
		t.Fatal(err)
	} else if oids, err := rgwGCOIDs(out); err != nil || len(oids) != 0 {
		t.Fatalf("gc list after processing: %v %v", oids, err)
	}
	if read := client.request(t, ctx, http.MethodGet, "/tc-rados/small", nil, http.StatusOK); !bytes.Equal(read, small) {
		t.Fatal("the small object changed")
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_RADOS_RECEIPT %s", encoded)
}
