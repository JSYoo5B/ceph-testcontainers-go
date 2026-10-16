//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"
)

// Output from TestRGWRadosReceipt runs on quay.io/ceph/ceph:v20.2.4, with
// the bucket stats reduced to the fields that the parser reads; v19.2.5
// printed the same shapes. The garbage collector's tag ends in a NUL.
const (
	rgwBucketStatsSample = `{"bucket":"tc-rados","tenant":"","id":"926dc47e-de0d-4e0f-a4b2-66e27a54ddb5.4151.1","marker":"926dc47e-de0d-4e0f-a4b2-66e27a54ddb5.4151.1","num_shards":11}`
	rgwMarkerSample      = "926dc47e-de0d-4e0f-a4b2-66e27a54ddb5.4151.1"
	rgwTailSample        = rgwMarkerSample + "__shadow_.Gadxc-hjiw6gxOr7MqD7HumIxynL-Lm_1"
	rgwGCListSample      = `[{"tag":"926dc47e-de0d-4e0f-a4b2-66e27a54ddb5.4151.11550822698481696624\u0000","time":"2026-10-16T20:21:40.188967+0000","objs":[{"pool":"default.rgw.buckets.data","oid":"` + rgwTailSample + `","key":"","instance":""}]}]`
	radosStatSample      = "default.rgw.buckets.data/b0cece1e-00d9-4f92-b31a-31766fc02ec1.4175.1__shadow_.zli2yx9r9efjQrM6g7EqnJdIZWyIqFZ_1 mtime 2026-10-16T18:21:10.000000+0000, size 2097152"
)

func TestRGWRadosParsers(t *testing.T) {
	stats, err := parseRGWBucketStats([]byte(rgwBucketStatsSample))
	if err != nil || stats.Bucket != "tc-rados" || stats.Marker != rgwMarkerSample || stats.ID != rgwMarkerSample || stats.NumShards != 11 {
		t.Fatalf("bucket stats: %+v %v", stats, err)
	}
	for _, malformed := range []string{`{}`, `{"bucket":"b","id":"i"}`, `[]`} {
		if _, err := parseRGWBucketStats([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}

	listing := []string{rgwMarkerSample + "_large", rgwMarkerSample + "_small", rgwTailSample,
		"other.1_large", "other.1__shadow_.x_1", rgwMarkerSample + "_large.extra"}
	head, tails := rgwObjectParts(listing, rgwMarkerSample, "large")
	if head != rgwMarkerSample+"_large" || !slices.Equal(tails, []string{rgwTailSample}) {
		t.Fatalf("large: head %q tails %v", head, tails)
	}
	if head, _ := rgwObjectParts(listing, rgwMarkerSample, "missing"); head != "" {
		t.Errorf("missing key found %q", head)
	}

	if size, err := parseRADOSStatSize(radosStatSample); err != nil || size != 2097152 {
		t.Fatalf("rados stat: %d %v", size, err)
	}
	for _, malformed := range []string{"", "pool/oid mtime x", "pool/oid mtime x, size big"} {
		if size, err := parseRADOSStatSize(malformed); err == nil {
			t.Errorf("%q parsed as %d", malformed, size)
		}
	}

	oids, err := rgwGCOIDs([]byte(rgwGCListSample))
	if err != nil || !slices.Equal(oids, []string{rgwTailSample}) {
		t.Fatalf("gc list: %v %v", oids, err)
	}
	if oids, err := rgwGCOIDs([]byte(`[]`)); err != nil || len(oids) != 0 {
		t.Fatalf("empty gc list: %v %v", oids, err)
	}
	if _, err := rgwGCOIDs([]byte(`{}`)); err == nil {
		t.Error("an object parsed as a gc list")
	}
}

// The review's case: a key that is not in any shard before DELETE must stop
// the receipt, so its absence afterwards cannot pass for a removal.
func TestRGWIndexShardOf(t *testing.T) {
	shard0, shard1 := ".dir."+rgwMarkerSample+".0", ".dir."+rgwMarkerSample+".1"
	shards := map[string][]string{shard0: {"large"}, shard1: {"small"}, ".dir.other.1.0": {"large"}}
	for key, want := range map[string]string{"large": shard0, "small": shard1} {
		if got, err := rgwIndexShardOf(shards, rgwMarkerSample, key); err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", key, got, err, want)
		}
	}
	missing := map[string][]string{shard0: {}, shard1: {"small"}, ".dir.other.1.0": {"large"}}
	if got, err := rgwIndexShardOf(missing, rgwMarkerSample, "large"); err == nil {
		t.Errorf("a key in no shard of the bucket was found in %q", got)
	}
	twice := map[string][]string{shard0: {"large"}, shard1: {"large", "small"}}
	if got, err := rgwIndexShardOf(twice, rgwMarkerSample, "large"); err == nil {
		t.Errorf("a key in two shards was placed in %q", got)
	}
	// A bucket whose ID extends this one's is not this bucket.
	longer := map[string][]string{".dir." + rgwMarkerSample + "1.0": {"large"}}
	if got, err := rgwIndexShardOf(longer, rgwMarkerSample, "large"); err == nil {
		t.Errorf("another bucket's shard %q matched", got)
	}
}
