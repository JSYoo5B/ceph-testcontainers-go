//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_compressed_copy)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_compressed_copy_accounting_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

const (
	ccPlacement = "default-placement"
	ccColdClass = "COLD"
	ccColdPool  = "tc-copy.cold.data"
	ccObject    = "obj"
)

type ccCase struct {
	ctx     context.Context
	gateway *rgw.Gateway
	client  harness.S3HTTPClient
}

func (c *ccCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, headers)
}

func (c *ccCase) admin(t *testing.T, args ...string) []byte {
	t.Helper()
	return harness.RGWBackendAdmin(t, c.ctx, c.gateway, args...)
}

// ccCompression is the decoded user.rgw.compression attribute of an
// object, as radosgw-admin object stat prints it.
type ccCompression struct {
	Type     string `json:"compression_type"`
	OrigSize uint64 `json:"orig_size"`
	Blocks   []struct {
		Len uint64 `json:"len"`
	} `json:"blocks"`
}

// stored is the sum of the compressed block lengths, the raw bytes the
// object occupies.
func (c ccCompression) stored() uint64 {
	var n uint64
	for _, b := range c.Blocks {
		n += b.Len
	}
	return n
}

// ccIndexEntry is the bucket index entry of the case's one object: Size is
// the raw (possibly compressed) byte count, AccountedSize the logical one.
type ccIndexEntry struct {
	Size          uint64 `json:"size"`
	AccountedSize uint64 `json:"accounted_size"`
	StorageClass  string `json:"storage_class"`
}

// ccStats is the rgw.main usage of a bucket: Size is the accounted
// (logical) total, SizeUtilized the raw one, SizeActual the rounded one.
type ccStats struct {
	Size         uint64 `json:"size"`
	SizeActual   uint64 `json:"size_actual"`
	SizeUtilized uint64 `json:"size_utilized"`
	NumObjects   uint64 `json:"num_objects"`
}

// ccObservation is everything the case reads about the object of one
// bucket, through radosgw-admin and S3.
type ccObservation struct {
	Bucket      string
	Compression *ccCompression
	Index       ccIndexEntry
	Stats       ccStats
	ListedSize  int64
	Data        []byte
}

func (o ccObservation) String() string {
	compression := "none"
	if o.Compression != nil {
		compression = fmt.Sprintf("%s orig_size=%d stored=%d", o.Compression.Type, o.Compression.OrigSize, o.Compression.stored())
	}
	return fmt.Sprintf("bucket=%s compression=(%s) index=(size=%d accounted_size=%d storage_class=%q) stats=(size=%d size_actual=%d size_utilized=%d num_objects=%d) listed_size=%d get_bytes=%d",
		o.Bucket, compression, o.Index.Size, o.Index.AccountedSize, o.Index.StorageClass,
		o.Stats.Size, o.Stats.SizeActual, o.Stats.SizeUtilized, o.Stats.NumObjects, o.ListedSize, len(o.Data))
}

// observe reads the object of bucket: its compression attribute, its bucket
// index entry, the bucket statistics once they count it, its S3 listing
// size and its bytes through a fresh GET.
func (c *ccCase) observe(t *testing.T, bucket string) ccObservation {
	t.Helper()
	o := ccObservation{Bucket: bucket}
	var stat struct {
		Compression *ccCompression `json:"compression"`
	}
	if err := json.Unmarshal(c.admin(t, "object", "stat", "--bucket", bucket, "--object", ccObject), &stat); err != nil {
		t.Fatalf("object stat %s/%s: %v", bucket, ccObject, err)
	}
	o.Compression = stat.Compression

	var entries []struct {
		Type  string `json:"type"`
		Entry struct {
			Name string       `json:"name"`
			Meta ccIndexEntry `json:"meta"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(c.admin(t, "bi", "list", "--bucket", bucket), &entries); err != nil {
		t.Fatalf("bi list %s: %v", bucket, err)
	}
	if len(entries) != 1 || entries[0].Type != "plain" || entries[0].Entry.Name != ccObject {
		t.Fatalf("bi list %s: %+v, want the one plain entry %q", bucket, entries, ccObject)
	}
	o.Index = entries[0].Entry.Meta

	// The statistics are updated with the index entry; wait a bounded time
	// for them to count the object rather than read one stale sample.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var stats struct {
			Usage map[string]ccStats `json:"usage"`
		}
		if err := json.Unmarshal(c.admin(t, "bucket", "stats", "--bucket", bucket), &stats); err != nil {
			t.Fatalf("bucket stats %s: %v", bucket, err)
		}
		o.Stats = stats.Usage["rgw.main"]
		if o.Stats.NumObjects == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if o.Stats.NumObjects != 1 {
		t.Fatalf("bucket stats %s: %+v, want one object", bucket, o.Stats)
	}

	listed := c.signed(t, c.ctx, http.MethodGet, "/"+bucket, nil, nil)
	harness.RGWBackendRequire(t, listed, http.StatusOK)
	var listing struct {
		Contents []struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(listed.Body, &listing); err != nil || len(listing.Contents) != 1 || listing.Contents[0].Key != ccObject {
		t.Fatalf("listing of %s: %+v (%v)", bucket, listing.Contents, err)
	}
	o.ListedSize = listing.Contents[0].Size

	got := c.signed(t, c.ctx, http.MethodGet, "/"+bucket+"/"+ccObject, nil, nil)
	harness.RGWBackendRequire(t, got, http.StatusOK)
	o.Data = got.Body
	t.Logf("RGW_CC observe %s", o)
	return o
}

// copyObject copies source to bucket/obj with headers and requires a
// CopyObjectResult. A 200 whose body is an S3 Error document, in any
// namespace, fails with that error's code and message.
func (c *ccCase) copyObject(t *testing.T, source, bucket string, headers http.Header) {
	t.Helper()
	h := headers.Clone()
	if h == nil {
		h = http.Header{}
	}
	h.Set("X-Amz-Copy-Source", source)
	r := c.signed(t, c.ctx, http.MethodPut, "/"+bucket+"/"+ccObject, nil, h)
	t.Logf("RGW_CC copy source=%s dest=%s/%s storage-class=%q status=%d code=%s", source, bucket, ccObject, h.Get("X-Amz-Storage-Class"), r.Code, harness.RGWBackendErrorCode(r.Body))
	harness.RGWBackendRequire(t, r, http.StatusOK)
	classified, err := harness.RGWClassifyResultBody(r.Body, "CopyObjectResult")
	if err != nil {
		t.Fatalf("copy to %s answered 200 with an unclassified body: %v", bucket, err)
	}
	if classified.Error {
		t.Fatalf("copy to %s answered 200 with an embedded S3 error %s %q", bucket, classified.Code, classified.Message)
	}
	var result struct {
		ETag string `xml:"ETag"`
	}
	if err := xml.Unmarshal(r.Body, &result); err != nil || result.ETag == "" {
		t.Fatalf("copy to %s answered a CopyObjectResult without an ETag (%v)", bucket, err)
	}
}

// configurePlacement adds the uncompressed COLD storage class to the
// default placement, enables zlib compression for STANDARD, restarts the
// gateway so that it loads the zone, and reads the zone back.
func (c *ccCase) configurePlacement(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	if _, err := cluster.CreatePool(c.ctx, ceph.PoolConfig{Name: ccColdPool, Application: "rgw"}); err != nil {
		t.Fatal(err)
	}
	// Admin runs in the gateway's own zone and zonegroup scope.
	c.admin(t, "zonegroup", "placement", "add", "--placement-id", ccPlacement, "--storage-class", ccColdClass)
	c.admin(t, "zone", "placement", "add", "--placement-id", ccPlacement, "--storage-class", ccColdClass, "--data-pool", ccColdPool)
	c.admin(t, "zone", "placement", "modify", "--placement-id", ccPlacement, "--storage-class", "STANDARD", "--compression", "zlib")

	grace := 3 * time.Second
	if err := c.gateway.Stop(c.ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := c.gateway.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	endpoint, err := c.gateway.S3Endpoint(c.ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.client.Endpoint = endpoint

	var readBack struct {
		Name       string `json:"name"`
		Placements []struct {
			Key string `json:"key"`
			Val struct {
				StorageClasses map[string]struct {
					DataPool        string `json:"data_pool"`
					CompressionType string `json:"compression_type"`
				} `json:"storage_classes"`
			} `json:"val"`
		} `json:"placement_pools"`
	}
	if err := json.Unmarshal(c.admin(t, "zone", "get"), &readBack); err != nil {
		t.Fatalf("zone get: %v", err)
	}
	for _, p := range readBack.Placements {
		if p.Key != ccPlacement {
			continue
		}
		standard, cold := p.Val.StorageClasses["STANDARD"], p.Val.StorageClasses[ccColdClass]
		t.Logf("RGW_CC zone=%s placement=%s STANDARD=%+v %s=%+v", readBack.Name, p.Key, standard, ccColdClass, cold)
		if standard.CompressionType == "zlib" && cold.DataPool == ccColdPool && cold.CompressionType == "" {
			return
		}
	}
	t.Fatalf("condition not established: zone %s does not read back zlib STANDARD and uncompressed %s", readBack.Name, ccColdClass)
}

// traceCopies returns the copy_obj placement lines that the gateway logged
// at debug_rgw 20; they name the source and destination placement rules and
// pools of each copy.
func (c *ccCase) traceCopies(t *testing.T) []string {
	t.Helper()
	logs, err := c.gateway.Logs(c.ctx)
	if err != nil {
		t.Logf("RGW_CC trace unavailable: %v", err)
		return nil
	}
	defer func() { _ = logs.Close() }()
	var lines []string
	scanner := bufio.NewScanner(io.LimitReader(logs, 64<<20))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		if line := scanner.Text(); strings.Contains(line, "copy_obj(): src_rule=") {
			lines = append(lines, line[strings.Index(line, "copy_obj(): "):])
		}
	}
	return lines
}

// cleanup deletes the object and then each bucket of buckets, the ones the
// test created.
func (c *ccCase) cleanup(t *testing.T, buckets []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	for _, bucket := range buckets {
		if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket+"/"+ccObject, nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_CC cleanup failed: delete %s/%s: %d", bucket, ccObject, r.Code)
			failed = true
		}
		if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket, nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_CC cleanup failed: delete bucket %s: %d %s", bucket, r.Code, harness.RGWBackendErrorCode(r.Body))
			failed = true
		}
	}
	// The placement edits stay with the zone; the cluster that holds them is
	// removed by its own cleanup.
	t.Logf("RGW_CC cleanup buckets=%d failed=%t", len(buckets), failed)
}

// TestRGWCompressedCopyAccounting requires that a copy whose data is
// rewritten keeps the object's logical size in the destination bucket
// index and statistics. The source is zlib-compressed (STANDARD storage
// class), the destination storage class COLD is not, so RGW decompresses
// the data and writes no compression attribute: the accounted size must be
// the logical length, not the compressed byte count it read.
//
// Tentacle 20.2.4 accounts such a copy with the number of raw bytes read
// from the compressed source, so the destination's index entry, listing
// size and bucket statistics report the compressed length while GET
// returns every byte. Tentacle 20.2.5 and Umbrella 21.2.0 pass. The
// controls (an uncompressed source, and a compressed destination with its
// own compression metadata) pass on every tested release.
func TestRGWCompressedCopyAccounting(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_CC ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := cluster.Ceph(ctx, "versions")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_CC daemons=%s", strings.Join(strings.Fields(string(versions)), " "))
	t.Logf("RGW_CC gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	c := &ccCase{ctx: ctx, gateway: gateway, client: harness.S3HTTPClient{AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}}
	c.configurePlacement(t, cluster)

	// One object per bucket, so no other object can mask a counter.
	const (
		source        = "tc-cc-source"
		plainSource   = "tc-cc-plain-source"
		uncompressed  = "tc-cc-copy-uncompressed"
		compressed    = "tc-cc-copy-compressed"
		plainControl  = "tc-cc-copy-plain-control"
		storageHeader = "X-Amz-Storage-Class"
	)
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) { c.cleanup(t, created) },
		source, plainSource, uncompressed, compressed, plainControl); err != nil {
		t.Fatal(err)
	}

	var payload bytes.Buffer
	for i := 0; payload.Len() < 1<<20; i++ {
		fmt.Fprintf(&payload, "compressible line %08d of the copy accounting case\n", i)
	}
	data := payload.Bytes()
	logical := uint64(len(data))
	harness.RGWBackendRequire(t, c.signed(t, ctx, http.MethodPut, "/"+source+"/"+ccObject, data, nil), http.StatusOK)
	harness.RGWBackendRequire(t, c.signed(t, ctx, http.MethodPut, "/"+plainSource+"/"+ccObject, data, http.Header{storageHeader: {ccColdClass}}), http.StatusOK)

	src := c.observe(t, source)
	if src.Compression == nil || src.Compression.Type != "zlib" || src.Compression.OrigSize != logical ||
		src.Compression.stored() >= logical || src.Index.Size >= logical || src.Index.AccountedSize != logical {
		t.Fatalf("condition not established: the source is not stored zlib-compressed with logical size %d: %s", logical, src)
	}
	if plain := c.observe(t, plainSource); plain.Compression != nil || plain.Index.StorageClass != ccColdClass || plain.Index.AccountedSize != logical {
		t.Fatalf("condition not established: the plain source is not an uncompressed %s object of %d bytes: %s", ccColdClass, logical, plain)
	}

	// debug_rgw 20 for the copies only, to record the placement each copy
	// resolved; the oracle does not depend on it.
	harness.MustCeph(t, ctx, cluster, "config", "set", "client.admin", "debug_rgw", "20/20")
	c.copyObject(t, source+"/"+ccObject, uncompressed, http.Header{storageHeader: {ccColdClass}})
	c.copyObject(t, source+"/"+ccObject, compressed, nil)
	c.copyObject(t, plainSource+"/"+ccObject, plainControl, http.Header{storageHeader: {ccColdClass}})
	harness.MustCeph(t, ctx, cluster, "config", "rm", "client.admin", "debug_rgw")
	for _, line := range c.traceCopies(t) {
		t.Logf("RGW_CC trace %s", line)
	}

	requireLogical := func(t *testing.T, o ccObservation) {
		t.Helper()
		if !bytes.Equal(o.Data, data) {
			t.Fatalf("%s: GET returned %d bytes that differ from the %d written", o.Bucket, len(o.Data), len(data))
		}
		if o.Index.AccountedSize != logical || uint64(o.ListedSize) != logical || o.Stats.Size != logical {
			t.Errorf("%s: logical size %d, but index accounted_size=%d, listed Size=%d, bucket stats size=%d",
				o.Bucket, logical, o.Index.AccountedSize, o.ListedSize, o.Stats.Size)
		}
	}

	t.Run("compressed-source-uncompressed-destination", func(t *testing.T) {
		o := c.observe(t, uncompressed)
		// The copy rewrote the data without compression: no compression
		// metadata, the raw bytes are the logical ones.
		if o.Compression != nil || o.Index.StorageClass != ccColdClass || o.Index.Size != logical || o.Stats.SizeUtilized != logical {
			t.Fatalf("condition not established: the destination is not an uncompressed %s copy: %s", ccColdClass, o)
		}
		requireLogical(t, o)
	})

	t.Run("compressed-destination-control", func(t *testing.T) {
		o := c.observe(t, compressed)
		if o.Compression == nil || o.Compression.OrigSize != logical || o.Index.Size >= logical {
			t.Fatalf("control not established: the STANDARD copy is not stored compressed: %s", o)
		}
		requireLogical(t, o)
	})

	t.Run("uncompressed-source-control", func(t *testing.T) {
		o := c.observe(t, plainControl)
		if o.Compression != nil || o.Index.Size != logical {
			t.Fatalf("control not established: the copy of the plain source is not uncompressed: %s", o)
		}
		requireLogical(t, o)
	})
}
