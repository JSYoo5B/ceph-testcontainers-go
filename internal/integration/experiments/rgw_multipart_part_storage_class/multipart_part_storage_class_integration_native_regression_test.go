//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_multipart_part_class)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_multipart_part_storage_class_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

const (
	mpPlacement = "default-placement"
	mpColdClass = "COLD"
	mpColdPool  = "tc-mp.cold.data"
	mpObject    = "obj"
	// The first part is the smallest valid non-final part, 5 MiB; the
	// second is short. Each part's bytes are distinguishable.
	mpFirstPart  = 5 << 20
	mpSecondPart = 1 << 16
)

type mpCase struct {
	ctx     context.Context
	gateway *rgw.Gateway
	client  harness.S3HTTPClient
	uploads map[string]string
}

func (c *mpCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, headers)
}

func (c *mpCase) admin(t *testing.T, args ...string) []byte {
	t.Helper()
	return harness.RGWBackendAdmin(t, c.ctx, c.gateway, args...)
}

// configurePlacement adds the COLD storage class on its own data pool to
// the default placement, restarts the gateway so that it loads the zone,
// and requires the zone to read the mapping back.
func (c *mpCase) configurePlacement(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	if _, err := cluster.CreatePool(c.ctx, ceph.PoolConfig{Name: mpColdPool, Application: "rgw"}); err != nil {
		t.Fatal(err)
	}
	c.admin(t, "zonegroup", "placement", "add", "--placement-id", mpPlacement, "--storage-class", mpColdClass)
	c.admin(t, "zone", "placement", "add", "--placement-id", mpPlacement, "--storage-class", mpColdClass, "--data-pool", mpColdPool)
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
	var zone struct {
		Name       string `json:"name"`
		Placements []struct {
			Key string `json:"key"`
			Val struct {
				StorageClasses map[string]struct {
					DataPool string `json:"data_pool"`
				} `json:"storage_classes"`
			} `json:"val"`
		} `json:"placement_pools"`
	}
	if err := json.Unmarshal(c.admin(t, "zone", "get"), &zone); err != nil {
		t.Fatalf("zone get: %v", err)
	}
	for _, p := range zone.Placements {
		if p.Key == mpPlacement {
			t.Logf("RGW_MP zone=%s placement=%s storage_classes=%+v", zone.Name, p.Key, p.Val.StorageClasses)
			standard, cold := p.Val.StorageClasses["STANDARD"], p.Val.StorageClasses[mpColdClass]
			if cold.DataPool == mpColdPool && standard.DataPool != "" && standard.DataPool != mpColdPool {
				return
			}
		}
	}
	t.Fatalf("condition not established: zone %s does not map %s to %s apart from STANDARD", zone.Name, mpColdClass, mpColdPool)
}

// upload completes a two-part upload of parts to bucket/obj with the
// storage class header, if any, on the initiation.
func (c *mpCase) upload(t *testing.T, bucket, class string, parts [][]byte) {
	t.Helper()
	path := "/" + bucket + "/" + mpObject
	init := http.Header{}
	if class != "" {
		init.Set("X-Amz-Storage-Class", class)
	}
	started := c.signed(t, c.ctx, http.MethodPost, path+"?uploads", nil, init)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var initiated struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &initiated); err != nil || initiated.ID == "" {
		t.Fatalf("initiate %s: no upload ID (%v)", path, err)
	}
	c.uploads[initiated.ID] = path
	var complete strings.Builder
	complete.WriteString("<CompleteMultipartUpload>")
	for i, part := range parts {
		r := c.signed(t, c.ctx, http.MethodPut, fmt.Sprintf("%s?partNumber=%d&uploadId=%s", path, i+1, url.QueryEscape(initiated.ID)), part, nil)
		harness.RGWBackendRequire(t, r, http.StatusOK)
		fmt.Fprintf(&complete, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, r.Headers.Get("ETag"))
	}
	complete.WriteString("</CompleteMultipartUpload>")
	done := c.signed(t, c.ctx, http.MethodPost, path+"?uploadId="+url.QueryEscape(initiated.ID), []byte(complete.String()), nil)
	harness.RGWBackendRequire(t, done, http.StatusOK)
	classified, err := harness.RGWClassifyResultBody(done.Body, "CompleteMultipartUploadResult")
	if err != nil || classified.Error {
		t.Fatalf("complete %s: %+v %v", path, classified, err)
	}
	delete(c.uploads, initiated.ID)
}

func (c *mpCase) readPart(t *testing.T, method, bucket string, part int, want []byte) mpRead {
	t.Helper()
	r := c.signed(t, c.ctx, method, fmt.Sprintf("/%s/%s?partNumber=%d", bucket, mpObject, part), nil, nil)
	read := mpRead{Method: method, Part: part, Status: r.Code, PartsCount: r.Headers.Get("X-Amz-Mp-Parts-Count"), Code: harness.RGWBackendErrorCode(r.Body)}
	read.Length, _ = strconv.ParseInt(r.Headers.Get("Content-Length"), 10, 64)
	if method == http.MethodGet && want != nil {
		read.BodyMatches = bytes.Equal(r.Body, want)
	}
	t.Logf("RGW_MP read bucket=%s %s", bucket, read)
	return read
}

// cleanup aborts the uploads left open, then deletes the objects and the
// buckets the test created.
func (c *mpCase) cleanup(t *testing.T, buckets []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	for id, path := range c.uploads {
		if r := c.signed(t, ctx, http.MethodDelete, path+"?uploadId="+url.QueryEscape(id), nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_MP cleanup failed: abort %s: %d", path, r.Code)
			failed = true
		}
	}
	for _, bucket := range buckets {
		if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket+"/"+mpObject, nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_MP cleanup failed: delete %s/%s: %d", bucket, mpObject, r.Code)
			failed = true
		}
		if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket, nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_MP cleanup failed: delete bucket %s: %d %s", bucket, r.Code, harness.RGWBackendErrorCode(r.Body))
			failed = true
		}
	}
	t.Logf("RGW_MP cleanup buckets=%d failed=%t", len(buckets), failed)
}

// TestRGWMultipartPartStorageClass requires that every numbered part of a
// completed multipart object can be read with GET and HEAD ?partNumber,
// whatever the object's storage class. The trigger object is uploaded to the
// COLD storage class, whose data pool is not the default one; the control
// object uses STANDARD. A whole-object GET, a part number beyond the last
// part (a refusal control) and the placement of each object's part heads are
// recorded alongside.
func TestRGWMultipartPartStorageClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_MP ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_MP gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	c := &mpCase{ctx: ctx, gateway: gateway, uploads: map[string]string{},
		client: harness.S3HTTPClient{AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 60 * time.Second}}}
	c.configurePlacement(t, cluster)

	const cold, standard = "tc-mp-cold", "tc-mp-standard"
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) { c.cleanup(t, created) }, cold, standard); err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{bytes.Repeat([]byte("first part of the storage class case\n"), mpFirstPart/37+1)[:mpFirstPart],
		bytes.Repeat([]byte("SECOND PART\n"), mpSecondPart/12+1)[:mpSecondPart]}
	whole := append(append([]byte{}, parts[0]...), parts[1]...)
	c.upload(t, cold, mpColdClass, parts)
	c.upload(t, standard, "", parts)

	// The manifest names where RGW placed the object: the head placement
	// rule and the tail (part) placement rule with its storage class.
	placement := func(bucket string) string {
		var stat struct {
			Manifest struct {
				HeadRule string `json:"head_placement_rule"`
				Tail     struct {
					Bucket struct {
						Pool string `json:"name"`
					} `json:"bucket"`
					Rule string `json:"placement_rule"`
				} `json:"tail_placement"`
				Prefix string `json:"prefix"`
			} `json:"manifest"`
		}
		if err := json.Unmarshal(c.admin(t, "object", "stat", "--bucket", bucket, "--object", mpObject), &stat); err != nil {
			t.Fatalf("object stat %s/%s: %v", bucket, mpObject, err)
		}
		t.Logf("RGW_MP placement bucket=%s head_rule=%q tail_rule=%q prefix_multipart=%t", bucket, stat.Manifest.HeadRule, stat.Manifest.Tail.Rule, strings.Contains(stat.Manifest.Prefix, "multipart") || stat.Manifest.Prefix != "")
		return stat.Manifest.Tail.Rule
	}
	if rule := placement(cold); !strings.HasSuffix(rule, "/"+mpColdClass) {
		t.Fatalf("condition not established: the parts of %s are not placed in %s (tail rule %q)", cold, mpColdClass, rule)
	}
	if rule := placement(standard); strings.Contains(rule, mpColdClass) {
		t.Fatalf("control not established: the parts of %s are placed in %s (tail rule %q)", standard, mpColdClass, rule)
	}

	for _, test := range []struct {
		name, bucket, class string
	}{{"cold-storage-class", cold, mpColdClass}, {"standard-control", standard, "STANDARD"}} {
		t.Run(test.name, func(t *testing.T) {
			head := c.signed(t, ctx, http.MethodHead, "/"+test.bucket+"/"+mpObject, nil, nil)
			class := head.Headers.Get("X-Amz-Storage-Class")
			if class == "" {
				class = "STANDARD"
			}
			got := c.signed(t, ctx, http.MethodGet, "/"+test.bucket+"/"+mpObject, nil, nil)
			t.Logf("RGW_MP whole bucket=%s head=%d storage-class=%q get=%d bytes=%d matches=%t", test.bucket, head.Code, class, got.Code, len(got.Body), bytes.Equal(got.Body, whole))
			if head.Code != http.StatusOK || class != test.class || got.Code != http.StatusOK || !bytes.Equal(got.Body, whole) {
				t.Fatalf("condition not established: %s is not a readable %s object", test.bucket, test.class)
			}
			for i, part := range parts {
				for _, method := range []string{http.MethodGet, http.MethodHead} {
					if read := c.readPart(t, method, test.bucket, i+1, part); !partOK(read, part) {
						t.Errorf("%s of part %d of the %s object: %s, want its %d bytes", method, i+1, test.class, read, len(part))
					}
				}
			}
			if beyond := c.readPart(t, http.MethodGet, test.bucket, len(parts)+1, nil); beyond.Status < 400 {
				t.Errorf("part %d of a %d-part object was not refused: %s", len(parts)+1, len(parts), beyond)
			}
		})
	}
}
