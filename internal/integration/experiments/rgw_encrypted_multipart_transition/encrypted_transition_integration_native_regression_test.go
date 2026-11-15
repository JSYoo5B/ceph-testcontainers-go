//go:build native_regression && (all || (integration && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_encrypted_multipart_transition)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_encrypted_multipart_transition_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

const (
	etBucket    = "lc-encrypted-layout"
	etColdClass = "COLD"
	etColdPool  = "lc.cold"
	// The first part is 3,071 bytes longer than 5 MiB, so it ends inside a
	// 4 KiB encryption chunk; the second part is short. Each part's bytes are
	// distinguishable.
	etFirstPart = 5<<20 + 3071
	// The range starts 32 bytes before the part boundary and ends 128 bytes
	// after it.
	etBeforeBoundary = 32
	etAfterBoundary  = 128
	// The crypt attribute that keeps the part lengths of an encrypted
	// multipart object whose manifest no longer shows them.
	etPartLengthsAttr = "user.rgw.crypt.part-lengths"
)

// image is the Ceph image the case runs on: Tentacle 20.2.1, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_encrypted_multipart_transition_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.2 and later, Squid 19.2.7) does not
// reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all"

// The objects of the case. The lifecycle rule selects the prefix move/.
const (
	etSubject      = "/move/encrypted"
	etPlainControl = "/move/plain"
	etKeptControl  = "/keep/encrypted"
)

type etCase struct {
	ctx     context.Context
	gateway *rgw.Gateway
	client  harness.S3HTTPClient
	whole   []byte
	parts   [][]byte
}

// request sends one signed request with headers and returns its answer.
// The body is read in full; a transport failure stops the test.
func (c *etCase) request(t *testing.T, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, c.ctx, c.client, "s3", method, "/"+etBucket+path, payload, headers)
}

// get sends one GET of the object with headers and records the answer
// against want. A transport failure is recorded, not fatal: a gateway that
// fails while it decrypts can cut the body off after the headers.
func (c *etCase) get(t *testing.T, kind, path string, headers http.Header, want etWant) etRead {
	t.Helper()
	request, err := http.NewRequestWithContext(c.ctx, http.MethodGet, c.client.Endpoint+"/"+etBucket+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range headers {
		request.Header[key] = slices.Clone(values)
	}
	harness.S3FeatureSign(request, c.client, nil, time.Now().UTC(), "s3")
	response, err := c.client.HTTP.Do(request)
	if err != nil {
		return observe(kind, want, 0, "", nil, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return observe(kind, want, response.StatusCode, response.Header.Get("Content-Range"), body, err)
}

// inexact sends a full GET and a GET across the part boundary of the
// object, logs each answer and returns those that are not the expected one.
func (c *etCase) inexact(t *testing.T, stage, path string, headers http.Header) []string {
	t.Helper()
	first, last := etFirstPart-etBeforeBoundary, etFirstPart+etAfterBoundary
	ranged := headers.Clone()
	ranged.Set("Range", fmt.Sprintf("bytes=%d-%d", first, last))
	var bad []string
	for _, read := range []struct {
		kind    string
		headers http.Header
		want    etWant
	}{{"full", headers, fullWant(c.whole)}, {"range", ranged, rangeWant(c.whole, first, last)}} {
		got := c.get(t, read.kind, path, read.headers, read.want)
		t.Logf("RGW_ET %s %s %s exact=%t", stage, path, got, got.exact(read.want))
		if !got.exact(read.want) {
			bad = append(bad, fmt.Sprintf("%s %s", path, got))
		}
	}
	return bad
}

// requireExact fails the test unless every read of the object is exact.
func (c *etCase) requireExact(t *testing.T, stage, path string, headers http.Header) {
	t.Helper()
	if bad := c.inexact(t, stage, path, headers); len(bad) > 0 {
		t.Fatalf("control failed %s: %s", stage, strings.Join(bad, "; "))
	}
}

// requireHead fails the test unless HEAD reports the storage class and the
// encryption mode the object was written with.
func (c *etCase) requireHead(t *testing.T, stage, path, class string, headers http.Header) {
	t.Helper()
	head := c.request(t, http.MethodHead, path, nil, headers)
	harness.RGWBackendRequire(t, head, http.StatusOK)
	got := head.Headers.Get("X-Amz-Storage-Class")
	if got == "" {
		got = "STANDARD"
	}
	algorithm := headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm")
	t.Logf("RGW_ET %s %s head storage-class=%s sse-c=%q", stage, path, got, head.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"))
	if got != class || head.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != algorithm {
		t.Fatalf("condition not established %s: %s storage-class=%q sse-c=%q, want %s and %q", stage, path, got,
			head.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"), class, algorithm)
	}
}

// upload completes a two-part multipart upload of the object with headers
// on every request.
func (c *etCase) upload(t *testing.T, path string, headers http.Header) {
	t.Helper()
	started := c.request(t, http.MethodPost, path+"?uploads", nil, headers)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var start struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &start); err != nil || start.ID == "" {
		t.Fatalf("%s: missing upload ID: %v", path, err)
	}
	if started.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") {
		t.Fatalf("condition not established: initiation of %s returned SSE-C %q", path, started.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"))
	}
	query := "?uploadId=" + url.QueryEscape(start.ID)
	var complete strings.Builder
	complete.WriteString("<CompleteMultipartUpload>")
	for i, part := range c.parts {
		r := c.request(t, http.MethodPut, path+query+fmt.Sprintf("&partNumber=%d", i+1), part, headers)
		harness.RGWBackendRequire(t, r, http.StatusOK)
		tag := r.Headers.Get("ETag")
		if tag == "" {
			t.Fatalf("%s: part %d missing ETag", path, i+1)
		}
		fmt.Fprintf(&complete, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, tag)
	}
	complete.WriteString("</CompleteMultipartUpload>")
	done := c.request(t, http.MethodPost, path+query, []byte(complete.String()), headers)
	harness.RGWBackendRequire(t, done, http.StatusOK)
	result, err := harness.RGWClassifyResultBody(done.Body, "CompleteMultipartUploadResult")
	if err != nil || result.Error {
		t.Fatalf("%s: completion result %+v: %v", path, result, err)
	}
}

// logStat logs whether the object carries the part-lengths crypt
// attribute, the record that the correction writes on
// transition. It is evidence only and decides nothing.
func (c *etCase) logStat(t *testing.T, stage, path string) {
	t.Helper()
	var stat struct {
		Attrs map[string]json.RawMessage `json:"attrs"`
	}
	raw := harness.RGWBackendAdmin(t, c.ctx, c.gateway, "object", "stat", "--bucket", etBucket, "--object", strings.TrimPrefix(path, "/"))
	if err := json.Unmarshal(raw, &stat); err != nil {
		t.Logf("RGW_ET %s %s stat unreadable: %v", stage, path, err)
		return
	}
	_, parts := stat.Attrs[etPartLengthsAttr]
	t.Logf("RGW_ET %s %s stat %s=%t", stage, path, etPartLengthsAttr, parts)
}

func TestRGWEncryptedMultipartTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	// Given: one gateway with a COLD storage class on its own pool and three
	// two-part objects of the same layout, uploaded over HTTPS. The subject
	// and the plaintext control are under move/; the encrypted control is
	// not.
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_ET ceph=%s image=%s", harness.StaticFacts(cluster).CephVersion, image)
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_enable_lc_threads", Value: "false"},
		{Section: "client.admin", Name: "rgw_lc_debug_interval", Value: "2"},
	} {
		if _, err := override.SetConfig(ctx, cluster, setting); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: etColdPool, Application: "rgw", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	material, roots := harness.RGWTestCertificate(t)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	harness.RGWBackendAdmin(t, ctx, gateway, "zonegroup", "placement", "add", "--placement-id", "default-placement", "--storage-class", etColdClass)
	harness.RGWBackendAdmin(t, ctx, gateway, "zone", "placement", "add", "--placement-id", "default-placement", "--storage-class", etColdClass, "--data-pool", etColdPool)
	grace := 3 * time.Second
	if err := gateway.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	key := bytes.Repeat([]byte{'k'}, 32)
	digest := md5.Sum(key)
	sse := http.Header{}
	sse.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	sse.Set("X-Amz-Server-Side-Encryption-Customer-Key", base64.StdEncoding.EncodeToString(key))
	sse.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", base64.StdEncoding.EncodeToString(digest[:]))
	parts := [][]byte{bytes.Repeat([]byte("abc123XY"), (etFirstPart+7)/8)[:etFirstPart], bytes.Repeat([]byte("different"), 9000)}
	c := &etCase{
		ctx:     ctx,
		gateway: gateway,
		client: harness.S3HTTPClient{Endpoint: endpoint, Region: gateway.Region, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey,
			HTTP: &http.Client{Transport: transport, Timeout: 30 * time.Second}},
		parts: parts,
		whole: bytes.Join(parts, nil),
	}
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) {
		clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, bucket := range created {
			if _, err := gateway.Admin(clean, "bucket", "rm", "--bucket", strings.TrimPrefix(bucket, "/"), "--purge-objects"); err != nil {
				t.Errorf("RGW_ET cleanup failed: %v", err)
			}
		}
	}, etBucket); err != nil {
		t.Fatal(err)
	}
	objects := []struct {
		path    string
		headers http.Header
	}{{etSubject, sse}, {etPlainControl, http.Header{}}, {etKeptControl, sse}}
	for _, object := range objects {
		c.upload(t, object.path, object.headers)
		c.requireHead(t, "before", object.path, "STANDARD", object.headers)
		c.requireExact(t, "before", object.path, object.headers)
		c.logStat(t, "before", object.path)
	}
	t.Logf("RGW_ET layout parts=%d,%d boundary=%d range=%d-%d", len(parts[0]), len(parts[1]), etFirstPart, etFirstPart-etBeforeBoundary, etFirstPart+etAfterBoundary)

	// When: a manual lifecycle pass moves the objects under move/ to COLD.
	// Both must report COLD and the encrypted control STANDARD, and the two
	// controls must still read back exactly.
	rule := []byte(`<LifecycleConfiguration><Rule><ID>move</ID><Prefix>move/</Prefix><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>` + etColdClass + `</StorageClass></Transition></Rule></LifecycleConfiguration>`)
	harness.RGWBackendRequire(t, c.request(t, http.MethodPut, "?lifecycle", rule, nil), http.StatusOK)
	// Age the objects beyond a full two-second debug day before the pass.
	time.Sleep(4 * time.Second)
	harness.RGWBackendAdmin(t, ctx, gateway, "lc", "process", "--bucket", etBucket)
	c.requireHead(t, "after", etSubject, etColdClass, sse)
	c.requireHead(t, "after", etPlainControl, etColdClass, http.Header{})
	c.requireHead(t, "after", etKeptControl, "STANDARD", sse)
	for _, object := range objects {
		c.logStat(t, "after", object.path)
	}
	c.requireExact(t, "after", etPlainControl, http.Header{})
	c.requireExact(t, "after", etKeptControl, sse)

	// Then: the transitioned encrypted object must still return its
	// plaintext, in full and across the part boundary.
	symptoms := c.inexact(t, "after", etSubject, sse)
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_encrypted_multipart_transition result=%s image=%s symptoms=%q", result, image, symptoms)
}
