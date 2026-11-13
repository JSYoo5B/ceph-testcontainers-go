//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_slo_head_segment_reads)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_slo_head_segment_reads_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_slo_head_segment_reads_test.image=<image>"
//
// No release fixes it: Squid 19.2.5 and Umbrella 21.2.0 read the segments
// too.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

const (
	// sloDataPool is the data pool of the default zone's default placement.
	sloDataPool = "default.rgw.buckets.data"
	sloUser     = "tc-slo"
	// Every layout carries the same payload: three segments, or two parts,
	// of the sizes below.
	sloSegmentSize = 3 << 20
	sloSegments    = 3
	sloPayloadSize = sloSegments * sloSegmentSize
	sloFirstPart   = 5 << 20
	// The history size and duration the case gives the OSD, so the history
	// keeps every operation of the test.
	sloHistorySize     = 20000
	sloHistoryDuration = 3600
)

// The buckets of the case, one per layout, so each layout's objects have
// their own prefix in the data pool.
const (
	sloBucket         = "tc-slo"
	sloSegmentBucket  = "tc-slo-segments"
	dloBucket         = "tc-dlo"
	dloSegmentBucket  = "tc-dlo-segments"
	s3Bucket          = "tc-s3"
	sloObject         = "slo"
	dloObject         = "dlo"
	singleObject      = "single"
	multipartObject   = "multipart"
	segmentNamePrefix = "segment-"
)

// sloCase is the gateway under test with its two clients, which act for the
// same user: the Swift storage URL and token, and the S3 signer.
type sloCase struct {
	ctx     context.Context
	gateway *rgw.Gateway
	http    *http.Client
	storage string
	token   string
	s3      harness.S3HTTPClient
}

// swift sends one Swift request for path below the storage URL.
func (c *sloCase) swift(t *testing.T, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(c.ctx, method, c.storage+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range headers {
		request.Header[key] = slices.Clone(values)
	}
	request.Header.Set("X-Auth-Token", c.token)
	return harness.RGWBackendDo(t, c.http, request)
}

func (c *sloCase) signed(t *testing.T, method, path string, payload []byte) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, c.ctx, c.s3, "s3", method, path, payload, nil)
}

// marker returns the prefix of the objects of bucket in the data pool.
func (c *sloCase) marker(t *testing.T, bucket string) string {
	t.Helper()
	marker, err := parseBucketMarker(harness.RGWBackendAdmin(t, c.ctx, c.gateway, "bucket", "stats", "--bucket", bucket))
	if err != nil {
		t.Fatalf("bucket %s: %v", bucket, err)
	}
	return marker + "_"
}

// requireHead requires the answer of a plain HEAD: 200, the payload's length
// and no body.
func requireHead(t *testing.T, r harness.RGWBackendResponse) {
	t.Helper()
	if length := r.Headers.Get("Content-Length"); r.Code != http.StatusOK || length != strconv.Itoa(sloPayloadSize) || len(r.Body) != 0 {
		t.Fatalf("condition not established: %s answered %d with Content-Length %q and %d body bytes, want 200, %d and none", r.Operation, r.Code, length, len(r.Body), sloPayloadSize)
	}
}

// requireGet requires the answer of a full GET: 200 and exactly payload.
func requireGet(t *testing.T, r harness.RGWBackendResponse, payload []byte) {
	t.Helper()
	if r.Code != http.StatusOK || !bytes.Equal(r.Body, payload) {
		t.Fatalf("condition not established: %s answered %d with %d bytes, want 200 and the %d bytes of the payload", r.Operation, r.Code, len(r.Body), len(payload))
	}
}

// TestRGWSLOHeadSegmentReads reports whether a plain HEAD of a Swift Static
// Large Object makes the gateway read the payload of every segment from
// RADOS.
//
// Given one gateway on one OSD, a user with an S3 key and a Swift subuser,
// the same 9 MiB payload stored as a static large object over three
// segments, as a dynamic large object over three segments, as an S3 single
// PUT and as an S3 multipart upload, each read back exactly, an OSD op
// history that keeps every operation, and the controls of the observation
// (a window without a request records nothing in the data pool; a HEAD of
// the single PUT, of the multipart upload and of the dynamic large object
// records operations on its head object and no data read; a full GET of the
// static large object records data reads that cover every segment), when
// one HEAD without a range or a query is sent for the static large object,
// then the defect is REPRODUCED if the OSD recorded data reads that cover
// every segment during it, and NOT_REPRODUCED if it recorded the request on
// the manifest and no data read on any segment. Tentacle 20.2.4 iterates the
// segments of a static large object before it looks at the request's method,
// so it reproduces; so do Squid 19.2.5 and Umbrella 21.2.0.
//
// A failure of the given or when steps or of a control fails the test, and
// so does a window that shows neither observation; the result does not.
func TestRGWSLOHeadSegmentReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("RGW_SLO_HEAD image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_SLO_HEAD gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Given: one user with an S3 key and a Swift subuser. The client opens a
	// connection per request, so no request is sent twice over a connection
	// that the gateway closed.
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: sloUser, DisplayName: "static large object case"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	swiftKey := harness.RGWBackendRandom(t, 20)
	harness.RGWBackendAdmin(t, ctx, gateway, "subuser", "create", "--uid", sloUser, "--subuser", sloUser+":swift", "--access", "full", "--key-type", "swift", "--secret-key", swiftKey)
	c := &sloCase{ctx: ctx, gateway: gateway, http: &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{DisableKeepAlives: true}}}
	c.s3 = harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: c.http}
	login, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/auth/1.0", nil)
	if err != nil {
		t.Fatal(err)
	}
	login.Header.Set("X-Auth-User", sloUser+":swift")
	login.Header.Set("X-Auth-Key", swiftKey)
	auth := harness.RGWBackendDo(t, c.http, login)
	storage, err := url.Parse(auth.Headers.Get("X-Storage-Url"))
	c.token = auth.Headers.Get("X-Auth-Token")
	if auth.Code/100 != 2 || err != nil || storage.Path == "" || c.token == "" {
		t.Fatalf("condition not established: Swift authentication answered %d without a storage URL or a token (%v)", auth.Code, err)
	}
	c.storage = endpoint + strings.TrimSuffix(storage.Path, "/")

	// The payload, and the four layouts that carry it.
	payload := make([]byte, sloPayloadSize)
	if _, err := rand.NewChaCha8([32]byte{1}).Read(payload); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []string{sloBucket, sloSegmentBucket, dloBucket, dloSegmentBucket, s3Bucket} {
		harness.RGWBackendRequire(t, c.swift(t, http.MethodPut, "/"+bucket, nil, nil), http.StatusCreated)
	}
	type manifestEntry struct {
		Path string `json:"path"`
		ETag string `json:"etag"`
		Size int    `json:"size_bytes"`
	}
	var manifest []manifestEntry
	for i := range sloSegments {
		name, data := fmt.Sprintf("%s%04d", segmentNamePrefix, i), payload[i*sloSegmentSize:(i+1)*sloSegmentSize]
		for _, bucket := range []string{sloSegmentBucket, dloSegmentBucket} {
			stored := c.swift(t, http.MethodPut, "/"+bucket+"/"+name, data, nil)
			harness.RGWBackendRequire(t, stored, http.StatusCreated)
			if bucket == sloSegmentBucket {
				manifest = append(manifest, manifestEntry{Path: "/" + bucket + "/" + name, ETag: strings.Trim(stored.Headers.Get("ETag"), `"`), Size: len(data)})
			}
		}
	}
	document, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	harness.RGWBackendRequire(t, c.swift(t, http.MethodPut, "/"+sloBucket+"/"+sloObject+"?multipart-manifest=put", document, nil), http.StatusCreated)
	harness.RGWBackendRequire(t, c.swift(t, http.MethodPut, "/"+dloBucket+"/"+dloObject, nil, http.Header{"X-Object-Manifest": {dloSegmentBucket + "/" + segmentNamePrefix}}), http.StatusCreated)
	harness.RGWBackendRequire(t, c.signed(t, http.MethodPut, "/"+s3Bucket+"/"+singleObject, payload), http.StatusOK)
	c.multipart(t, "/"+s3Bucket+"/"+multipartObject, payload[:sloFirstPart], payload[sloFirstPart:])

	// Every layout is read back, and the large objects are what they are
	// meant to be, before anything is measured.
	sloPath, dloPath := "/"+sloBucket+"/"+sloObject, "/"+dloBucket+"/"+dloObject
	for _, path := range []string{sloPath, dloPath} {
		requireGet(t, c.swift(t, http.MethodGet, path, nil, nil), payload)
	}
	for _, object := range []string{singleObject, multipartObject} {
		requireGet(t, c.signed(t, http.MethodGet, "/"+s3Bucket+"/"+object, nil), payload)
	}
	if got := c.swift(t, http.MethodHead, sloPath, nil, nil).Headers.Get("X-Static-Large-Object"); !strings.EqualFold(got, "true") {
		t.Fatalf("condition not established: %s is not a static large object (X-Static-Large-Object %q)", sloPath, got)
	}
	if got, want := c.swift(t, http.MethodHead, dloPath, nil, nil).Headers.Get("X-Object-Manifest"), dloSegmentBucket+"/"+segmentNamePrefix; got != want {
		t.Fatalf("condition not established: %s is not a dynamic large object over %s (X-Object-Manifest %q)", dloPath, want, got)
	}
	sloMarker, sloSegmentMarker := c.marker(t, sloBucket), c.marker(t, sloSegmentBucket)
	dloMarker, dloSegmentMarker, s3Marker := c.marker(t, dloBucket), c.marker(t, dloSegmentBucket), c.marker(t, s3Bucket)
	stored := strings.Fields(harness.ExecOutput(t, ctx, client, "timeout", "60", "rados", "-p", sloDataPool, "ls"))
	manifestObject := sloMarker + sloObject
	var segments []segment
	for i := range sloSegments {
		segments = append(segments, segment{Object: fmt.Sprintf("%s%s%04d", sloSegmentMarker, segmentNamePrefix, i), Size: sloSegmentSize})
	}
	for _, object := range append([]string{manifestObject, dloMarker + dloObject, s3Marker + singleObject, s3Marker + multipartObject}, segments[0].Object, segments[1].Object, segments[2].Object) {
		if !slices.Contains(stored, object) {
			t.Fatalf("condition not established: the data pool has no object %s: %v", object, stored)
		}
	}
	pool, err := cluster.Pool(ctx, sloDataPool)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pool.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// The observation: the OSD's history of client operations, which is
	// made to keep every operation of the test, and its counter of bytes
	// returned for reads.
	osd, err := harness.FindOSD(ctx, cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	for option, value := range map[string]int{"osd_op_history_size": sloHistorySize, "osd_op_history_duration": sloHistoryDuration} {
		if out, err := osd.AdminSocket(ctx, "config", "set", option, strconv.Itoa(value)); err != nil {
			t.Fatalf("set %s: %v: %s", option, err, out)
		}
	}
	history := func() opHistory {
		t.Helper()
		report, err := osd.AdminSocket(ctx, "dump_historic_ops")
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseOpHistory(report)
		if err != nil {
			t.Fatal(err)
		}
		if got.Size != sloHistorySize {
			t.Fatalf("condition not established: the op history keeps %d entries, want %d", got.Size, sloHistorySize)
		}
		return got
	}
	outBytes := func() int64 {
		t.Helper()
		report, err := osd.AdminSocket(ctx, "perf", "dump", "osd")
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseReadOutBytes(report)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	// window sends request, if any, and returns what the OSD recorded since
	// the window before it. It closes the window with an operation of its
	// own on a new object of the data pool, sent after the response, and
	// waits until the history holds it.
	seen, windows := map[string]bool{}, 0
	window := func(name string, request func()) sloWindow {
		t.Helper()
		windows++
		marker := fmt.Sprintf("tc-slo-window-%d", windows)
		before := outBytes()
		if request != nil {
			request()
		}
		harness.ExecOutput(t, ctx, client, "timeout", "60", "rados", "-p", sloDataPool, "create", marker)
		closed := func(op osdOp) bool { return op.Pool == state.ID && op.Object == marker }
		got := history()
		for deadline := time.Now().Add(time.Minute); !slices.ContainsFunc(got.Ops, closed); got = history() {
			if time.Now().After(deadline) {
				t.Fatalf("window %s not observed: the op history does not hold the operation on %s", name, marker)
			}
			time.Sleep(200 * time.Millisecond)
		}
		for _, op := range got.Ops {
			if windows > 1 && !seen[op.ReqID] && op.Pool == state.ID && op.Object != marker {
				t.Logf("RGW_SLO_HEAD window=%s object=%s operation=%v", name, op.Object, op.Subs)
			}
		}
		ops, err := windowOps(got, seen, state.ID, marker)
		if err != nil {
			t.Fatalf("window %s: %v", name, err)
		}
		recorded := sloWindow{Ops: ops, OutBytes: outBytes() - before}
		t.Logf("RGW_SLO_HEAD window=%s out-bytes=%d ops=%+v", name, recorded.OutBytes, recorded.Ops)
		return recorded
	}
	idle := func(name string) {
		t.Helper()
		if got := window(name, nil); len(got.Ops) != 0 {
			t.Fatalf("control not established: the window %s without a request recorded operations in the data pool: %+v", name, got.Ops)
		}
	}
	window("baseline", nil)
	idle("idle-before")
	// The HEADs that read no data, each through the protocol that created
	// the object.
	for _, control := range []struct {
		name, head string
		request    func() harness.RGWBackendResponse
		prefixes   []string
	}{
		{"head-single-put", s3Marker + singleObject, func() harness.RGWBackendResponse {
			return c.signed(t, http.MethodHead, "/"+s3Bucket+"/"+singleObject, nil)
		}, []string{s3Marker}},
		{"head-multipart", s3Marker + multipartObject, func() harness.RGWBackendResponse {
			return c.signed(t, http.MethodHead, "/"+s3Bucket+"/"+multipartObject, nil)
		}, []string{s3Marker}},
		{"head-dynamic-large-object", dloMarker + dloObject, func() harness.RGWBackendResponse { return c.swift(t, http.MethodHead, dloPath, nil, nil) }, []string{dloMarker, dloSegmentMarker}},
	} {
		got := window(control.name, func() { requireHead(t, control.request()) })
		if err := requireMetadataOnly(got.Ops, control.head, control.prefixes...); err != nil {
			t.Fatalf("%s: %v", control.name, err)
		}
	}
	// The positive control: a full GET of the static large object.
	get := window("get-static-large-object", func() { requireGet(t, c.swift(t, http.MethodGet, sloPath, nil, nil), payload) })
	idle("idle-between")

	// When: one plain HEAD of the static large object.
	head := window("head-static-large-object", func() { requireHead(t, c.swift(t, http.MethodHead, sloPath, nil, nil)) })

	// Then: nothing arrives late, and the HEAD window is judged against the
	// GET window.
	idle("idle-after")
	symptoms, err := judgeSLOHead(manifestObject, segments, get, head)
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	verdict := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		verdict = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_slo_head_segment_reads result=%s image=%s symptoms=%q", verdict, image, symptoms)
}

// multipart stores parts as one completed S3 multipart upload at path.
func (c *sloCase) multipart(t *testing.T, path string, parts ...[]byte) {
	t.Helper()
	started := harness.RGWBackendSigned(t, c.ctx, c.s3, "s3", http.MethodPost, path+"?uploads", nil, nil)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var initiated struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &initiated); err != nil || initiated.ID == "" {
		t.Fatalf("initiate %s: no upload ID (%v)", path, err)
	}
	var complete strings.Builder
	complete.WriteString("<CompleteMultipartUpload>")
	for i, part := range parts {
		r := c.signed(t, http.MethodPut, fmt.Sprintf("%s?partNumber=%d&uploadId=%s", path, i+1, url.QueryEscape(initiated.ID)), part)
		harness.RGWBackendRequire(t, r, http.StatusOK)
		fmt.Fprintf(&complete, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, r.Headers.Get("ETag"))
	}
	complete.WriteString("</CompleteMultipartUpload>")
	done := c.signed(t, http.MethodPost, path+"?uploadId="+url.QueryEscape(initiated.ID), []byte(complete.String()))
	harness.RGWBackendRequire(t, done, http.StatusOK)
	if classified, err := harness.RGWClassifyResultBody(done.Body, "CompleteMultipartUploadResult"); err != nil || classified.Error {
		t.Fatalf("complete %s: %+v %v", path, classified, err)
	}
}
