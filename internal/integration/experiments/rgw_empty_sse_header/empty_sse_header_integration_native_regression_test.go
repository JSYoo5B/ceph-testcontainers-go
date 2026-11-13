//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_empty_sse_header)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_empty_sse_header_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

const sseHeader = "X-Amz-Server-Side-Encryption"

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_empty_sse_header_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

type sseCase struct {
	ctx    context.Context
	client harness.S3HTTPClient
	// objects records every key that a request may have created, accepted
	// or not, so the cleanup deletes exactly what the test owns.
	objects map[string]bool
}

func (c *sseCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, headers)
}

// sseHeaders returns headers with the SSE algorithm header present with
// value, which may be empty.
func sseHeaders(value string) http.Header {
	return http.Header{sseHeader: {value}}
}

type sseFailure struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// sseResult is the native answer to one PUT.
type sseResult struct {
	Code       int
	Failure    sseFailure
	Encryption string
	VersionID  string
}

func (c *sseCase) put(t *testing.T, path string, data []byte, headers http.Header) sseResult {
	t.Helper()
	c.objects[path] = true
	r := c.signed(t, c.ctx, http.MethodPut, path, data, headers)
	result := sseResult{Code: r.Code, Encryption: r.Headers.Get(sseHeader), VersionID: r.Headers.Get("X-Amz-Version-Id")}
	if r.Code != http.StatusOK {
		_ = xml.Unmarshal(r.Body, &result.Failure)
	}
	sent, present := headers[sseHeader]
	t.Logf("RGW_SSE put=%s header-present=%t header=%q status=%d code=%s message=%q response-sse=%q version=%q",
		path, present, strings.Join(sent, ","), r.Code, result.Failure.Code, result.Failure.Message, result.Encryption, result.VersionID)
	return result
}

// get reads path with a fresh request; Data is nil for 404 NoSuchKey.
type sseObject struct {
	Data       []byte
	ETag       string
	Encryption string
	VersionID  string
}

func (c *sseCase) get(t *testing.T, path string) sseObject {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodGet, path, nil, nil)
	switch {
	case r.Code == http.StatusOK:
		return sseObject{Data: r.Body, ETag: r.Headers.Get("ETag"), Encryption: r.Headers.Get(sseHeader), VersionID: r.Headers.Get("X-Amz-Version-Id")}
	case r.Code == http.StatusNotFound && harness.RGWBackendErrorCode(r.Body) == "NoSuchKey":
		return sseObject{}
	default:
		t.Fatalf("fresh GET %s: status=%d code=%s", path, r.Code, harness.RGWBackendErrorCode(r.Body))
		return sseObject{}
	}
}

type sseVersion struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
	ETag      string `xml:"ETag"`
	IsLatest  bool   `xml:"IsLatest"`
}

// versions lists every version and delete marker under prefix.
func (c *sseCase) versions(t *testing.T, bucket, prefix string) (versions, markers []sseVersion) {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodGet, bucket+"?versions&prefix="+url.QueryEscape(prefix), nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var listing struct {
		IsTruncated bool         `xml:"IsTruncated"`
		Versions    []sseVersion `xml:"Version"`
		Markers     []sseVersion `xml:"DeleteMarker"`
	}
	if err := xml.Unmarshal(r.Body, &listing); err != nil || listing.IsTruncated {
		t.Fatalf("version listing of %s/%s: truncated=%t %v", bucket, prefix, listing.IsTruncated, err)
	}
	t.Logf("RGW_SSE versions bucket=%s prefix=%s versions=%+v markers=%+v", bucket, prefix, listing.Versions, listing.Markers)
	return listing.Versions, listing.Markers
}

// requireNoDefaultEncryption requires that bucket has no default
// encryption, so no implicit default can explain a result.
func (c *sseCase) requireNoDefaultEncryption(t *testing.T, bucket string) {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodGet, bucket+"?encryption", nil, nil)
	code := harness.RGWBackendErrorCode(r.Body)
	t.Logf("RGW_SSE bucket=%s get-encryption status=%d code=%s", bucket, r.Code, code)
	if r.Code != http.StatusNotFound || code != "ServerSideEncryptionConfigurationNotFoundError" {
		t.Fatalf("condition not established: bucket %s has a default encryption configuration (status %d %s)", bucket, r.Code, code)
	}
}

// refused reports whether r is the validation refusal that the invalid
// algorithm control also gets: 400 InvalidArgument with the same message,
// never a SigV4 refusal.
func refused(r sseResult, control sseFailure) bool {
	return r.Code == http.StatusBadRequest && r.Failure.Code == "InvalidArgument" && r.Failure.Message == control.Message
}

// cleanup deletes the versions the test owns in each bucket of buckets, the
// ones the test created, and then the bucket.
func (c *sseCase) cleanup(t *testing.T, buckets []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	for _, bucket := range buckets {
		r := c.signed(t, ctx, http.MethodGet, bucket+"?versions", nil, nil)
		var listing struct {
			Versions []sseVersion `xml:"Version"`
			Markers  []sseVersion `xml:"DeleteMarker"`
		}
		if r.Code != http.StatusOK || xml.Unmarshal(r.Body, &listing) != nil {
			t.Errorf("RGW_SSE cleanup failed: list %s: %d", bucket, r.Code)
			failed = true
			continue
		}
		for _, v := range append(listing.Versions, listing.Markers...) {
			if !c.objects[bucket+"/"+v.Key] {
				t.Errorf("RGW_SSE cleanup: %s/%s is not owned by the test; left in place", bucket, v.Key)
				failed = true
				continue
			}
			if d := c.signed(t, ctx, http.MethodDelete, bucket+"/"+v.Key+"?versionId="+url.QueryEscape(v.VersionID), nil, nil); d.Code != http.StatusNoContent {
				t.Errorf("RGW_SSE cleanup failed: delete %s/%s version %s: %d", bucket, v.Key, v.VersionID, d.Code)
				failed = true
			}
		}
		if d := c.signed(t, ctx, http.MethodDelete, bucket, nil, nil); d.Code != http.StatusNoContent {
			t.Errorf("RGW_SSE cleanup failed: delete bucket %s: %d %s", bucket, d.Code, harness.RGWBackendErrorCode(d.Body))
			failed = true
		}
	}
	t.Logf("RGW_SSE cleanup buckets=%d objects=%d failed=%t", len(buckets), len(c.objects), failed)
}

// requireWireHeader sends the empty-header PUT through the same signer and
// transport to a local listener and requires that the request carries the
// header with an empty value and signs it. A client that drops empty headers
// would make the native result say nothing about the empty value.
func requireWireHeader(t *testing.T, client harness.S3HTTPClient) {
	t.Helper()
	type seen struct {
		present bool
		value   string
		signed  string
	}
	got := make(chan seen, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values, present := r.Header[sseHeader]
		got <- seen{present: present, value: strings.Join(values, ","), signed: r.Header.Get("Authorization")}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	local := client
	local.Endpoint = server.URL
	harness.RGWBackendSigned(t, t.Context(), local, "s3", http.MethodPut, "/wire/empty", []byte("wire"), sseHeaders(""))
	s := <-got
	_, signedHeaders, _ := strings.Cut(s.signed, "SignedHeaders=")
	signedHeaders, _, _ = strings.Cut(signedHeaders, ",")
	signed := slices.Contains(strings.Split(signedHeaders, ";"), "x-amz-server-side-encryption")
	t.Logf("RGW_SSE wire header-present=%t value=%q signed=%t", s.present, s.value, signed)
	if !s.present || s.value != "" || !signed {
		t.Fatalf("condition not established: the client did not send and sign an empty %s header", sseHeader)
	}
}

// TestRGWEmptySSEHeader reports whether RGW accepts a PUT whose
// x-amz-server-side-encryption header is present with an empty value,
// instead of refusing it as it refuses an invalid algorithm.
//
// Given a gateway whose buckets have no default encryption, a client that
// sends and signs the empty header, and the controls (an invalid algorithm is
// refused and creates nothing; an omitted header writes an ordinary object),
// when the empty header is sent for a new key and over an existing version,
// then the defect is REPRODUCED if either PUT is accepted or changes what is
// stored, and NOT_REPRODUCED if both get the control's refusal and nothing
// changes. Tentacle 20.2.4 tests the header's value instead of its presence,
// so it reproduces; Tentacle 20.2.5 and Umbrella 21.2.0 do not.
//
// A failure of the given or when steps fails the test; the result does not.
func TestRGWEmptySSEHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	t.Logf("RGW_SSE image=%s", image)
	cluster, _ := harness.NewServiceClusterWithOptions(t, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_SSE ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := cluster.Ceph(ctx, "versions")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_SSE daemons=%s", strings.Join(strings.Fields(string(versions)), " "))
	t.Logf("RGW_SSE gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := &sseCase{ctx: ctx, objects: map[string]bool{},
		client: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}}
	requireWireHeader(t, c.client)

	const bucket, versioned = "/tc-sse-empty", "/tc-sse-empty-versioned"
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) { c.cleanup(t, created) }, bucket, versioned); err != nil {
		t.Fatal(err)
	}
	c.requireNoDefaultEncryption(t, bucket)
	c.requireNoDefaultEncryption(t, versioned)

	// The invalid nonempty algorithm is refused by every tested release; its
	// message is the validation refusal the empty header must also get.
	invalid := c.put(t, bucket+"/invalid", []byte("invalid algorithm"), sseHeaders("AES-INVALID"))
	if invalid.Code != http.StatusBadRequest || invalid.Failure.Code != "InvalidArgument" || invalid.Failure.Message == "" {
		t.Fatalf("control not established: invalid algorithm answered %d %s %q", invalid.Code, invalid.Failure.Code, invalid.Failure.Message)
	}
	if c.get(t, bucket+"/invalid").Data != nil {
		t.Fatal("control not established: the refused invalid-algorithm PUT created an object")
	}

	t.Run("omitted", func(t *testing.T) {
		data := []byte("header omitted")
		if r := c.put(t, bucket+"/omitted", data, nil); r.Code != http.StatusOK || r.Encryption != "" {
			t.Fatalf("PUT without the header: status %d, response encryption %q", r.Code, r.Encryption)
		}
		if got := c.get(t, bucket+"/omitted"); !bytes.Equal(got.Data, data) || got.Encryption != "" {
			t.Fatalf("GET of the object written without the header: %d bytes, encryption %q", len(got.Data), got.Encryption)
		}
	})

	// Each symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	t.Run("empty-new-key", func(t *testing.T) {
		data := []byte("header present and empty")
		r := c.put(t, bucket+"/empty", data, sseHeaders(""))
		got := c.get(t, bucket+"/empty")
		t.Logf("RGW_SSE empty-new-key stored=%t bytes-match=%t etag=%s encryption=%q", got.Data != nil, bytes.Equal(got.Data, data), got.ETag, got.Encryption)
		if !refused(r, invalid.Failure) {
			symptoms = append(symptoms, fmt.Sprintf("new key: status %d %s, not the control's 400 InvalidArgument", r.Code, r.Failure.Code))
		}
		if got.Data != nil {
			symptoms = append(symptoms, fmt.Sprintf("new key: the PUT left an object (encryption %q)", got.Encryption))
		}
	})

	t.Run("empty-overwrite", func(t *testing.T) {
		state := c.signed(t, ctx, http.MethodPut, versioned+"?versioning", []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`), nil)
		harness.RGWBackendRequire(t, state, http.StatusOK)
		seed := []byte("seed bytes")
		seeded := c.put(t, versioned+"/key", seed, nil)
		if seeded.Code != http.StatusOK || seeded.VersionID == "" || seeded.VersionID == "null" {
			t.Fatalf("condition not established: seed PUT answered %d with version %q", seeded.Code, seeded.VersionID)
		}
		before := c.get(t, versioned+"/key")
		r := c.put(t, versioned+"/key", []byte("replacement from an empty header"), sseHeaders(""))
		after := c.get(t, versioned+"/key")
		if !refused(r, invalid.Failure) {
			symptoms = append(symptoms, fmt.Sprintf("overwrite: status %d %s, not the control's 400 InvalidArgument", r.Code, r.Failure.Code))
		}
		if !bytes.Equal(after.Data, seed) || after.ETag != before.ETag || after.VersionID != seeded.VersionID {
			symptoms = append(symptoms, fmt.Sprintf("overwrite: the key changed, version %q -> %q", seeded.VersionID, after.VersionID))
		}
		if list, markers := c.versions(t, versioned, "key"); len(list) != 1 || len(markers) != 0 || list[0].VersionID != seeded.VersionID {
			symptoms = append(symptoms, fmt.Sprintf("overwrite: %d versions and %d delete markers, want only %q", len(list), len(markers), seeded.VersionID))
		}
	})
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_empty_sse_header result=%s image=%s symptoms=%q", result, image, symptoms)
}
