//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t)
	rgw, err := cluster.StartRGW(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := rgw.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	user, err := rgw.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-rgw-storage-check"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	client := s3HTTPClient{
		endpoint: endpoint, accessKey: access, secretKey: secret, region: rgw.Region,
		http: &http.Client{Timeout: 45 * time.Second},
	}
	const bucket = "/tc-rgw-poc"
	client.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	var buckets struct {
		Buckets []struct {
			Name string `xml:"Name"`
		} `xml:"Buckets>Bucket"`
	}
	if err := xml.Unmarshal(client.request(t, ctx, http.MethodGet, "/", nil, http.StatusOK), &buckets); err != nil {
		t.Fatal(err)
	}
	if len(buckets.Buckets) != 1 || buckets.Buckets[0].Name != strings.TrimPrefix(bucket, "/") {
		t.Fatalf("unexpected S3 bucket listing: %+v", buckets)
	}

	rgwFixtureUserUsage(t, ctx, rgw, user, "zero-before-writes", 0, 0)

	payload := bytes.Repeat([]byte("RGW signed S3 roundtrip\n"), 4096)
	keys := []string{"payload-0", "payload-1", "nested/payload-2", "nested/payload-3"}
	for _, key := range keys {
		client.request(t, ctx, http.MethodPut, bucket+"/"+key, payload, http.StatusOK)
	}
	verify := func() {
		t.Helper()
		for _, key := range keys {
			actual := client.request(t, ctx, http.MethodGet, bucket+"/"+key, nil, http.StatusOK)
			if !bytes.Equal(actual, payload) {
				t.Fatalf("S3 object %s payload changed: got %d bytes, want %d", key, len(actual), len(payload))
			}
		}
		listed := client.listKeys(t, ctx, bucket)
		if !slices.Equal(listed, keys) {
			t.Fatalf("S3 list: got %v, want %v", listed, keys)
		}
	}
	slices.Sort(keys)
	verify()
	rgwFixtureUserUsage(t, ctx, rgw, user, "four-objects", uint64(len(keys)), uint64(len(keys)*len(payload)))

	// Private buckets must reject both unsigned and incorrectly signed requests.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+bucket+"/"+keys[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unsigned private S3 read returned %d, want 403", response.StatusCode)
	}
	wrongKey := client
	wrongKey.secretKey += "wrong"
	wrongKey.request(t, ctx, http.MethodGet, bucket+"/"+keys[0], nil, http.StatusForbidden)
	t.Logf("host HTTP S3 client: created bucket, signed PUT/GET/ListObjectsV2 for %d objects; invalid/absent credentials rejected", len(keys))

	advanceServiceTopology(t, ctx, cluster)
	verify()
	client.request(t, ctx, http.MethodPut, bucket+"/after-topology", payload, http.StatusOK)
	if actual := client.request(t, ctx, http.MethodGet, bucket+"/after-topology", nil, http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("S3 write/read after topology change changed payload")
	}
	t.Log("S3 payloads survived OSD add/remove; new writes/read after replacement succeeded")
	rgwFixtureUserUsage(t, ctx, rgw, user, "after-topology-five-objects", uint64(len(keys)+1), uint64((len(keys)+1)*len(payload)))

	for _, key := range append(keys, "after-topology") {
		client.request(t, ctx, http.MethodDelete, bucket+"/"+key, nil, http.StatusNoContent)
	}
	client.request(t, ctx, http.MethodGet, bucket+"/payload-0", nil, http.StatusNotFound)
	if listed := client.listKeys(t, ctx, bucket); len(listed) != 0 {
		t.Fatalf("S3 delete left objects: %v", listed)
	}
	rgwFixtureUserUsage(t, ctx, rgw, user, "zero-after-object-delete", 0, 0)
	client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	t.Log("S3 object deletion, missing-object 404, empty listing and bucket deletion succeeded")
}

// Explicit synchronization prepares deterministic native accounting for the
// fixture assertion. UserUsage itself performs only read-only stats queries.
func rgwFixtureUserUsage(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, user *ceph.RGWUser, stage string, objects, sizeBytes uint64) {
	t.Helper()
	if _, err := gateway.Admin(ctx, "user", "stats", "--uid", user.ID(), "--sync-stats"); err != nil {
		t.Fatal(err)
	}
	usage, err := gateway.UserUsage(ctx, user)
	if err != nil || usage.UserID != user.ID() || usage.Scope != "user" || usage.OwnerID != user.ID() || usage.NumObjects != objects || usage.SizeBytes != sizeBytes {
		t.Fatalf("native user storage assertion %s: usage=%+v error=%v expected_objects=%d expected_bytes=%d", stage, usage, err, objects, sizeBytes)
	}
	t.Logf("RGW_USER_USAGE stage=%s scope=%s owner_id=%q user_id=%q num_objects=%d size_bytes=%d size_actual_bytes=%d", stage, usage.Scope, usage.OwnerID, usage.UserID, usage.NumObjects, usage.SizeBytes, usage.SizeActualBytes)
}

// This deliberately small test-only SigV4 client avoids adding a native client
// or an SDK dependency. Production consumers can use their ordinary S3 SDK.
type s3HTTPClient struct {
	endpoint  string
	accessKey string
	secretKey string
	region    string
	http      *http.Client
}

func (s s3HTTPClient) request(t *testing.T, ctx context.Context, method, path string, payload []byte, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, s.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	s.sign(req, payload, time.Now().UTC())
	response, err := s.http.Do(req)
	if err != nil {
		t.Fatalf("S3 %s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("S3 %s %s returned %d, want %d: %s", method, path, response.StatusCode, wantStatus, body)
	}
	return body
}

func (s s3HTTPClient) listKeys(t *testing.T, ctx context.Context, bucket string) []string {
	t.Helper()
	var listing struct {
		IsTruncated bool `xml:"IsTruncated"`
		Contents    []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(s.request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	if listing.IsTruncated {
		t.Fatal("small S3 test listing was unexpectedly truncated")
	}
	keys := make([]string, 0, len(listing.Contents))
	for _, object := range listing.Contents {
		keys = append(keys, object.Key)
	}
	slices.Sort(keys)
	return keys
}

func (s s3HTTPClient) sign(req *http.Request, payload []byte, now time.Time) {
	now = now.UTC()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := s3Hash(payload)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	const signedHeaders = "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + req.URL.Host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + stamp + "\n"
	canonicalQuery := strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20")
	canonicalRequest := strings.Join([]string{req.Method, s3CanonicalURI(req.URL.Path), canonicalQuery, canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := day + "/" + s.region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + s3Hash([]byte(canonicalRequest))
	key := s3HMAC([]byte("AWS4"+s.secretKey), day)
	key = s3HMAC(key, s.region)
	key = s3HMAC(key, "s3")
	key = s3HMAC(key, "aws4_request")
	signature := hex.EncodeToString(s3HMAC(key, stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.accessKey, scope, signedHeaders, signature))
}

// AWS4 encodes decoded UTF-8 path bytes using only unreserved characters and
// literal slashes. Go's URL.EscapedPath also permits reserved characters such
// as colon, so it cannot serve as the signed URI. Do not normalize S3 paths or
// encode URL.RawPath a second time; Ceph v20.2.4 rgw_auth_s3.h decodes once and
// applies these same encoding rules to its canonical URI.
func s3CanonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	const uppercaseHex = "0123456789ABCDEF"
	var canonical strings.Builder
	canonical.Grow(len(path))
	for i := range len(path) {
		c := path[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' || c == '/' {
			canonical.WriteByte(c)
		} else {
			canonical.WriteByte('%')
			canonical.WriteByte(uppercaseHex[c>>4])
			canonical.WriteByte(uppercaseHex[c&15])
		}
	}
	return canonical.String()
}

func s3Hash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func s3HMAC(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write([]byte(value))
	return hash.Sum(nil)
}
