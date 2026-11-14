//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_ssec_completion)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_ssec_multipart_completion_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

const ssecBucket = "/tc-ssec-completion"

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_ssec_multipart_completion_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

// ssecKey is a customer key of the test. Only its headers leave the test;
// nothing logs them.
type ssecKey struct{ raw []byte }

func newSSECKey(t *testing.T) ssecKey {
	t.Helper()
	k := ssecKey{raw: make([]byte, 32)}
	if _, err := rand.Read(k.raw); err != nil {
		t.Fatal(err)
	}
	return k
}

// headers returns the SSE-C request headers with key k; copy-source
// variants are not needed by the case.
func (k ssecKey) headers() http.Header {
	digest := md5.Sum(k.raw)
	h := http.Header{}
	h.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	h.Set("X-Amz-Server-Side-Encryption-Customer-Key", base64.StdEncoding.EncodeToString(k.raw))
	h.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", base64.StdEncoding.EncodeToString(digest[:]))
	return h
}

type ssecCase struct {
	ctx    context.Context
	client harness.S3HTTPClient
	// uploads and objects record every upload ID and completed key right
	// after creation, so the cleanup aborts or deletes exactly what the test
	// owns.
	uploads map[string]string
	objects map[string]bool
}

func (c *ssecCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, headers)
}

// ssecUpload is one multipart upload of the case with a single part.
type ssecUpload struct {
	key      string
	id       string
	partETag string
	// partSHA256 is the part's base64 SHA-256 when the upload declares the
	// SHA256 checksum algorithm, empty otherwise.
	partSHA256 string
}

// start initiates an upload of key with the given initiation headers,
// uploads data as its only part with partHeaders and records the upload
// for cleanup. sha256Checksum adds the part checksum header.
func (c *ssecCase) start(t *testing.T, key string, data []byte, initHeaders, partHeaders http.Header, sha256Checksum bool) ssecUpload {
	t.Helper()
	path := ssecBucket + "/" + key
	init := initHeaders.Clone()
	if init == nil {
		init = http.Header{}
	}
	if sha256Checksum {
		init.Set("X-Amz-Checksum-Algorithm", "SHA256")
	}
	started := c.signed(t, c.ctx, http.MethodPost, path+"?uploads", nil, init)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var result struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &result); err != nil || result.ID == "" {
		t.Fatalf("multipart upload ID for %s: %v", path, err)
	}
	c.uploads[result.ID] = key
	u := ssecUpload{key: key, id: result.ID}
	t.Logf("RGW_SSEC upload=%s key=%s initiate-sse-c-algorithm=%q checksum-algorithm=%q", u.id, key,
		started.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"), started.Headers.Get("X-Amz-Checksum-Algorithm"))
	if initHeaders.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "" &&
		started.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "AES256" {
		t.Fatalf("condition not established: initiation of %s does not report SSE-C", path)
	}

	part := partHeaders.Clone()
	if part == nil {
		part = http.Header{}
	}
	if sha256Checksum {
		digest := sha256.Sum256(data)
		u.partSHA256 = base64.StdEncoding.EncodeToString(digest[:])
		part.Set("X-Amz-Checksum-Sha256", u.partSHA256)
	}
	uploaded := c.signed(t, c.ctx, http.MethodPut, path+"?partNumber=1&uploadId="+url.QueryEscape(u.id), data, part)
	harness.RGWBackendRequire(t, uploaded, http.StatusOK)
	u.partETag = uploaded.Headers.Get("ETag")
	if sha256Checksum && uploaded.Headers.Get("X-Amz-Checksum-Sha256") != u.partSHA256 {
		t.Fatalf("condition not established: part of %s returned checksum %q, want the sent SHA-256", path, uploaded.Headers.Get("X-Amz-Checksum-Sha256"))
	}

	// The upload's checksum algorithm, as RGW stored it, decides which
	// completion rule applies: the initiation echoes it, and ListParts
	// reports it for the upload or as the part's checksum.
	listed := c.signed(t, c.ctx, http.MethodGet, path+"?uploadId="+url.QueryEscape(u.id), nil, nil)
	harness.RGWBackendRequire(t, listed, http.StatusOK)
	var parts struct {
		ChecksumAlgorithm string `xml:"ChecksumAlgorithm"`
		Parts             []struct {
			Number         int    `xml:"PartNumber"`
			ETag           string `xml:"ETag"`
			Size           int64  `xml:"Size"`
			ChecksumSHA256 string `xml:"ChecksumSHA256"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(listed.Body, &parts); err != nil || len(parts.Parts) != 1 || parts.Parts[0].Size != int64(len(data)) {
		t.Fatalf("condition not established: parts of %s: %+v (%v)", path, parts.Parts, err)
	}
	declared := started.Headers.Get("X-Amz-Checksum-Algorithm")
	t.Logf("RGW_SSEC upload=%s list-parts checksum-algorithm=%q part-etag=%s part-checksum-sha256=%q size=%d",
		u.id, parts.ChecksumAlgorithm, parts.Parts[0].ETag, parts.Parts[0].ChecksumSHA256, parts.Parts[0].Size)
	switch {
	case sha256Checksum && (declared != "SHA256" || parts.Parts[0].ChecksumSHA256 != u.partSHA256 || parts.ChecksumAlgorithm != "" && parts.ChecksumAlgorithm != "SHA256"):
		t.Fatalf("condition not established: upload of %s does not keep the SHA256 checksum algorithm (initiation %q, listed %q)", path, declared, parts.ChecksumAlgorithm)
	case !sha256Checksum && (declared != "" || parts.ChecksumAlgorithm != "" || parts.Parts[0].ChecksumSHA256 != ""):
		t.Fatalf("condition not established: upload of %s without a checksum reports one (initiation %q, listed %q)", path, declared, parts.ChecksumAlgorithm)
	}
	return u
}

// complete sends CompleteMultipartUpload for u with headers. A 200 whose
// body is an S3 Error document counts as a failed completion, with its HTTP
// status kept. A failed upload is aborted at once, so a failed completion
// is never reused by another step; a 200 that is neither a result nor an
// error is aborted too, and its key is kept for cleanup in case it was
// completed, before the test fails.
func (c *ssecCase) complete(t *testing.T, u ssecUpload, headers http.Header) ssecCompletion {
	t.Helper()
	path := ssecBucket + "/" + u.key
	var body strings.Builder
	body.WriteString("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + u.partETag + "</ETag>")
	if u.partSHA256 != "" {
		body.WriteString("<ChecksumSHA256>" + u.partSHA256 + "</ChecksumSHA256>")
	}
	body.WriteString("</Part></CompleteMultipartUpload>")
	done := c.signed(t, c.ctx, http.MethodPost, path+"?uploadId="+url.QueryEscape(u.id), []byte(body.String()), headers)
	result, unclassified := ssecClassifyCompletion(done.Code, done.Body)
	switch {
	case unclassified != nil:
		c.objects[u.key] = true
		aborted := c.signed(t, c.ctx, http.MethodDelete, path+"?uploadId="+url.QueryEscape(u.id), nil, nil)
		harness.RGWBackendRequire(t, aborted, http.StatusNoContent, http.StatusNotFound)
		delete(c.uploads, u.id)
	case result.completed():
		delete(c.uploads, u.id)
		c.objects[u.key] = true
	default:
		aborted := c.signed(t, c.ctx, http.MethodDelete, path+"?uploadId="+url.QueryEscape(u.id), nil, nil)
		harness.RGWBackendRequire(t, aborted, http.StatusNoContent)
		delete(c.uploads, u.id)
	}
	t.Logf("RGW_SSEC upload=%s complete status=%d embedded-error=%t code=%s message=%q etag=%s checksum-sha256=%q",
		u.id, result.Status, result.Embedded, result.Failure.Code, result.Failure.Message, result.ETag, result.ChecksumSHA256)
	if unclassified != nil {
		t.Fatalf("completion of %s: %v", path, unclassified)
	}
	return result
}

// requireBytes reads key with a fresh request and headers and requires
// exactly data.
func (c *ssecCase) requireBytes(t *testing.T, key string, headers http.Header, data []byte) harness.RGWBackendResponse {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodGet, ssecBucket+"/"+key, nil, headers)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	if !bytes.Equal(r.Body, data) {
		t.Fatalf("GET %s: %d bytes that differ from the %d uploaded", key, len(r.Body), len(data))
	}
	return r
}

// requireEncryptedAtRest requires that key reads its bytes only with key k:
// without customer headers RGW must refuse the GET, and with k it must
// decrypt to data and report SSE-C.
func (c *ssecCase) requireEncryptedAtRest(t *testing.T, key string, k ssecKey, data []byte) {
	t.Helper()
	plain := c.signed(t, c.ctx, http.MethodGet, ssecBucket+"/"+key, nil, nil)
	t.Logf("RGW_SSEC object=%s get-without-key status=%d code=%s", key, plain.Code, harness.RGWBackendErrorCode(plain.Body))
	if plain.Code != http.StatusBadRequest {
		t.Fatalf("GET %s without the customer key answered %d, want 400: the object is not stored with SSE-C", key, plain.Code)
	}
	r := c.requireBytes(t, key, k.headers(), data)
	if r.Headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "AES256" {
		t.Fatalf("GET %s with the customer key does not report SSE-C", key)
	}
}

// requireAbsent requires that no object key exists after a refused
// completion.
func (c *ssecCase) requireAbsent(t *testing.T, key string, k ssecKey) {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodHead, ssecBucket+"/"+key, nil, k.headers())
	if r.Code != http.StatusNotFound {
		t.Fatalf("HEAD %s after a refused completion answered %d, want 404", key, r.Code)
	}
}

// cleanup aborts every upload and deletes every object the test still owns,
// then the bucket, when buckets (the ones the test created) holds it.
// Failures are reported as cleanup failures, separately from the
// behavioural result.
func (c *ssecCase) cleanup(t *testing.T, buckets []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	if !slices.Contains(buckets, ssecBucket) {
		t.Logf("RGW_SSEC cleanup buckets=0 uploads=%d objects=%d failed=false", len(c.uploads), len(c.objects))
		return
	}
	for id, key := range c.uploads {
		if r := c.signed(t, ctx, http.MethodDelete, ssecBucket+"/"+key+"?uploadId="+url.QueryEscape(id), nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_SSEC cleanup failed: abort upload %s of %s: %d", id, key, r.Code)
			failed = true
		}
	}
	for key := range c.objects {
		if r := c.signed(t, ctx, http.MethodDelete, ssecBucket+"/"+key, nil, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_SSEC cleanup failed: delete %s: %d", key, r.Code)
			failed = true
		}
	}
	if r := c.signed(t, ctx, http.MethodDelete, ssecBucket, nil, nil); r.Code != http.StatusNoContent {
		t.Errorf("RGW_SSEC cleanup failed: delete bucket: %d %s", r.Code, harness.RGWBackendErrorCode(r.Body))
		failed = true
	}
	t.Logf("RGW_SSEC cleanup buckets=%d uploads=%d objects=%d failed=%t", len(buckets), len(c.uploads), len(c.objects), failed)
}

// ssecCompositeSHA256 is the checksum S3 reports for a completed multipart
// upload with SHA256: the SHA-256 of the concatenated part digests, with the
// part count.
func ssecCompositeSHA256(t *testing.T, parts ...string) string {
	t.Helper()
	var joined []byte
	for _, p := range parts {
		raw, err := base64.StdEncoding.DecodeString(p)
		if err != nil {
			t.Fatal(err)
		}
		joined = append(joined, raw...)
	}
	digest := sha256.Sum256(joined)
	return fmt.Sprintf("%s-%d", base64.StdEncoding.EncodeToString(digest[:]), len(parts))
}

// TestRGWSSECMultipartCompletion reports whether RGW refuses to complete an
// SSE-C multipart upload without an additional checksum when the completion
// carries no customer-key headers, which S3 completes.
//
// Given a gateway on verified HTTPS and the controls (an unencrypted upload
// completes; with no checksum, the correct key completes and another valid
// key is refused by the key MD5 check; with the SHA256 checksum algorithm,
// the completion needs the key and answers the composite checksum), when an
// SSE-C upload without a checksum is completed without the key, then the
// defect is REPRODUCED if the completion is refused, and NOT_REPRODUCED if
// it completes and the object reads back encrypted with the key. Tentacle
// 20.2.4 decrypts the upload's metadata while it parses every completion, so
// it answers 400 InvalidArgument and reproduces; Tentacle 20.2.5 and Umbrella
// 21.2.0 do not.
//
// A failure of the given or when steps, or of a control, fails the test;
// the result does not.
func TestRGWSSECMultipartCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	t.Logf("RGW_SSEC image=%s", image)
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_SSEC ceph=%s", harness.StaticFacts(cluster).CephVersion)
	// SSE-C requires a secure transport (rgw_crypt_require_ssl stays at its
	// default), so every request goes over verified HTTPS.
	material, roots := harness.RGWTestCertificate(t)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := cluster.Ceph(ctx, "versions")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_SSEC daemons=%s", strings.Join(strings.Fields(string(versions)), " "))
	t.Logf("RGW_SSEC gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	endpoint, err := gateway.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	c := &ssecCase{ctx: ctx, uploads: map[string]string{}, objects: map[string]bool{},
		client: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region,
			HTTP: &http.Client{Transport: transport, Timeout: 30 * time.Second}}}
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) { c.cleanup(t, created) }, ssecBucket); err != nil {
		t.Fatal(err)
	}

	payload := func(name string) []byte {
		return bytes.Repeat([]byte("ssec multipart "+name+" payload\n"), 512)
	}
	owner, other := newSSECKey(t), newSSECKey(t)

	t.Run("unencrypted-control", func(t *testing.T) {
		data := payload("plain")
		u := c.start(t, "plain", data, nil, nil, false)
		if done := c.complete(t, u, nil); !done.completed() {
			t.Fatalf("unencrypted completion: status %d embedded-error %t %s", done.Status, done.Embedded, done.Failure.Code)
		}
		c.requireBytes(t, "plain", nil, data)
	})

	t.Run("no-checksum-correct-key", func(t *testing.T) {
		data := payload("correct key")
		u := c.start(t, "no-checksum-correct-key", data, owner.headers(), owner.headers(), false)
		if done := c.complete(t, u, owner.headers()); !done.completed() {
			t.Fatalf("completion with the correct key: status %d embedded-error %t %s %q", done.Status, done.Embedded, done.Failure.Code, done.Failure.Message)
		}
		c.requireEncryptedAtRest(t, "no-checksum-correct-key", owner, data)
	})

	t.Run("no-checksum-wrong-key", func(t *testing.T) {
		data := payload("wrong key")
		u := c.start(t, "no-checksum-wrong-key", data, owner.headers(), owner.headers(), false)
		done := c.complete(t, u, other.headers())
		// The other key is well formed and its MD5 is correct, so only the
		// comparison with the stored key MD5 can refuse it.
		if done.Status != http.StatusBadRequest || done.Failure.Code != "InvalidArgument" || !strings.Contains(done.Failure.Message, "MD5") {
			t.Fatalf("completion with another valid key: status %d embedded-error %t %s %q, want 400 InvalidArgument from the key MD5 check", done.Status, done.Embedded, done.Failure.Code, done.Failure.Message)
		}
		c.requireAbsent(t, "no-checksum-wrong-key", owner)
	})

	// Each symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	t.Run("no-checksum-omitted-key", func(t *testing.T) {
		data := payload("omitted key")
		u := c.start(t, "no-checksum-omitted-key", data, owner.headers(), owner.headers(), false)
		done := c.complete(t, u, nil)
		if !done.completed() {
			c.requireAbsent(t, "no-checksum-omitted-key", owner)
			symptoms = append(symptoms, fmt.Sprintf("completion without a checksum and without the customer key: status %d embedded-error %t %s %q, S3 wants a completed 200", done.Status, done.Embedded, done.Failure.Code, done.Failure.Message))
			return
		}
		c.requireEncryptedAtRest(t, "no-checksum-omitted-key", owner, data)
	})

	t.Run("sha256-omitted-key", func(t *testing.T) {
		data := payload("sha256 omitted key")
		u := c.start(t, "sha256-omitted-key", data, owner.headers(), owner.headers(), true)
		done := c.complete(t, u, nil)
		if done.Status != http.StatusBadRequest || done.Failure.Code != "InvalidArgument" {
			t.Fatalf("completion of a SHA256 SSE-C upload without the customer key: status %d embedded-error %t %s %q, want 400 InvalidArgument", done.Status, done.Embedded, done.Failure.Code, done.Failure.Message)
		}
		c.requireAbsent(t, "sha256-omitted-key", owner)
	})

	t.Run("sha256-correct-key", func(t *testing.T) {
		data := payload("sha256 correct key")
		u := c.start(t, "sha256-correct-key", data, owner.headers(), owner.headers(), true)
		done := c.complete(t, u, owner.headers())
		if !done.completed() {
			t.Fatalf("completion of a SHA256 SSE-C upload with the correct key: status %d embedded-error %t %s %q", done.Status, done.Embedded, done.Failure.Code, done.Failure.Message)
		}
		if want := ssecCompositeSHA256(t, u.partSHA256); done.ChecksumSHA256 != want {
			t.Errorf("completion checksum %q, want the composite SHA-256 %q", done.ChecksumSHA256, want)
		}
		c.requireEncryptedAtRest(t, "sha256-correct-key", owner, data)
	})
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_ssec_multipart_completion result=%s image=%s symptoms=%q", result, image, symptoms)
}
