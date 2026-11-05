//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This deliberately small test-only SigV4 client avoids adding a native client
// or an SDK dependency. Production consumers can use their ordinary S3 SDK.
type S3HTTPClient struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	HTTP      *http.Client
}

func (s S3HTTPClient) Request(t *testing.T, ctx context.Context, method, path string, payload []byte, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, s.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	s.Sign(req, payload, time.Now().UTC())
	response, err := s.HTTP.Do(req)
	if err != nil {
		t.Fatalf("S3 %s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("S3 %s %s returned %d, want %d: %s", method, path, response.StatusCode, wantStatus, body)
	}
	return body
}

func (s S3HTTPClient) ListKeys(t *testing.T, ctx context.Context, bucket string) []string {
	t.Helper()
	var listing struct {
		IsTruncated bool `xml:"IsTruncated"`
		Contents    []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(s.Request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
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

func (s S3HTTPClient) Sign(req *http.Request, payload []byte, now time.Time) {
	now = now.UTC()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := S3Hash(payload)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	const signedHeaders = "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + req.URL.Host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + stamp + "\n"
	canonicalQuery := strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20")
	canonicalRequest := strings.Join([]string{req.Method, S3CanonicalURI(req.URL.Path), canonicalQuery, canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := day + "/" + s.Region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + S3Hash([]byte(canonicalRequest))
	key := S3HMAC([]byte("AWS4"+s.SecretKey), day)
	key = S3HMAC(key, s.Region)
	key = S3HMAC(key, "s3")
	key = S3HMAC(key, "aws4_request")
	signature := hex.EncodeToString(S3HMAC(key, stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.AccessKey, scope, signedHeaders, signature))
}

// AWS4 encodes decoded UTF-8 path bytes using only unreserved characters and
// literal slashes. Go's URL.EscapedPath also permits reserved characters such
// as colon, so it cannot serve as the signed URI. Do not normalize S3 paths or
// encode URL.RawPath a second time; Ceph v20.2.4 rgw_auth_s3.h decodes once and
// applies these same encoding rules to its canonical URI.
func S3CanonicalURI(path string) string {
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

func S3Hash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func S3HMAC(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write([]byte(value))
	return hash.Sum(nil)
}

func RGWScenarioRequest(ctx context.Context, client S3HTTPClient, method, path string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	client.Sign(request, nil, time.Now().UTC())
	response, err := client.HTTP.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func RGWBackendErrorCode(body []byte) string {
	var failure struct {
		Code  string `xml:"Code" json:"Code"`
		Error struct {
			Code string `xml:"Code" json:"Code"`
		} `xml:"Error" json:"Error"`
	}
	if xml.Unmarshal(body, &failure) != nil {
		_ = json.Unmarshal(body, &failure)
	}
	if failure.Code != "" {
		return failure.Code
	}
	// Swift's default formatter is plain text. Only these known native codes
	// may become diagnostics; arbitrary bodies can contain sensitive data.
	if plain := strings.TrimSpace(string(body)); slices.Contains([]string{"AccessDenied", "NoSuchKey", "InvalidTenantName"}, plain) {
		return plain
	}
	return failure.Error.Code
}

func RequireRGWUserPlacementDenied(t *testing.T, body []byte) {
	t.Helper()
	var result struct{ Code string }
	if xml.Unmarshal(body, &result) != nil || result.Code != "AccessDenied" {
		t.Fatalf("restricted placement denial has unexpected code %q", result.Code)
	}
}

func RGWPlacementPoolPayload(t *testing.T, ctx context.Context, control testcontainers.Container, pool, class string, payload []byte) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	expectedHash := S3Hash(payload)
	code, reader, err := control.Exec(probeCtx, []string{"python3", "-c", rgwPlacementPoolPayloadScript, pool, strconv.Itoa(len(payload)), expectedHash}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	var proof struct {
		Pool, Object, SHA256 string
		Size                 int
	}
	if err != nil || code != 0 || json.Unmarshal(data, &proof) != nil || proof.Pool != pool || proof.Object == "" || proof.Size != len(payload) || proof.SHA256 != expectedHash {
		t.Fatalf("native %s data pool %s did not contain the exact S3 payload: %s error=%v", class, pool, data, err)
	}
	t.Logf("native %s data pool %s contains %d payload bytes SHA256=%s in object %s", class, pool, proof.Size, proof.SHA256, proof.Object)
}

// RGW shadow object names are opaque. Read native objects with their actual
// locator/namespace and prove their full content, using a fixture smaller than
// one RGW stripe. An empty head or a merely nonempty pool is insufficient proof.
const rgwPlacementPoolPayloadScript = `import hashlib, json, os, rados, sys, threading
pool, size, expected_hash = sys.argv[1:]
size = int(size)
assert 0 < size < 4 * 1024 * 1024, 'fixture must fit in one native RGW stripe'
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
match = None
observed = []
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin',
                 conf={'rados_mon_op_timeout': '15', 'rados_osd_op_timeout': '15'}) as cluster:
    with cluster.open_ioctx(pool) as io:
        for index, obj in enumerate(io.list_objects()):
            assert index < 32, 'unexpected object count in fresh placement data pool'
            native_size, _ = obj.stat()
            observed.append({'object': obj.key, 'size': native_size})
            if native_size != size:
                continue
            data = obj.read(size + 1)
            actual_hash = hashlib.sha256(data).hexdigest()
            if len(data) == size and actual_hash == expected_hash:
                match = {'pool': pool, 'object': obj.key, 'size': len(data), 'sha256': actual_hash}
                break
assert match is not None, 'exact S3 payload absent from actual pool: ' + json.dumps(observed)
deadline.cancel()
print(json.dumps(match, sort_keys=True))
`
