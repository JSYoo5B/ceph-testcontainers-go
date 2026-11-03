//go:build all || (integration && features)

package harness

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func S3FeatureRequest(t *testing.T, ctx context.Context, client S3HTTPClient, method, path string, payload []byte, headers http.Header, want int) ([]byte, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range headers {
		req.Header[name] = slices.Clone(values)
	}
	if len(payload) > 0 {
		digest := md5.Sum(payload)
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(digest[:]))
	}
	S3FeatureSign(req, client, payload, time.Now().UTC(), "s3")
	response, err := client.HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("S3 feature %s %s returned %d want %d: %s", method, path, response.StatusCode, want, body)
	}
	return body, response.Header.Clone()
}

// Sign every x-amz header, including ACL, retention and temporary session-token
// headers. Keep authentication material out of error output.
func S3FeatureSign(req *http.Request, client S3HTTPClient, payload []byte, now time.Time, service string) {
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := S3Hash(payload)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	names := []string{"host"}
	for name := range req.Header {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, "x-amz-") || name == "content-md5" || name == "content-type" {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
	var canonical strings.Builder
	for _, name := range names {
		value := req.Header.Get(name)
		if name == "host" {
			value = req.URL.Host
		}
		canonical.WriteString(name + ":" + strings.Join(strings.Fields(value), " ") + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonicalQuery := strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20")
	canonicalRequest := strings.Join([]string{req.Method, req.URL.EscapedPath(), canonicalQuery, canonical.String(), signedHeaders, payloadHash}, "\n")
	scope := day + "/" + client.Region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + S3Hash([]byte(canonicalRequest))
	key := S3HMAC([]byte("AWS4"+client.SecretKey), day)
	key = S3HMAC(key, client.Region)
	key = S3HMAC(key, service)
	key = S3HMAC(key, "aws4_request")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", client.AccessKey, scope, signedHeaders, hex.EncodeToString(S3HMAC(key, stringToSign))))
}
