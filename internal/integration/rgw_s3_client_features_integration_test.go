//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
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

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWS3ClientFeatures(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testRGWS3ClientFeatures(t, host) })
	}
}

func testRGWS3ClientFeatures(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, _ := newServiceCluster(t, opts...)
	// Native test clock: one lifecycle day is two seconds. Disable only the
	// background worker; explicitly process our fresh bucket with the native CLI.
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_lc_debug_interval", Value: "2"},
		{Section: "client.admin", Name: "rgw_enable_lc_threads", Value: "false"},
	} {
		change, err := cluster.TemporaryConfig(ctx, setting)
		if change != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
				defer stop()
				if err := change.Restore(cleanup); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	users := make([]*ceph.RGWUser, 2)
	clients := make([]s3HTTPClient, 2)
	for i, id := range []string{"tc-s3-owner", "tc-s3-reader"} {
		users[i], err = gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		access, secret, err := users[i].Credentials()
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 20 * time.Second}}
	}
	owner, reader := clients[0], clients[1]
	req := func(client s3HTTPClient, method, path string, payload []byte, headers http.Header, want int) ([]byte, http.Header) {
		t.Helper()
		return s3FeatureRequest(t, ctx, client, method, path, payload, headers, want)
	}
	const versionBucket = "/tc-s3-versions"
	req(owner, http.MethodPut, versionBucket, nil, nil, http.StatusOK)
	req(owner, http.MethodPut, versionBucket+"?versioning", []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`), nil, http.StatusOK)
	first := []byte("frozen version one")
	second := []byte("current version two")
	_, head1 := req(owner, http.MethodPut, versionBucket+"/versioned", first, nil, http.StatusOK)
	_, head2 := req(owner, http.MethodPut, versionBucket+"/versioned", second, nil, http.StatusOK)
	v1, v2 := head1.Get("X-Amz-Version-Id"), head2.Get("X-Amz-Version-Id")
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatal("native S3 version IDs not distinct")
	}
	for _, version := range []struct {
		id   string
		data []byte
	}{{v1, first}, {v2, second}} {
		actual, _ := req(owner, http.MethodGet, versionBucket+"/versioned?versionId="+url.QueryEscape(version.id), nil, nil, http.StatusOK)
		if !bytes.Equal(actual, version.data) {
			t.Fatal("native prior version bytes changed")
		}
	}
	_, marker := req(owner, http.MethodDelete, versionBucket+"/versioned", nil, nil, http.StatusNoContent)
	if marker.Get("X-Amz-Delete-Marker") != "true" || marker.Get("X-Amz-Version-Id") == "" {
		t.Fatal("native delete marker missing")
	}
	req(owner, http.MethodGet, versionBucket+"/versioned", nil, nil, http.StatusNotFound)
	listing, _ := req(owner, http.MethodGet, versionBucket+"?versions", nil, nil, http.StatusOK)
	var versions struct {
		Versions []struct {
			ID string `xml:"VersionId"`
		} `xml:"Version"`
		Markers []struct {
			ID string `xml:"VersionId"`
		} `xml:"DeleteMarker"`
	}
	if xml.Unmarshal(listing, &versions) != nil || len(versions.Versions) != 2 || len(versions.Markers) != 1 {
		t.Fatalf("native version listing: %s", listing)
	}
	for _, id := range []string{v1, v2, marker.Get("X-Amz-Version-Id")} {
		req(owner, http.MethodDelete, versionBucket+"/versioned?versionId="+url.QueryEscape(id), nil, nil, http.StatusNoContent)
	}
	req(owner, http.MethodDelete, versionBucket, nil, nil, http.StatusNoContent)

	const bucket = "/tc-s3-protocol"
	req(owner, http.MethodPut, bucket, nil, nil, http.StatusOK)
	upload := func(key string) string {
		t.Helper()
		data, _ := req(owner, http.MethodPost, bucket+"/"+key+"?uploads", nil, nil, http.StatusOK)
		var state struct {
			ID string `xml:"UploadId"`
		}
		if xml.Unmarshal(data, &state) != nil || state.ID == "" {
			t.Fatalf("native multipart upload ID: %s", data)
		}
		return state.ID
	}
	id := upload("multipart")
	parts := [][]byte{bytes.Repeat([]byte("abcde"), 1<<20), bytes.Repeat([]byte("tail"), 4096)}
	var completion strings.Builder
	completion.WriteString("<CompleteMultipartUpload>")
	for i, part := range parts {
		_, headers := req(owner, http.MethodPut, fmt.Sprintf("%s/multipart?partNumber=%d&uploadId=%s", bucket, i+1, url.QueryEscape(id)), part, nil, http.StatusOK)
		etag := headers.Get("ETag")
		if etag == "" {
			t.Fatal("native multipart part ETag missing")
		}
		fmt.Fprintf(&completion, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, etag)
	}
	completion.WriteString("</CompleteMultipartUpload>")
	done, _ := req(owner, http.MethodPost, bucket+"/multipart?uploadId="+url.QueryEscape(id), []byte(completion.String()), nil, http.StatusOK)
	if bytes.Contains(done, []byte("<Error>")) || !bytes.Contains(done, []byte("<CompleteMultipartUploadResult")) {
		t.Fatalf("native multipart completion body: %s", done)
	}
	joined := bytes.Join(parts, nil)
	actual, _ := req(owner, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusOK)
	if !bytes.Equal(actual, joined) {
		t.Fatal("multipart assembled bytes differ")
	}
	toAbort := upload("aborted")
	req(owner, http.MethodPut, bucket+"/aborted?partNumber=1&uploadId="+url.QueryEscape(toAbort), []byte("owned incomplete part"), nil, http.StatusOK)
	req(owner, http.MethodDelete, bucket+"/aborted?uploadId="+url.QueryEscape(toAbort), nil, nil, http.StatusNoContent)
	uploads, _ := req(owner, http.MethodGet, bucket+"?uploads", nil, nil, http.StatusOK)
	var pending struct {
		Uploads []struct {
			ID string `xml:"UploadId"`
		} `xml:"Upload"`
	}
	if xml.Unmarshal(uploads, &pending) != nil || len(pending.Uploads) != 0 {
		t.Fatal("completed/aborted upload still native pending")
	}
	req(owner, http.MethodGet, bucket+"/aborted", nil, nil, http.StatusNotFound)

	// ACL grant/revoke and bucket policy are client operations; provisioning
	// ordinary distinct principals is the module's responsibility.
	req(reader, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusForbidden)
	req(owner, http.MethodPut, bucket+"/multipart?acl", nil, http.Header{"X-Amz-Acl": []string{"public-read"}}, http.StatusOK)
	actual, _ = req(reader, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusOK)
	if !bytes.Equal(actual, joined) {
		t.Fatal("ACL permitted wrong bytes")
	}
	req(owner, http.MethodPut, bucket+"/multipart?acl", nil, http.Header{"X-Amz-Acl": []string{"private"}}, http.StatusOK)
	req(reader, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusForbidden)
	policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + users[1].ID()}, "Action": "s3:GetObject", "Resource": "arn:aws:s3:::" + strings.TrimPrefix(bucket, "/") + "/multipart"}}})
	req(owner, http.MethodPut, bucket+"?policy", policy, nil, http.StatusNoContent)
	actual, _ = req(reader, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusOK)
	if !bytes.Equal(actual, joined) {
		t.Fatal("bucket policy permitted wrong bytes")
	}
	req(reader, http.MethodPut, bucket+"/denied-write", []byte("policy excludes write"), nil, http.StatusForbidden)
	req(owner, http.MethodDelete, bucket+"?policy", nil, nil, http.StatusNoContent)
	req(reader, http.MethodGet, bucket+"/multipart", nil, nil, http.StatusForbidden)
	req(owner, http.MethodDelete, bucket+"/multipart", nil, nil, http.StatusNoContent)
	req(owner, http.MethodDelete, bucket, nil, nil, http.StatusNoContent)
	rgwS3LifecycleAndObjectLock(t, ctx, gateway, owner, users[0])
	for _, user := range users {
		if err := gateway.RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("native S3 versions/frozen bytes/delete markers, 5 MiB multipart assembly and abort, ACL grant/revoke, ordinary-user bucket policy grant/revoke and denied writes; all client-owned data and users explicitly removed")
}

func s3FeatureRequest(t *testing.T, ctx context.Context, client s3HTTPClient, method, path string, payload []byte, headers http.Header, want int) ([]byte, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(payload))
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
	s3FeatureSign(req, client, payload, time.Now().UTC(), "s3")
	response, err := client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
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
func s3FeatureSign(req *http.Request, client s3HTTPClient, payload []byte, now time.Time, service string) {
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := s3Hash(payload)
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
	scope := day + "/" + client.region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + s3Hash([]byte(canonicalRequest))
	key := s3HMAC([]byte("AWS4"+client.secretKey), day)
	key = s3HMAC(key, client.region)
	key = s3HMAC(key, service)
	key = s3HMAC(key, "aws4_request")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", client.accessKey, scope, signedHeaders, hex.EncodeToString(s3HMAC(key, stringToSign))))
}
