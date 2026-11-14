//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_multipart_completion_replay_etag)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_multipart_completion_replay_etag_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all"

func TestMultipartCompletionReplayETag(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	material, roots := harness.RGWTestCertificate(t)
	g, err := rgw.Start(ctx, cluster, rgw.Config{TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := g.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: g.AccessKey, SecretKey: g.SecretKey, Region: g.Region, HTTP: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
	const bucket = "tc-replay-etag"
	const path = "/" + bucket + "/object"
	if err := harness.RGWBackendBucketCreator(t, ctx, client)(bucket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := g.Admin(cleanup, "bucket", "rm", "--bucket", bucket, "--purge-objects"); err != nil {
			t.Error(err)
		}
	})
	begin := harness.RGWBackendSigned(t, ctx, client, "s3", "POST", path+"?uploads", nil, nil)
	harness.RGWBackendRequire(t, begin, 200)
	var init struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(begin.Body, &init); err != nil || init.ID == "" {
		t.Fatalf("upload ID: %q, %v", init.ID, err)
	}
	query := path + "?uploadId=" + url.QueryEscape(init.ID)
	payload := []byte("multipart replay body")
	part := harness.RGWBackendSigned(t, ctx, client, "s3", "PUT", query+"&partNumber=1", payload, nil)
	harness.RGWBackendRequire(t, part, 200)
	sum := md5.Sum(payload)
	partTag := part.Headers.Get("ETag")
	if partTag != fmt.Sprintf("\"%x\"", sum) {
		t.Fatalf("unexpected part ETag %q", partTag)
	}
	expected := fmt.Sprintf("\"%x-1\"", md5.Sum(sum[:]))
	body := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + partTag + "</ETag></Part></CompleteMultipartUpload>")
	request, err := http.NewRequestWithContext(ctx, "POST", endpoint+query, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	harness.S3FeatureSign(request, client, body, time.Now().UTC(), "s3")
	complete := func() (string, []string) {
		t.Helper()
		req := request.Clone(ctx)
		req.Body, err = request.GetBody()
		if err != nil {
			t.Fatal(err)
		}
		r := harness.RGWBackendDo(t, client.HTTP, req)
		harness.RGWBackendRequire(t, r, 200)
		var result struct {
			XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
			ETag    *string  `xml:"ETag"`
		}
		if err := xml.Unmarshal(r.Body, &result); err != nil || result.ETag == nil {
			t.Fatalf("completion XML %q: %v", r.Body, err)
		}
		t.Logf("HTTP %d ETag headers=%q; XML ETag=%q", r.Code, r.Headers.Values("ETag"), *result.ETag)
		return *result.ETag, r.Headers.Values("ETag")
	}
	first, firstHeaders := complete()
	if first != expected {
		t.Fatalf("first XML ETag=%q, want %q", first, expected)
	}
	replay, replayHeaders := complete()
	if !slices.Equal(firstHeaders, []string{expected}) {
		t.Logf("HTTP ETag comparison unavailable: first headers=%q, expected one %q", firstHeaders, expected)
	}
	get := harness.RGWBackendSigned(t, ctx, client, "s3", "GET", path, nil, nil)
	harness.RGWBackendRequire(t, get, 200)
	if !bytes.Equal(get.Body, payload) || get.Headers.Get("ETag") != expected {
		t.Fatal("stored object payload or ETag differs from completed upload")
	}
	verdict, symptoms, err := replayETagVerdict(expected, replay, firstHeaders, replayHeaders)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_multipart_completion_replay_etag result=%s image=%s symptoms=%q", verdict, image, symptoms)
}
