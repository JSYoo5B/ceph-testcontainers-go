//go:build native_regression && (all || (integration && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_encrypted_multipart_transition)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_encrypted_multipart_transition_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func TestRGWEncryptedMultipartTransition(t *testing.T) {
	t.Run("selected-release-controls", func(t *testing.T) {
		cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
		checkTransition(t, cluster, false)
	})
	for _, row := range []struct{ release, image string }{
		{"20.2.2", "quay.io/ceph/ceph:v20.2.2"},
		{"20.2.4", "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"},
	} {
		t.Run(row.release, func(t *testing.T) {
			cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, row.image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
			checkTransition(t, cluster, true)
		})
	}
}

func checkTransition(t *testing.T, cluster *ceph.Container, transitionEncrypted bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_enable_lc_threads", Value: "false"},
		{Section: "client.admin", Name: "rgw_lc_debug_interval", Value: "2"},
	} {
		if _, err := override.SetConfig(ctx, cluster, setting); err != nil {
			t.Fatal(err)
		}
	}
	const pool = "lc.cold"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rgw", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	material, roots := harness.RGWTestCertificate(t)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	harness.RGWBackendAdmin(t, ctx, gateway, "zonegroup", "placement", "add", "--placement-id", "default-placement", "--storage-class", "COLD")
	harness.RGWBackendAdmin(t, ctx, gateway, "zone", "placement", "add", "--placement-id", "default-placement", "--storage-class", "COLD", "--data-pool", pool)
	grace := 3 * time.Second
	if err := gateway.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ep, err := gateway.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := harness.S3HTTPClient{Endpoint: ep, Region: gateway.Region, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, HTTP: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
	const bucket = "lc-encrypted-layout"
	req := func(method, path string, data []byte, h http.Header, code int) ([]byte, http.Header) {
		t.Helper()
		return harness.S3FeatureRequest(t, ctx, client, method, "/"+bucket+path, data, h, code)
	}
	req(http.MethodPut, "", nil, nil, http.StatusOK)
	t.Cleanup(func() {
		clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := gateway.Admin(clean, "bucket", "rm", "--bucket", bucket, "--purge-objects"); err != nil {
			t.Error(err)
		}
	})
	t.Logf("gateway: %s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	key := bytes.Repeat([]byte{'k'}, 32)
	digest := md5.Sum(key)
	headers := http.Header{}
	headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	headers.Set("X-Amz-Server-Side-Encryption-Customer-Key", base64.StdEncoding.EncodeToString(key))
	headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", base64.StdEncoding.EncodeToString(digest[:]))
	const firstSize = 5<<20 + 3071
	parts := [][]byte{bytes.Repeat([]byte("abc123XY"), (firstSize+7)/8)[:firstSize], bytes.Repeat([]byte("different"), 9000)}
	whole := bytes.Join(parts, nil)
	upload := func(path string, headers http.Header) {
		t.Helper()
		raw, h := req(http.MethodPost, path+"?uploads", nil, headers, http.StatusOK)
		var start struct {
			ID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal(raw, &start); err != nil || start.ID == "" {
			t.Fatalf("%s: missing upload ID: %v", path, err)
		}
		if h.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") {
			t.Fatalf("%s: initiation returned an unexpected encryption algorithm", path)
		}
		query := "?uploadId=" + url.QueryEscape(start.ID)
		var complete strings.Builder
		complete.WriteString("<CompleteMultipartUpload>")
		for i, part := range parts {
			_, h := req(http.MethodPut, path+query+fmt.Sprintf("&partNumber=%d", i+1), part, headers, http.StatusOK)
			tag := h.Get("ETag")
			if tag == "" {
				t.Fatalf("%s: part %d missing ETag", path, i+1)
			}
			fmt.Fprintf(&complete, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, tag)
		}
		complete.WriteString("</CompleteMultipartUpload>")
		raw, _ = req(http.MethodPost, path+query, []byte(complete.String()), headers, http.StatusOK)
		result, err := harness.RGWClassifyResultBody(raw, "CompleteMultipartUploadResult")
		if err != nil || result.Error {
			t.Fatalf("%s: completion result %+v: %v", path, result, err)
		}
	}
	check := func(path, wantClass string, headers http.Header) {
		t.Helper()
		_, head := req(http.MethodHead, path, nil, headers, http.StatusOK)
		class := head.Get("X-Amz-Storage-Class")
		if class == "" {
			class = "STANDARD"
		}
		algorithm := headers.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm")
		if class != wantClass || head.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != algorithm {
			t.Fatalf("%s: storage=%q SSE-C=%q, want %s and %q", path, class, head.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"), wantClass, algorithm)
		}
		data, _ := req(http.MethodGet, path, nil, headers, http.StatusOK)
		if !bytes.Equal(data, whole) {
			t.Fatalf("%s in %s: full content mismatch (%d bytes)", path, class, len(data))
		}
		first, last := len(parts[0])-32, len(parts[0])+128
		ranged := headers.Clone()
		ranged.Set("Range", fmt.Sprintf("bytes=%d-%d", first, last))
		data, response := req(http.MethodGet, path, nil, ranged, http.StatusPartialContent)
		if response.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", first, last, len(whole)) || !bytes.Equal(data, whole[first:last+1]) {
			t.Fatalf("%s in %s: cross-part range %q (%d bytes) differs", path, class, response.Get("Content-Range"), len(data))
		}
		t.Logf("%s class=%s SSE-C=%q full=200/%d bytes range=206/%d bytes", path, class, algorithm, len(whole), len(data))
	}
	const selected = "/move/object"
	const control = "/keep/object"
	selectedHeaders := http.Header{}
	if transitionEncrypted {
		selectedHeaders = headers
	}
	upload(selected, selectedHeaders)
	check(selected, "STANDARD", selectedHeaders)
	upload(control, headers)
	check(control, "STANDARD", headers)
	rule := []byte(`<LifecycleConfiguration><Rule><ID>move</ID><Prefix>move/</Prefix><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>COLD</StorageClass></Transition></Rule></LifecycleConfiguration>`)
	req(http.MethodPut, "?lifecycle", rule, nil, http.StatusOK)
	// Age the objects beyond a full two-second debug day before the manual pass.
	time.Sleep(4 * time.Second)
	harness.RGWBackendAdmin(t, ctx, gateway, "lc", "process", "--bucket", bucket)
	check(selected, "COLD", selectedHeaders)
	check(control, "STANDARD", headers)
}
