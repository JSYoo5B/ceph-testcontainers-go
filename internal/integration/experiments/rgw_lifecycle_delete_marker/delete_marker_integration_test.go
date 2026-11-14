//go:build all || (integration && features && (!ci || (ci_topology && (!ci_batch || ci_batch_rgw_lifecycle_delete_marker))))

//ci: timeout=15m job-timeout=25 image-roles=essential,client,rgw

package rgw_lifecycle_delete_marker_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func TestRGWLifecycleDeleteMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_enable_lc_threads", Value: "false"},
		{Section: "client.admin", Name: "rgw_lc_debug_interval", Value: "2"},
	} {
		if _, err := override.SetConfig(ctx, cluster, setting); err != nil {
			t.Fatal(err)
		}
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := harness.S3HTTPClient{Endpoint: endpoint, Region: gateway.Region, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, HTTP: &http.Client{Timeout: 30 * time.Second}}
	const bucket = "lc-marker-inventory"
	req := func(method, suffix, data string, status int) ([]byte, http.Header) {
		t.Helper()
		return harness.S3FeatureRequest(t, ctx, client, method, "/"+bucket+suffix, []byte(data), nil, status)
	}
	req(http.MethodPut, "", "", http.StatusOK)
	t.Cleanup(func() {
		clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := gateway.Admin(clean, "bucket", "rm", "--bucket", bucket, "--purge-objects"); err != nil {
			t.Error(err)
		}
	})
	req(http.MethodPut, "?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", http.StatusOK)
	type version struct {
		Key    string
		ID     string `xml:"VersionId"`
		Latest bool   `xml:"IsLatest"`
	}
	versionOf := func(method, key, data string, latest bool) version {
		t.Helper()
		status := http.StatusOK
		if method == http.MethodDelete {
			status = http.StatusNoContent
		}
		_, h := req(method, "/"+key, data, status)
		id := h.Get("X-Amz-Version-Id")
		if id == "" || id == "null" {
			t.Fatalf("%s %s returned no version ID", method, key)
		}
		if method == http.MethodDelete && h.Get("X-Amz-Delete-Marker") != "true" {
			t.Fatalf("DELETE %s did not create a marker", key)
		}
		return version{key, id, latest}
	}
	eligible := versionOf(http.MethodDelete, "marker/alone", "", true)
	held := versionOf(http.MethodPut, "marker/held", "underlying data", false)
	ineligible := versionOf(http.MethodDelete, "marker/held", "", true)
	old := versionOf(http.MethodPut, "witness", "old data", false)
	current := versionOf(http.MethodPut, "witness", "current data", true)
	inventory := func(markers, data []version) {
		t.Helper()
		raw, _ := req(http.MethodGet, "?versions", "", http.StatusOK)
		var got struct {
			XMLName   xml.Name  `xml:"ListVersionsResult"`
			Truncated bool      `xml:"IsTruncated"`
			Markers   []version `xml:"DeleteMarker"`
			Data      []version `xml:"Version"`
		}
		if err := xml.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.Truncated || len(got.Data) != len(data) || len(got.Markers) != len(markers) {
			t.Fatalf("unexpected inventory: %s", raw)
		}
		for _, v := range data {
			if !slices.Contains(got.Data, v) {
				t.Fatalf("want data %+v in %s", v, raw)
			}
		}
		for _, v := range markers {
			if !slices.Contains(got.Markers, v) {
				t.Fatalf("want marker %+v in %s", v, raw)
			}
		}
	}
	inventory([]version{eligible, ineligible}, []version{held, old, current})
	const rules = `<LifecycleConfiguration>
 <Rule><ID>markers</ID><Prefix>marker/</Prefix><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>
 <Rule><ID>witness</ID><Prefix>witness</Prefix><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule>
 </LifecycleConfiguration>`
	req(http.MethodPut, "?lifecycle", rules, http.StatusOK)
	// Two debug days ensure the witness's noncurrent version has aged a full day.
	time.Sleep(4 * time.Second)
	harness.RGWBackendAdmin(t, ctx, gateway, "lc", "process", "--bucket", bucket)
	inventory([]version{ineligible}, []version{held, current})
}
