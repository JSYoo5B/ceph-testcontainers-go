//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func testRGWUserPlacementPolicy(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, control := newServiceCluster(t, options...)
	for _, pool := range []string{"tc-user-index", "tc-user-extra", "tc-user-standard", "tc-user-ia"} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rgw", PGNum: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "placement-user", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	maxBuckets := 12
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-placement-writer", DisplayName: "Placement fixture writer", Email: "writer@example.test", AdminCaps: "users=read", MaxBuckets: &maxBuckets})
	if err != nil {
		t.Fatal(err)
	}
	for _, quota := range []struct {
		set   func(context.Context, *rgw.User, rgw.Quota) error
		value rgw.Quota
	}{{gateway.SetUserQuota, rgw.Quota{Enabled: true, MaxSizeBytes: 4 << 20, MaxObjects: 20}}, {gateway.SetBucketQuota, rgw.Quota{Enabled: true, MaxSizeBytes: 2 << 20, MaxObjects: 10}}} {
		if err := quota.set(ctx, user, quota.value); err != nil {
			t.Fatal(err)
		}
	}
	baseline, err := gateway.UserInfo(ctx, user)
	if err != nil || baseline.DefaultPlacement != "" || len(baseline.PlacementTags) != 0 || baseline.Admin || baseline.System {
		t.Fatal("fresh ordinary fixture user had unexpected placement policy")
	}
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 20 * time.Second}}
	const original, granted, selected = "/tc-user-existing", "/tc-user-explicit-grant", "/tc-user-default-ia"
	oldPayload := []byte("original default bucket must retain its placement and object")
	client.request(t, ctx, http.MethodPut, original, nil, http.StatusOK)
	client.request(t, ctx, http.MethodPut, original+"/original", oldPayload, http.StatusOK)
	inline := false
	placement, err := gateway.CreatePlacement(ctx, rgw.PlacementConfig{Name: "tc-user-tiered", IndexPool: "tc-user-index", DataExtraPool: "tc-user-extra", InlineData: &inline, Tags: []string{"premium"},
		StorageClasses: []rgw.StorageClassConfig{{Name: "STANDARD", DataPool: "tc-user-standard"}, {Name: "STANDARD_IA", DataPool: "tc-user-ia"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyPlacement(ctx, placement); err != nil {
		t.Fatal(err)
	}
	client.endpoint, err = gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := rgwFixtureWaitStatus(t, ctx, client, original+"/original", http.StatusOK); !bytes.Equal(actual, oldPayload) {
		t.Fatal("activating fresh restricted target changed an existing object")
	}
	location, targetName := placement.LocationConstraint, placement.Name
	createBody := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + location + `</LocationConstraint></CreateBucketConfiguration>`)
	requireRGWUserPlacementDenied(t, client.request(t, ctx, http.MethodPut, "/tc-user-no-tag", createBody, http.StatusForbidden))
	if _, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "tc-user-no-tag"); err == nil {
		t.Fatal("denied creation left a native bucket")
	}
	// Public descriptor fields cannot redirect this owned native operation.
	placement.Name, placement.ZoneID = "caller-mutated", "caller-mutated"
	if err := gateway.SetUserPlacement(ctx, user, placement, rgw.UserPlacementConfig{Tags: []string{"premium"}}); err != nil {
		t.Fatal(err)
	}
	rgwPlacementWaitCreateBucket(t, ctx, client, granted, createBody)
	requireRGWUserBucketPlacement(t, ctx, gateway, granted, targetName)
	if err := gateway.SetUserPlacement(ctx, user, placement, rgw.UserPlacementConfig{StorageClass: "STANDARD_IA"}); err != nil {
		t.Fatal(err)
	}
	info, err := gateway.UserInfo(ctx, user)
	if err != nil || info.DefaultPlacement != targetName || info.DefaultStorageClass != "STANDARD_IA" || !slices.Equal(info.PlacementTags, []string{"premium"}) {
		t.Fatal("user default target/class or preserved tag grant did not read back")
	}
	requireRGWUserPlacementUnrelatedPolicy(t, baseline, info)
	// No LocationConstraint and no x-amz-storage-class: the server-side user
	// defaults must select both the target and STANDARD_IA.
	rgwPlacementWaitCreateBucket(t, ctx, client, selected, nil)
	requireRGWUserBucketPlacement(t, ctx, gateway, selected, targetName+"/STANDARD_IA")
	classPayload := bytes.Repeat([]byte("user-default-storage-class-native-pool\n"), 3072)
	client.request(t, ctx, http.MethodPut, selected+"/default-class", classPayload, http.StatusOK)
	if actual := client.request(t, ctx, http.MethodGet, selected+"/default-class", nil, http.StatusOK); !bytes.Equal(actual, classPayload) {
		t.Fatal("user-default STANDARD_IA object bytes changed")
	}
	standardPayload := bytes.Repeat([]byte("explicit-header-overrides-user-default\n"), 2048)
	rgwPlacementRequest(t, ctx, client, http.MethodPut, selected+"/explicit-standard", standardPayload, "STANDARD", http.StatusOK)
	var listing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.request(t, ctx, http.MethodGet, selected+"?list-type=2", nil, http.StatusOK), &listing); err != nil || len(listing.Contents) != 2 {
		t.Fatal("class fixture object listing is incomplete")
	}
	wantClass := map[string]string{"default-class": "STANDARD_IA", "explicit-standard": "STANDARD"}
	for _, object := range listing.Contents {
		if object.StorageClass != wantClass[object.Key] {
			t.Fatalf("S3 object %q reports class %q", object.Key, object.StorageClass)
		}
	}
	rgwPlacementPoolPayload(t, ctx, control, "tc-user-ia", "user-default STANDARD_IA", classPayload)
	rgwPlacementPoolPayload(t, ctx, control, "tc-user-standard", "explicit STANDARD", standardPayload)
	// Both previously created buckets retain their creation-time placement,
	// even after the user's default class changes.
	requireRGWUserBucketPlacement(t, ctx, gateway, original, "default-placement")
	requireRGWUserBucketPlacement(t, ctx, gateway, granted, targetName)
	if err := gateway.SetUserPlacement(ctx, user, placement, rgw.UserPlacementConfig{StorageClass: "STANDARD_IA", Tags: []string{"revoked"}}); err != nil {
		t.Fatal(err)
	}
	info, err = gateway.UserInfo(ctx, user)
	if err != nil || !slices.Equal(info.PlacementTags, []string{"revoked"}) {
		t.Fatal("matching placement grant survived nonmatching tag replacement")
	}
	requireRGWUserPlacementUnrelatedPolicy(t, baseline, info)
	// Native user metadata caches may lag the authoritative policy readback.
	// Probe fresh empty buckets, removing any transiently allowed creation,
	// until requests observe the new denial. Existing bucket data is retained.
	rgwUserPlacementWaitCreationDenied(t, ctx, client, createBody)
	for _, object := range []struct {
		path    string
		payload []byte
	}{{original + "/original", oldPayload}, {selected + "/default-class", classPayload}, {selected + "/explicit-standard", standardPayload}} {
		if actual := client.request(t, ctx, http.MethodGet, object.path, nil, http.StatusOK); !bytes.Equal(actual, object.payload) {
			t.Fatal("placement permission revocation changed existing object access or bytes")
		}
		client.request(t, ctx, http.MethodDelete, object.path, nil, http.StatusNoContent)
	}
	for _, bucket := range []string{original, granted, selected} {
		client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	}
	if err := gateway.RemoveUser(ctx, user); err != nil {
		t.Fatal(err)
	}
	t.Log("fresh target tags denied ungranted creation; grant allowed explicit location; user defaults selected STANDARD_IA without S3 selectors and exact native pool payload; explicit STANDARD override worked; prior buckets stayed immutable; revoke denied new buckets while existing objects/credentials remained usable and unrelated user policy stayed intact")
}

func requireRGWUserBucketPlacement(t *testing.T, ctx context.Context, gateway *rgw.Gateway, bucket, expected string) {
	t.Helper()
	data, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(bucket, "/"))
	var stats struct {
		PlacementRule string `json:"placement_rule"`
	}
	if err != nil || json.Unmarshal(data, &stats) != nil || stats.PlacementRule != expected {
		t.Fatalf("bucket %s placement=%q expected=%q error=%v", bucket, stats.PlacementRule, expected, err)
	}
}

func requireRGWUserPlacementUnrelatedPolicy(t *testing.T, before, after rgw.UserInfo) {
	t.Helper()
	after.DefaultPlacement, after.DefaultStorageClass, after.PlacementTags = before.DefaultPlacement, before.DefaultStorageClass, before.PlacementTags
	if !reflect.DeepEqual(before, after) {
		t.Fatal("changing user placement changed unrelated user identity, caps, quotas or flags")
	}
}

func requireRGWUserPlacementDenied(t *testing.T, body []byte) {
	t.Helper()
	var result struct{ Code string }
	if xml.Unmarshal(body, &result) != nil || result.Code != "AccessDenied" {
		t.Fatalf("restricted placement denial has unexpected code %q", result.Code)
	}
}

func rgwUserPlacementWaitCreationDenied(t *testing.T, parent context.Context, client s3HTTPClient, location []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	for attempt := 0; ; attempt++ {
		bucket := fmt.Sprintf("/tc-user-revoke-observation-%d", attempt)
		body := location
		if attempt%2 == 0 {
			body = nil // implicitly selected user default is also restricted
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, client.endpoint+bucket, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		client.sign(req, body, time.Now().UTC())
		response, err := client.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode == http.StatusForbidden {
			requireRGWUserPlacementDenied(t, data)
			for i, body := range [][]byte{nil, location} {
				requireRGWUserPlacementDenied(t, client.request(t, ctx, http.MethodPut, fmt.Sprintf("/tc-user-revoke-confirm-%d", i), body, http.StatusForbidden))
			}
			t.Logf("new bucket requests observed revoked placement permission after %d transient cache grants", attempt)
			return
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("revoked placement returned unexpected HTTP %d", response.StatusCode)
		}
		client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
		select {
		case <-ctx.Done():
			t.Fatalf("revoked placement continued to allow new buckets: %v", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
