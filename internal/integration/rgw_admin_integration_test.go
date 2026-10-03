//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWUserAdministration(t *testing.T) {
	testRGWUserAdministration(t)
}

func TestHostNetworkRGWUserAdministration(t *testing.T) {
	testRGWUserAdministration(t, ceph.WithHostNetwork())
}

func testRGWUserAdministration(t *testing.T, customizers ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, customizers...)
	gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{Name: "fixture", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create := func(id, caps string) (*ceph.RGWUser, s3HTTPClient) {
		t.Helper()
		user, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: id, AdminCaps: caps})
		if err != nil {
			t.Fatal(err)
		}
		access, secret, err := user.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return user, s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 15 * time.Second}}
	}
	alice, aliceS3 := create("tc-fixture-alice", "")
	bob, bobS3 := create("tc-fixture-bob", "")
	observer, observerS3 := create("tc-fixture-observer", "users=read")
	if _, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: alice.ID(), DisplayName: "overwrite"}); err == nil {
		t.Fatal("existing RGW identity was accepted")
	}
	const bucket = "/tc-rgw-internal-alice"
	const bobBucket = "/tc-rgw-internal-bob"
	payload := []byte("RGW owned user fixture roundtrip")
	aliceS3.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	aliceS3.request(t, ctx, http.MethodPut, bucket+"/payload", payload, http.StatusOK)
	bobS3.request(t, ctx, http.MethodGet, bucket+"/payload", nil, http.StatusForbidden)
	bobS3.request(t, ctx, http.MethodPut, bobBucket, nil, http.StatusOK)
	bobS3.request(t, ctx, http.MethodPut, bobBucket+"/payload", payload, http.StatusOK)
	t.Log("two independent ordinary S3 users: own writes succeeded, cross-user private object access denied")

	// A scoped Admin Ops capability enables client-side rgw/admin testing
	// without granting Ceph's global RGW admin or multisite system flag.
	aliceS3.request(t, ctx, http.MethodGet, "/admin/user?uid="+alice.ID(), nil, http.StatusForbidden)
	var nativeAdminInfo struct {
		ID string `json:"user_id"`
	}
	if err := json.Unmarshal(observerS3.request(t, ctx, http.MethodGet, "/admin/user?uid="+alice.ID(), nil, http.StatusOK), &nativeAdminInfo); err != nil || nativeAdminInfo.ID != alice.ID() {
		t.Fatal("explicit users=read capability did not expose the expected Admin Ops user info")
	}
	observerInfo, err := gateway.UserInfo(ctx, observer)
	if err != nil {
		t.Fatal(err)
	}
	if observerInfo.Admin || observerInfo.System || !slices.ContainsFunc(observerInfo.AdminCaps, func(cap ceph.RGWAdminCapability) bool {
		return cap.Type == "users" && cap.Permission == "read"
	}) {
		t.Fatal("Admin Ops user did not retain only its explicitly requested capability")
	}
	t.Log("ordinary user Admin Ops denied; explicit users=read permits Admin Ops GET without global admin/system flags")

	userQuota := ceph.RGWQuota{Enabled: true, MaxSizeBytes: 1 << 20, MaxObjects: 8}
	bucketQuota := ceph.RGWQuota{Enabled: true, MaxSizeBytes: 1 << 19, MaxObjects: 4}
	if err := gateway.SetUserQuota(ctx, alice, userQuota); err != nil {
		t.Fatal(err)
	}
	if err := gateway.SetBucketQuota(ctx, alice, bucketQuota); err != nil {
		t.Fatal(err)
	}
	aliceInfo, err := gateway.UserInfo(ctx, alice)
	if err != nil || aliceInfo.UserQuota != userQuota || aliceInfo.BucketQuota != bucketQuota || aliceInfo.Admin || aliceInfo.System {
		t.Fatal("RGW native quota policy did not persist with the expected byte/object limits")
	}
	t.Log("user and per-bucket quota byte/object limits read back from authoritative native user policy; cache enforcement timing left to RGW")

	if err := gateway.SuspendUser(ctx, alice, true); err != nil {
		t.Fatal(err)
	}
	rgwFixtureWaitStatus(t, ctx, aliceS3, bucket+"/payload", http.StatusForbidden)
	bobS3.request(t, ctx, http.MethodPut, bobBucket+"/while-alice-suspended", payload, http.StatusOK)
	if info, err := gateway.UserInfo(ctx, alice); err != nil || !info.Suspended {
		t.Fatal("native suspension flag did not persist")
	}
	if err := gateway.SuspendUser(ctx, alice, false); err != nil {
		t.Fatal(err)
	}
	if actual := rgwFixtureWaitStatus(t, ctx, aliceS3, bucket+"/payload", http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("suspending and resuming user changed its stored object")
	}
	t.Log("suspension denied new signed HTTP requests; another identity stayed writable; resumption restored the original object")

	if err := gateway.RemoveUser(ctx, alice); err == nil {
		t.Fatal("user owning a populated bucket was removed without purge-data")
	}
	if actual := aliceS3.request(t, ctx, http.MethodGet, bucket+"/payload", nil, http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("failed user removal purged caller data")
	}
	aliceS3.request(t, ctx, http.MethodDelete, bucket+"/payload", nil, http.StatusNoContent)
	aliceS3.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	if err := gateway.RemoveUser(ctx, alice); err != nil {
		t.Fatal(err)
	}
	if err := gateway.RemoveUser(ctx, alice); err != nil {
		t.Fatal(err)
	}
	rgwFixtureWaitStatus(t, ctx, aliceS3, "/", http.StatusForbidden)
	if _, err := gateway.UserInfo(ctx, alice); err == nil {
		t.Fatal("removed owned handle still allowed native policy access")
	}

	for _, key := range []string{"payload", "while-alice-suspended"} {
		bobS3.request(t, ctx, http.MethodDelete, bobBucket+"/"+key, nil, http.StatusNoContent)
	}
	bobS3.request(t, ctx, http.MethodDelete, bobBucket, nil, http.StatusNoContent)
	for _, user := range []*ceph.RGWUser{bob, observer} {
		if err := gateway.RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("nonempty identity removal refused without data loss; empty users removed idempotently and fresh requests with old keys denied")
}

func rgwFixtureWaitStatus(t *testing.T, ctx context.Context, client s3HTTPClient, path string, wantStatus int) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastStatus int
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		client.sign(req, nil, time.Now().UTC())
		response, err := client.http.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			lastStatus = response.StatusCode
			if lastStatus == wantStatus {
				return body
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("RGW user policy did not affect a new S3 request: last HTTP status %d, want %d: %v", lastStatus, wantStatus, ctx.Err())
		case <-ticker.C:
		}
	}
}
