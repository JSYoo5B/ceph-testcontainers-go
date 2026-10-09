//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func testRGWUserAdministration(t *testing.T, customizers ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, customizers...)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "fixture", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create := func(id, caps string) (*rgw.User, s3HTTPClient) {
		t.Helper()
		user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: id, AdminCaps: caps})
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
	if _, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: alice.ID(), DisplayName: "overwrite"}); err == nil {
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
	if observerInfo.Admin || observerInfo.System || !slices.ContainsFunc(observerInfo.AdminCaps, func(cap rgw.AdminCapability) bool {
		return cap.Type == "users" && cap.Permission == "read"
	}) {
		t.Fatal("Admin Ops user did not retain only its explicitly requested capability")
	}
	t.Log("ordinary user Admin Ops denied; explicit users=read permits Admin Ops GET without global admin/system flags")

	userQuota := rgw.Quota{Enabled: true, MaxSizeBytes: 1 << 20, MaxObjects: 8}
	bucketQuota := rgw.Quota{Enabled: true, MaxSizeBytes: 1 << 19, MaxObjects: 4}
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
	t.Log("user and per-bucket quota byte/object limits read back from authoritative native user policy")
	quotaUser, quotaS3 := create("tc-fixture-quota", "")
	rgwFixtureUserAggregateQuota(t, ctx, gateway, quotaUser, quotaS3, bob, bobS3)

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
	for _, user := range []*rgw.User{bob, observer} {
		if err := gateway.RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("nonempty identity removal refused without data loss; empty users removed idempotently and fresh requests with old keys denied")
}

// An ordinary user's quota is shared by its buckets. Account aggregate quota
// and an individual bucket quota are separate server admission conditions.
func rgwFixtureUserAggregateQuota(t *testing.T, ctx context.Context, gateway *rgw.Gateway, user *rgw.User, client s3HTTPClient, neighbor *rgw.User, neighborS3 s3HTTPClient) {
	t.Helper()
	before, err := gateway.UserInfo(ctx, user)
	if err != nil || before.ID != user.ID() || before.Type != "rgw" || before.AccountID != "" || before.Admin || before.System || before.Suspended || before.UserQuota.Enabled || before.BucketQuota.Enabled {
		t.Fatal("user-quota fixture requires a fresh ordinary owned user with independent bucket quota disabled")
	}
	neighborBefore, err := gateway.UserInfo(ctx, neighbor)
	if err != nil {
		t.Fatal(err)
	}
	const first, second, outside = "/tc-user-quota-first", "/tc-user-quota-second", "/tc-user-quota-outside"
	bucketIDs := make(map[string]string)
	readBucketID := func(bucket string, owner *rgw.User) string {
		t.Helper()
		var native struct {
			ID, Bucket, Owner string
		}
		data, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(bucket, "/"))
		if err != nil || json.Unmarshal(data, &native) != nil || native.ID == "" || native.Bucket != strings.TrimPrefix(bucket, "/") || native.Owner != owner.ID() {
			t.Fatal("user-quota fixture native bucket identity changed")
		}
		return native.ID
	}
	for _, bucket := range []string{first, second} {
		client.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		bucketIDs[bucket] = readBucketID(bucket, user)
	}
	neighborS3.request(t, ctx, http.MethodPut, outside, nil, http.StatusOK)
	bucketIDs[outside] = readBucketID(outside, neighbor)
	limited := rgw.Quota{Enabled: true, MaxSizeBytes: -1, MaxObjects: 1}
	if err := gateway.SetUserQuota(ctx, user, limited); err != nil {
		t.Fatal(err)
	}
	current, err := gateway.UserInfo(ctx, user)
	if err != nil || current.UserQuota != limited {
		t.Fatal("ordinary aggregate user quota did not read back")
	}
	current.UserQuota = before.UserQuota
	if !reflect.DeepEqual(current, before) {
		t.Fatal("ordinary user quota changed unrelated native identity or policy")
	}
	payload := []byte("one ordinary user, two buckets, aggregate admission")
	client.request(t, ctx, http.MethodPut, first+"/kept", payload, http.StatusOK)
	if _, err := gateway.Admin(ctx, "user", "stats", "--uid", user.ID(), "--sync-stats"); err != nil {
		t.Fatal(err)
	}
	// The second bucket is empty, and its bucket quota remains disabled. Only
	// this ordinary user's aggregate quota can reject its first object.
	rgwFixtureUserQuotaWrite(t, ctx, client, second+"/rejected", payload, false)
	client.request(t, ctx, http.MethodGet, second+"/rejected", nil, http.StatusNotFound)
	if actual := client.request(t, ctx, http.MethodGet, first+"/kept", nil, http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("ordinary user quota rejection changed stored bytes")
	}
	neighborS3.request(t, ctx, http.MethodPut, outside+"/unaffected", payload, http.StatusOK)
	if actual := neighborS3.request(t, ctx, http.MethodGet, outside+"/unaffected", nil, http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("ordinary user quota changed another principal's admission or bytes")
	}
	for _, bucket := range []string{first, second, outside} {
		owner := user
		if bucket == outside {
			owner = neighbor
		}
		if readBucketID(bucket, owner) != bucketIDs[bucket] {
			t.Fatal("user-quota restore refuses a replaced native bucket")
		}
	}
	current, err = gateway.UserInfo(ctx, user)
	if err != nil || current.UserQuota != limited {
		t.Fatal("user-quota restore refuses an outside policy edit")
	}
	current.UserQuota = before.UserQuota
	if !reflect.DeepEqual(current, before) {
		t.Fatal("user-quota restore refuses changed native identity or unrelated policy")
	}
	if err := gateway.SetUserQuota(ctx, user, before.UserQuota); err != nil {
		t.Fatal(err)
	}
	if restored, err := gateway.UserInfo(ctx, user); err != nil || !reflect.DeepEqual(restored, before) {
		t.Fatal("user-quota restore did not preserve exact original native policy and owned key")
	}
	rgwFixtureUserQuotaWrite(t, ctx, client, second+"/rejected", payload, true)
	for _, bucket := range []string{first, second} {
		key := "kept"
		if bucket == second {
			key = "rejected"
		}
		if readBucketID(bucket, user) != bucketIDs[bucket] || !bytes.Equal(client.request(t, ctx, http.MethodGet, bucket+"/"+key, nil, http.StatusOK), payload) {
			t.Fatal("ordinary user quota restore changed native bucket identity, original credentials or exact bytes")
		}
		client.request(t, ctx, http.MethodDelete, bucket+"/"+key, nil, http.StatusNoContent)
		client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	}
	if unchanged, err := gateway.UserInfo(ctx, neighbor); err != nil || !reflect.DeepEqual(unchanged, neighborBefore) {
		t.Fatal("ordinary user quota changed the sibling principal's native policy or owned key")
	}
	neighborS3.request(t, ctx, http.MethodDelete, outside+"/unaffected", nil, http.StatusNoContent)
	neighborS3.request(t, ctx, http.MethodDelete, outside, nil, http.StatusNoContent)
	if err := gateway.RemoveUser(ctx, user); err != nil {
		t.Fatal(err)
	}
	if err := gateway.RemoveUser(ctx, user); err != nil {
		t.Fatal(err)
	}
	t.Log("ordinary native user aggregate quota: second empty bucket denied with exact QuotaExceeded while per-bucket quota stayed disabled; sibling principal remained writable; original quota/key/bucket IDs and exact bytes recovered after restore; owned objects/buckets/user explicitly removed")
}

func rgwFixtureUserQuotaWrite(t *testing.T, parent context.Context, client s3HTTPClient, path string, payload []byte, wantAdmission bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	started := time.Now()
	for attempt := 0; ; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, client.endpoint+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		client.sign(request, payload, time.Now().UTC())
		response, err := client.http.Do(request)
		if err != nil {
			t.Fatal("ordinary user quota HTTP transport failed")
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil || ctx.Err() != nil {
			t.Fatal("ordinary user quota HTTP response failed or exceeded its deadline")
		}
		switch response.StatusCode {
		case http.StatusOK:
			if wantAdmission {
				t.Logf("ordinary user quota restore admitted previously denied bucket write after %s", time.Since(started).Round(time.Millisecond))
				return
			}
			// Native accounting may lag the policy readback. Remove each
			// transiently admitted owned object before observing another write.
			client.request(t, ctx, http.MethodDelete, path, nil, http.StatusNoContent)
		case http.StatusForbidden:
			var failure struct{ Code string }
			if xml.Unmarshal(body, &failure) != nil || failure.Code != "QuotaExceeded" {
				t.Fatalf("ordinary user quota returned unrelated HTTP denial: code=%s (body redacted)", failure.Code)
			}
			if !wantAdmission {
				t.Logf("ordinary user aggregate QuotaExceeded observed after %s and %d transient accounting-cache grants", time.Since(started).Round(time.Millisecond), attempt)
				return
			}
		default:
			t.Fatalf("ordinary user quota returned unexpected HTTP %d (body redacted)", response.StatusCode)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ordinary user quota admission=%t did not take effect: %v", wantAdmission, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
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
