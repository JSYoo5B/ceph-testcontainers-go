//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// testRGWRateLimitOverride throttles a SUT identity through
// User.OverrideRateLimit, first on one bucket the SUT created and then on the
// user itself, and proves on a real gateway: 503 SlowDown on the selected
// scope, admitted requests in the dimension the policy leaves unlimited, an
// unaffected control scope, refusal of an overlapping override, of an outside
// policy edit and of a bucket the SUT replaced, exact restoration of the prior
// native policy and access again after Restore. On Ceph 19 the user scope is
// refused as unsupported and changes nothing.
func testRGWRateLimitOverride(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}, opts...)...)
	// One gateway: the budgets are counted per gateway process.
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var users []*rgw.User
	var clients []harness.S3HTTPClient
	for _, id := range []string{"tc-ratelimit-sut", "tc-ratelimit-other"} {
		user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		access, secret, err := user.CreationKey().Credentials()
		if err != nil {
			t.Fatal(err)
		}
		users = append(users, user)
		clients = append(clients, harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Timeout: 15 * time.Second}})
	}
	sut, other := clients[0], clients[1]
	const limited, control, otherBucket = "tc-ratelimit-limited", "tc-ratelimit-control", "tc-ratelimit-other"
	payload := bytes.Repeat([]byte("rate-limited-object\n"), 512)
	// The SUT creates its own buckets; the fixture only references them.
	for _, bucket := range []struct {
		client harness.S3HTTPClient
		name   string
	}{{sut, limited}, {sut, control}, {other, otherBucket}} {
		bucket.client.Request(t, ctx, http.MethodPut, "/"+bucket.name, nil, http.StatusOK)
		bucket.client.Request(t, ctx, http.MethodPut, "/"+bucket.name+"/kept", payload, http.StatusOK)
	}
	readPolicy := func(args ...string) map[string]any {
		t.Helper()
		data := rgwBackendAdmin(t, ctx, gateway, append([]string{"ratelimit", "get"}, args...)...)
		var native map[string]map[string]any
		if json.Unmarshal(data, &native) != nil || len(native) != 1 {
			t.Fatal("decode native rate-limit policy")
		}
		for _, policy := range native {
			return policy
		}
		return nil
	}
	bucketScope := func(bucket string) []string { return []string{"--ratelimit-scope", "bucket", "--bucket", bucket} }
	userScope := []string{"--ratelimit-scope", "user", "--uid", users[0].ID()}
	equalPolicy := func(a, b map[string]any) bool {
		left, _ := json.Marshal(a)
		right, _ := json.Marshal(b)
		return bytes.Equal(left, right)
	}
	// A disabled, non-default prior policy, so restoration must put back
	// every field rather than merely disable the override's policy.
	rgwBackendAdmin(t, ctx, gateway, append([]string{"ratelimit", "set", "--max-write-ops", "1000", "--max-read-bytes", "1048576"}, bucketScope(limited)...)...)
	limitedBefore, controlBefore, userBefore := readPolicy(bucketScope(limited)...), readPolicy(bucketScope(control)...), readPolicy(userScope...)
	if limitedBefore["enabled"] != false || limitedBefore["max_write_ops"] != float64(1000) {
		t.Fatalf("prior native bucket policy was not stored: %v", limitedBefore)
	}

	get := func(callCtx context.Context, client harness.S3HTTPClient, bucket string) rgwBackendResponse {
		return rgwBackendSigned(t, callCtx, client, "s3", http.MethodGet, "/"+bucket+"/kept", nil, nil)
	}
	put := func(callCtx context.Context, client harness.S3HTTPClient, bucket string) rgwBackendResponse {
		return rgwBackendSigned(t, callCtx, client, "s3", http.MethodPut, "/"+bucket+"/kept", payload, nil)
	}
	// waitSlowDown polls until the gateway rejects with native 503 SlowDown,
	// checking that every admitted request still did its work.
	waitSlowDown := func(operation string, probe func(context.Context) rgwBackendResponse) {
		t.Helper()
		rgwRecordsWait(t, ctx, operation, func(callCtx context.Context) bool {
			response := probe(callCtx)
			switch {
			case response.code == http.StatusOK:
				if strings.HasPrefix(response.operation, http.MethodGet+" ") && !bytes.Equal(response.body, payload) {
					t.Fatalf("%s: admitted read changed the object", operation)
				}
				return false
			case response.code == http.StatusServiceUnavailable && rgwRecordsErrorCode(response.body) == "SlowDown":
				return true
			}
			t.Fatalf("%s: status=%d code=%s, want 200 or 503 SlowDown (body redacted)", operation, response.code, rgwRecordsErrorCode(response.body))
			return false
		})
	}
	requireAdmitted := func(operation string, probe func(context.Context) rgwBackendResponse) {
		t.Helper()
		for range 3 {
			response := probe(ctx)
			rgwBackendRequire(t, response, http.StatusOK)
			if strings.HasPrefix(response.operation, http.MethodGet+" ") && !bytes.Equal(response.body, payload) {
				t.Fatalf("%s: read changed the object", operation)
			}
		}
	}
	waitAdmitted := func(probe func(context.Context) rgwBackendResponse) {
		t.Helper()
		for range 3 {
			response := rgwBackendWaitStatus(t, ctx, probe, http.StatusOK)
			if strings.HasPrefix(response.operation, http.MethodGet+" ") && !bytes.Equal(response.body, payload) {
				t.Fatal("read after Restore changed the object")
			}
		}
	}

	t.Log("bucket scope: reads of one SUT bucket throttled")
	read := rgw.RateLimit{Enabled: true, MaxReadOps: 1}
	change, err := users[0].OverrideRateLimit(ctx, rgw.RateLimitScope{Bucket: limited}, read)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_ = change.Restore(cleanup)
	})
	if err != nil {
		t.Fatal(err)
	}
	applied := readPolicy(bucketScope(limited)...)
	if applied["enabled"] != true || applied["max_read_ops"] != float64(1) || applied["max_write_ops"] != float64(0) || applied["max_read_bytes"] != float64(0) ||
		!equalPolicy(readPolicy(bucketScope(control)...), controlBefore) || !equalPolicy(readPolicy(userScope...), userBefore) {
		t.Fatalf("native readback of the bucket override: %v", applied)
	}
	waitSlowDown("bucket read SlowDown", func(callCtx context.Context) rgwBackendResponse { return get(callCtx, sut, limited) })
	// Writes to the throttled bucket stay admitted (unlimited dimension), and
	// so do reads of the SUT's control bucket and of another user's bucket.
	requireAdmitted("write to the throttled bucket", func(callCtx context.Context) rgwBackendResponse { return put(callCtx, sut, limited) })
	requireAdmitted("control bucket read", func(callCtx context.Context) rgwBackendResponse { return get(callCtx, sut, control) })
	requireAdmitted("other user read", func(callCtx context.Context) rgwBackendResponse { return get(callCtx, other, otherBucket) })

	if second, err := users[0].OverrideRateLimit(ctx, rgw.RateLimitScope{Bucket: limited}, read); err == nil || second.Restore(ctx) != nil {
		t.Fatal("an overlapping override of the same bucket was not refused")
	}
	// An outside edit is refused instead of overwritten.
	rgwBackendAdmin(t, ctx, gateway, append([]string{"ratelimit", "set", "--max-read-ops", "5"}, bucketScope(limited)...)...)
	if err := change.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore after an outside edit: %v, want ceph.ErrInvalidRequest", err)
	}
	if policy := readPolicy(bucketScope(limited)...); policy["max_read_ops"] != float64(5) {
		t.Fatalf("a refused Restore changed the outside policy: %v", policy)
	}
	rgwBackendAdmin(t, ctx, gateway, append([]string{"ratelimit", "set", "--max-read-ops", "1"}, bucketScope(limited)...)...)
	if err := change.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if restored := readPolicy(bucketScope(limited)...); !equalPolicy(restored, limitedBefore) || !equalPolicy(readPolicy(bucketScope(control)...), controlBefore) {
		t.Fatalf("Restore did not put back the exact prior policy: %v, want %v", restored, limitedBefore)
	}
	waitAdmitted(func(callCtx context.Context) rgwBackendResponse { return get(callCtx, sut, limited) })

	t.Log("user scope: writes the SUT signs throttled")
	write := rgw.RateLimit{Enabled: true, MaxWriteOps: 1}
	userSupported := harness.CapabilitySupported(t, ctx, cluster, harness.CapabilityRGWUserRateLimit)
	userChange, err := users[0].OverrideRateLimit(ctx, rgw.RateLimitScope{}, write)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_ = userChange.Restore(cleanup)
	})
	if !userSupported {
		// A Ceph 19 gateway stores a user policy and never applies it to
		// requests the user's S3 keys sign: the override is refused before
		// any change, so the policy and access stay as they were.
		if refusal := harness.RequireCapabilityRefused(t, err, cluster, harness.CapabilityRGWUserRateLimit, "user-scope rate limit"); refusal.Target != "cluster" {
			t.Fatalf("user-scope refusal names %q, want the cluster", refusal.Target)
		}
		if policy := readPolicy(userScope...); !equalPolicy(policy, userBefore) {
			t.Fatalf("a refused user override changed the user policy: %v, want %v", policy, userBefore)
		}
		if err := userChange.Restore(ctx); err != nil {
			t.Fatalf("Restore of a refused user override: %v", err)
		}
		requireAdmitted("write after a refused user override", func(callCtx context.Context) rgwBackendResponse { return put(callCtx, sut, control) })
	} else {
		if err != nil {
			t.Fatal(err)
		}
		if policy := readPolicy(userScope...); policy["enabled"] != true || policy["max_write_ops"] != float64(1) || policy["max_read_ops"] != float64(0) {
			t.Fatalf("native readback of the user override: %v", policy)
		}
		waitSlowDown("user write SlowDown", func(callCtx context.Context) rgwBackendResponse { return put(callCtx, sut, control) })
		requireAdmitted("user read", func(callCtx context.Context) rgwBackendResponse { return get(callCtx, sut, control) })
		requireAdmitted("other user write", func(callCtx context.Context) rgwBackendResponse { return put(callCtx, other, otherBucket) })
		if err := userChange.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if restored := readPolicy(userScope...); !equalPolicy(restored, userBefore) {
			t.Fatalf("Restore did not put back the exact prior user policy: %v, want %v", restored, userBefore)
		}
		waitAdmitted(func(callCtx context.Context) rgwBackendResponse { return put(callCtx, sut, control) })
	}

	t.Log("bucket scope: a bucket the SUT replaces is refused")
	replaced, err := users[0].OverrideRateLimit(ctx, rgw.RateLimitScope{Bucket: limited}, read)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_ = replaced.Restore(cleanup)
	})
	if err != nil {
		t.Fatal(err)
	}
	// The handle does not own the bucket: the SUT deletes it and creates it
	// again (deletion is a write, which the policy leaves unlimited).
	sut.Request(t, ctx, http.MethodDelete, "/"+limited+"/kept", nil, http.StatusNoContent)
	sut.Request(t, ctx, http.MethodDelete, "/"+limited, nil, http.StatusNoContent)
	sut.Request(t, ctx, http.MethodPut, "/"+limited, nil, http.StatusOK)
	sut.Request(t, ctx, http.MethodPut, "/"+limited+"/kept", payload, http.StatusOK)
	fresh := readPolicy(bucketScope(limited)...)
	if err := replaced.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore of a replaced bucket: %v, want ceph.ErrInvalidRequest", err)
	}
	if policy := readPolicy(bucketScope(limited)...); !equalPolicy(policy, fresh) || policy["enabled"] != false {
		t.Fatalf("a refused Restore changed the new bucket's policy: %v", policy)
	}
	waitAdmitted(func(callCtx context.Context) rgwBackendResponse { return get(callCtx, sut, limited) })
	userResult := "503 SlowDown on the user scope"
	if !userSupported {
		userResult = "user scope refused as unsupported without a change"
	}
	t.Logf("native rate limit: 503 SlowDown on the selected bucket scope, %s, unlimited dimension and control scopes admitted, overlap/outside edit/replaced bucket refused, exact prior policies restored, access after Restore", userResult)
}
