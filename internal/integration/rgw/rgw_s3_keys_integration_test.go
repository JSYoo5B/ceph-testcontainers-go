//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"net/http"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// TestRGWS3KeyRotation rotates the S3 keys of one owned user in a real
// gateway. Two keys created through the fixture work beside the creation key
// and a key the test added natively; the selected old keys are revoked. A
// fresh AWS SDK process, which plays the SUT, then sees the old keys refused
// and the new one accepted with the original object intact, and the
// test's own SigV4 client and the native user record confirm it
// independently. Key commands that fail after their change are settled
// without being replayed, cancellation after dispatch settles a created key, the last fixture
// key cannot be revoked, and removing the user leaves another user intact.
func TestRGWS3KeyRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "keys", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	probe := buildS3Probe(t, ctx, endpoint, gateway.Region)
	client := func(key *rgw.S3Key) harness.S3HTTPClient {
		t.Helper()
		access, secret, err := key.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Timeout: 15 * time.Second}}
	}
	accessOf := func(key *rgw.S3Key) string {
		t.Helper()
		access, _, err := key.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return access
	}

	owner, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-key-owner"})
	if err != nil {
		t.Fatal(err)
	}
	bystander, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-key-bystander"})
	if err != nil {
		t.Fatal(err)
	}
	creation, bystanderKey := owner.CreationKey(), bystander.CreationKey()
	const bucket, object = "tc-key-rotation", "original"
	payload := []byte("data written before the key rotation")
	client(creation).Request(t, ctx, http.MethodPut, "/"+bucket, nil, http.StatusOK)
	client(creation).Request(t, ctx, http.MethodPut, "/"+bucket+"/"+object, payload, http.StatusOK)
	client(bystanderKey).Request(t, ctx, http.MethodPut, "/tc-key-bystander", nil, http.StatusOK)

	// A key the test adds natively is not the fixture's: it is never
	// checked, adopted or removed.
	data, err := gateway.Admin(ctx, "key", "create", "--uid", owner.ID(), "--key-type", "s3", "--gen-access-key", "--gen-secret")
	if err != nil {
		t.Fatal(err)
	}
	nativeKeys := rgwDecodeKeys(t, data)
	nativeAccess := unrelatedKey(t, nativeKeys, accessOf(creation))
	nativeS3 := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: nativeAccess, SecretKey: nativeKeys[nativeAccess], Region: gateway.Region, HTTP: &http.Client{Timeout: 15 * time.Second}}

	first, err := owner.CreateS3Key(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := owner.CreateS3Key(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if accessOf(owner.CreationKey()) != accessOf(creation) || accessOf(first) == accessOf(second) {
		t.Fatal("creating keys changed what an existing key handle names")
	}
	keys := rgwNativeKeys(t, ctx, gateway, owner.ID())
	if len(keys) != 4 || keys[nativeAccess] != nativeKeys[nativeAccess] {
		t.Fatalf("the user holds %d keys after two creations, want the creation, native and two new keys", len(keys))
	}
	want := sha256Hex(payload)
	for name, key := range map[string]*rgw.S3Key{"creation": creation, "first": first, "second": second} {
		if body := client(key).Request(t, ctx, http.MethodGet, "/"+bucket+"/"+object, nil, http.StatusOK); sha256Hex(body) != want {
			t.Fatalf("the %s key read other data", name)
		}
		if result := probe.run(t, ctx, key, "get", bucket, object, ""); result.Outcome != "accepted" || result.SHA256 != want {
			t.Fatalf("the SUT with the %s key: %+v", name, result)
		}
	}
	t.Log("the creation key and two created keys of one user work, in the SUT and the test's own client")

	// Revoke the selected old keys: the first created one and the creation key.
	if err := first.Revoke(ctx); err != nil {
		t.Fatal(err)
	}
	if err := creation.Revoke(ctx); err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string]*rgw.S3Key{"first": first, "creation": creation} {
		probe.waitOutcome(t, ctx, key, "get", bucket, object, "refused")
		if status, _ := rgwS3Status(t, ctx, client(key), "/"+bucket+"/"+object); status != http.StatusForbidden {
			t.Fatalf("the test's own client with the revoked %s key got %d", name, status)
		}
	}
	if result := probe.run(t, ctx, second, "get", bucket, object, ""); result.Outcome != "accepted" || result.SHA256 != want {
		t.Fatalf("the SUT with the remaining key: %+v", result)
	}
	if result := probe.run(t, ctx, second, "put", bucket, "after-rotation", "written with the new key"); result.Outcome != "accepted" {
		t.Fatalf("the SUT wrote with the remaining key: %+v", result)
	}
	if body := client(second).Request(t, ctx, http.MethodGet, "/"+bucket+"/"+object, nil, http.StatusOK); sha256Hex(body) != want {
		t.Fatal("the original object changed during the rotation")
	}
	if status, _ := rgwS3Status(t, ctx, nativeS3, "/"+bucket+"/"+object); status != http.StatusOK {
		t.Fatalf("the natively added key got %d after the rotation", status)
	}
	keys = rgwNativeKeys(t, ctx, gateway, owner.ID())
	if _, ok := keys[accessOf(first)]; ok || len(keys) != 2 || keys[accessOf(second)] == "" || keys[nativeAccess] == "" {
		t.Fatal("native state does not hold exactly the remaining and the natively added key")
	}
	if _, ok := keys[accessOf(creation)]; ok {
		t.Fatal("native state still holds the revoked creation key")
	}
	if status, _ := rgwS3Status(t, ctx, client(bystanderKey), "/tc-key-bystander"); status != http.StatusOK {
		t.Fatal("rotating one user's keys affected another user")
	}
	t.Log("revoked keys refused in a fresh SUT process and the test's own client; the remaining key reads the original data; native and other users' keys intact")

	// The revoked handles still name their key pairs, and Revoke is not undone.
	if err := first.Revoke(ctx); err != nil {
		t.Fatalf("a second Revoke: %v", err)
	}
	if err := second.Revoke(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("revoking the last fixture key: %v", err)
	}
	canceled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if key, err := owner.CreateS3Key(canceled); key != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled CreateS3Key returned %v, %v", key != nil, err)
	}
	if keys := rgwNativeKeys(t, ctx, gateway, owner.ID()); len(keys) != 2 {
		t.Fatal("a canceled CreateS3Key changed the user's keys")
	}

	// Partial failures: radosgw-admin reports a failure after its change.
	arm := installRGWKeyFaultShim(t, ctx, gateway)
	arm("pause-key")
	settled, cancelErr := cancelRGWCreate(t, ctx, gateway, owner.CreateS3Key)
	if !errors.Is(cancelErr, context.Canceled) || settled == nil {
		t.Fatalf("cancel after native key create: handle=%t error=%v", settled != nil, cancelErr)
	}
	afterCancel := rgwNativeKeys(t, ctx, gateway, owner.ID())
	access, secret, credErr := settled.Credentials()
	if credErr != nil || secret == "" || afterCancel[access] != secret || keys[access] != "" {
		t.Fatal("canceled key creation did not return its confirmed native key")
	}
	delete(afterCancel, access)
	if !maps.Equal(afterCancel, keys) {
		t.Fatal("canceled key creation changed other keys or created extra keys")
	}
	if err := settled.Revoke(ctx); err != nil || !maps.Equal(rgwNativeKeys(t, ctx, gateway, owner.ID()), keys) {
		t.Fatalf("remove settled key: %v", err)
	}
	arm("create")
	partial, err := owner.CreateS3Key(ctx)
	if err == nil || partial == nil {
		t.Fatalf("a key creation that failed after its change: key %v, error %v", partial != nil, err)
	}
	keys = rgwNativeKeys(t, ctx, gateway, owner.ID())
	if len(keys) != 3 || keys[accessOf(partial)] == "" {
		t.Fatalf("a failed key creation was replayed or lost: %d keys", len(keys))
	}
	if status, _ := rgwS3Status(t, ctx, client(partial), "/"+bucket+"/"+object); status != http.StatusOK {
		t.Fatal("the settled key does not work")
	}
	arm("rm")
	if err := partial.Revoke(ctx); err != nil {
		t.Fatalf("a removal that failed after its change: %v", err)
	}
	removeRGWKeyFaultShim(t, ctx, gateway)
	if keys := rgwNativeKeys(t, ctx, gateway, owner.ID()); len(keys) != 2 {
		t.Fatal("the settled removal left the key")
	}
	if _, err := owner.Info(ctx); err != nil {
		t.Fatalf("the owned user after partial failures: %v", err)
	}
	t.Log("key commands that failed after their change were settled from native state, never replayed")

	// Cleanup: the user goes with all its keys; the other user stays.
	remaining := client(second)
	remaining.Request(t, ctx, http.MethodDelete, "/"+bucket+"/"+object, nil, http.StatusNoContent)
	remaining.Request(t, ctx, http.MethodDelete, "/"+bucket+"/after-rotation", nil, http.StatusNoContent)
	remaining.Request(t, ctx, http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
	if _, err := gateway.Admin(ctx, "metadata", "get", "user:"+owner.ID()); err != nil {
		t.Fatalf("read user metadata before removal: %v", err)
	}
	if err := owner.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Revoke(ctx); err != nil {
		t.Fatalf("revoking a key of a removed user: %v", err)
	}
	if _, err := gateway.Admin(ctx, "metadata", "get", "user:"+owner.ID()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed user metadata: got %v, want ENOENT", err)
	}
	rgwFixtureWaitStatus(t, ctx, nativeS3, "/", http.StatusForbidden)
	if _, err := bystander.Info(ctx); err != nil {
		t.Fatal(err)
	}
	if status, _ := rgwS3Status(t, ctx, client(bystanderKey), "/tc-key-bystander"); status != http.StatusOK {
		t.Fatal("removing the rotated user affected another user")
	}
	t.Log("removing the rotated user removed its keys and left the other user intact")
}
