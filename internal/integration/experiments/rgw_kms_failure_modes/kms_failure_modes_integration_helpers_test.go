//go:build all || (integration && features)

package rgw_kms_failure_modes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func rgwKMSFailureModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	f := harness.NewRGWExternalKMS(t, ctx)
	t.Run("token-replacement", func(t *testing.T) {
		f.Vault.Call(t, ctx, http.MethodPost, "/v1/auth/token/revoke", map[string]string{"token": f.KMSToken}, http.StatusNoContent)
		// Independent evidence: Vault no longer knows the token.
		f.Vault.Call(t, ctx, http.MethodPost, "/v1/auth/token/lookup", map[string]string{"token": f.KMSToken}, http.StatusBadRequest, http.StatusForbidden)
		revoked := harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodGet, f.Bucket+"/encrypted", nil, nil)
		harness.RGWBackendRequire(t, revoked, http.StatusForbidden)
		if harness.RGWBackendErrorCode(revoked.Body) != "AccessDenied" || f.Identity(t, f.Trusted) != f.Object {
			t.Fatal("revoked KMS token did not fail with AccessDenied on the retained object")
		}
		// RGW reads the token file on every Vault request: replacing it at the
		// same path, with the same mode, recovers the running gateway.
		f.KMSToken = f.Vault.Token(t, ctx, "rgw-read-owned")
		if err := f.Gateways[0].CopyToContainer(ctx, []byte(f.KMSToken), harness.RGWKMSTokenFile+".next", 0o600); err != nil {
			t.Fatal("stage replacement token")
		}
		code, _, err := f.Gateways[0].Exec(ctx, []string{"mv", harness.RGWKMSTokenFile + ".next", harness.RGWKMSTokenFile}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			t.Fatalf("replace token file: exit=%d", code)
		}
		f.ReadExact(t, f.Trusted, "replaced token")
		t.Log("revoked token: 403 AccessDenied; same-path replacement recovered the running gateway")
	})

	t.Run("kms-network-cut", func(t *testing.T) {
		before, err := f.Vault.Container.MappedPort(ctx, "8200/tcp")
		if err != nil {
			t.Fatal(err)
		}
		cut, err := fault.InterruptNetwork(ctx, f.Cluster, f.Vault.Container, ceph.PublicNetworkPlane)
		if cut != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				if err := cut.Restore(cleanup); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		attached, err := f.Vault.Container.Networks(ctx)
		if err != nil || slices.Contains(attached, harness.StaticFacts(f.Cluster).NetworkName) {
			t.Fatalf("Vault is still attached to the f.Cluster bridge: %v %v", attached, err)
		}
		// Only the f.Trusted gateway reaches Vault at all; its request rides the
		// connection that RGW keeps to Vault, which libcurl's low-speed limit
		// aborts. The other gateway shows that KMS-independent requests work.
		started := time.Now()
		failed := harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodGet, f.Bucket+"/encrypted", nil, nil)
		harness.RGWBackendRequire(t, failed, http.StatusInternalServerError)
		t.Logf("KMS unreachable: status=%d code=%s after %s", failed.Code, harness.RGWBackendErrorCode(failed.Body), time.Since(started).Round(time.Millisecond))
		for _, client := range f.Clients {
			f.ReadPlain(t, client, "KMS disconnected")
		}
		if err := cut.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		address, err := f.Vault.Container.ContainerIP(ctx)
		if err != nil || address != cut.OriginalAddress.String() {
			t.Fatalf("Vault reattached at %q, want %s: %v", address, cut.OriginalAddress, err)
		}
		// The first requests after reattachment can still fail while RGW opens
		// a new connection; the same f.Object must then read its exact bytes.
		started = time.Now()
		attempts := 0
		harness.RGWBackendWaitStatus(t, ctx, func(callCtx context.Context) harness.RGWBackendResponse {
			attempts++
			return harness.RGWBackendSigned(t, callCtx, f.Trusted, "s3", http.MethodGet, f.Bucket+"/encrypted", nil, nil)
		}, http.StatusOK)
		t.Logf("KMS reconnect: first exact read after %s, %d attempts", time.Since(started).Round(time.Millisecond), attempts)
		f.ReadExact(t, f.Trusted, "KMS reconnected")
		// Docker publishes the ports again when the endpoint is reattached, so
		// the host port can differ. f.Vault.do resolves it on every request.
		after, err := f.Vault.Container.MappedPort(ctx, "8200/tcp")
		if err != nil {
			t.Fatal(err)
		}
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tc/metadata/allowed", nil, http.StatusOK)
		t.Logf("KMS reconnect: same address %s, exact bytes; Vault host port %s -> %s", address, before.Port(), after.Port())
	})

	t.Run("sse-s3", func(t *testing.T) {
		deleteBucket := func(name string) {
			t.Helper()
			harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodDelete, "/"+name, nil, nil), http.StatusNoContent)
			gone := harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodGet, "/"+name+"?list-type=2", nil, nil)
			harness.RGWBackendRequire(t, gone, http.StatusNotFound)
			if harness.RGWBackendErrorCode(gone.Body) != "NoSuchBucket" {
				t.Fatal("deleted SSE-S3 f.Bucket is still visible")
			}
		}
		// The KMS operator removes a key that RGW left behind.
		removeOrphan := func(key string) {
			t.Helper()
			f.Vault.Call(t, ctx, http.MethodPost, "/v1/tctransit/keys/"+key+"/config", map[string]any{"deletion_allowed": true}, http.StatusOK, http.StatusNoContent)
			f.Vault.Call(t, ctx, http.MethodDelete, "/v1/tctransit/keys/"+key, nil, http.StatusNoContent)
			f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+key, nil, http.StatusNotFound)
		}

		// Bucket deletion first marks the key deletable, then deletes it.
		// Whether the deletion follows depends on the release: Squid stops
		// after Vault answers the config update with a body, and the key
		// stays. The audit tells the outcomes apart; the key state is read
		// from Vault, never inferred from the S3 response.
		removed := rgwSSES3Bucket(t, ctx, f.Trusted, f.Gateways[0], f.Vault, "tc-sse-s3-removed")
		deleteBucket("tc-sse-s3-removed")
		first := rgwSSES3KeyAudit(f.Vault.Audit(t, ctx), "tctransit/keys/"+removed)
		retained := f.Vault.Do(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+removed, nil).Code == http.StatusOK
		switch {
		case first.configured == 0:
			t.Fatal("bucket deletion did not mark its Transit key deletable")
		case !retained && first.deleted == 1 && first.refused == 0:
			t.Log("SSE-S3 f.Bucket deletion removed its own Transit key")
		case retained && first.deleted == 0 && first.refused == 0:
			t.Log("SSE-S3 f.Bucket deletion marked its Transit key deletable and sent no deletion; the key stays")
			removeOrphan(removed)
		default:
			t.Fatalf("Transit key retained=%t with audited deletions=%d refused=%d", retained, first.deleted, first.refused)
		}
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+harness.RGWSSES3Control, nil, http.StatusOK)

		// A f.Bucket DELETE that RGW reports as successful while Vault refuses the
		// key deletion leaves the key behind. Nothing retries it.
		orphan := rgwSSES3Bucket(t, ctx, f.Trusted, f.Gateways[0], f.Vault, "tc-sse-s3-orphan")
		harness.RGWSSES3SetPolicy(t, ctx, f.Vault, false)
		deleteBucket("tc-sse-s3-orphan")
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+orphan, nil, http.StatusOK)
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+harness.RGWSSES3Control, nil, http.StatusOK)
		second := rgwSSES3KeyAudit(f.Vault.Audit(t, ctx), "tctransit/keys/"+orphan)
		if second.configured == 0 || second.deleted != 0 || (retained && second.refused != 0) || (!retained && second.refused == 0) {
			t.Fatalf("refused key deletion: audited config updates=%d deletions=%d refused=%d", second.configured, second.deleted, second.refused)
		}
		harness.RGWSSES3SetPolicy(t, ctx, f.Vault, true)
		// Restoring the permission does not remove the orphan: the f.Bucket it
		// belonged to no longer exists.
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+orphan, nil, http.StatusOK)
		removeOrphan(orphan)
		f.Vault.Call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+harness.RGWSSES3Control, nil, http.StatusOK)
		t.Logf("SSE-S3 f.Bucket DELETE 204 with key deletion refused=%d: orphan Transit key removed by the KMS operator", second.refused)
	})

	f.Close(t)
}

// rgwSSES3Bucket creates a bucket with default SSE-S3 encryption, writes and
// reads one object without an encryption header, deletes the object and
// returns the bucket's Transit key, which Vault lists beside the control key.
func rgwSSES3Bucket(t *testing.T, ctx context.Context, client harness.S3HTTPClient, gateway *rgw.Gateway, vault *harness.RGWKMSVault, name string) string {
	t.Helper()
	harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name, nil, nil), http.StatusOK)
	policy := []byte(`<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
	harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name+"?encryption", policy, nil), http.StatusOK)
	data := bytes.Repeat([]byte("sse-s3-default-encryption\n"), 4096)
	put := harness.RGWBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name+"/object", data, nil)
	harness.RGWBackendRequire(t, put, http.StatusOK)
	if put.Headers.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		t.Fatal("bucket default encryption did not select SSE-S3")
	}
	read := harness.RGWBackendSigned(t, ctx, client, "s3", http.MethodGet, "/"+name+"/object", nil, nil)
	harness.RGWBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.Body, data) || read.Headers.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		t.Fatal("SSE-S3 read changed bytes or mode")
	}
	var stats harness.RGWBackendBucket
	if json.Unmarshal(harness.RGWBackendAdmin(t, ctx, gateway, "bucket", "stats", "--bucket", name), &stats) != nil || stats.ID == "" {
		t.Fatal("SSE-S3 bucket native identity unavailable")
	}
	var listed struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	if json.Unmarshal(vault.Call(t, ctx, "LIST", "/v1/tctransit/keys", nil, http.StatusOK), &listed) != nil {
		t.Fatal("decode Transit key listing")
	}
	// The default key template is the bucket ID.
	want := []string{stats.ID, harness.RGWSSES3Control}
	slices.Sort(want)
	slices.Sort(listed.Data.Keys)
	if !slices.Equal(listed.Data.Keys, want) {
		t.Fatalf("Transit keys = %v, want the bucket ID and the control key", listed.Data.Keys)
	}
	harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, client, "s3", http.MethodDelete, "/"+name+"/object", nil, nil), http.StatusNoContent)
	return stats.ID
}

// rgwSSES3KeyOutcome counts the completed Vault requests on one Transit key
// that bucket deletion makes.
type rgwSSES3KeyOutcome struct{ configured, deleted, refused int }

func rgwSSES3KeyAudit(audit []byte, path string) rgwSSES3KeyOutcome {
	var outcome rgwSSES3KeyOutcome
	for _, line := range bytes.Split(audit, []byte("\n")) {
		var event struct {
			Type    string `json:"type"`
			Error   string `json:"error"`
			Request struct {
				Path      string `json:"path"`
				Operation string `json:"operation"`
			} `json:"request"`
		}
		if json.Unmarshal(line, &event) != nil || event.Type != "response" {
			continue
		}
		switch {
		case event.Request.Path == path+"/config" && event.Request.Operation == "update" && event.Error == "":
			outcome.configured++
		case event.Request.Path == path && event.Request.Operation == "delete" && event.Error == "":
			outcome.deleted++
		case event.Request.Path == path && event.Request.Operation == "delete" && strings.Contains(event.Error, "permission denied"):
			outcome.refused++
		}
	}
	return outcome
}
