//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// rgwKMSVaultHost is the Vault alias on the cluster bridge and the extra
	// SAN of its dev-TLS certificate, so RGW verifies the host name it dials.
	rgwKMSVaultHost = "tc-vault"
	rgwKMSAddress   = "https://" + rgwKMSVaultHost + ":8200"
	// The two encryption modes take separate tokens: SSE-KMS reads one KV-v2
	// key, SSE-S3 manages Transit keys. The CA file serves both.
	rgwKMSTokenFile   = "/tc/kms-token"
	rgwSSES3TokenFile = "/tc/sse-s3-token"
	rgwKMSCAFile      = "/tc/vault-ca.pem"
	rgwKMSAuditFile   = "/tmp/tc-kms-audit.json"
	rgwSSES3Policy    = "rgw-sse-s3-transit"
	rgwSSES3Control   = "unrelated-control"
)

// rgwKMSVault is the test-owned Vault. The fixture never manages it; this
// type plays the KMS operator of the consumer's test.
type rgwKMSVault struct {
	container testcontainers.Container
	rootToken string
	ca        []byte
	http      *http.Client
}

func rgwExternalKMS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	vault := rgwKMSStartVault(t, ctx, cluster)
	vault.call(t, ctx, http.MethodPut, "/v1/sys/audit/tc", map[string]any{"type": "file", "options": map[string]string{"file_path": rgwKMSAuditFile}}, http.StatusNoContent)

	// SSE-KMS: a KV-v2 key the SUT names, read-only for RGW. "denied" exists
	// but lies outside the token's policy.
	vault.call(t, ctx, http.MethodPost, "/v1/sys/mounts/tc", map[string]any{"type": "kv", "options": map[string]string{"version": "2"}}, http.StatusNoContent)
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	kmsKey := base64.StdEncoding.EncodeToString(keyBytes)
	for _, key := range []string{"allowed", "denied"} {
		vault.call(t, ctx, http.MethodPost, "/v1/tc/data/"+key, map[string]any{"data": map[string]string{"key": kmsKey}}, http.StatusOK)
	}
	vault.call(t, ctx, http.MethodPut, "/v1/sys/policies/acl/rgw-read-owned", map[string]any{"policy": `path "tc/data/allowed" { capabilities = ["read"] }`}, http.StatusNoContent)
	kmsToken := vault.token(t, ctx, "rgw-read-owned")

	// SSE-S3: RGW creates and deletes one Transit key per bucket. The control
	// key belongs to nobody in this test and must survive every step.
	vault.call(t, ctx, http.MethodPost, "/v1/sys/mounts/tctransit", map[string]any{"type": "transit"}, http.StatusNoContent)
	vault.call(t, ctx, http.MethodPost, "/v1/tctransit/keys/"+rgwSSES3Control, map[string]any{"type": "chacha20-poly1305", "derived": true}, http.StatusOK, http.StatusNoContent)
	rgwSSES3SetPolicy(t, ctx, vault, true)
	sseS3Token := vault.token(t, ctx, rgwSSES3Policy)

	settings := []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_crypt_s3_kms_backend", Value: "vault"},
		{Section: "client.admin", Name: "rgw_crypt_vault_auth", Value: "token"},
		{Section: "client.admin", Name: "rgw_crypt_vault_token_file", Value: rgwKMSTokenFile},
		{Section: "client.admin", Name: "rgw_crypt_vault_addr", Value: rgwKMSAddress},
		{Section: "client.admin", Name: "rgw_crypt_vault_prefix", Value: "/v1/tc/data"},
		{Section: "client.admin", Name: "rgw_crypt_vault_secret_engine", Value: "kv"},
		{Section: "client.admin", Name: "rgw_crypt_vault_verify_ssl", Value: "true"},
		{Section: "client.admin", Name: "rgw_crypt_vault_ssl_cacert", Value: rgwKMSCAFile},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_backend", Value: "vault"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_auth", Value: "token"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_token_file", Value: rgwSSES3TokenFile},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_addr", Value: rgwKMSAddress},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_prefix", Value: "/v1/tctransit"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_secret_engine", Value: "transit"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_verify_ssl", Value: "true"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_ssl_cacert", Value: rgwKMSCAFile},
		{Section: "client.admin", Name: "rgw_crypt_require_ssl", Value: "true"},
		// A Vault request on a connection whose peer vanished waits until
		// libcurl's low-speed limit aborts it, 5 minutes by default. Bound it
		// so the KMS cut fails within the S3 client's timeout.
		{Section: "client.admin", Name: "rgw_curl_low_speed_time", Value: "5"},
	}
	for _, setting := range settings {
		change, err := override.SetConfig(ctx, cluster, setting)
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if err := change.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Every gateway runs as client.admin and reads the same settings at start.
	// What differs per gateway is the content of the files at those paths: the
	// second one trusts an unrelated CA for the KMS leg, and the SUT-facing
	// certificate is the same for both.
	material, sutRoots := rgwTestCertificate(t)
	unrelatedCA := newRGWTestAuthority(t).PEM
	gateways := make([]*rgw.Gateway, 2)
	clients := make([]harness.S3HTTPClient, 2)
	for i, name := range []string{"kms-trusted", "kms-wrong-ca"} {
		trust := vault.ca
		if i == 1 {
			trust = unrelatedCA
		}
		var err error
		gateways[i], err = rgw.Start(ctx, cluster, rgw.Config{Name: name, TLS: material, SkipUserCreation: true}, testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(kmsToken)), ContainerFilePath: rgwKMSTokenFile, FileMode: 0o600},
			testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(sseS3Token)), ContainerFilePath: rgwSSES3TokenFile, FileMode: 0o600},
			testcontainers.ContainerFile{Reader: bytes.NewReader(trust), ContainerFilePath: rgwKMSCAFile, FileMode: 0o600},
		))
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := gateways[i].S3SecureEndpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: sutRoots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
		t.Cleanup(transport.CloseIdleConnections)
		clients[i] = harness.S3HTTPClient{Endpoint: endpoint, Region: gateways[i].Region, HTTP: &http.Client{Transport: transport, Timeout: 60 * time.Second}}
	}
	user, err := gateways[0].CreateUser(ctx, rgw.UserConfig{ID: "tc-kms-owner"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	for i := range clients {
		clients[i].AccessKey, clients[i].SecretKey = access, secret
	}
	trusted, wrongCA := clients[0], clients[1]

	const bucket = "/tc-kms"
	payload := bytes.Repeat([]byte("external-kms-known-bytes\n"), 8192)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket, nil, nil), http.StatusOK)
	selected := http.Header{"X-Amz-Server-Side-Encryption": {"aws:kms"}, "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": {"allowed"}}
	put := rgwBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket+"/encrypted", payload, selected)
	rgwBackendRequire(t, put, http.StatusOK)
	if put.headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" || put.headers.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "allowed" {
		t.Fatal("SSE-KMS response lacks the selected mode and key")
	}
	plain := []byte("object without server-side encryption")
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket+"/plain", plain, nil), http.StatusOK)
	identity := func(client harness.S3HTTPClient) rgwBackendListedObject {
		t.Helper()
		response := rgwBackendSigned(t, ctx, client, "s3", http.MethodGet, bucket+"?list-type=2&prefix=encrypted", nil, nil)
		rgwBackendRequire(t, response, http.StatusOK)
		var listing struct {
			Contents []rgwBackendListedObject `xml:"Contents"`
		}
		if xml.Unmarshal(response.body, &listing) != nil || len(listing.Contents) != 1 || listing.Contents[0].Key != "encrypted" || listing.Contents[0].ETag == "" || listing.Contents[0].Size != int64(len(payload)) {
			t.Fatal("encrypted object listing identity unavailable")
		}
		return listing.Contents[0]
	}
	object := identity(trusted)
	readExact := func(client harness.S3HTTPClient, step string) {
		t.Helper()
		read := rgwBackendSigned(t, ctx, client, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) || read.headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" {
			t.Fatalf("%s: SSE-KMS read changed bytes or mode", step)
		}
		if identity(client) != object {
			t.Fatalf("%s: encrypted object identity changed", step)
		}
	}
	// The gateway that cannot verify Vault still serves everything that needs
	// no key: the plain object and the listing of the encrypted one.
	readPlain := func(client harness.S3HTTPClient, step string) {
		t.Helper()
		read := rgwBackendSigned(t, ctx, client, "s3", http.MethodGet, bucket+"/plain", nil, nil)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, plain) || identity(client) != object {
			t.Fatalf("%s: KMS-independent request failed", step)
		}
	}

	t.Run("backend-trust", func(t *testing.T) {
		readExact(trusted, "trusted CA")
		for round := range 2 {
			refused := rgwBackendSigned(t, ctx, wrongCA, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
			// The KMS TLS handshake fails inside RGW; it maps that to 500.
			rgwBackendRequire(t, refused, http.StatusInternalServerError)
			readPlain(wrongCA, "unrelated CA")
			readExact(trusted, "sibling after refusal")
			t.Logf("round %d: unrelated KMS CA status=%d code=%s; sibling exact bytes", round, refused.code, rgwBackendErrorCode(refused.body))
		}
		denied := selected.Clone()
		denied.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", "denied")
		failed := rgwBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket+"/denied-key", []byte("outside the token policy"), denied)
		rgwBackendRequire(t, failed, http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError)
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodGet, bucket+"/denied-key", nil, nil), http.StatusNotFound)
		counts, err := rgwBackendVaultAuditProof(vault.audit(t, ctx))
		if err != nil {
			t.Fatal(err)
		}
		if counts["tc/data/allowed"] < 3 || counts["tc/data/denied"] == 0 {
			t.Fatalf("Vault audit lacks the RGW key reads: allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
		}
		t.Logf("SSE-KMS over verified HTTPS on both legs; Vault audit allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
	})

	t.Run("token-replacement", func(t *testing.T) {
		vault.call(t, ctx, http.MethodPost, "/v1/auth/token/revoke", map[string]string{"token": kmsToken}, http.StatusNoContent)
		// Independent evidence: Vault no longer knows the token.
		vault.call(t, ctx, http.MethodPost, "/v1/auth/token/lookup", map[string]string{"token": kmsToken}, http.StatusBadRequest, http.StatusForbidden)
		revoked := rgwBackendSigned(t, ctx, trusted, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, revoked, http.StatusForbidden)
		if rgwBackendErrorCode(revoked.body) != "AccessDenied" || identity(trusted) != object {
			t.Fatal("revoked KMS token did not fail with AccessDenied on the retained object")
		}
		// RGW reads the token file on every Vault request: replacing it at the
		// same path, with the same mode, recovers the running gateway.
		kmsToken = vault.token(t, ctx, "rgw-read-owned")
		if err := gateways[0].CopyToContainer(ctx, []byte(kmsToken), rgwKMSTokenFile+".next", 0o600); err != nil {
			t.Fatal("stage replacement token")
		}
		code, _, err := gateways[0].Exec(ctx, []string{"mv", rgwKMSTokenFile + ".next", rgwKMSTokenFile}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			t.Fatalf("replace token file: exit=%d", code)
		}
		readExact(trusted, "replaced token")
		t.Log("revoked token: 403 AccessDenied; same-path replacement recovered the running gateway")
	})

	t.Run("kms-network-cut", func(t *testing.T) {
		before, err := vault.container.MappedPort(ctx, "8200/tcp")
		if err != nil {
			t.Fatal(err)
		}
		cut, err := fault.InterruptNetwork(ctx, cluster, vault.container, ceph.PublicNetworkPlane)
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
		attached, err := vault.container.Networks(ctx)
		if err != nil || slices.Contains(attached, harness.StaticFacts(cluster).NetworkName) {
			t.Fatalf("Vault is still attached to the cluster bridge: %v %v", attached, err)
		}
		// Only the trusted gateway reaches Vault at all; its request rides the
		// connection that RGW keeps to Vault, which libcurl's low-speed limit
		// aborts. The other gateway shows that KMS-independent requests work.
		started := time.Now()
		failed := rgwBackendSigned(t, ctx, trusted, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, failed, http.StatusInternalServerError)
		t.Logf("KMS unreachable: status=%d code=%s after %s", failed.code, rgwBackendErrorCode(failed.body), time.Since(started).Round(time.Millisecond))
		for _, client := range clients {
			readPlain(client, "KMS disconnected")
		}
		if err := cut.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		address, err := vault.container.ContainerIP(ctx)
		if err != nil || address != cut.OriginalAddress.String() {
			t.Fatalf("Vault reattached at %q, want %s: %v", address, cut.OriginalAddress, err)
		}
		// The first requests after reattachment can still fail while RGW opens
		// a new connection; the same object must then read its exact bytes.
		started = time.Now()
		attempts := 0
		rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
			attempts++
			return rgwBackendSigned(t, callCtx, trusted, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		}, http.StatusOK)
		t.Logf("KMS reconnect: first exact read after %s, %d attempts", time.Since(started).Round(time.Millisecond), attempts)
		readExact(trusted, "KMS reconnected")
		// Docker publishes the ports again when the endpoint is reattached, so
		// the host port can differ. vault.call resolves it on every request.
		after, err := vault.container.MappedPort(ctx, "8200/tcp")
		if err != nil {
			t.Fatal(err)
		}
		vault.call(t, ctx, http.MethodGet, "/v1/tc/metadata/allowed", nil, http.StatusOK)
		t.Logf("KMS reconnect: same address %s, exact bytes; Vault host port %s -> %s", address, before.Port(), after.Port())
	})

	t.Run("sse-s3", func(t *testing.T) {
		removed := rgwSSES3Bucket(t, ctx, trusted, gateways[0], vault, "tc-sse-s3-removed")
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, "/tc-sse-s3-removed", nil, nil), http.StatusNoContent)
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+removed, nil, http.StatusNotFound)
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+rgwSSES3Control, nil, http.StatusOK)
		t.Log("SSE-S3 bucket deletion removed its own Transit key; control key kept")

		// A bucket DELETE that RGW reports as successful while Vault refuses the
		// key deletion leaves the key behind. Nothing retries it.
		orphan := rgwSSES3Bucket(t, ctx, trusted, gateways[0], vault, "tc-sse-s3-orphan")
		rgwSSES3SetPolicy(t, ctx, vault, false)
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, "/tc-sse-s3-orphan", nil, nil), http.StatusNoContent)
		gone := rgwBackendSigned(t, ctx, trusted, "s3", http.MethodGet, "/tc-sse-s3-orphan?list-type=2", nil, nil)
		rgwBackendRequire(t, gone, http.StatusNotFound)
		if rgwBackendErrorCode(gone.body) != "NoSuchBucket" {
			t.Fatal("deleted SSE-S3 bucket is still visible")
		}
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+orphan, nil, http.StatusOK)
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+rgwSSES3Control, nil, http.StatusOK)
		if denied := rgwSSES3DeniedDeletes(vault.audit(t, ctx), "tctransit/keys/"+orphan); denied == 0 {
			t.Fatal("Vault audit has no refused deletion of the bucket's Transit key")
		}
		rgwSSES3SetPolicy(t, ctx, vault, true)
		// The KMS operator removes the orphan; restoring the permission alone
		// does not, and the bucket it belonged to no longer exists.
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+orphan, nil, http.StatusOK)
		vault.call(t, ctx, http.MethodPost, "/v1/tctransit/keys/"+orphan+"/config", map[string]any{"deletion_allowed": true}, http.StatusOK, http.StatusNoContent)
		vault.call(t, ctx, http.MethodDelete, "/v1/tctransit/keys/"+orphan, nil, http.StatusNoContent)
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+orphan, nil, http.StatusNotFound)
		vault.call(t, ctx, http.MethodGet, "/v1/tctransit/keys/"+rgwSSES3Control, nil, http.StatusOK)
		t.Log("SSE-S3 bucket DELETE 204 with refused key deletion: orphan Transit key audited and removed by the KMS operator")
	})

	for _, key := range []string{"encrypted", "plain"} {
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, bucket+"/"+key, nil, nil), http.StatusNoContent)
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, bucket, nil, nil), http.StatusNoContent)
	if err := user.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}

// rgwSSES3Bucket creates a bucket with default SSE-S3 encryption, writes and
// reads one object without an encryption header, deletes the object and
// returns the bucket's Transit key, which Vault lists beside the control key.
func rgwSSES3Bucket(t *testing.T, ctx context.Context, client harness.S3HTTPClient, gateway *rgw.Gateway, vault *rgwKMSVault, name string) string {
	t.Helper()
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name, nil, nil), http.StatusOK)
	policy := []byte(`<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name+"?encryption", policy, nil), http.StatusOK)
	data := bytes.Repeat([]byte("sse-s3-default-encryption\n"), 4096)
	put := rgwBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+name+"/object", data, nil)
	rgwBackendRequire(t, put, http.StatusOK)
	if put.headers.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		t.Fatal("bucket default encryption did not select SSE-S3")
	}
	read := rgwBackendSigned(t, ctx, client, "s3", http.MethodGet, "/"+name+"/object", nil, nil)
	rgwBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.body, data) || read.headers.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		t.Fatal("SSE-S3 read changed bytes or mode")
	}
	var stats rgwBackendBucket
	if json.Unmarshal(rgwBackendAdmin(t, ctx, gateway, "bucket", "stats", "--bucket", name), &stats) != nil || stats.ID == "" {
		t.Fatal("SSE-S3 bucket native identity unavailable")
	}
	var listed struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	if json.Unmarshal(vault.call(t, ctx, "LIST", "/v1/tctransit/keys", nil, http.StatusOK), &listed) != nil {
		t.Fatal("decode Transit key listing")
	}
	// The default key template is the bucket ID.
	want := []string{stats.ID, rgwSSES3Control}
	slices.Sort(want)
	slices.Sort(listed.Data.Keys)
	if !slices.Equal(listed.Data.Keys, want) {
		t.Fatalf("Transit keys = %v, want the bucket ID and the control key", listed.Data.Keys)
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, client, "s3", http.MethodDelete, "/"+name+"/object", nil, nil), http.StatusNoContent)
	return stats.ID
}

// rgwSSES3SetPolicy grants RGW the Transit operations of SSE-S3, with or
// without deletion.
func rgwSSES3SetPolicy(t *testing.T, ctx context.Context, vault *rgwKMSVault, deletion bool) {
	t.Helper()
	capabilities := `"create", "update", "read", "list"`
	if deletion {
		capabilities += `, "delete"`
	}
	vault.call(t, ctx, http.MethodPut, "/v1/sys/policies/acl/"+rgwSSES3Policy, map[string]any{"policy": `path "tctransit/*" { capabilities = [` + capabilities + `] }`}, http.StatusNoContent)
}

// rgwSSES3DeniedDeletes counts completed Vault deletions of path refused for
// lack of permission.
func rgwSSES3DeniedDeletes(audit []byte, path string) int {
	denied := 0
	for _, line := range bytes.Split(audit, []byte("\n")) {
		var event struct {
			Type    string `json:"type"`
			Error   string `json:"error"`
			Request struct {
				Path      string `json:"path"`
				Operation string `json:"operation"`
			} `json:"request"`
		}
		if json.Unmarshal(line, &event) == nil && event.Type == "response" && event.Request.Path == path && event.Request.Operation == "delete" && strings.Contains(event.Error, "permission denied") {
			denied++
		}
	}
	return denied
}

func rgwKMSStartVault(t *testing.T, ctx context.Context, cluster *ceph.Container) *rgwKMSVault {
	t.Helper()
	image := os.Getenv("CEPH_TEST_VAULT_IMAGE")
	if image == "" {
		image = "hashicorp/vault:1.21.4"
	}
	vault := &rgwKMSVault{rootToken: rgwBackendRandom(t, 24)}
	opts := []testcontainers.ContainerCustomizer{
		network.WithNetworkName([]string{rgwKMSVaultHost}, harness.StaticFacts(cluster).NetworkName),
		testcontainers.WithExposedPorts("8200/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) { hc.CapAdd = []string{"CAP_IPC_LOCK"} }),
		testcontainers.WithEnv(map[string]string{"VAULT_DEV_ROOT_TOKEN_ID": vault.rootToken}),
		testcontainers.WithEntrypoint("vault"),
		// The dev server writes its CA and certificate into an existing
		// directory; the extra SAN is the bridge alias RGW dials.
		testcontainers.WithCmd("server", "-dev", "-dev-no-store-token", "-dev-listen-address=0.0.0.0:8200", "-dev-tls", "-dev-tls-cert-dir=/tmp", "-dev-tls-san="+rgwKMSVaultHost),
		testcontainers.WithWaitStrategy(wait.ForAll(wait.ForListeningPort("8200/tcp"), wait.ForFile("/tmp/vault-ca.pem"))),
		// Dev-server logs contain the root token.
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
	}
	var err error
	vault.container, err = testcontainers.Run(ctx, image, opts...)
	if vault.container != nil {
		testcontainers.CleanupContainer(t, vault.container)
	}
	if err != nil {
		t.Fatal(strings.ReplaceAll(err.Error(), vault.rootToken, "[redacted]"))
	}
	reader, err := vault.container.CopyFileFromContainer(ctx, "/tmp/vault-ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	vault.ca, err = io.ReadAll(reader)
	_ = reader.Close()
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(vault.ca) {
		t.Fatal("Vault dev-TLS CA unreadable")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	vault.http = &http.Client{Transport: transport, Timeout: 20 * time.Second}
	t.Logf("Vault dev-TLS image=%s", image)
	return vault
}

// call sends one management request with the root token. It resolves the
// mapped port on every call: when its bridge endpoint is reattached, Docker
// can publish the port on another host port.
func (vault *rgwKMSVault) call(t *testing.T, ctx context.Context, method, path string, data any, want ...int) []byte {
	t.Helper()
	endpoint, err := vault.container.PortEndpoint(ctx, "8200/tcp", "https")
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	if data != nil {
		body = []byte(rgwBackendJSON(t, data))
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Vault-Token", vault.rootToken)
	request.Header.Set("Content-Type", "application/json")
	response := rgwBackendDo(t, vault.http, request)
	if !slices.Contains(want, response.code) {
		// Vault bodies can contain keys and tokens.
		t.Fatalf("Vault %s %s status=%d want=%v (body redacted)", method, path, response.code, want)
	}
	return response.body
}

// token issues a non-renewable token that has only policy.
func (vault *rgwKMSVault) token(t *testing.T, ctx context.Context, policy string) string {
	t.Helper()
	created := vault.call(t, ctx, http.MethodPost, "/v1/auth/token/create", map[string]any{
		"policies": []string{policy}, "no_default_policy": true, "ttl": "1h", "renewable": false,
	}, http.StatusOK)
	var auth struct {
		Auth struct {
			Token    string   `json:"client_token"`
			Policies []string `json:"policies"`
		} `json:"auth"`
	}
	if json.Unmarshal(created, &auth) != nil || auth.Auth.Token == "" || !slices.Equal(auth.Auth.Policies, []string{policy}) {
		t.Fatal(errors.New("Vault did not issue a token with exactly the requested policy"))
	}
	return auth.Auth.Token
}

func (vault *rgwKMSVault) audit(t *testing.T, ctx context.Context) []byte {
	t.Helper()
	reader, err := vault.container.CopyFileFromContainer(ctx, rgwKMSAuditFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
