//go:build all || (integration && features)

package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// RGWKMSVaultHost is the Vault alias on the cluster bridge and the extra
	// SAN of its dev-TLS certificate, so RGW verifies the host name it dials.
	RGWKMSVaultHost = "tc-vault"
	RGWKMSAddress   = "https://" + RGWKMSVaultHost + ":8200"
	// The two encryption modes take separate tokens: SSE-KMS reads one KV-v2
	// key, SSE-S3 manages Transit keys. The CA file serves both.
	RGWKMSTokenFile   = "/tc/kms-token"
	RGWSSES3TokenFile = "/tc/sse-s3-token"
	RGWKMSCAFile      = "/tc/vault-ca.pem"
	RGWKMSAuditFile   = "/tmp/tc-kms-audit.json"
	RGWSSES3Policy    = "rgw-sse-s3-transit"
	RGWSSES3Control   = "unrelated-control"
)

// RGWKMSVault is the test-owned Vault. The fixture never manages it; this
// type plays the KMS operator of the consumer's test.
type RGWKMSVault struct {
	Container testcontainers.Container
	CA        []byte
	rootToken string
	http      *http.Client
}

// RGWExternalKMS is the SSE-KMS and SSE-S3 scenario state: the cluster, the
// test-owned Vault, the two gateways and their S3 clients, the owner and the
// objects written at setup. KMSToken is the token in the trusted gateway's
// token file.
type RGWExternalKMS struct {
	Ctx              context.Context
	Cluster          *ceph.Container
	Vault            *RGWKMSVault
	Gateways         []*rgw.Gateway
	Clients          []S3HTTPClient
	Trusted, WrongCA S3HTTPClient
	User             *rgw.User
	KMSToken         string
	Bucket           string
	Payload, Plain   []byte
	Selected         http.Header
	Object           RGWBackendListedObject
}

// NewRGWExternalKMS starts a cluster, a test-owned Vault over dev TLS and two
// gateways configured for SSE-KMS and SSE-S3 through Vault: kms-trusted
// trusts Vault's CA, kms-wrong-ca an unrelated one. It writes one SSE-KMS
// object and one plain object in a bucket owned by one user.
func NewRGWExternalKMS(t *testing.T, ctx context.Context) *RGWExternalKMS {
	t.Helper()
	cluster, _ := NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	vault := StartRGWKMSVault(t, ctx, cluster)
	vault.Call(t, ctx, http.MethodPut, "/v1/sys/audit/tc", map[string]any{"type": "file", "options": map[string]string{"file_path": RGWKMSAuditFile}}, http.StatusNoContent)

	// SSE-KMS: a KV-v2 key the SUT names, read-only for RGW. "denied" exists
	// but lies outside the token's policy.
	vault.Call(t, ctx, http.MethodPost, "/v1/sys/mounts/tc", map[string]any{"type": "kv", "options": map[string]string{"version": "2"}}, http.StatusNoContent)
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	kmsKey := base64.StdEncoding.EncodeToString(keyBytes)
	for _, key := range []string{"allowed", "denied"} {
		vault.Call(t, ctx, http.MethodPost, "/v1/tc/data/"+key, map[string]any{"data": map[string]string{"key": kmsKey}}, http.StatusOK)
	}
	vault.Call(t, ctx, http.MethodPut, "/v1/sys/policies/acl/rgw-read-owned", map[string]any{"policy": `path "tc/data/allowed" { capabilities = ["read"] }`}, http.StatusNoContent)
	kmsToken := vault.Token(t, ctx, "rgw-read-owned")

	// SSE-S3: RGW creates and deletes one Transit key per bucket. The control
	// key belongs to nobody in this test and must survive every step.
	vault.Call(t, ctx, http.MethodPost, "/v1/sys/mounts/tctransit", map[string]any{"type": "transit"}, http.StatusNoContent)
	vault.Call(t, ctx, http.MethodPost, "/v1/tctransit/keys/"+RGWSSES3Control, map[string]any{"type": "chacha20-poly1305", "derived": true}, http.StatusOK, http.StatusNoContent)
	RGWSSES3SetPolicy(t, ctx, vault, true)
	sseS3Token := vault.Token(t, ctx, RGWSSES3Policy)

	settings := []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_crypt_s3_kms_backend", Value: "vault"},
		{Section: "client.admin", Name: "rgw_crypt_vault_auth", Value: "token"},
		{Section: "client.admin", Name: "rgw_crypt_vault_token_file", Value: RGWKMSTokenFile},
		{Section: "client.admin", Name: "rgw_crypt_vault_addr", Value: RGWKMSAddress},
		{Section: "client.admin", Name: "rgw_crypt_vault_prefix", Value: "/v1/tc/data"},
		{Section: "client.admin", Name: "rgw_crypt_vault_secret_engine", Value: "kv"},
		{Section: "client.admin", Name: "rgw_crypt_vault_verify_ssl", Value: "true"},
		{Section: "client.admin", Name: "rgw_crypt_vault_ssl_cacert", Value: RGWKMSCAFile},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_backend", Value: "vault"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_auth", Value: "token"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_token_file", Value: RGWSSES3TokenFile},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_addr", Value: RGWKMSAddress},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_prefix", Value: "/v1/tctransit"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_secret_engine", Value: "transit"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_verify_ssl", Value: "true"},
		{Section: "client.admin", Name: "rgw_crypt_sse_s3_vault_ssl_cacert", Value: RGWKMSCAFile},
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
	material, sutRoots := RGWTestCertificate(t)
	unrelatedCA := NewRGWTestAuthority(t).PEM
	gateways := make([]*rgw.Gateway, 2)
	clients := make([]S3HTTPClient, 2)
	for i, name := range []string{"kms-trusted", "kms-wrong-ca"} {
		trust := vault.CA
		if i == 1 {
			trust = unrelatedCA
		}
		var err error
		gateways[i], err = rgw.Start(ctx, cluster, rgw.Config{Name: name, TLS: material, SkipUserCreation: true}, testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(kmsToken)), ContainerFilePath: RGWKMSTokenFile, FileMode: 0o600},
			testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(sseS3Token)), ContainerFilePath: RGWSSES3TokenFile, FileMode: 0o600},
			testcontainers.ContainerFile{Reader: bytes.NewReader(trust), ContainerFilePath: RGWKMSCAFile, FileMode: 0o600},
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
		clients[i] = S3HTTPClient{Endpoint: endpoint, Region: gateways[i].Region, HTTP: &http.Client{Transport: transport, Timeout: 60 * time.Second}}
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
	RGWBackendRequire(t, RGWBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket, nil, nil), http.StatusOK)
	selected := http.Header{"X-Amz-Server-Side-Encryption": {"aws:kms"}, "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": {"allowed"}}
	put := RGWBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket+"/encrypted", payload, selected)
	RGWBackendRequire(t, put, http.StatusOK)
	if put.Headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" || put.Headers.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "allowed" {
		t.Fatal("SSE-KMS response lacks the selected mode and key")
	}
	plain := []byte("object without server-side encryption")
	RGWBackendRequire(t, RGWBackendSigned(t, ctx, trusted, "s3", http.MethodPut, bucket+"/plain", plain, nil), http.StatusOK)
	k := &RGWExternalKMS{Ctx: ctx, Cluster: cluster, Vault: vault, Gateways: gateways, Clients: clients, Trusted: trusted, WrongCA: wrongCA,
		User: user, KMSToken: kmsToken, Bucket: bucket, Payload: payload, Plain: plain, Selected: selected}
	k.Object = k.Identity(t, trusted)
	return k
}

// Identity reads the listing identity of the encrypted object through client.
func (k *RGWExternalKMS) Identity(t *testing.T, client S3HTTPClient) RGWBackendListedObject {
	t.Helper()
	response := RGWBackendSigned(t, k.Ctx, client, "s3", http.MethodGet, k.Bucket+"?list-type=2&prefix=encrypted", nil, nil)
	RGWBackendRequire(t, response, http.StatusOK)
	var listing struct {
		Contents []RGWBackendListedObject `xml:"Contents"`
	}
	if xml.Unmarshal(response.Body, &listing) != nil || len(listing.Contents) != 1 || listing.Contents[0].Key != "encrypted" || listing.Contents[0].ETag == "" || listing.Contents[0].Size != int64(len(k.Payload)) {
		t.Fatal("encrypted object listing identity unavailable")
	}
	return listing.Contents[0]
}

// ReadExact requires client to read the encrypted object's exact bytes as
// SSE-KMS, with an unchanged listing identity.
func (k *RGWExternalKMS) ReadExact(t *testing.T, client S3HTTPClient, step string) {
	t.Helper()
	read := RGWBackendSigned(t, k.Ctx, client, "s3", http.MethodGet, k.Bucket+"/encrypted", nil, nil)
	RGWBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.Body, k.Payload) || read.Headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" {
		t.Fatalf("%s: SSE-KMS read changed bytes or mode", step)
	}
	if k.Identity(t, client) != k.Object {
		t.Fatalf("%s: encrypted object identity changed", step)
	}
}

// ReadPlain requires client to serve what needs no key: the plain object and
// the listing of the encrypted one.
func (k *RGWExternalKMS) ReadPlain(t *testing.T, client S3HTTPClient, step string) {
	t.Helper()
	read := RGWBackendSigned(t, k.Ctx, client, "s3", http.MethodGet, k.Bucket+"/plain", nil, nil)
	RGWBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.Body, k.Plain) || k.Identity(t, client) != k.Object {
		t.Fatalf("%s: KMS-independent request failed", step)
	}
}

// Close deletes the two objects, the bucket and the user.
func (k *RGWExternalKMS) Close(t *testing.T) {
	t.Helper()
	ctx, trusted, bucket, user := k.Ctx, k.Trusted, k.Bucket, k.User
	for _, key := range []string{"encrypted", "plain"} {
		RGWBackendRequire(t, RGWBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, bucket+"/"+key, nil, nil), http.StatusNoContent)
	}
	RGWBackendRequire(t, RGWBackendSigned(t, ctx, trusted, "s3", http.MethodDelete, bucket, nil, nil), http.StatusNoContent)
	if err := user.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}

// RGWSSES3SetPolicy grants RGW the Transit operations of SSE-S3, with or
// without deletion.
func RGWSSES3SetPolicy(t *testing.T, ctx context.Context, vault *RGWKMSVault, deletion bool) {
	t.Helper()
	capabilities := `"create", "update", "read", "list"`
	if deletion {
		capabilities += `, "delete"`
	}
	vault.Call(t, ctx, http.MethodPut, "/v1/sys/policies/acl/"+RGWSSES3Policy, map[string]any{"policy": `path "tctransit/*" { capabilities = [` + capabilities + `] }`}, http.StatusNoContent)
}

func StartRGWKMSVault(t *testing.T, ctx context.Context, cluster *ceph.Container) *RGWKMSVault {
	t.Helper()
	image := os.Getenv("CEPH_TEST_VAULT_IMAGE")
	if image == "" {
		image = "hashicorp/vault:1.21.4"
	}
	vault := &RGWKMSVault{rootToken: RGWBackendRandom(t, 24)}
	opts := []testcontainers.ContainerCustomizer{
		network.WithNetworkName([]string{RGWKMSVaultHost}, StaticFacts(cluster).NetworkName),
		testcontainers.WithExposedPorts("8200/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) { hc.CapAdd = []string{"CAP_IPC_LOCK"} }),
		testcontainers.WithEnv(map[string]string{"VAULT_DEV_ROOT_TOKEN_ID": vault.rootToken}),
		testcontainers.WithEntrypoint("vault"),
		// The dev server writes its CA and certificate into an existing
		// directory; the extra SAN is the bridge alias RGW dials.
		testcontainers.WithCmd("server", "-dev", "-dev-no-store-token", "-dev-listen-address=0.0.0.0:8200", "-dev-tls", "-dev-tls-cert-dir=/tmp", "-dev-tls-san="+RGWKMSVaultHost),
		testcontainers.WithWaitStrategy(wait.ForAll(wait.ForListeningPort("8200/tcp"), wait.ForFile("/tmp/vault-ca.pem"))),
		// Dev-server logs contain the root token.
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
	}
	var err error
	vault.Container, err = testcontainers.Run(ctx, image, opts...)
	if vault.Container != nil {
		testcontainers.CleanupContainer(t, vault.Container)
	}
	if err != nil {
		t.Fatal(strings.ReplaceAll(err.Error(), vault.rootToken, "[redacted]"))
	}
	reader, err := vault.Container.CopyFileFromContainer(ctx, "/tmp/vault-ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	vault.CA, err = io.ReadAll(reader)
	_ = reader.Close()
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(vault.CA) {
		t.Fatal("Vault dev-TLS CA unreadable")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	vault.http = &http.Client{Transport: transport, Timeout: 20 * time.Second}
	t.Logf("Vault dev-TLS image=%s", image)
	return vault
}

// Call sends one management request with the root token and requires one of
// the wanted statuses.
func (vault *RGWKMSVault) Call(t *testing.T, ctx context.Context, method, path string, data any, want ...int) []byte {
	t.Helper()
	response := vault.Do(t, ctx, method, path, data)
	if !slices.Contains(want, response.Code) {
		// Vault bodies can contain keys and tokens.
		t.Fatalf("Vault %s %s status=%d want=%v (body redacted)", method, path, response.Code, want)
	}
	return response.Body
}

// Do resolves the mapped port on every call: when its bridge endpoint is
// reattached, Docker can publish the port on another host port.
func (vault *RGWKMSVault) Do(t *testing.T, ctx context.Context, method, path string, data any) RGWBackendResponse {
	t.Helper()
	endpoint, err := vault.Container.PortEndpoint(ctx, "8200/tcp", "https")
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	if data != nil {
		body = []byte(RGWBackendJSON(t, data))
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Vault-Token", vault.rootToken)
	request.Header.Set("Content-Type", "application/json")
	return RGWBackendDo(t, vault.http, request)
}

// Token issues a non-renewable token that has only policy.
func (vault *RGWKMSVault) Token(t *testing.T, ctx context.Context, policy string) string {
	t.Helper()
	created := vault.Call(t, ctx, http.MethodPost, "/v1/auth/token/create", map[string]any{
		"policies": []string{policy}, "no_default_policy": true, "ttl": "1h", "renewable": false,
	}, http.StatusOK)
	var auth struct {
		Auth struct {
			Token    string   `json:"client_token"`
			Policies []string `json:"policies"`
		} `json:"auth"`
	}
	if json.Unmarshal(created, &auth) != nil || auth.Auth.Token == "" || !slices.Equal(auth.Auth.Policies, []string{policy}) {
		t.Fatal("Vault did not issue a token with exactly the requested policy")
	}
	return auth.Auth.Token
}

func (vault *RGWKMSVault) Audit(t *testing.T, ctx context.Context) []byte {
	t.Helper()
	reader, err := vault.Container.CopyFileFromContainer(ctx, RGWKMSAuditFile)
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
