//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRGWProtocolBackends(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { rgwProtocolBackends(t, host) })
	}
}

func rgwProtocolBackends(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, _ := newServiceCluster(t, opts...)
	vault := rgwBackendStartVault(t, ctx, cluster)
	// Both RGWs use the same session-token CryptoKey. Never log key material.
	code, reader, err := cluster.Container.Exec(ctx, []string{"ceph-authtool", "--gen-print-key"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatal("generate STS session-token CryptoKey failed")
	}
	stsKey, err := io.ReadAll(reader)
	if err != nil || len(bytes.TrimSpace(stsKey)) == 0 {
		t.Fatal("read STS session-token CryptoKey failed")
	}
	settings := []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_sts_key", Value: strings.TrimSpace(string(stsKey))},
		{Section: "client.admin", Name: "rgw_s3_auth_use_sts", Value: "true"},
		{Section: "client.admin", Name: "rgw_enable_apis", Value: "s3,swift,swift_auth,sts,iam,admin"},
		{Section: "client.admin", Name: "rgw_swift_account_in_url", Value: "true"},
		{Section: "client.admin", Name: "rgw_crypt_s3_kms_backend", Value: "vault"},
		{Section: "client.admin", Name: "rgw_crypt_vault_auth", Value: "token"},
		{Section: "client.admin", Name: "rgw_crypt_vault_token_file", Value: "/tc/vault-token"},
		{Section: "client.admin", Name: "rgw_crypt_vault_addr", Value: vault.nativeEndpoint},
		{Section: "client.admin", Name: "rgw_crypt_vault_prefix", Value: "/v1/tc/data"},
		{Section: "client.admin", Name: "rgw_crypt_vault_secret_engine", Value: "kv"},
		// HTTP is explicit in this isolated backend recipe. The independent TLS
		// fixture tests the production-facing TLS transport/CA contract.
		{Section: "client.admin", Name: "rgw_crypt_require_ssl", Value: "false"},
	}
	for _, setting := range settings {
		change, err := cluster.TemporaryConfig(ctx, setting)
		if change != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				if err := change.Restore(cleanup); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	gateways := make([]*ceph.RGWContainer, 2)
	endpoints := make([]string, 2)
	for i, name := range []string{"protocol-a", "protocol-b"} {
		gateways[i], err = cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{Name: name, SkipUserCreation: true},
			testcontainers.WithFiles(testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(vault.gatewayToken)), ContainerFilePath: "/tc/vault-token", FileMode: 0o600}))
		if err != nil {
			t.Fatal(err)
		}
		endpoints[i], err = gateways[i].S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	users := make([]*ceph.RGWUser, 2)
	clients := make([]s3HTTPClient, 2)
	for i, id := range []string{"tc-backend-owner", "tc-backend-assumer"} {
		users[i], err = gateways[0].CreateUser(ctx, ceph.RGWUserConfig{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		access, secret, err := users[i].Credentials()
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = s3HTTPClient{endpoint: endpoints[0], accessKey: access, secretKey: secret, region: gateways[0].Region, http: &http.Client{Timeout: 20 * time.Second}}
	}
	t.Run("sts", func(t *testing.T) { rgwBackendSTS(t, ctx, gateways, endpoints, users, clients) })
	t.Run("swift", func(t *testing.T) { rgwBackendSwift(t, ctx, gateways[0], endpoints, users[0], clients[0]) })
	t.Run("sse-kms", func(t *testing.T) { rgwBackendKMS(t, ctx, gateways, endpoints, clients[0], vault) })
	for _, user := range users {
		if err := gateways[0].RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
}

func rgwBackendSTS(t *testing.T, ctx context.Context, gateways []*ceph.RGWContainer, endpoints []string, users []*ceph.RGWUser, clients []s3HTTPClient) {
	t.Helper()
	const roleName, policyName, bucket = "tc-backend-read", "read-owned", "/tc-backend-sts"
	trust := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + users[1].ID()}, "Action": "sts:AssumeRole",
	}}})
	allow := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::tc-backend-sts/allowed",
	}}})
	listing := rgwBackendAdmin(t, ctx, gateways[0], "role", "list", "--format", "json")
	var roles []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(listing, &roles) != nil {
		t.Fatal("decode native role listing")
	}
	for _, role := range roles {
		if role.Name == roleName {
			t.Fatal("role recipe refuses to adopt an existing role")
		}
	}
	created := rgwBackendAdmin(t, ctx, gateways[0], "role", "create", "--role-name", roleName, "--path", "/fixture/", "--assume-role-policy-doc", trust, "--format", "json")
	var role struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		ARN  string `json:"arn"`
	}
	if json.Unmarshal(created, &role) != nil || role.ID == "" || role.Name != roleName || !strings.HasSuffix(role.ARN, "role/fixture/"+roleName) {
		t.Fatal("native fresh role ID/ARN unavailable")
	}
	verifyIdentity := func() {
		t.Helper()
		var current struct {
			ID string `json:"id"`
		}
		data := rgwBackendAdmin(t, ctx, gateways[0], "role", "get", "--role-name", roleName, "--format", "json")
		if json.Unmarshal(data, &current) != nil || current.ID != role.ID {
			t.Fatal("role fixture native identity changed")
		}
	}
	verifyIdentity()
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "put", "--role-name", roleName, "--policy-name", policyName, "--policy-doc", allow)
	owner, assumer := clients[0], clients[1]
	payload := bytes.Repeat([]byte("temporary-role-authorized-data\n"), 4096)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket, nil, nil), http.StatusOK)
	for _, key := range []string{"allowed", "excluded"} {
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket+"/"+key, payload, nil), http.StatusOK)
	}
	assume := func(callCtx context.Context, client s3HTTPClient, session string) rgwBackendResponse {
		form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"}, "RoleArn": {role.ARN}, "RoleSessionName": {session}, "DurationSeconds": {"900"}}
		return rgwBackendSigned(t, callCtx, client, "sts", http.MethodPost, "/", []byte(form.Encode()), http.Header{"Content-Type": {"application/x-www-form-urlencoded"}})
	}
	// Owner is deliberately outside the trust policy; ownership of the S3
	// bucket must not grant the right to assume this role.
	rgwBackendRequire(t, assume(ctx, owner, "untrusted"), http.StatusForbidden)
	response := assume(ctx, assumer, "trusted-consumer")
	rgwBackendRequire(t, response, http.StatusOK)
	var assumed struct {
		Result struct {
			Credentials struct {
				Access     string    `xml:"AccessKeyId"`
				Secret     string    `xml:"SecretAccessKey"`
				Token      string    `xml:"SessionToken"`
				Expiration time.Time `xml:"Expiration"`
			} `xml:"Credentials"`
			User struct {
				ARN string `xml:"Arn"`
			} `xml:"AssumedRoleUser"`
		} `xml:"AssumeRoleResult"`
	}
	if xml.Unmarshal(response.body, &assumed) != nil || assumed.Result.Credentials.Access == "" || assumed.Result.Credentials.Secret == "" || assumed.Result.Credentials.Token == "" || !assumed.Result.Credentials.Expiration.After(time.Now()) {
		t.Fatal("STS did not issue live temporary credentials")
	}
	if !strings.Contains(assumed.Result.User.ARN, "assumed-role/") {
		t.Fatal("STS assumed-role identity unavailable")
	}
	temporary := owner
	temporary.accessKey, temporary.secretKey = assumed.Result.Credentials.Access, assumed.Result.Credentials.Secret
	token := http.Header{"X-Amz-Security-Token": {assumed.Result.Credentials.Token}}
	for _, endpoint := range endpoints {
		temporary.endpoint = endpoint
		read := rgwBackendSigned(t, ctx, temporary, "s3", http.MethodGet, bucket+"/allowed", nil, token)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) {
			t.Fatal("STS-authorized object bytes changed")
		}
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, temporary, "s3", http.MethodGet, bucket+"/excluded", nil, token), http.StatusForbidden)
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, temporary, "s3", http.MethodPut, bucket+"/denied-write", []byte("not granted"), token), http.StatusForbidden)
		rgwBackendAuthDenied(t, rgwBackendSigned(t, ctx, temporary, "s3", http.MethodGet, bucket+"/allowed", nil, nil))
	}
	verifyIdentity()
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "put", "--role-name", roleName, "--policy-name", policyName, "--policy-doc", deny)
	for _, endpoint := range endpoints {
		temporary.endpoint = endpoint
		rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
			return rgwBackendSigned(t, callCtx, temporary, "s3", http.MethodGet, bucket+"/allowed", nil, token)
		}, http.StatusForbidden)
	}
	verifyIdentity()
	denyTrust := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Deny", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + users[1].ID()}, "Action": "sts:AssumeRole",
	}}})
	rgwBackendAdmin(t, ctx, gateways[0], "role-trust-policy", "modify", "--role-name", roleName, "--assume-role-policy-doc", denyTrust)
	for _, endpoint := range endpoints {
		assumer.endpoint = endpoint
		rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse { return assume(callCtx, assumer, "revoked-trust") }, http.StatusForbidden)
	}
	// Restore both policies, then prove the previously issued token once again
	// reads exact bytes. This tests native policy mutation, not token expiry.
	verifyIdentity()
	rgwBackendAdmin(t, ctx, gateways[0], "role-trust-policy", "modify", "--role-name", roleName, "--assume-role-policy-doc", trust)
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "put", "--role-name", roleName, "--policy-name", policyName, "--policy-doc", allow)
	for _, endpoint := range endpoints {
		temporary.endpoint = endpoint
		read := rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
			return rgwBackendSigned(t, callCtx, temporary, "s3", http.MethodGet, bucket+"/allowed", nil, token)
		}, http.StatusOK)
		if !bytes.Equal(read.body, payload) {
			t.Fatal("restored role permission did not preserve bytes")
		}
	}
	verifyIdentity()
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "delete", "--role-name", roleName, "--policy-name", policyName)
	rgwBackendAdmin(t, ctx, gateways[0], "role", "delete", "--role-name", roleName)
	for _, key := range []string{"allowed", "excluded"} {
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket+"/"+key, nil, nil), http.StatusNoContent)
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket, nil, nil), http.StatusNoContent)
	t.Log("STS: shared-key two-gateway temporary credentials, exact allowed bytes, trust/action/resource denial, existing-session role-policy revoke/restore and future-session trust revoke; owned role and bucket removed")
}

func rgwBackendSwift(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, endpoints []string, user *ceph.RGWUser, owner s3HTTPClient) {
	t.Helper()
	const bucket = "tc-backend-swift"
	subuser := user.ID() + ":swift"
	secret := rgwBackendRandom(t, 24)
	rgwBackendAdmin(t, ctx, gateway, "subuser", "create", "--uid", user.ID(), "--subuser", subuser, "--access", "full", "--key-type", "swift", "--secret", secret)
	var info struct {
		Subusers []struct {
			ID          string `json:"id"`
			Permissions string `json:"permissions"`
		} `json:"subusers"`
		SwiftKeys []struct {
			User   string `json:"user"`
			Secret string `json:"secret_key"`
		} `json:"swift_keys"`
	}
	data := rgwBackendAdmin(t, ctx, gateway, "user", "info", "--uid", user.ID(), "--format", "json")
	if json.Unmarshal(data, &info) != nil || len(info.SwiftKeys) != 1 || info.SwiftKeys[0].User != subuser || info.SwiftKeys[0].Secret != secret || len(info.Subusers) != 1 || info.Subusers[0].ID != subuser || info.Subusers[0].Permissions != "full-control" {
		t.Fatal("native fresh Swift subuser/key readback mismatch")
	}
	bad := rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[0]+"/auth", nil, http.Header{"X-Auth-User": {subuser}, "X-Auth-Key": {"wrong-key"}})
	rgwBackendRequire(t, bad, http.StatusUnauthorized)
	auth := rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[0]+"/auth", nil, http.Header{"X-Auth-User": {subuser}, "X-Auth-Key": {secret}})
	rgwBackendRequire(t, auth, http.StatusOK, http.StatusNoContent)
	token, storageURL := auth.headers.Get("X-Auth-Token"), auth.headers.Get("X-Storage-Url")
	if token == "" || storageURL == "" {
		t.Fatal("Swift auth did not issue token/storage URL")
	}
	storage, err := url.Parse(storageURL)
	if err != nil || !strings.HasSuffix(storage.Path, "/AUTH_"+user.ID()) {
		t.Fatal("Swift storage URL does not select the owned account")
	}
	// Use the server-returned account path with each externally reachable RGW.
	// This also avoids assuming an advertised internal bridge URL is reachable
	// directly from a macOS test orchestrator.
	headers := http.Header{"X-Auth-Token": {token}, "Content-Type": {"application/octet-stream"}}
	path := storage.Path + "/" + bucket
	payload := bytes.Repeat([]byte("native-swift-coexists-with-s3\n"), 4096)
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodPut, endpoints[0]+path, nil, headers), http.StatusCreated)
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodPut, endpoints[0]+path+"/swift-object", payload, headers), http.StatusCreated)
	for _, endpoint := range endpoints {
		read := rgwBackendUnsigned(t, ctx, http.MethodGet, endpoint+path+"/swift-object", nil, headers)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) {
			t.Fatal("Swift token read changed exact bytes")
		}
	}
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[1]+path+"/swift-object", nil, nil), http.StatusUnauthorized)
	read := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, "/"+bucket+"/swift-object", nil, nil)
	rgwBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.body, payload) {
		t.Fatal("S3 did not read the same Swift object")
	}
	s3Data := bytes.Repeat([]byte("s3-to-swift\n"), 4096)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, "/"+bucket+"/s3-object", s3Data, nil), http.StatusOK)
	read = rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[1]+path+"/s3-object", nil, headers)
	rgwBackendRequire(t, read, http.StatusOK)
	if !bytes.Equal(read.body, s3Data) {
		t.Fatal("Swift did not read the same S3 object")
	}
	for _, key := range []string{"swift-object", "s3-object"} {
		rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodDelete, endpoints[0]+path+"/"+key, nil, headers), http.StatusNoContent)
	}
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodDelete, endpoints[0]+path, nil, headers), http.StatusNoContent)
	rgwBackendAdmin(t, ctx, gateway, "subuser", "rm", "--uid", user.ID(), "--subuser", subuser, "--purge-keys")
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[0]+"/auth", nil, http.Header{"X-Auth-User": {subuser}, "X-Auth-Key": {secret}}), http.StatusUnauthorized)
	rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
		return rgwBackendUnsigned(t, callCtx, http.MethodGet, endpoints[1]+storage.Path, nil, headers)
	}, http.StatusUnauthorized)
	// Removing a Swift subuser must not revoke this user's independent S3 key.
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, "/", nil, nil), http.StatusOK)
	t.Log("Swift: fresh native subuser/key/token, wrong-key and missing-token denial, two-gateway token use, S3/Swift shared object bytes both directions, explicit container/subuser/key cleanup")
}

type rgwBackendVault struct {
	container      testcontainers.Container
	endpoint       string
	nativeEndpoint string
	rootToken      string
	gatewayToken   string
	key            string
}

func rgwBackendStartVault(t *testing.T, ctx context.Context, cluster *ceph.Container) *rgwBackendVault {
	t.Helper()
	image := os.Getenv("CEPH_TEST_VAULT_IMAGE")
	if image == "" {
		image = "hashicorp/vault:1.21.4"
	}
	vault := &rgwBackendVault{rootToken: rgwBackendRandom(t, 24)}
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("vault"),
		testcontainers.WithCmd("server", "-dev", "-dev-listen-address=0.0.0.0:8200", "-dev-root-token-id="+vault.rootToken),
		testcontainers.WithExposedPorts("8200/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/v1/sys/health").WithPort("8200/tcp").WithStatusCodeMatcher(func(code int) bool { return code == http.StatusOK })),
	}
	if cluster.UsesHostNetwork() {
		// Host-network RGW can route to a Docker bridge IP on the same Linux
		// host/VM. Vault keeps a random mapped host port for the Go orchestrator.
		opts = append(opts, network.WithBridgeNetwork())
	} else {
		opts = append(opts, network.WithNetworkName(nil, cluster.NetworkName()))
	}
	var err error
	vault.container, err = testcontainers.Run(ctx, image, opts...)
	if vault.container != nil {
		testcontainers.CleanupContainer(t, vault.container)
	}
	if err != nil {
		t.Fatal(err)
	}
	vault.endpoint, err = vault.container.PortEndpoint(ctx, "8200/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	ip, err := vault.container.ContainerIP(ctx)
	if err != nil || net.ParseIP(ip) == nil {
		t.Fatal("Vault internal bridge IP unavailable")
	}
	vault.nativeEndpoint = "http://" + net.JoinHostPort(ip, "8200")
	vault.call(t, ctx, http.MethodPost, "/v1/sys/mounts/tc", map[string]any{"type": "kv", "options": map[string]string{"version": "2"}}, http.StatusNoContent)
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	vault.key = base64.StdEncoding.EncodeToString(keyBytes)
	for _, key := range []string{"allowed", "denied"} {
		vault.call(t, ctx, http.MethodPost, "/v1/tc/data/"+key, map[string]any{"data": map[string]string{"key": vault.key}}, http.StatusOK)
	}
	vault.call(t, ctx, http.MethodPut, "/v1/sys/policies/acl/rgw-read-owned", map[string]any{
		"policy": `path "tc/data/allowed" { capabilities = ["read"] }`,
	}, http.StatusNoContent)
	created := vault.call(t, ctx, http.MethodPost, "/v1/auth/token/create", map[string]any{
		"policies": []string{"rgw-read-owned"}, "no_default_policy": true, "ttl": "1h", "renewable": false,
	}, http.StatusOK)
	var auth struct {
		Auth struct {
			Token    string   `json:"client_token"`
			Policies []string `json:"policies"`
		} `json:"auth"`
	}
	if json.Unmarshal(created, &auth) != nil || auth.Auth.Token == "" || !slices.Equal(auth.Auth.Policies, []string{"rgw-read-owned"}) {
		t.Fatal("Vault did not issue exact scoped read-only token")
	}
	vault.gatewayToken = auth.Auth.Token
	vault.call(t, ctx, http.MethodPut, "/v1/sys/audit/rgw", map[string]any{"type": "file", "options": map[string]string{"file_path": "/tmp/tc-rgw-vault-audit.json"}}, http.StatusNoContent)
	t.Logf("real Vault KV-v2 fixture image=%s; read-only RGW token has one owned-key path", image)
	return vault
}

func (vault *rgwBackendVault) call(t *testing.T, ctx context.Context, method, path string, data any, want int) []byte {
	t.Helper()
	var body []byte
	if data != nil {
		body = []byte(rgwBackendJSON(t, data))
	}
	response := rgwBackendUnsigned(t, ctx, method, vault.endpoint+path, body,
		http.Header{"X-Vault-Token": {vault.rootToken}, "Content-Type": {"application/json"}})
	if response.code != want {
		// Vault bodies can contain keys, tokens and audit accessors.
		t.Fatalf("Vault %s %s status=%d want=%d (body redacted)", method, path, response.code, want)
	}
	return response.body
}

func rgwBackendKMS(t *testing.T, ctx context.Context, _ []*ceph.RGWContainer, endpoints []string, owner s3HTTPClient, vault *rgwBackendVault) {
	t.Helper()
	const bucket = "/tc-backend-kms"
	encoded := http.Header{"X-Amz-Server-Side-Encryption": {"aws:kms"}, "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": {"allowed"}}
	payload := bytes.Repeat([]byte("real-vault-backed-encrypted-payload\n"), 32768)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket, nil, nil), http.StatusOK)
	put := rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket+"/encrypted", payload, encoded)
	rgwBackendRequire(t, put, http.StatusOK)
	if put.headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" || put.headers.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "allowed" {
		t.Fatal("native KMS response lacks selected encryption/key identity")
	}
	for _, endpoint := range endpoints {
		owner.endpoint = endpoint
		read := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) || read.headers.Get("X-Amz-Server-Side-Encryption") != "aws:kms" {
			t.Fatal("Vault-backed fresh gateway read changed bytes/encryption mode")
		}
	}
	denied := encoded.Clone()
	denied.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", "denied")
	failed := rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket+"/denied-key", []byte("Vault policy excludes this existing key"), denied)
	rgwBackendRequire(t, failed, http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"/denied-key", nil, nil), http.StatusNotFound)
	// Removing the real backend secret prevents decryption; this is a positive
	// control for the KMS dependency, not an HTTP mock's canned response.
	vault.call(t, ctx, http.MethodDelete, "/v1/tc/data/allowed", nil, http.StatusNoContent)
	for _, endpoint := range endpoints {
		owner.endpoint = endpoint
		failed := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, failed, http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError)
	}
	vault.call(t, ctx, http.MethodPost, "/v1/tc/data/allowed", map[string]any{"data": map[string]string{"key": vault.key}}, http.StatusOK)
	for _, endpoint := range endpoints {
		owner.endpoint = endpoint
		read := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) {
			t.Fatal("restoring original Vault key did not recover exact object bytes")
		}
	}
	reader, err := vault.container.CopyFileFromContainer(ctx, "/tmp/tc-rgw-vault-audit.json")
	if err != nil {
		t.Fatal(err)
	}
	audit, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	counts, err := rgwBackendVaultAuditProof(audit)
	if err != nil {
		t.Fatal(err)
	}
	if counts["tc/data/allowed"] < 4 || counts["tc/data/denied"] == 0 {
		t.Fatalf("native Vault audit did not prove RGW key fetch/permission denial: allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket+"/encrypted", nil, nil), http.StatusNoContent)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket, nil, nil), http.StatusNoContent)
	vault.call(t, ctx, http.MethodDelete, "/v1/tc/metadata/allowed", nil, http.StatusNoContent)
	vault.call(t, ctx, http.MethodDelete, "/v1/tc/metadata/denied", nil, http.StatusNoContent)
	vault.call(t, ctx, http.MethodPost, "/v1/auth/token/revoke", map[string]string{"token": vault.gatewayToken}, http.StatusNoContent)
	t.Logf("SSE-KMS: real Vault KV-v2, scoped allowed/denied key audit reads=%d/%d, two-gateway exact bytes, actual key deletion prevents decrypt and same-key restoration recovers data; owned object/key versions/token removed", counts["tc/data/allowed"], counts["tc/data/denied"])
}

type rgwBackendResponse struct {
	code    int
	body    []byte
	headers http.Header
}

func rgwBackendSigned(t *testing.T, ctx context.Context, client s3HTTPClient, service, method, path string, payload []byte, headers http.Header) rgwBackendResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range headers {
		request.Header[key] = slices.Clone(values)
	}
	s3FeatureSign(request, client, payload, time.Now().UTC(), service)
	return rgwBackendDo(t, client.http, request)
}

func rgwBackendUnsigned(t *testing.T, ctx context.Context, method, endpoint string, payload []byte, headers http.Header) rgwBackendResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range headers {
		request.Header[key] = slices.Clone(values)
	}
	return rgwBackendDo(t, &http.Client{Timeout: 20 * time.Second}, request)
}

func rgwBackendDo(t *testing.T, client *http.Client, request *http.Request) rgwBackendResponse {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return rgwBackendResponse{response.StatusCode, body, response.Header.Clone()}
}

func rgwBackendRequire(t *testing.T, response rgwBackendResponse, allowed ...int) {
	t.Helper()
	if !slices.Contains(allowed, response.code) {
		var failure struct {
			Code  string `xml:"Code"`
			Error struct {
				Code string `xml:"Code"`
			} `xml:"Error"`
		}
		_ = xml.Unmarshal(response.body, &failure)
		t.Fatalf("native protocol status=%d want=%v code=%s/%s (body redacted)", response.code, allowed, failure.Code, failure.Error.Code)
	}
}

func rgwBackendAuthDenied(t *testing.T, response rgwBackendResponse) {
	t.Helper()
	rgwBackendRequire(t, response, http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden)
}

func rgwBackendWaitStatus(t *testing.T, parent context.Context, probe func(context.Context) rgwBackendResponse, want int) rgwBackendResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		response := probe(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if response.code == want {
			return response
		}
		select {
		case <-ctx.Done():
			rgwBackendRequire(t, response, want)
			return response
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Count completed native Vault transactions, not two audit entries for each
// attempt. Values remain HMAC-redacted and never enter diagnostics.
func rgwBackendVaultAuditProof(data []byte) (map[string]int, error) {
	type auditEvent struct {
		Type  string `json:"type"`
		Error string `json:"error"`
		Auth  struct {
			Policies      []string `json:"policies"`
			PolicyResults *struct {
				Allowed bool `json:"allowed"`
			} `json:"policy_results"`
		} `json:"auth"`
		Request struct {
			ID        string `json:"id"`
			Path      string `json:"path"`
			Operation string `json:"operation"`
		} `json:"request"`
		Response struct {
			Data struct {
				Data map[string]json.RawMessage `json:"data"`
			} `json:"data"`
		} `json:"response"`
	}
	counts := map[string]int{}
	requests := map[string]auditEvent{}
	responses := map[string]bool{}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event auditEvent
		if json.Unmarshal(line, &event) != nil {
			return nil, errors.New("decode real Vault audit event (redacted)")
		}
		if event.Request.Operation != "read" || !slices.Equal(event.Auth.Policies, []string{"rgw-read-owned"}) || (event.Request.Path != "tc/data/allowed" && event.Request.Path != "tc/data/denied") {
			continue
		}
		if event.Request.ID == "" {
			return nil, errors.New("Vault key read audit is missing native request identity (redacted)")
		}
		if event.Type == "request" {
			if _, duplicate := requests[event.Request.ID]; duplicate {
				return nil, errors.New("Vault key read audit has duplicate native request identity (redacted)")
			}
			requests[event.Request.ID] = event
			continue
		}
		if event.Type != "response" {
			return nil, errors.New("Vault key read audit has unknown native entry type (redacted)")
		}
		request, exists := requests[event.Request.ID]
		if !exists || request.Request != event.Request || responses[event.Request.ID] {
			return nil, errors.New("Vault key read response lacks a unique matching request (redacted)")
		}
		responses[event.Request.ID] = true
		if event.Request.Path == "tc/data/allowed" {
			var key string
			if event.Error == "" && json.Unmarshal(event.Response.Data.Data["key"], &key) == nil && key != "" && (event.Auth.PolicyResults == nil || event.Auth.PolicyResults.Allowed) {
				counts[event.Request.Path]++
			}
		} else if strings.Contains(strings.ToLower(event.Error), "permission denied") && (event.Auth.PolicyResults == nil || !event.Auth.PolicyResults.Allowed) {
			counts[event.Request.Path]++
		}
	}
	return counts, nil
}

func rgwBackendAdmin(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, args ...string) []byte {
	t.Helper()
	result, err := gateway.Admin(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func rgwBackendJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func rgwBackendRandom(t *testing.T, size int) string {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}
