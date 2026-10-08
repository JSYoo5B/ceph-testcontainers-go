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
	"log"
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
	testcontainervault "github.com/testcontainers/testcontainers-go/modules/vault"
	"github.com/testcontainers/testcontainers-go/network"
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
	t.Run("sts", func(t *testing.T) { rgwBackendSTS(t, ctx, cluster, gateways, endpoints, users, clients) })
	t.Run("swift", func(t *testing.T) { rgwBackendSwift(t, ctx, gateways[0], endpoints, users[0], clients[0]) })
	t.Run("sse-kms", func(t *testing.T) { rgwBackendKMS(t, ctx, gateways, endpoints, clients[0], vault) })
	for _, user := range users {
		if err := gateways[0].RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
}

func rgwBackendSTS(t *testing.T, ctx context.Context, cluster *ceph.Container, gateways []*ceph.RGWContainer, endpoints []string, users []*ceph.RGWUser, clients []s3HTTPClient) {
	t.Helper()
	const roleName, policyName, bucket = "tc-backend-read", "read-owned", "/tc-backend-sts"
	trust := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + users[1].ID()}, "Action": "sts:AssumeRole",
	}}})
	allow := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::tc-backend-sts/allowed",
	}}})
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`
	denyTrust := rgwBackendJSON(t, map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Deny", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + users[1].ID()}, "Action": "sts:AssumeRole",
	}}})
	// A brand-new RGW role metadata pool does not yet exist. Native role
	// list returns ENOENT before its formatter flushes; probe the exact role
	// instead and preserve the exit code through the public container API.
	rgwBackendRequireRoleAbsent(t, ctx, cluster, roleName)
	created := rgwBackendAdmin(t, ctx, gateways[0], "role", "create", "--role-name", roleName, "--path", "/fixture/", "--assume-role-policy-doc", trust, "--format", "json")
	var role rgwBackendRole
	if json.Unmarshal(created, &role) != nil || role.ID == "" || role.Name != roleName || !strings.HasSuffix(role.ARN, "role/fixture/"+roleName) {
		t.Fatal("native fresh role ID/ARN unavailable")
	}
	owner, assumer := clients[0], clients[1]
	roleRemoved, bucketRemoved, bucketID := false, false, ""
	// Register immediately after native identity capture. A failed AssumeRole
	// probe must not leave this role or its bucket behind for the later user
	// removal. Refuse foreign identity/policy changes rather than purging them.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if !roleRemoved {
			data, err := gateways[0].Admin(cleanup, "role", "get", "--role-name", roleName, "--format", "json")
			var current rgwBackendRole
			if err != nil || json.Unmarshal(data, &current) != nil {
				t.Error("STS cleanup role get failed (output redacted)")
			} else if err := rgwBackendOwnedRole(current, role, policyName, []string{trust, denyTrust}, []string{allow, deny}); err != nil {
				t.Error(err)
			} else {
				if len(current.Policies) != 0 {
					_, err = gateways[0].Admin(cleanup, "role-policy", "delete", "--role-name", roleName, "--policy-name", policyName)
				}
				if err == nil {
					_, err = gateways[0].Admin(cleanup, "role", "delete", "--role-name", roleName)
				}
				if err != nil {
					t.Error("STS cleanup owned role deletion failed (output redacted)")
				}
			}
		}
		if bucketRemoved || bucketID == "" {
			return
		}
		if _, err := gateways[0].UserInfo(cleanup, users[0]); err != nil {
			t.Error("STS cleanup owned bucket user identity changed (output redacted)")
			return
		}
		data, err := gateways[0].Admin(cleanup, "bucket", "stats", "--bucket", strings.TrimPrefix(bucket, "/"))
		var current rgwBackendBucket
		if err != nil || json.Unmarshal(data, &current) != nil || current.ID != bucketID || current.Name != strings.TrimPrefix(bucket, "/") || current.Owner != users[0].ID() {
			t.Error("STS cleanup bucket native identity changed or unavailable (output redacted)")
			return
		}
		// Delete only the fixture's explicit keys. An unexpected object makes
		// the final bucket DELETE fail; there is no purge of foreign contents.
		for _, key := range []string{"allowed", "excluded", "denied-write", ""} {
			path := bucket
			if key != "" {
				path += "/" + key
			}
			if err := rgwBackendCleanupDelete(cleanup, owner, path); err != nil {
				t.Error(err)
				return
			}
		}
	})
	t.Log("STS fresh owned role identity captured; failure cleanup registered")
	verifyIdentity := func() {
		t.Helper()
		var current rgwBackendRole
		data := rgwBackendAdmin(t, ctx, gateways[0], "role", "get", "--role-name", roleName, "--format", "json")
		if json.Unmarshal(data, &current) != nil {
			t.Fatal("role fixture native identity changed")
		}
		if err := rgwBackendOwnedRole(current, role, policyName, []string{trust, denyTrust}, []string{allow, deny}); err != nil {
			t.Fatal(err)
		}
	}
	verifyIdentity()
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "put", "--role-name", roleName, "--policy-name", policyName, "--policy-doc", allow)
	t.Log("STS owned trust and resource permission prepared")
	payload := bytes.Repeat([]byte("temporary-role-authorized-data\n"), 4096)
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket, nil, nil), http.StatusOK)
	var nativeBucket rgwBackendBucket
	data := rgwBackendAdmin(t, ctx, gateways[0], "bucket", "stats", "--bucket", strings.TrimPrefix(bucket, "/"))
	if json.Unmarshal(data, &nativeBucket) != nil || nativeBucket.ID == "" || nativeBucket.Name != strings.TrimPrefix(bucket, "/") || nativeBucket.Owner != users[0].ID() {
		t.Fatal("STS fresh owned bucket native identity unavailable")
	}
	bucketID = nativeBucket.ID
	for _, key := range []string{"allowed", "excluded"} {
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodPut, bucket+"/"+key, payload, nil), http.StatusOK)
	}
	assume := func(callCtx context.Context, client s3HTTPClient, session string) rgwBackendResponse {
		form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"}, "RoleArn": {role.ARN}, "RoleSessionName": {session}, "DurationSeconds": {"900"}}
		return rgwBackendSigned(t, callCtx, client, "sts", http.MethodPost, "/", []byte(form.Encode()), http.Header{"Content-Type": {"application/x-www-form-urlencoded"}})
	}
	// Owner is deliberately outside the trust policy; ownership of the S3
	// bucket must not grant the right to assume this role.
	t.Log("STS untrusted owner AssumeRole negative control")
	rgwBackendRequire(t, assume(ctx, owner, "untrusted"), http.StatusForbidden)
	t.Log("STS trusted principal AssumeRole credential issuance")
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
	t.Log("STS temporary credentials exact bytes and permission denials on both gateways")
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
	t.Log("STS existing-session resource permission revocation on both gateways")
	rgwBackendAdmin(t, ctx, gateways[0], "role-policy", "put", "--role-name", roleName, "--policy-name", policyName, "--policy-doc", deny)
	for _, endpoint := range endpoints {
		temporary.endpoint = endpoint
		rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
			return rgwBackendSigned(t, callCtx, temporary, "s3", http.MethodGet, bucket+"/allowed", nil, token)
		}, http.StatusForbidden)
	}
	verifyIdentity()
	t.Log("STS future-session trust revocation on both gateways")
	rgwBackendAdmin(t, ctx, gateways[0], "role-trust-policy", "modify", "--role-name", roleName, "--assume-role-policy-doc", denyTrust)
	for _, endpoint := range endpoints {
		assumer.endpoint = endpoint
		rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse { return assume(callCtx, assumer, "revoked-trust") }, http.StatusForbidden)
	}
	// Restore both policies, then prove the previously issued token once again
	// reads exact bytes. This tests native policy mutation, not token expiry.
	verifyIdentity()
	t.Log("STS trust and permission restore recovers existing-session exact bytes")
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
	roleRemoved = true
	for _, key := range []string{"allowed", "excluded"} {
		rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket+"/"+key, nil, nil), http.StatusNoContent)
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodDelete, bucket, nil, nil), http.StatusNoContent)
	bucketRemoved = true
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
	t.Log("Swift wrong-key auth control")
	bad := rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[0]+"/auth", nil, http.Header{"X-Auth-User": {subuser}, "X-Auth-Key": {"wrong-key"}})
	rgwBackendRequire(t, bad, http.StatusUnauthorized)
	t.Log("Swift valid-key auth control")
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
	// Missing-token requests are anonymous and, with account-in-URL enabled,
	// may fail tenant-name validation before authorization. An unrecognized
	// token scheme must instead be denied by the configured native auth
	// engines. Avoid HMAC mismatch: native debug logs can print a recomputed
	// valid token on that particular failure path.
	badToken := headers.Clone()
	badToken.Set("X-Auth-Token", "tc-unrecognized-token")
	t.Log("Swift unrecognized-token object control")
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[1]+path+"/swift-object", nil, badToken), http.StatusUnauthorized)
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
	// Account listing exists independently of the now-removed bucket. Record
	// a successful authenticated request before revoking its credential.
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[1]+storage.Path, nil, headers), http.StatusOK, http.StatusNoContent)
	rgwBackendAdmin(t, ctx, gateway, "subuser", "rm", "--uid", user.ID(), "--subuser", subuser, "--purge-keys")
	data = rgwBackendAdmin(t, ctx, gateway, "user", "info", "--uid", user.ID(), "--format", "json")
	info.Subusers, info.SwiftKeys = nil, nil
	if json.Unmarshal(data, &info) != nil || len(info.Subusers) != 0 || len(info.SwiftKeys) != 0 {
		t.Fatal("Swift purge did not remove the owned subuser/key")
	}
	t.Log("Swift removed-key auth control")
	// Removing the native Swift index causes get_user_by_swift to fail with
	// EACCES; native Swift maps that to 403 (wrong-key EPERM maps to 401).
	rgwBackendRequire(t, rgwBackendUnsigned(t, ctx, http.MethodGet, endpoints[0]+"/auth", nil, http.Header{"X-Auth-User": {subuser}, "X-Auth-Key": {secret}}), http.StatusForbidden)
	t.Log("Swift previously issued token after key removal")
	revoked := rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
		return rgwBackendUnsigned(t, callCtx, http.MethodGet, endpoints[1]+storage.Path, nil, headers)
	}, http.StatusUnauthorized)
	if rgwBackendErrorCode(revoked.body) != "AccessDenied" {
		t.Fatal("Swift revoked token did not fail native authorization")
	}
	// Removing a Swift subuser must not revoke this user's independent S3 key.
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, "/", nil, nil), http.StatusOK)
	t.Log("Swift: fresh native subuser/key/token, wrong-key and unrecognized-token denial, two-gateway token use, S3/Swift shared object bytes both directions, exact revoked-key lookup and explicit cleanup")
}

type rgwBackendVault struct {
	container      *testcontainervault.VaultContainer
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
		testcontainervault.WithToken(vault.rootToken),
		testcontainers.WithEntrypoint("vault"),
		testcontainers.WithCmd("server", "-dev", "-dev-no-store-token", "-dev-listen-address=0.0.0.0:8200"),
		// Dev-server startup logs contain the root token, including on a failed
		// readiness check. Preserve the returned error without logging secrets.
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
	}
	if cluster.UsesHostNetwork() {
		// Host-network RGW can route to a Docker bridge IP on the same Linux
		// host/VM. Vault keeps a random mapped host port for the Go orchestrator.
		opts = append(opts, network.WithBridgeNetwork())
	} else {
		opts = append(opts, network.WithNetworkName(nil, cluster.NetworkName()))
	}
	var err error
	vault.container, err = testcontainervault.Run(ctx, image, opts...)
	if vault.container != nil {
		testcontainers.CleanupContainer(t, vault.container)
	}
	if err != nil {
		t.Fatal(err)
	}
	vault.endpoint, err = vault.container.HttpHostAddress(ctx)
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
	readIdentity := func() rgwBackendListedObject {
		t.Helper()
		response := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"?list-type=2", nil, nil)
		rgwBackendRequire(t, response, http.StatusOK)
		var listing struct {
			Contents []rgwBackendListedObject `xml:"Contents"`
		}
		if xml.Unmarshal(response.body, &listing) != nil || len(listing.Contents) != 1 || listing.Contents[0].Key != "encrypted" || listing.Contents[0].ETag == "" || listing.Contents[0].Size != int64(len(payload)) {
			t.Fatal("KMS owned object listing identity/size unavailable")
		}
		return listing.Contents[0]
	}
	objectIdentity := readIdentity()
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
		// Vault's missing-key response propagates ENOENT through native KMS
		// and maps to S3 NoSuchKey. The object itself must still be listed
		// under the same key, ETag and size; 404 alone is not dependency proof.
		rgwBackendRequire(t, failed, http.StatusNotFound)
		if rgwBackendErrorCode(failed.body) != "NoSuchKey" || readIdentity() != objectIdentity {
			t.Fatal("KMS missing backend key did not retain owned S3 object identity")
		}
	}
	vault.call(t, ctx, http.MethodPost, "/v1/tc/data/allowed", map[string]any{"data": map[string]string{"key": vault.key}}, http.StatusOK)
	for _, endpoint := range endpoints {
		owner.endpoint = endpoint
		read := rgwBackendSigned(t, ctx, owner, "s3", http.MethodGet, bucket+"/encrypted", nil, nil)
		rgwBackendRequire(t, read, http.StatusOK)
		if !bytes.Equal(read.body, payload) {
			t.Fatal("restoring original Vault key did not recover exact object bytes")
		}
		if readIdentity() != objectIdentity {
			t.Fatal("restoring original Vault key changed owned S3 object identity")
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
	code      int
	body      []byte
	headers   http.Header
	operation string
}

type rgwBackendListedObject struct {
	Key  string `xml:"Key"`
	ETag string `xml:"ETag"`
	Size int64  `xml:"Size"`
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
	return rgwBackendResponse{code: response.StatusCode, body: body, headers: response.Header.Clone(), operation: request.Method + " " + request.URL.EscapedPath()}
}

func rgwBackendErrorCode(body []byte) string {
	var failure struct {
		Code  string `xml:"Code" json:"Code"`
		Error struct {
			Code string `xml:"Code" json:"Code"`
		} `xml:"Error" json:"Error"`
	}
	if xml.Unmarshal(body, &failure) != nil {
		_ = json.Unmarshal(body, &failure)
	}
	if failure.Code != "" {
		return failure.Code
	}
	// Swift's default formatter is plain text. Only these known native codes
	// may become diagnostics; arbitrary bodies can contain sensitive data.
	if plain := strings.TrimSpace(string(body)); slices.Contains([]string{"AccessDenied", "NoSuchKey", "InvalidTenantName"}, plain) {
		return plain
	}
	return failure.Error.Code
}

func rgwBackendRequire(t *testing.T, response rgwBackendResponse, allowed ...int) {
	t.Helper()
	if !slices.Contains(allowed, response.code) {
		t.Fatalf("native protocol %s status=%d want=%v code=%s (body redacted)", response.operation, response.code, allowed, rgwBackendErrorCode(response.body))
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
		// Only command/subcommand are safe: later arguments can be role
		// policies or secret keys. Native output remains redacted by Admin.
		t.Fatalf("RGW backend native %s: %v", strings.Join(args[:min(2, len(args))], " "), err)
	}
	return result
}

type rgwBackendRole struct {
	ID          string `json:"RoleId"`
	Name        string `json:"RoleName"`
	ARN         string `json:"Arn"`
	Path        string `json:"Path"`
	Account     string `json:"AccountId"`
	Description string `json:"Description"`
	Duration    uint64 `json:"MaxSessionDuration"`
	Trust       string `json:"AssumeRolePolicyDocument"`
	Policies    []struct {
		Name  string `json:"PolicyName"`
		Value string `json:"PolicyValue"`
	} `json:"PermissionPolicies"`
	Managed []json.RawMessage `json:"ManagedPermissionPolicies"`
	Tags    []json.RawMessage `json:"Tags"`
}

type rgwBackendBucket struct {
	ID    string `json:"id"`
	Name  string `json:"bucket"`
	Owner string `json:"owner"`
}

func rgwBackendOwnedRole(current, owned rgwBackendRole, policy string, trusts, permissions []string) error {
	if owned.ID == "" || current.ID != owned.ID || current.Name != owned.Name || current.ARN != owned.ARN || current.Path != owned.Path || current.Account != owned.Account || current.Description != owned.Description || current.Duration != owned.Duration {
		return errors.New("STS role fixture native identity or metadata changed (output redacted)")
	}
	if !slices.Contains(trusts, current.Trust) || len(current.Managed) != 0 || len(current.Tags) != 0 || len(current.Policies) > 1 {
		return errors.New("STS role fixture has an unrelated native policy or tag change (output redacted)")
	}
	if len(current.Policies) == 1 && (current.Policies[0].Name != policy || !slices.Contains(permissions, current.Policies[0].Value)) {
		return errors.New("STS role fixture inline policy changed outside this recipe (output redacted)")
	}
	return nil
}

func rgwBackendCleanupDelete(ctx context.Context, client s3HTTPClient, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, client.endpoint+path, nil)
	if err != nil {
		return errors.New("STS cleanup request construction failed")
	}
	s3FeatureSign(request, client, nil, time.Now().UTC(), "s3")
	response, err := client.http.Do(request)
	if err != nil {
		return errors.New("STS cleanup owned S3 DELETE transport failed (output redacted)")
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, response.Body); err != nil || ctx.Err() != nil {
		return errors.New("STS cleanup owned S3 DELETE response failed (output redacted)")
	}
	if response.StatusCode != http.StatusNoContent {
		return errors.New("STS cleanup owned S3 DELETE rejected (output redacted)")
	}
	return nil
}

func rgwBackendRequireRoleAbsent(t *testing.T, parent context.Context, cluster *ceph.Container, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	// This recipe uses a fresh standalone default zone. CLI belongs to the
	// control image; the separate RGW role is not required to contain it.
	code, reader, err := cluster.Container.Exec(ctx, []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json", "role", "get", "--role-name", name}, tcexec.Multiplexed())
	if err != nil || ctx.Err() != nil {
		t.Fatal("RGW backend native role get failed (output redacted)")
	}
	data, err := io.ReadAll(reader)
	if err != nil || ctx.Err() != nil || code != 2 || len(bytes.TrimSpace(data)) != 0 {
		t.Fatalf("role recipe refuses adoption or uncertain native role absence: exit=%d (output redacted)", code)
	}
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
