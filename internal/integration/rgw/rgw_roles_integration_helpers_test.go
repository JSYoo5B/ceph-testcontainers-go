//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rgwRoles creates an owned role through a real gateway with STS enabled and
// hands its ARN to a fresh AWS SDK process, which plays the SUT.
func rgwRoles(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	// The minimum server-side STS setup: a session-token key and STS
	// authentication of S3 requests, read by gateways at start.
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_sts_key", Value: rgwRolesSTSKey(t, ctx, cluster)},
		{Section: "client.admin", Name: "rgw_s3_auth_use_sts", Value: "true"},
	} {
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
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "roles", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	probe := buildS3Probe(t, ctx, endpoint, gateway.Region)
	owner, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-role-owner"})
	if err != nil {
		t.Fatal(err)
	}
	assumer, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-role-assumer"})
	if err != nil {
		t.Fatal(err)
	}
	ownerKey, assumerKey := owner.CreationKey(), assumer.CreationKey()
	access, secret, err := ownerKey.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	ownerS3 := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Timeout: 15 * time.Second}}
	const bucket = "tc-role-data"
	payload := []byte("data that only the role's temporary credentials read")
	ownerS3.Request(t, ctx, http.MethodPut, "/"+bucket, nil, http.StatusOK)
	ownerS3.Request(t, ctx, http.MethodPut, "/"+bucket+"/allowed", payload, http.StatusOK)
	ownerS3.Request(t, ctx, http.MethodPut, "/"+bucket+"/excluded", payload, http.StatusOK)
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/` + assumer.ID() + `"]},"Action":["sts:AssumeRole"]}]}`
	read := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::` + bucket + `/allowed"]}]}`

	// A role the test creates natively is the caller's: the fixture refuses
	// its name and never changes it.
	if _, err := gateway.Admin(ctx, "role", "create", "--role-name", "tc-caller-role", "--path", "/", "--assume-role-policy-doc", trust); err != nil {
		t.Fatal(err)
	}
	callerRole := rgwRoleDocument(t, ctx, gateway, "tc-caller-role")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if _, err := gateway.Admin(cleanup, "role", "delete", "--role-name", "tc-caller-role"); err != nil {
			t.Error("delete the caller's role (output redacted)")
		}
	})
	if role, err := gateway.CreateRole(ctx, rgw.RoleConfig{Name: "tc-caller-role", TrustPolicy: trust}); !errors.Is(err, ceph.ErrInvalidRequest) || role != nil {
		t.Fatalf("CreateRole over the caller's role: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if role, err := gateway.CreateRole(canceled, rgw.RoleConfig{Name: "tc-canceled", TrustPolicy: trust}); !errors.Is(err, context.Canceled) || role != nil {
		t.Fatalf("a canceled CreateRole: %v", err)
	}
	rgwRequireRoleAbsent(t, ctx, gateway, "tc-canceled")

	arm := installRGWKeyFaultShim(t, ctx, gateway)
	arm("pause-role")
	settled, cancelErr := cancelRGWCreate(t, ctx, gateway, func(call context.Context) (*rgw.Role, error) {
		return gateway.CreateRole(call, rgw.RoleConfig{Name: "tc-cancel-dispatched", TrustPolicy: trust})
	})
	if !errors.Is(cancelErr, context.Canceled) || settled == nil {
		t.Fatalf("cancel after native role create: handle=%t error=%v", settled != nil, cancelErr)
	}
	confirmed, arnErr := settled.ARN()
	document := rgwRoleDocument(t, ctx, gateway, "tc-cancel-dispatched")
	if arnErr != nil || confirmed == "" || document.ARN != confirmed || document.ID == "" || document.Trust != trust || len(document.Policies) != 0 {
		t.Fatalf("native role differs from the settled handle: %v", arnErr)
	}
	if err := settled.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	rgwRequireRoleAbsent(t, ctx, gateway, "tc-cancel-dispatched")
	removeRGWKeyFaultShim(t, ctx, gateway)

	role, err := gateway.CreateRole(ctx, rgw.RoleConfig{Name: "tc-reader", Path: "/fixture/", TrustPolicy: trust,
		Policies: []rgw.RolePolicy{{Name: "read-allowed", Document: read}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := role.Remove(cleanup); err != nil {
			t.Error(err)
		}
	})
	arn, err := role.ARN()
	if err != nil {
		t.Fatal(err)
	}
	native := rgwRoleDocument(t, ctx, gateway, "tc-reader")
	if native.ARN != arn || native.ID == "" || native.Path != "/fixture/" || native.Trust != trust || len(native.Policies) != 1 ||
		native.Policies[0].Name != "read-allowed" || native.Policies[0].Value != read {
		t.Fatal("the native role differs from the fixture's request or its handed-off ARN")
	}
	t.Log("owned role created with its trust and permission policy, and read back natively")

	// The SUT assumes the role with STS and uses the temporary credentials.
	if result := probe.runAs(t, ctx, ownerKey, arn, "get", bucket, "allowed", ""); result.Role != "refused" {
		t.Fatalf("an untrusted principal assumed the role: %+v", result)
	}
	if result := probe.run(t, ctx, assumerKey, "get", bucket, "allowed", ""); result.Outcome != "refused" {
		t.Fatalf("the assumer read the object without the role: %+v", result)
	}
	result := probe.runAs(t, ctx, assumerKey, arn, "get", bucket, "allowed", "")
	if result.Role != "assumed" || !strings.Contains(result.AssumedARN, "assumed-role/fixture/tc-reader/") || result.Outcome != "accepted" || result.SHA256 != sha256Hex(payload) {
		t.Fatalf("the trusted principal through the role: %+v", result)
	}
	if result := probe.runAs(t, ctx, assumerKey, arn, "get", bucket, "excluded", ""); result.Role != "assumed" || result.Outcome != "refused" {
		t.Fatalf("the role read an object its policy excludes: %+v", result)
	}
	if result := probe.runAs(t, ctx, assumerKey, arn, "put", bucket, "denied-write", "not granted"); result.Role != "assumed" || result.Outcome != "refused" {
		t.Fatalf("the role wrote although its policy grants only reads: %+v", result)
	}
	t.Log("the SUT's AssumeRole: untrusted principal refused; temporary credentials read the allowed object and are refused elsewhere")

	// A policy the test puts natively is the caller's: Remove refuses the
	// role and changes nothing until the test removes it again.
	if _, err := gateway.Admin(ctx, "role-policy", "put", "--role-name", "tc-reader", "--policy-name", "caller-policy", "--policy-doc", read); err != nil {
		t.Fatal(err)
	}
	if err := role.Remove(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Remove of a role with a caller's policy: %v", err)
	}
	if edited := rgwRoleDocument(t, ctx, gateway, "tc-reader"); edited.ID != native.ID || len(edited.Policies) != 2 {
		t.Fatal("the refused Remove changed the role")
	}
	if _, err := gateway.Admin(ctx, "role-policy", "delete", "--role-name", "tc-reader", "--policy-name", "caller-policy"); err != nil {
		t.Fatal(err)
	}

	// RGW refuses a policy that is valid JSON but not a valid policy: the
	// role is created and tracked without it, and Remove deletes it.
	partial, err := gateway.CreateRole(ctx, rgw.RoleConfig{Name: "tc-partial", TrustPolicy: trust,
		Policies: []rgw.RolePolicy{{Name: "invalid", Document: `{"Version":"2012-10-17","Statement":[{"Effect":"Sometimes","Action":["s3:GetObject"],"Resource":["*"]}]}`}}})
	if err == nil || partial == nil {
		t.Fatalf("a refused policy returned role %v, error %v", partial, err)
	}
	if document := rgwRoleDocument(t, ctx, gateway, "tc-partial"); len(document.Policies) != 0 {
		t.Fatal("RGW holds the refused policy")
	}
	if err := partial.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	rgwRequireRoleAbsent(t, ctx, gateway, "tc-partial")
	t.Log("a partial creation, with its permission policy refused by RGW, is removed")

	if err := role.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	rgwRequireRoleAbsent(t, ctx, gateway, "tc-reader")
	if current := rgwRoleDocument(t, ctx, gateway, "tc-caller-role"); current.ID != callerRole.ID || current.Trust != callerRole.Trust || len(current.Policies) != 0 {
		t.Fatal("the fixture changed the caller's role")
	}
	deadline := time.Now().Add(time.Minute)
	for probe.runAs(t, ctx, assumerKey, arn, "get", bucket, "allowed", "").Role != "refused" {
		if time.Now().After(deadline) {
			t.Fatal("STS still issues credentials for the removed role")
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Log("the removed role is gone natively and refused by STS; the caller's role is unchanged")
	for _, object := range []string{"allowed", "excluded"} {
		ownerS3.Request(t, ctx, http.MethodDelete, "/"+bucket+"/"+object, nil, http.StatusNoContent)
	}
	ownerS3.Request(t, ctx, http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
	for _, user := range []*rgw.User{owner, assumer} {
		if err := user.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// rgwRolesSTSKey generates the gateways' session-token key in the cluster;
// it is never logged.
func rgwRolesSTSKey(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	code, reader, err := cluster.Container.Exec(ctx, []string{"ceph-authtool", "--gen-print-key"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatal("generate the STS session-token key")
	}
	key, err := io.ReadAll(reader)
	if err != nil || len(bytes.TrimSpace(key)) == 0 {
		t.Fatal("read the STS session-token key")
	}
	return strings.TrimSpace(string(key))
}

// rgwRole is the part of a native role get that the scenario checks.
type rgwRole struct {
	ID       string `json:"RoleId"`
	ARN      string `json:"Arn"`
	Path     string `json:"Path"`
	Trust    string `json:"AssumeRolePolicyDocument"`
	Policies []struct {
		Name  string `json:"PolicyName"`
		Value string `json:"PolicyValue"`
	} `json:"PermissionPolicies"`
}

// rgwRoleDocument reads role name independently of the typed API.
func rgwRoleDocument(t *testing.T, ctx context.Context, gateway *rgw.Gateway, name string) rgwRole {
	t.Helper()
	data, err := gateway.Admin(ctx, "role", "get", "--role-name", name)
	if err != nil {
		t.Fatalf("native role get: %v", err)
	}
	var role rgwRole
	if err := json.Unmarshal(data, &role); err != nil {
		t.Fatal("decode the native role")
	}
	return role
}

// rgwRequireRoleAbsent checks that native state holds no role name: role get
// exits with ENOENT.
func rgwRequireRoleAbsent(t *testing.T, ctx context.Context, gateway *rgw.Gateway, name string) {
	t.Helper()
	_, err := gateway.Admin(ctx, "role", "get", "--role-name", name)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) || failure.ExitCode != 2 {
		t.Fatalf("role %s is not absent: %v", name, err)
	}
}
