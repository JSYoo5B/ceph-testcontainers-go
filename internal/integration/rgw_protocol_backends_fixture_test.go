//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRGWBackendSTSFormContentTypeIsSigned(t *testing.T) {
	payload := []byte("Action=AssumeRole&Version=2011-06-15")
	request, err := http.NewRequest(http.MethodPost, "http://rgw.invalid/", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	s3FeatureSign(request, s3HTTPClient{accessKey: "fixture-access", secretKey: "fixture-secret", region: "default"}, payload, time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC), "sts")
	// Ceph's secure SigV4 verifier requires CONTENT_TYPE in the canonical
	// signed headers whenever this form header is present. Omitting it denies
	// even a principal explicitly granted AssumeRole by the role trust policy.
	if !strings.Contains(request.Header.Get("Authorization"), "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,") {
		t.Fatal("STS form Content-Type is absent from the canonical SigV4 signed headers")
	}
}

func TestRGWBackendRoleCleanupRefusesForeignPolicy(t *testing.T) {
	const native = `{"RoleId":"owned-id","RoleName":"owned-role","Arn":"owned-arn","Path":"/fixture/","AssumeRolePolicyDocument":"owned-trust","MaxSessionDuration":3600}`
	var owned rgwBackendRole
	if err := json.Unmarshal([]byte(native), &owned); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, delta string
		allowed     bool
	}{
		{name: "captured native identity", delta: `{}`, allowed: true},
		{name: "owned inline policy", delta: `{"PermissionPolicies":[{"PolicyName":"owned-policy","PolicyValue":"owned-permission"}]}`, allowed: true},
		{name: "replacement role", delta: `{"RoleId":"replacement-id"}`},
		{name: "changed trust", delta: `{"AssumeRolePolicyDocument":"foreign-trust"}`},
		{name: "foreign inline policy", delta: `{"PermissionPolicies":[{"PolicyName":"foreign-policy","PolicyValue":"owned-permission"}]}`},
		{name: "changed owned permission", delta: `{"PermissionPolicies":[{"PolicyName":"owned-policy","PolicyValue":"foreign-permission"}]}`},
		{name: "managed policy", delta: `{"ManagedPermissionPolicies":[{"PolicyArn":"foreign-arn"}]}`},
		{name: "foreign tags", delta: `{"Tags":[{"Key":"foreign","Value":"tag"}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := owned
			if err := json.Unmarshal([]byte(test.delta), &current); err != nil {
				t.Fatal(err)
			}
			err := rgwBackendOwnedRole(current, owned, "owned-policy", []string{"owned-trust"}, []string{"owned-permission"})
			if (err == nil) != test.allowed {
				t.Fatalf("identity-guarded cleanup admission: allowed=%v error=%v", test.allowed, err)
			}
		})
	}
}

func TestRGWBackendAuditProofRequiresCompletedVaultTransactions(t *testing.T) {
	event := func(kind, id, path, failure string, allowed bool, key bool) []byte {
		value := map[string]any{
			"type": kind, "error": failure,
			"auth":    map[string]any{"policies": []string{"rgw-read-owned"}, "policy_results": map[string]bool{"allowed": allowed}},
			"request": map[string]string{"id": id, "operation": "read", "path": path},
		}
		if kind == "response" {
			data := map[string]string{}
			if key {
				data["key"] = "hmac-sha256:opaque-fixture-key"
			}
			value["response"] = map[string]any{"data": map[string]any{"data": data}}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return append(encoded, '\n')
	}
	request := event("request", "allowed-1", "tc/data/allowed", "", true, false)
	response := event("response", "allowed-1", "tc/data/allowed", "", true, true)
	deniedRequest := event("request", "denied-1", "tc/data/denied", "", false, false)
	deniedResponse := event("response", "denied-1", "tc/data/denied", "permission denied", false, false)
	for _, test := range []struct {
		name            string
		audit           []byte
		allowed, denied int
		invalid         bool
	}{
		{name: "attempt alone", audit: request},
		{name: "one completed read", audit: append(append([]byte{}, request...), response...), allowed: 1},
		{name: "permission denied response", audit: append(append([]byte{}, deniedRequest...), deniedResponse...), denied: 1},
		{name: "unclassified denied attempt", audit: append(append([]byte{}, deniedRequest...), event("response", "denied-1", "tc/data/denied", "upstream unavailable", false, false)...)},
		{name: "allowed path error", audit: append(append([]byte{}, request...), event("response", "allowed-1", "tc/data/allowed", "permission denied", false, false)...)},
		{name: "deleted key response", audit: append(append([]byte{}, request...), event("response", "allowed-1", "tc/data/allowed", "", true, false)...)},
		{name: "duplicate response", audit: append(append(append([]byte{}, request...), response...), response...), invalid: true},
		{name: "unmatched response", audit: response, invalid: true},
		{name: "missing identity", audit: event("request", "", "tc/data/allowed", "", true, false), invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			counts, err := rgwBackendVaultAuditProof(test.audit)
			if (err != nil) != test.invalid || counts["tc/data/allowed"] != test.allowed || counts["tc/data/denied"] != test.denied {
				t.Fatalf("completed read evidence: counts=%v error=%v", counts, err)
			}
		})
	}
}

func TestRGWBackendStatusProbeReceivesBoundedContext(t *testing.T) {
	parent, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	rgwBackendWaitStatus(t, parent, func(probe context.Context) rgwBackendResponse {
		deadline, bounded := probe.Deadline()
		if !bounded || time.Until(deadline) > time.Second || probe == parent {
			t.Fatal("status probe did not receive its bounded child context")
		}
		return rgwBackendResponse{code: http.StatusOK}
	}, http.StatusOK)
}
