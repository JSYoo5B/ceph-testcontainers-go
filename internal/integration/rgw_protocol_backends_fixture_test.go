//go:build integration && features

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

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
