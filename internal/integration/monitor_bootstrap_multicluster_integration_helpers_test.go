//go:build all || (integration && topology && multicluster)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func monitorBootstrapRBDPeerConfig(t *testing.T, ctx context.Context, destination *ceph.Container, pool, peer string) (string, string) {
	t.Helper()
	policy, err := destination.PoolStatus(ctx, pool)
	if err != nil || policy.ID <= 0 {
		t.Fatalf("read RBD receiving pool identity: error=%v", err)
	}
	raw, err := destination.Ceph(ctx, "config-key", "get", fmt.Sprintf("rbd/mirror/peer/%d/%s", policy.ID, peer))
	if err != nil {
		t.Fatal("read native RBD remote peer bootstrap failed; content withheld")
	}
	var value struct {
		MonHost string `json:"mon_host"`
		Key     string `json:"key"`
	}
	if json.Unmarshal(raw, &value) != nil || value.MonHost == "" || value.Key == "" || len(monitorBootstrapAddressEndpoints(value.MonHost)) != 6 {
		t.Fatal("invalid native RBD remote peer bootstrap; content withheld")
	}
	return value.MonHost, value.Key
}

// Native RBD joins vectors with commas and includes endpoint nonces; the public
// mon_host value joins vectors with spaces and omits nonces. Compare the full
// type/IP/port multiset instead, retaining duplicates rather than hiding them.
func monitorBootstrapAddressEndpoints(addresses string) []string {
	values := regexp.MustCompile(`v[12]:[^,[:space:]]+`).FindAllString(addresses, -1)
	for i, value := range values {
		value = strings.TrimRight(value, "]")
		values[i], _, _ = strings.Cut(value, "/")
	}
	slices.Sort(values)
	return values
}

func monitorBootstrapPeerConfig(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peer string) map[string]json.RawMessage {
	t.Helper()
	raw, err := source.Ceph(ctx, "config-key", "get", "cephfs/mirror/peer/"+filesystem+"/"+peer)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || len(value) == 0 {
		t.Fatal("invalid native CephFS peer config; content withheld")
	}
	return value
}

func monitorBootstrapSetPeerConfig(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peer string, value map[string]json.RawMessage) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/tmp/tc-cephfs-peer-config.json"
	if err := source.ControlContainer().CopyToContainer(ctx, raw, path, 0o600); err != nil {
		t.Fatal(err)
	}
	topologyExecOutput(t, ctx, source.ControlContainer(), "ceph", "config-key", "set", "cephfs/mirror/peer/"+filesystem+"/"+peer, "-i", path)
	topologyExecOutput(t, ctx, source.ControlContainer(), "rm", "-f", path)
}

func monitorBootstrapAssertPeerConfig(t *testing.T, before, after map[string]json.RawMessage, addresses string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("remote refresh lost or added peer config fields")
	}
	for key, value := range before {
		if key == "mon_host" {
			var current string
			if json.Unmarshal(after[key], &current) != nil || current != addresses {
				t.Fatal("remote peer monitor addresses differ from the current destination monmap")
			}
		} else if !bytes.Equal(value, after[key]) {
			t.Fatalf("remote refresh changed peer field %s; values withheld", key)
		}
	}
}
