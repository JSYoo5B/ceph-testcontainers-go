//go:build all || (integration && hostnetwork)

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type hostNetworkDaemonEndpoints struct {
	cluster   int
	daemon    string
	addresses []string
}

func hostNetworkAssertDistinctAddresses(t *testing.T, ctx context.Context, clusters []*ceph.Container) []hostNetworkDaemonEndpoints {
	t.Helper()
	seen := make(map[string]int)
	var groups []hostNetworkDaemonEndpoints
	for i, cluster := range clusters {
		for _, daemon := range []string{"mon", "mgr", "osd"} {
			data, err := cluster.Ceph(ctx, daemon, "dump", "--format", "json")
			if err != nil {
				t.Fatal(err)
			}
			var dump any
			if err := json.Unmarshal(data, &dump); err != nil {
				t.Fatal(err)
			}
			endpoints, err := hostNetworkNormalizeEndpoints(dump, harness.StaticFacts(cluster).PublicAddress)
			if err != nil {
				t.Fatalf("cluster %d %s advertised endpoints: %v", i, daemon, err)
			}
			if len(endpoints) == 0 {
				t.Fatalf("cluster %d %s dump does not advertise any %s endpoints: %s", i, daemon, harness.StaticFacts(cluster).PublicAddress, data)
			}
			for _, endpoint := range endpoints {
				if owner, exists := seen[endpoint]; exists && owner != i {
					t.Fatalf("clusters %d and %d advertise the same endpoint %s", owner, i, endpoint)
				}
				seen[endpoint] = i
			}
			groups = append(groups, hostNetworkDaemonEndpoints{cluster: i, daemon: daemon, addresses: endpoints})
			t.Logf("cluster %d %s endpoints: %v", i, daemon, endpoints)
		}
	}
	if os.Getenv("CEPH_TEST_HOST_TCP_REQUIRED") == "1" {
		hostNetworkAssertRunnerTCP(t, ctx, groups)
	}
	return groups
}

// Dump fields differ between Ceph versions. Normalize actual IP/port pairs
// across address vectors and legacy fields rather than nonce-bearing strings.
func hostNetworkNormalizeEndpoints(dump any, publicAddress string) ([]string, error) {
	prefix := net.JoinHostPort(publicAddress, "")
	pattern := regexp.MustCompile(`(?:v[12]:)?(` + regexp.QuoteMeta(prefix) + `[0-9]+)(?:/[0-9]+)?`)
	addresses := make(map[string]bool)
	var visit func(any) error
	visit = func(value any) error {
		switch value := value.(type) {
		case string:
			for _, match := range pattern.FindAllStringSubmatch(value, -1) {
				_, port, err := net.SplitHostPort(match[1])
				if err != nil {
					return fmt.Errorf("invalid endpoint %q: %w", match[1], err)
				}
				number, err := strconv.Atoi(port)
				if err != nil || number > 65535 {
					return fmt.Errorf("invalid endpoint port %q", match[1])
				}
				// MGR can retain a legacy :0 sentinel alongside real vectors.
				if number == 0 {
					continue
				}
				addresses[match[1]] = true
			}
		case []any:
			for _, child := range value {
				if err := visit(child); err != nil {
					return err
				}
			}
		case map[string]any:
			for _, child := range value {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(dump); err != nil {
		return nil, err
	}
	var endpoints []string
	for address := range addresses {
		endpoints = append(endpoints, address)
	}
	slices.Sort(endpoints)
	return endpoints, nil
}

// This connects from the Go test runner, using each advertised IP unchanged.
// A successful TCP connection is not CephX authentication or native RADOS I/O.
func hostNetworkAssertRunnerTCP(t *testing.T, ctx context.Context, groups []hostNetworkDaemonEndpoints) {
	t.Helper()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	for _, group := range groups {
		for _, address := range group.addresses {
			connection, err := dialer.DialContext(ctx, "tcp", address)
			if err != nil {
				t.Fatalf("test runner cannot reach cluster %d %s advertised endpoint %s: %v", group.cluster, group.daemon, address, err)
			}
			_ = connection.Close()
		}
		t.Logf("test runner TCP reached cluster %d %s advertised endpoints: %v", group.cluster, group.daemon, group.addresses)
	}
}

func hostNetworkAdvanceTopology(t *testing.T, ctx context.Context, cluster *ceph.Container, clusters []*ceph.Container) {
	t.Helper()
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	original := clusteraccess.OSDs(t, cluster)[0]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	harness.LogStatus(t, ctx, cluster, "host service data after OSD addition")
	if err := original.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	harness.LogStatus(t, ctx, cluster, "host service data after original OSD removal")
	t.Logf("host service topology 2 -> 3 -> 2: added osd.%d, removed original osd.%d", added.ID, original.ID)
}

func hostNetworkRadosProbe(t *testing.T, ctx context.Context, client testcontainers.Container, fsid string, payload []byte, stage string) {
	t.Helper()
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", hostNetworkRadosScript, fsid, hex.EncodeToString(payload), stage}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("native host namespace RADOS probe exited %d: %s", code, output)
	}
	var result struct {
		FSID    string `json:"fsid"`
		Payload string `json:"payload"`
		Stage   string `json:"stage"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode native RADOS probe: %v: %s", err, output)
	}
	if result.FSID != fsid || result.Payload != hex.EncodeToString(payload) || result.Stage != stage {
		t.Fatalf("native RADOS probe connected to wrong cluster or changed bytes: %+v", result)
	}
}

const hostNetworkRadosScript = `import json
import rados
import sys

fsid, payload_hex, stage = sys.argv[1:]
payload = bytes.fromhex(payload_hex)
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
    "keyring": "/etc/ceph/ceph.client.admin.keyring",
    "rados_mon_op_timeout": "30",
    "rados_osd_op_timeout": "30",
}) as cluster:
    assert cluster.get_fsid() == fsid, "connected to another cluster"
    with cluster.open_ioctx("tc-host-isolation") as ioctx:
        if stage == "seed":
            ioctx.write_full("same-object", payload)
        assert ioctx.read("same-object", len(payload) + 1) == payload, "retained payload changed"
        fresh = payload + stage.encode("ascii")
        ioctx.write_full("new-" + stage, fresh)
        assert ioctx.read("new-" + stage, len(fresh) + 1) == fresh, "new roundtrip changed"
    print(json.dumps({"fsid": cluster.get_fsid(), "payload": payload_hex, "stage": stage}))
`
