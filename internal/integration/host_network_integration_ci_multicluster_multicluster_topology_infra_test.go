//go:build all || (integration && hostnetwork && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_infra))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This test exercises native librados in the Docker daemon's host namespace.
// On Docker Desktop that namespace belongs to its Linux VM; successful I/O
// here does not prove that a native macOS process can reach advertised OSDs.
func TestHostNetworkMultiCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	clusters := hostNetworkRunPair(t, ctx, image, imageOpts)

	fsids := make([]string, 2)
	keyrings := make([][]byte, 2)
	clients := make([]testcontainers.Container, 2)
	for i, cluster := range clusters {
		if !cluster.UsesHostNetwork() || cluster.PublicAddress() != "127.0.0.1" {
			t.Fatalf("cluster %d has unexpected networking: host=%t address=%q", i, cluster.UsesHostNetwork(), cluster.PublicAddress())
		}
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fsids[i] = status.FSID
		config, keyring, err := cluster.ConnectionConfig()
		if err != nil {
			t.Fatal(err)
		}
		if len(config) == 0 || len(keyring) == 0 {
			t.Fatalf("cluster %d returned empty connection files", i)
		}
		keyrings[i] = keyring
		cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-host-isolation", "8")
		cephCommand(t, ctx, cluster, "osd", "pool", "set", "tc-host-isolation", "pg_autoscale_mode", "off")
		cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-host-isolation", "rados")
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
			testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})),
		)
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		inspect, err := client.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if inspect.HostConfig.NetworkMode != "host" {
			t.Fatalf("client %d is not in the Docker host network namespace", i)
		}
		clients[i] = client
	}
	if fsids[0] == "" || fsids[1] == "" || fsids[0] == fsids[1] || bytes.Equal(keyrings[0], keyrings[1]) {
		t.Fatal("host network clusters do not have independent FSIDs and credentials")
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	t.Logf("concurrent host network clusters: FSIDs %s and %s", fsids[0], fsids[1])

	payloads := [][]byte{
		bytes.Repeat([]byte("host-network-cluster-A\n"), 128),
		bytes.Repeat([]byte("host-network-cluster-B\n"), 128),
	}
	for i := range clients {
		hostNetworkRadosProbe(t, ctx, clients[i], fsids[i], payloads[i], "seed")
	}
	for i := range clients {
		hostNetworkRadosProbe(t, ctx, clients[i], fsids[i], payloads[i], "initial")
	}
	for i, cluster := range clusters {
		hostNetworkAdvanceTopology(t, ctx, cluster, clusters)
		for j := range clients {
			hostNetworkRadosProbe(t, ctx, clients[j], fsids[j], payloads[j], fmt.Sprintf("after-topology-%d", i))
		}
	}
	t.Log("native Python librados: identical pool/object names remained independent after both OSD 2 -> 3 -> 2 replacements; fresh sessions read retained bytes and wrote new objects")
	if os.Getenv("CEPH_TEST_HOST_TCP_REQUIRED") == "1" {
		t.Log("test runner TCP: every advertised MON/MGR/OSD endpoint was reachable before and after each OSD addition/removal; authenticated RADOS I/O ran separately in Linux client containers")
	}
}
