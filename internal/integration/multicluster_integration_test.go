//go:build all || (integration && multicluster)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Both clusters own separate MON/MGR/OSDs, FSIDs and credentials. Bridge-mode
// clusters have separate networks; host-mode clusters share the host namespace.
// Federation links own the
// additional networking needed by mirror daemons and CephFS MGR peer validation. A client's FSID and credentials are
// checked so copying cannot succeed by accidentally reading the source's pool.
func newMultiClusterPair(t *testing.T, customizers ...testcontainers.ContainerCustomizer) (*ceph.Container, *ceph.Container, testcontainers.Container, testcontainers.Container) {
	t.Helper()
	return newMultiClusterPairWithContext(t, t.Context(), customizers...)
}

func newMultiClusterPairWithContext(t *testing.T, ctx context.Context, customizers ...testcontainers.ContainerCustomizer) (*ceph.Container, *ceph.Container, testcontainers.Container, testcontainers.Container) {
	t.Helper()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(2))
	opts = append(opts, customizers...)
	clusters := make([]*ceph.Container, 2)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, image, opts...)
		if cluster != nil {
			t.Cleanup(func() {
				if t.Failed() {
					ctrs := append([]testcontainers.Container{cluster.Container}, cluster.ServiceContainers()...)
					ctrs = append(ctrs, osdContainers(cluster)...)
					for _, ctr := range ctrs {
						logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
						multiClusterLogContainer(t, logCtx, ctr)
						logCancel()
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				if err := cluster.Terminate(ctx); err != nil {
					t.Errorf("terminate multicluster fixture: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cluster
	}
	source, destination := clusters[0], clusters[1]
	a, err := source.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := destination.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sharedIsolatedNetwork := source.NetworkName() == destination.NetworkName() && !(source.UsesHostNetwork() && destination.UsesHostNetwork())
	if a.FSID == "" || b.FSID == "" || a.FSID == b.FSID || sharedIsolatedNetwork {
		t.Fatal("source and destination are not independent Ceph clusters")
	}
	t.Logf("independent clusters: source FSID=%s OSDs=%d; destination FSID=%s OSDs=%d", a.FSID, a.OSDMap.NumOSDs, b.FSID, b.OSDMap.NumOSDs)
	clients := make([]testcontainers.Container, 2)
	for i, cluster := range clusters {
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
			ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
		)
		if client != nil {
			cleanupMultiClusterContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
		var status ceph.Status
		if err := json.Unmarshal(multiClusterExecOutput(t, ctx, client, "ceph", "status", "--format", "json"), &status); err != nil {
			t.Fatal(err)
		}
		expected := []string{a.FSID, b.FSID}[i]
		if status.FSID != expected {
			t.Fatalf("client %d connected to FSID %s instead of %s", i, status.FSID, expected)
		}
	}
	if bytes.Equal(multiClusterReadFile(t, ctx, clients[0], "/etc/ceph/ceph.client.admin.keyring"),
		multiClusterReadFile(t, ctx, clients[1], "/etc/ceph/ceph.client.admin.keyring")) {
		t.Fatal("independent clusters unexpectedly share admin credentials")
	}
	return source, destination, clients[0], clients[1]
}

func cleanupMultiClusterContainer(t *testing.T, ctr testcontainers.Container) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
			multiClusterLogContainer(t, logCtx, ctr)
			logCancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := ctr.Terminate(ctx); err != nil {
			t.Errorf("terminate multicluster client/daemon: %v", err)
		}
	})
}

func multiClusterLogContainer(t *testing.T, ctx context.Context, ctr testcontainers.Container) {
	t.Helper()
	if ctr == nil {
		return
	}
	logs, err := ctr.Logs(ctx)
	if err != nil {
		return
	}
	data, _ := io.ReadAll(logs)
	logs.Close()
	if len(data) > 8000 {
		data = data[len(data)-8000:]
	}
	t.Logf("container %s logs:\n%s", ctr.GetContainerID(), data)
}

func multiClusterExecOutput(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) []byte {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", args[0], code, data)
	}
	return data
}

func multiClusterReadFile(t *testing.T, ctx context.Context, ctr testcontainers.Container, path string) []byte {
	t.Helper()
	reader, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Stop source data/control daemons without removing its network, which is still
// attached to multicluster link resources. The isolated MGR may remain
// running, but no source MON, OSD, MDS or RGW can serve or replicate data.
func stopMultiClusterSource(t *testing.T, ctx context.Context, source *ceph.Container) {
	t.Helper()
	ctrs := append(source.ServiceContainers(), osdContainers(source)...)
	ctrs = append(ctrs, source.Container)
	for _, ctr := range ctrs {
		timeout := 3 * time.Second
		if err := ctr.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		if ctr.IsRunning() {
			t.Fatalf("source container %s still running after Stop", ctr.GetContainerID())
		}
	}
	t.Log("source MON/OSDs and optional MDS/RGW stopped; destination reads must use its own data")
}
