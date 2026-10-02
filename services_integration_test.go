//go:build integration

package ceph_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Service tests run sequentially to fit the local Docker VM's 4 GiB budget.
func newServiceCluster(t *testing.T) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	image := os.Getenv("CEPH_TEST_IMAGE")
	if image == "" {
		image = ceph.DefaultImage
	}
	cluster, err := ceph.Run(t.Context(), image, ceph.WithOSDCount(2))
	if cluster != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() {
				ctrs := append([]testcontainers.Container{cluster.Container}, cluster.ServiceContainers()...)
				ctrs = append(ctrs, osdContainers(cluster)...)
				for _, ctr := range ctrs {
					if ctr == nil {
						continue
					}
					logs, err := ctr.Logs(ctx)
					if err != nil {
						continue
					}
					data, _ := io.ReadAll(logs)
					logs.Close()
					if len(data) > 16000 {
						data = data[len(data)-16000:]
					}
					t.Logf("container %s logs:\n%s", ctr.GetContainerID(), data)
				}
			}
			if err := cluster.Terminate(ctx); err != nil {
				t.Errorf("terminate service cluster: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(t.Context(), image, cluster.WithClient(),
		testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	)
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	return cluster, client
}

func advanceServiceTopology(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	original := cluster.OSDs()[0]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	logStatus(t, ctx, cluster, "service data after OSD addition")
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	logStatus(t, ctx, cluster, "service data after original OSD removal")
	t.Logf("service topology 2 -> 3 -> 2: added osd.%d, removed original osd.%d", added.ID, original.ID)
}
