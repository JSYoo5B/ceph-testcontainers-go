//go:build all || integration

package integration_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Integration tests may use one image for all roles or compatible role images.
// The control image also supplies the tools for the independent client container.
func integrationImages(t *testing.T) (string, []testcontainers.ContainerCustomizer) {
	t.Helper()
	imageFromEnv := func(name, fallback string) string {
		if image := os.Getenv(name); image != "" {
			return image
		}
		return fallback
	}
	control := imageFromEnv("CEPH_TEST_IMAGE", ceph.DefaultImage)
	osd := imageFromEnv("CEPH_TEST_OSD_IMAGE", control)
	rgw := imageFromEnv("CEPH_TEST_RGW_IMAGE", control)
	mds := imageFromEnv("CEPH_TEST_MDS_IMAGE", control)
	t.Logf("images: control/client=%s OSD=%s RGW=%s MDS=%s", control, osd, rgw, mds)
	return control, []testcontainers.ContainerCustomizer{
		ceph.WithOSDImage(osd), ceph.WithRGWImage(rgw), ceph.WithMDSImage(mds),
	}
}

// Service tests run sequentially to fit the local Docker VM's 4 GiB budget.
func newServiceCluster(t *testing.T, customizers ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(2))
	opts = append(opts, customizers...)
	return newServiceClusterWithOptions(t, image, opts...)
}

// This variant keeps the caller's initial topology, including zero OSDs.
func newServiceClusterWithOptions(t *testing.T, image string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	cluster, err := ceph.Run(t.Context(), image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() {
				ctrs := append([]testcontainers.Container{cluster.Container, cluster.ControlContainer()}, cluster.ServiceContainers()...)
				for _, monitor := range cluster.Monitors() {
					if monitor != nil && monitor.Container != nil {
						ctrs = append(ctrs, monitor.Container)
					}
				}
				ctrs = append(ctrs, osdContainers(cluster)...)
				seen := make(map[string]bool)
				for _, ctr := range ctrs {
					if ctr == nil {
						continue
					}
					if id := ctr.GetContainerID(); seen[id] {
						continue
					} else {
						seen[id] = true
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
		ceph.WithIdleEntrypoint(),
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
