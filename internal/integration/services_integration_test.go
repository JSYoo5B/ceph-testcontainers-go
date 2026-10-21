//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// integrationImage returns the image of the role whose variable is name.
// Locally a role without an image uses the control image, and the control
// role uses ceph.DefaultImage. Role-image CI (SCENARIO_IMAGE_LAYOUT=roles)
// prepares only the roles that a batch declares in its image-roles, so there
// a role without an image is an error rather than a fallback to the control
// image.
func integrationImage(getenv func(string) string, name string) (string, error) {
	if getenv("SCENARIO_IMAGE_CONTRACT") == "roles-v2" {
		aliases := map[string]string{"CEPH_TEST_IMAGE": "CEPH_TEST_ESSENTIAL_IMAGE", "CEPH_TEST_OSD_IMAGE": "CEPH_TEST_ESSENTIAL_IMAGE", "CEPH_TEST_MDS_IMAGE": "CEPH_TEST_CEPHFS_IMAGE"}
		if translated := aliases[name]; translated != "" {
			name = translated
		}
		image := getenv(name)
		if image == "" {
			return "", fmt.Errorf("%s is required by roles-v2; no fallback", name)
		}
		return image, nil
	}

	if image := getenv(name); image != "" {
		return image, nil
	}
	if getenv("SCENARIO_IMAGE_LAYOUT") == "roles" {
		return "", fmt.Errorf("%s is not set: role-image CI prepared the roles %q; add the role to the batch's image-roles", name, getenv("SCENARIO_IMAGE_ROLES"))
	}
	if name == "CEPH_TEST_IMAGE" {
		return ceph.DefaultImage, nil
	}
	return integrationImage(getenv, "CEPH_TEST_IMAGE")
}

// controlImage returns the control image, for a test that needs no other
// role.
func controlImage(t *testing.T) string {
	t.Helper()
	image, err := integrationImage(os.Getenv, "CEPH_TEST_IMAGE")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("image: control=%s", image)
	return image
}

// Integration tests may use one image for all roles or compatible role images.
// The control image also supplies the tools for the independent client container.
func integrationImages(t *testing.T) (string, []testcontainers.ContainerCustomizer) {
	t.Helper()
	if os.Getenv("SCENARIO_IMAGE_CONTRACT") == "roles-v2" {
		images := ceph.ImageSet{Essential: os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), Client: os.Getenv("CEPH_TEST_CLIENT_IMAGE"), CephFS: os.Getenv("CEPH_TEST_CEPHFS_IMAGE"), RBDAux: os.Getenv("CEPH_TEST_RBD_AUX_IMAGE"), RGW: os.Getenv("CEPH_TEST_RGW_IMAGE")}
		if images.Essential == "" {
			t.Fatal("missing roles-v2 essential image")
		}
		return images.Essential, []testcontainers.ContainerCustomizer{ceph.WithImages(images)}
	}
	images := make(map[string]string)
	for _, name := range []string{"CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE", "CEPH_TEST_RGW_IMAGE", "CEPH_TEST_MDS_IMAGE"} {
		image, err := integrationImage(os.Getenv, name)
		if err != nil {
			t.Fatal(err)
		}
		images[name] = image
	}
	control, osd, rgwGateway, mds := images["CEPH_TEST_IMAGE"], images["CEPH_TEST_OSD_IMAGE"], images["CEPH_TEST_RGW_IMAGE"], images["CEPH_TEST_MDS_IMAGE"]
	t.Logf("images: control/client=%s OSD=%s RGW=%s MDS=%s", control, osd, rgwGateway, mds)
	return control, []testcontainers.ContainerCustomizer{
		ceph.WithOSDImage(osd), rgw.WithImage(rgwGateway), cephfs.WithMDSImage(mds),
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
	return newServiceClusterRun(t, ceph.Run, image, opts...)
}

// clusterRun is ceph.Run or a service package's Run.
type clusterRun func(context.Context, string, ...testcontainers.ContainerCustomizer) (*ceph.Container, error)

// serviceRun adapts a service package's Run, which returns its own Container
// embedding *ceph.Container, to clusterRun.
func serviceRun[C any](run func(context.Context, string, ...testcontainers.ContainerCustomizer) (*C, error), cluster func(*C) *ceph.Container) clusterRun {
	return func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, error) {
		c, err := run(ctx, img, opts...)
		if c == nil {
			return nil, err
		}
		return cluster(c), err
	}
}

var (
	rbdRun    = serviceRun(rbd.Run, func(c *rbd.Container) *ceph.Container { return c.Container })
	cephfsRun = serviceRun(cephfs.Run, func(c *cephfs.Container) *ceph.Container { return c.Container })
	rgwRun    = serviceRun(rgw.Run, func(c *rgw.Container) *ceph.Container { return c.Container })
)

// newServiceClusterRun starts a cluster with run and attaches a client.
func newServiceClusterRun(t *testing.T, run clusterRun, image string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	cluster, err := run(t.Context(), image, opts...)
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
				// A MGR that did not register is a common partial bootstrap.
				ctrs = append(ctrs, cluster.ManagerContainer())
				for _, mgr := range cluster.Managers() {
					if mgr != nil && mgr.Container != nil {
						ctrs = append(ctrs, mgr.Container)
					}
				}
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
					if state, err := ctr.State(ctx); err == nil && state != nil {
						t.Logf("container %s state: status=%s exit=%d oom=%t started=%s finished=%s", ctr.GetContainerID(), state.Status, state.ExitCode, state.OOMKilled, state.StartedAt, state.FinishedAt)
					}
					logs, err := ctr.Logs(ctx)
					if err != nil {
						continue
					}
					data, _ := io.ReadAll(logs)
					_ = logs.Close()
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
	client, err := testcontainers.Run(t.Context(), nativeClientImage(t, image), cluster.WithClient(),
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

// nativeClientImage relocates independent toolkit oracles without changing
// arbitrary application images (for example Vault or a custom go-ceph client).
func nativeClientImage(t *testing.T, legacy string) string {
	t.Helper()
	if os.Getenv("SCENARIO_IMAGE_CONTRACT") != "roles-v2" {
		return legacy
	}
	image := os.Getenv("CEPH_TEST_CLIENT_IMAGE")
	if image == "" {
		t.Fatal("missing roles-v2 client image for native oracle")
	}
	return image
}

func nativeMirrorImage(t *testing.T, role, legacy string) string {
	t.Helper()
	if os.Getenv("SCENARIO_IMAGE_CONTRACT") != "roles-v2" {
		return legacy
	}
	variable := "CEPH_TEST_RBD_AUX_IMAGE"
	if role == "cephfs" {
		variable = "CEPH_TEST_CEPHFS_IMAGE"
	}
	image := os.Getenv(variable)
	if image == "" {
		t.Fatalf("missing roles-v2 %s mirror runtime", role)
	}
	return image
}
