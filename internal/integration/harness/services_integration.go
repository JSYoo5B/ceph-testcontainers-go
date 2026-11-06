//go:build all || integration

package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// IntegrationImage selects a role, or the aggregate image for a local run.
func IntegrationImage(getenv func(string) string, name string) (string, error) {
	if image := getenv(name); image != "" {
		return image, nil
	}
	if getenv("CEPH_TEST_ESSENTIAL_IMAGE") != "" || getenv("SCENARIO_IMAGE_LAYOUT") == "roles" {
		return "", fmt.Errorf("%s is required by the selected roles; no fallback", name)
	}
	if image := getenv("CEPH_TEST_IMAGE"); image != "" {
		return image, nil
	}
	return ceph.DefaultImage, nil
}

func EssentialImage(t *testing.T) string {
	t.Helper()
	image, err := IntegrationImage(os.Getenv, "CEPH_TEST_ESSENTIAL_IMAGE")
	if err != nil {
		t.Fatal(err)
	}
	return image
}

// IntegrationImages selects fixture images and skips mClock startup calibration
// for functional tests. Explicit storage policy and later config files can
// override this local default before the first OSD starts.
func IntegrationImages(t *testing.T) (string, []testcontainers.ContainerCustomizer) {
	t.Helper()
	image, opts := NativeIntegrationImages(t)
	return image, append(opts, FunctionalOSDConfig(t))
}

// FunctionalOSDConfig is the WithConfigFile option of IntegrationImages that
// skips mClock startup calibration, for a test that selects its images itself.
func FunctionalOSDConfig(t *testing.T) testcontainers.ContainerCustomizer {
	t.Helper()
	config := filepath.Join(t.TempDir(), "functional-osd.conf")
	if err := os.WriteFile(config, []byte("[osd]\nosd_mclock_skip_benchmark = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return ceph.WithConfigFile(config)
}

// NativeIntegrationImages selects images without the functional-test benchmark
// default. Use it only when testing native calibration or configuration precedence.
func NativeIntegrationImages(t *testing.T) (string, []testcontainers.ContainerCustomizer) {
	t.Helper()
	images := IntegrationImageSet(t)
	image := images.Essential
	if images.All != "" {
		image = images.All
	}
	return image, []testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images))}
}

// IntegrationImageSet is the image set of NativeIntegrationImages: the role
// images CI supplies, or an All set of the local aggregate image. A test that
// changes one role, such as MGR, starts from it.
func IntegrationImageSet(t *testing.T) ceph.ImageSet {
	t.Helper()
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") == "" && os.Getenv("SCENARIO_IMAGE_LAYOUT") != "roles" {
		return ceph.ImageSet{All: EssentialImage(t)}
	}
	images := ceph.ImageSet{Essential: os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), Client: os.Getenv("CEPH_TEST_CLIENT_IMAGE"), CephFS: os.Getenv("CEPH_TEST_CEPHFS_IMAGE"), RBDAux: os.Getenv("CEPH_TEST_RBD_AUX_IMAGE"), RGW: os.Getenv("CEPH_TEST_RGW_IMAGE")}
	if images.Essential == "" {
		t.Fatal("missing essential image")
	}
	return images
}

// Service tests run sequentially to fit the local Docker VM's 4 GiB budget.
func NewServiceCluster(t *testing.T, customizers ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	image, opts := IntegrationImages(t)
	opts = append(opts, customizers...)
	return NewServiceClusterWithOptions(t, image, opts...)
}

// This variant keeps the caller's initial topology, including zero OSDs.
func NewServiceClusterWithOptions(t *testing.T, image string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	return NewServiceClusterRun(t, ceph.Run, image, opts...)
}

// ClusterRun is ceph.Run or a service package's Run.
type ClusterRun func(context.Context, string, ...testcontainers.ContainerCustomizer) (*ceph.Container, error)

// serviceRun adapts a service package's Run, which returns its own Container
// embedding *ceph.Container, to ClusterRun.
func serviceRun[C any](run func(context.Context, string, ...testcontainers.ContainerCustomizer) (*C, error), cluster func(*C) *ceph.Container) ClusterRun {
	return func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, error) {
		c, err := run(ctx, img, opts...)
		if c == nil {
			return nil, err
		}
		return cluster(c), err
	}
}

var (
	RBDRun    = serviceRun(rbd.Run, func(c *rbd.Container) *ceph.Container { return c.Container })
	CEPHFSRun = serviceRun(cephfs.Run, func(c *cephfs.Container) *ceph.Container { return c.Container })
	RGWRun    = serviceRun(rgw.Run, func(c *rgw.Container) *ceph.Container { return c.Container })
)

// NewServiceClusterRun starts a cluster with run and attaches a client.
func NewServiceClusterRun(t *testing.T, run ClusterRun, image string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	cluster, err := run(t.Context(), image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() {
				serviceCtx, cancelServices := context.WithTimeout(context.Background(), 5*time.Second)
				services, inventoryErr := ObserveCluster(cluster).ServiceContainers(serviceCtx)
				cancelServices()
				if inventoryErr != nil {
					t.Logf("service inventory unavailable: %v", inventoryErr)
				}
				ctrs := append([]testcontainers.Container{cluster.Container, clusteraccess.ControlContainer(t, cluster)}, services...)
				for _, monitor := range clusteraccess.Monitors(t, cluster) {
					if monitor != nil && monitor.Container != nil {
						ctrs = append(ctrs, monitor.Container)
					}
				}
				ctrs = append(ctrs, OSDContainers(t, cluster)...)
				// A MGR that did not register is a common partial bootstrap.
				ctrs = append(ctrs, clusteraccess.InitialManager(t, cluster))
				for _, mgr := range clusteraccess.Managers(t, cluster) {
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
	client, err := testcontainers.Run(t.Context(), NativeClientImage(t, image), cluster.WithClient(),
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

func AdvanceServiceTopology(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	original := clusteraccess.OSDs(t, cluster)[0]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	LogStatus(t, ctx, cluster, "service data after OSD addition")
	if err := original.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	LogStatus(t, ctx, cluster, "service data after original OSD removal")
	t.Logf("service topology 2 -> 3 -> 2: added osd.%d, removed original osd.%d", added.ID, original.ID)
}

// ToolkitContainer returns an owned client runtime for a second native oracle.
// Keep daemon-local admin sockets and process observation on their daemon.
func ToolkitContainer(t *testing.T, cluster *ceph.Container) testcontainers.Container {
	t.Helper()
	client, err := cluster.ClientContainer(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// NativeClientImage relocates independent toolkit oracles without changing
// arbitrary application images (for example Vault or a custom go-ceph client).
func NativeClientImage(t *testing.T, aggregate string) string {
	t.Helper()
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") == "" && os.Getenv("SCENARIO_IMAGE_LAYOUT") != "roles" {
		return aggregate
	}
	image := os.Getenv("CEPH_TEST_CLIENT_IMAGE")
	if image == "" {
		t.Fatal("missing roles client image for native oracle")
	}
	return image
}

func NativeMirrorImage(t *testing.T, role, aggregate string) string {
	t.Helper()
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") == "" && os.Getenv("SCENARIO_IMAGE_LAYOUT") != "roles" {
		return aggregate
	}
	variable := "CEPH_TEST_RBD_AUX_IMAGE"
	if role == "cephfs" {
		variable = "CEPH_TEST_CEPHFS_IMAGE"
	}
	image := os.Getenv(variable)
	if image == "" {
		t.Fatalf("missing roles %s mirror runtime", role)
	}
	return image
}
