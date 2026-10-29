//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

// integrationImage selects a role, or the aggregate image for a local run.
func integrationImage(getenv func(string) string, name string) (string, error) {
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

func essentialImage(t *testing.T) string {
	t.Helper()
	image, err := integrationImage(os.Getenv, "CEPH_TEST_ESSENTIAL_IMAGE")
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func integrationImages(t *testing.T) (string, []testcontainers.ContainerCustomizer) {
	t.Helper()
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") == "" && os.Getenv("SCENARIO_IMAGE_LAYOUT") != "roles" {
		image := essentialImage(t)
		return image, []testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{All: image}))}
	}
	images := ceph.ImageSet{Essential: os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), Client: os.Getenv("CEPH_TEST_CLIENT_IMAGE"), CephFS: os.Getenv("CEPH_TEST_CEPHFS_IMAGE"), RBDAux: os.Getenv("CEPH_TEST_RBD_AUX_IMAGE"), RGW: os.Getenv("CEPH_TEST_RGW_IMAGE")}
	if images.Essential == "" {
		t.Fatal("missing essential image")
	}
	return images.Essential, []testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images))}
}

// Service tests run sequentially to fit the local Docker VM's 4 GiB budget.
func newServiceCluster(t *testing.T, customizers ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	image, opts := integrationImages(t)
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
				ctrs := append([]testcontainers.Container{cluster.Container, clusteraccess.ControlContainer(t, cluster)}, cluster.ServiceContainers()...)
				for _, monitor := range clusteraccess.Monitors(t, cluster) {
					if monitor != nil && monitor.Container != nil {
						ctrs = append(ctrs, monitor.Container)
					}
				}
				ctrs = append(ctrs, osdContainers(t, cluster)...)
				// A MGR that did not register is a common partial bootstrap.
				ctrs = append(ctrs, cluster.ManagerContainer())
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
	original := clusteraccess.OSDs(t, cluster)[0]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	logStatus(t, ctx, cluster, "service data after OSD addition")
	if err := original.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	logStatus(t, ctx, cluster, "service data after original OSD removal")
	t.Logf("service topology 2 -> 3 -> 2: added osd.%d, removed original osd.%d", added.ID, original.ID)
}

// toolkitContainer returns an owned client runtime for a second native oracle.
// Keep daemon-local admin sockets and process observation on their daemon.
func toolkitContainer(t *testing.T, cluster *ceph.Container) testcontainers.Container {
	t.Helper()
	client, err := cluster.ClientContainer(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// nativeClientImage relocates independent toolkit oracles without changing
// arbitrary application images (for example Vault or a custom go-ceph client).
func nativeClientImage(t *testing.T, aggregate string) string {
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

func nativeMirrorImage(t *testing.T, role, aggregate string) string {
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

// assertContainerFilesEqual compares the exact retained bytes without requiring
// an optional diffutils package in the client runtime.
func assertContainerFilesEqual(t *testing.T, ctx context.Context, ctr testcontainers.Container, first, second string) {
	t.Helper()
	read := func(path string) []byte {
		t.Helper()
		stream, err := ctr.CopyFileFromContainer(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(stream)
		if err := errors.Join(readErr, stream.Close()); err != nil {
			t.Fatal(err)
		}
		return data
	}
	expected, actual := read(first), read(second)
	if !bytes.Equal(expected, actual) {
		t.Fatalf("container file bytes differ: %s (%d bytes), %s (%d bytes)", first, len(expected), second, len(actual))
	}
}
