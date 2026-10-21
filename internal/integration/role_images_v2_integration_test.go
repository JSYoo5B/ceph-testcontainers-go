//go:build integration && role_images_v2 && !ci

// The dedicated roles-v2 workflow builds candidate images before this suite;
// the legacy published-image CI planner must not select it.
//ci: timeout=15m job-timeout=20 image-roles=essential,client,cephfs,rbd-aux,rgw

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func explicitRoleImage(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for roles-v2 native tests", name)
	}
	return value
}

func ownRoleCluster(t *testing.T, c *ceph.Container) {
	t.Helper()
	if c != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := c.Terminate(ctx); err != nil {
				t.Error(err)
			}
		})
	}
}

// The ordinary RBD fixture has neither an auxiliary nor an RGW image to fall
// back to. Its toolkit executor survives MON membership changes independently.
func TestRoleImagesV2OrdinaryRBD(t *testing.T) {
	images := ceph.ImageSet{Essential: explicitRoleImage(t, "CEPH_TEST_ESSENTIAL_IMAGE"), Client: explicitRoleImage(t, "CEPH_TEST_CLIENT_IMAGE")}
	c, err := ceph.RunWithImages(t.Context(), images, ceph.WithOSDCount(1))
	ownRoleCluster(t, c)
	if err != nil {
		t.Fatal(err)
	}
	client, err := c.ClientContainerContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if client == c.ControlContainer() {
		t.Fatal("toolkit silently uses core")
	}
	code, _, err := client.Exec(t.Context(), []string{"sh", "-c", "! command -v radosgw-admin && ! command -v rbd-mirror && ! command -v ceph-mon"})
	if err != nil || code != 0 {
		t.Fatalf("client is not a constrained toolkit: exit %d %v", code, err)
	}
	if _, err := c.CreatePool(t.Context(), ceph.PoolConfig{Name: "v2-rbd", PGNum: 1, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(t.Context(), c, "v2-rbd"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddMonitor(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddMonitor(t.Context(), "c"); err != nil {
		t.Fatal(err)
	}
	if err := c.Container.Stop(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := rbd.CreateNamespace(t.Context(), c, "v2-rbd", "after-mon-stop"); err != nil {
		t.Fatal(err)
	}
	if again, err := c.ClientContainerContext(t.Context()); err != nil || again != client {
		t.Fatalf("executor was replaced: %v", err)
	}
}

// No generic client image is supplied. Multisite setup must select the RGW
// administration runtime, while Gateway.Admin retains gateway-local scope.
func TestRoleImagesV2RGWWithoutClient(t *testing.T) {
	images := ceph.ImageSet{Essential: explicitRoleImage(t, "CEPH_TEST_ESSENTIAL_IMAGE"), RGW: explicitRoleImage(t, "CEPH_TEST_RGW_IMAGE")}
	source, err := ceph.RunWithImages(t.Context(), images)
	ownRoleCluster(t, source)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := ceph.RunWithImages(t.Context(), images)
	ownRoleCluster(t, destination)
	if err != nil {
		t.Fatal(err)
	}
	f, err := rgw.RunMultisite(t.Context(), images.RGW, rgw.MultisiteConfig{Source: source, Destination: destination})
	if f != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := f.Terminate(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Source.Admin(t.Context(), "user", "list"); err != nil {
		t.Fatal(err)
	}
}
