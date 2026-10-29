package harness_test

import (
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephharness "github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsharness "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdharness "github.com/JSYoo5B/ceph-testcontainers-go/rbd/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	rgwharness "github.com/JSYoo5B/ceph-testcontainers-go/rgw/harness"
)

// These examples are compiled, not run: they have no Output comment, so
// go test builds them and executes none, and no Docker is needed. They carry
// the code of the snippets in docs/ceph-harness.md and the service harness
// pages, so a rename that leaves a page stale fails the build of this package
// (TestDocsNameNoRemovedHarnessName also scans the pages for the old names).

var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithSharedRunOptions(ceph.WithOSDCount(1)))

// A shared cluster is created in a package variable and closed by RunTests in
// TestMain; each test leases a tenant.
func ExampleRunTests() {
	_ = func(m *testing.M) { os.Exit(cephharness.RunTests(m, shared)) }

	_ = func(t *testing.T) {
		t.Parallel()
		tenant := shared.Lease(t)
		client, err := testcontainers.Run(t.Context(), ceph.DefaultImage,
			tenant.WithClient(), ceph.WithIdleEntrypoint())
		testcontainers.CleanupContainer(t, client)
		if err != nil {
			t.Fatal(err)
		}
		// rados --id <tenant.Client().User()> -p <tenant.Pool()> -N <tenant.Namespace()> ...
	}
}

// The support of each service that a test leases is part of the construction.
func ExampleWithServices() {
	var services = cephharness.NewShared(ceph.DefaultImage,
		cephharness.WithServices(rbdharness.Support(), cephfsharness.Support(), rgwharness.Support()),
		cephharness.WithSharedRunOptions(
			ceph.WithOSDCount(1),
			rbd.WithPools(ceph.PoolConfig{Name: "rbd"}),
			cephfs.WithFilesystems(),
			rgw.WithGateways(),
		))

	_ = func(t *testing.T) {
		tenant := services.Lease(t)
		images, err := rbdharness.Lease(t.Context(), tenant, "rbd") // RBD namespace ID of pool rbd
		files, err2 := cephfsharness.Lease(t.Context(), tenant, "") // subvolume group ID
		objects, err3 := rgwharness.Lease(t.Context(), tenant)      // RGW tenant ID, user ID$user
		_, _, _, _, _, _ = images, files, objects, err, err2, err3
	}
}

// A test that needs the whole cluster takes it with Exclusive.
func ExampleSharedCluster_Exclusive() {
	_ = func(t *testing.T) {
		cluster := shared.Exclusive(t)
		if _, err := override.SetOSDFlag(t.Context(), cluster, ceph.OSDFlagNoOut, true); err != nil {
			t.Fatal(err)
		}
		osd, err := cluster.AddOSD(t.Context())
		_, _ = osd, err
	}
}

// The tenant has no read-only views of its own: its Observer reads the shared
// cluster for it.
func ExampleObserveTenant() {
	_ = func(t *testing.T) {
		tenant := shared.Lease(t)
		observer, err := cephharness.ObserveTenant(tenant)
		if err != nil {
			t.Fatal(err)
		}
		status, err := observer.Status(t.Context())
		_, _ = status, err
	}
}
