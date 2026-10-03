//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRBDNamespaces(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, admin := newServiceCluster(t, opts...)
			const pool = "tc-rbd-namespaces"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := cluster.InitRBDPool(ctx, pool); err != nil {
					t.Fatal(err)
				}
			}
			blue, err := cluster.CreateRBDNamespace(ctx, pool, "blue")
			if err != nil {
				t.Fatal(err)
			}
			red, err := cluster.CreateRBDNamespace(ctx, pool, "red")
			if err != nil {
				t.Fatal(err)
			}
			if blue.Name() != "blue" || blue.PoolName() != pool {
				t.Fatal("namespace descriptor lost its native names")
			}
			if _, err := cluster.CreateRBDNamespace(ctx, pool, "blue"); err == nil {
				t.Fatal("duplicate namespace creation was accepted")
			}
			execCommand(t, ctx, admin, "rbd", "namespace", "create", "--pool", pool, "--namespace", "foreign")
			if _, err := cluster.CreateRBDNamespace(ctx, pool, "foreign"); err == nil {
				t.Fatal("existing foreign namespace creation was accepted")
			}
			assertNamespaces := func(expected ...string) {
				t.Helper()
				names, err := cluster.ListRBDNamespaces(ctx, pool)
				if err != nil || !slices.Equal(names, expected) {
					t.Fatalf("native namespaces=%v expected=%v error=%v", names, expected, err)
				}
			}
			assertNamespaces("blue", "foreign", "red")
			execCommand(t, ctx, admin, "python3", "-c", rbdNamespaceAdminProbe, pool, "seed")
			if err := cluster.RemoveRBDNamespace(ctx, blue); err == nil {
				t.Fatal("nonempty namespace removal was accepted")
			}
			assertNamespaces("blue", "foreign", "red")
			identity, err := cluster.CreateClient(ctx, "rbd-blue", ceph.ClientCaps{
				Mon: "profile rbd",
				MGR: "profile rbd pool=" + pool + " namespace=blue",
				OSD: "profile rbd pool=" + pool + " namespace=blue",
			})
			if err != nil {
				t.Fatal(err)
			}
			image, _ := integrationImages(t)
			client, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(identity),
				testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados, rbd"})))
			if client != nil {
				testcontainers.CleanupContainer(t, client)
			}
			if err != nil {
				t.Fatal(err)
			}
			// A subprocess deadline also bounds native calls that are blocked below
			// the Go context. Each invocation creates a new authenticated session.
			for range 2 {
				execCommand(t, ctx, client, "python3", "-c", `import subprocess, sys
subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=40, check=True)`,
					rbdNamespaceScopedProbe, pool, identity.Name(), identity.KeyringPath())
			}
			// A namespace remains nonempty when its image is moved to trash.
			// Fixture removal must retain that recoverable data too.
			execCommand(t, ctx, admin, "rbd", "trash", "move", "--pool", pool, "--namespace", "blue", "shared")
			if err := cluster.RemoveRBDNamespace(ctx, blue); err == nil {
				t.Fatal("namespace containing an image in trash was removed")
			}
			var trash []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			if err := json.Unmarshal(rbdOutput(t, ctx, admin, "trash", "list", "--pool", pool, "--namespace", "blue", "--format", "json"), &trash); err != nil || len(trash) != 1 || trash[0].Name != "shared" || trash[0].ID == "" {
				t.Fatalf("namespace removal damaged image trash: entries=%+v error=%v", trash, err)
			}
			execCommand(t, ctx, admin, "rbd", "trash", "restore", "--pool", pool, "--namespace", "blue", trash[0].ID)
			execCommand(t, ctx, admin, "python3", "-c", rbdNamespaceAdminProbe, pool, "verify-and-remove")
			for _, ns := range []*ceph.RBDNamespace{blue, red} {
				if err := cluster.RemoveRBDNamespace(ctx, ns); err != nil {
					t.Fatal(err)
				}
				if err := cluster.RemoveRBDNamespace(ctx, ns); err != nil {
					t.Fatalf("namespace removal retry failed: %v", err)
				}
			}
			assertNamespaces("foreign")
			execCommand(t, ctx, admin, "rbd", "namespace", "remove", "--pool", pool, "--namespace", "foreign")
			assertNamespaces()
			t.Log("native librbd namespaces: same image name keeps independent bytes; scoped client read/write works and foreign namespace is denied; nonempty removal preserves images")
		})
	}
}

const rbdNamespaceAdminProbe = `import rados, rbd, sys
pool, phase = sys.argv[1:]
blue = bytes(range(256)) * 256
red = b"independent-red-namespace\n" * 4096
patch = b"namespace-scoped-client-write"
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "10", "rados_osd_op_timeout": "10"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        api = rbd.RBD()
        for namespace, expected in [("blue", blue), ("red", red)]:
            io.set_namespace(namespace)
            if phase == "seed":
                api.create(io, "shared", 8 << 20, old_format=False, features=rbd.RBD_FEATURE_LAYERING)
            with rbd.Image(io, "shared") as image:
                if phase == "seed":
                    assert image.write(expected, 0) == len(expected)
                    image.flush()
                elif namespace == "blue":
                    expected = patch + expected[len(patch):]
                assert image.read(0, len(expected)) == expected, "namespace bytes were mixed"
            if phase == "verify-and-remove":
                api.remove(io, "shared")
print("independent namespace image bytes verified in fresh native session")
`

const rbdNamespaceScopedProbe = `import rados, rbd, sys
pool, entity, keyring = sys.argv[1:]
patch = b"namespace-scoped-client-write"
with rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity,
                 conf={"keyring": keyring, "rados_mon_op_timeout": "5", "rados_osd_op_timeout": "5"}) as cluster:
    with cluster.open_ioctx(pool) as io:
        io.set_namespace("blue")
        with rbd.Image(io, "shared") as image:
            assert image.read(4096, 256) == bytes(range(256))
            assert image.write(patch, 0) == len(patch)
            image.flush()
            assert image.read(0, len(patch)) == patch
        io.set_namespace("red")
        try:
            image = rbd.Image(io, "shared")
        except rbd.PermissionError:
            pass
        else:
            image.close()
            raise AssertionError("namespace-scoped client opened foreign image")
        try:
            io.write_full("forbidden", b"denied")
        except rados.PermissionError:
            pass
        else:
            raise AssertionError("namespace-scoped client wrote foreign namespace")
print("scoped native RBD write/read succeeded and cross-namespace operations were denied")
`
