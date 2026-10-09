//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRBDNamespaces(t *testing.T) {
	parallelWhenEnabled(t)
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
			ownedIdentities := []*ceph.ClientConfig{identity}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				for _, identity := range ownedIdentities {
					if err := cluster.DeleteClient(cleanup, identity); err != nil {
						t.Errorf("remove owned RBD client: %v", err)
					}
				}
			})
			readOnlyCaps := ceph.ClientCaps{
				Mon: "profile rbd",
				MGR: "profile rbd-read-only pool=" + pool + " namespace=blue",
				OSD: "profile rbd-read-only pool=" + pool + " namespace=blue",
			}
			reader, err := cluster.CreateClient(ctx, "rbd-blue-reader", readOnlyCaps)
			if err != nil {
				t.Fatal(err)
			}
			ownedIdentities = append(ownedIdentities, reader)
			caps, err := cluster.ClientCapabilities(ctx, reader)
			if err != nil || caps != readOnlyCaps {
				t.Fatalf("native RBD read-only capabilities differ: %+v error=%v", caps, err)
			}
			image, _ := integrationImages(t)
			client, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(identity),
				ceph.WithIdleEntrypoint(),
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
			readerClient, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(reader),
				ceph.WithIdleEntrypoint(),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados, rbd"})))
			if readerClient != nil {
				testcontainers.CleanupContainer(t, readerClient)
			}
			if err != nil {
				t.Fatal(err)
			}
			status, err := cluster.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			poolState, err := cluster.PoolStatus(ctx, pool)
			if err != nil || poolState.ID <= 0 {
				t.Fatalf("native RBD namespace pool identity unavailable: %+v error=%v", poolState, err)
			}
			var nativeImage struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(rbdOutput(t, ctx, admin, "info", "--pool", pool, "--namespace", "blue", "shared", "--format", "json"), &nativeImage); err != nil || nativeImage.ID == "" {
				t.Fatal("native namespace image identity unavailable", err)
			}
			execCommand(t, ctx, readerClient, "python3", "-c", `import subprocess, sys
subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=40, check=True)`,
				rbdNamespaceReadOnlyProbe, pool, reader.Name(), reader.KeyringPath(), status.FSID, fmt.Sprint(poolState.ID), nativeImage.ID)
			// A fresh writer still reads and updates the same image after the
			// reader's denied writes, creation and foreign-namespace access.
			execCommand(t, ctx, client, "python3", "-c", `import subprocess, sys
subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=40, check=True)`,
				rbdNamespaceScopedProbe, pool, identity.Name(), identity.KeyringPath())
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
			for _, identity := range ownedIdentities {
				if err := cluster.DeleteClient(ctx, identity); err != nil {
					t.Fatal(err)
				}
			}
			t.Log("native librbd namespaces: independent bytes; RW principal remains healthy after profile rbd-read-only reads and writable-open/create/RADOS-write/foreign-namespace denials; nonempty removal preserves images; owned identities and namespaces removed")
		})
	}
}
