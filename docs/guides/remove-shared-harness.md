# Independent test fixtures

The shared-cluster and tenant harness has been removed from current source.
This is a source-breaking change under the existing 0.x policy; published
release tags retain their original API. There is no deprecated alias,
exclusive facade, retired mode or replacement sharing framework.

The removed packages are `ceph/harness`, `rbd/harness`, `cephfs/harness` and
`rgw/harness`. Remove their imports and package-level `NewShared` values,
`RunTests` calls, `SharedCluster`, `Tenant`, `Exclusive`, shared/lease options,
statistics, `SharedPool`, `ObserveTenant`, `ServiceSupport`, `WithServices`,
`Support` and service `Lease` adapters. `ceph.ErrSharedCluster` and the
harness-only raw-access policy are also removed.

## One owner for each test

Start `ceph.Run`, `rbd.Run`, `cephfs.Run` or `rgw.Run` inside the test that owns
it. Register cleanup immediately, including when startup returns a partial
fixture with an error. If `TestMain` existed only to call `RunTests`, remove
it. A test that formerly needed `Exclusive` now uses its own fixture directly.
Independent startup cost is accepted; do not recreate the removed framework
in a helper or package-level singleton.

This RADOS example gives the test an explicit pool, namespace and restricted
Cephx identity. The client container is registered after the cluster and is
therefore removed first. Cluster termination releases the owned fixture,
including partial startup and uncertain container-creation outcomes.

```golang
package app_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestApplication(t *testing.T) {
	ctx := t.Context()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithPools(ceph.PoolConfig{Name: "app", Application: "rados"}))
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	identity, err := cluster.CreateClient(ctx, "app", ceph.ClientCaps{
		Mon: "allow r",
		OSD: "allow rw pool=app namespace=case-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, ceph.DefaultImage,
		cluster.WithClientIdentity(identity), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		t.Fatal(err)
	}
	_ = observer // Live queries have no tenant lease or cleanup of their own.
	_ = client   // Run the application's real I/O against app/case-a.
}
```

RADOS namespaces are selected by the client operation; merely naming one in
Cephx caps does not write an object. Isolation tests must perform real I/O and
verify both allowed access and sibling namespace or identity denial. Do not
substitute framework bookkeeping or successful setup for those assertions.

## Explicit service resources

| Former adapter | Independent ownership |
| --- | --- |
| RBD tenant namespace | Start `rbd.Run`, create a namespace with `rbd.CreateNamespace`, create the client identity and image explicitly, and remove owned images before `rbd.RemoveNamespace`. |
| CephFS tenant group | Start `cephfs.Run` or `cephfs.Start`, call `Filesystem.CreateSubvolumeGroup` and `Filesystem.CreateSubvolume`, authorize the intended path, and remove owned subvolumes before their group and filesystem. |
| RGW tenant user | Start `rgw.Run`, select its owned gateway and call `Gateway.CreateUser` with explicit user/tenant configuration. Delete owned objects and buckets before removing the user. Native RGW tenants and accounts remain supported Ceph features. |
| Tenant observation | Use `ceph.Observe` on the independent cluster and query its real resources. |

The service setup options still compose with `ceph.Run`; they do not register
shared-cluster support. No service import acquires a shared lifetime or lease.
See the [RBD](../rbd.md), [CephFS](../cephfs.md) and [RGW](../rgw.md) references
for the exact resource and client operations.

## Cleanup and failures

A test owns every external client it attaches, so stop those clients before
fixture termination. Register explicit resource cleanup in reverse dependency
order. Cleanup needs a fresh bounded background context because `t.Context()`
has ended when Go runs test cleanups. Keep non-nil handles returned with errors:
an uncertain operation may still own a resource, and failed cleanup remains
retryable. Do not treat cancellation as automatic rollback.

The ordinary cluster lifecycle, native-command supervision, resource ownership,
liveness checks, locks, fault restoration and failure archives remain. Fault
and override handles retain their existing `Resume`/`Restore` responsibilities;
their lease names do not imply the removed tenant orchestration. Use
`TerminateOnCleanup` when checks, captures, failure archives and dependent
cleanup must be ordered around termination. See
[Contexts and cleanup](contexts-and-cleanup.md) and
[the lifecycle reference](../ceph.md#terminateoncleanup).

The extraction design and its old import-migration guide are historical records.
They do not describe an available current API or authorize retaining core
harness hooks, lease registries or tenant admission state.
