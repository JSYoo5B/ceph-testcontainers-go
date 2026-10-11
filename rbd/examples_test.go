package rbd_test

import (
	"context"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	// Without WithPools, Run creates and initializes the replicated pool rbd.
	cluster, err := rbd.Run(ctx, ceph.DefaultImage,
		rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	)
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	// Connect a native RBD client with cluster.ConnectionConfig().
}

func ExampleInitPool() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithPools(ceph.PoolConfig{Name: "images", Application: "rbd"}),
	)
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	if err := rbd.InitPool(ctx, cluster, "images"); err != nil {
		panic(err)
	}
	namespace, err := rbd.CreateNamespace(ctx, cluster, "images", "tenant-a")
	if err != nil {
		panic(err)
	}
	// Pass namespace.PoolName() and namespace.Name() to a native RBD client.
	// Images, snapshots and trash must be removed before deleting the namespace.
	if err := rbd.RemoveNamespace(ctx, cluster, namespace); err != nil {
		panic(err)
	}
}
