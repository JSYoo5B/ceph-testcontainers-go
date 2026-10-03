package ceph_test

import (
	"context"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleContainer_InitRBDPool() {
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
	if err := cluster.InitRBDPool(ctx, "images"); err != nil {
		panic(err)
	}
	namespace, err := cluster.CreateRBDNamespace(ctx, "images", "tenant-a")
	if err != nil {
		panic(err)
	}
	// Pass namespace.PoolName() and namespace.Name() to a native RBD client.
	// Images, snapshots and trash must be removed before deleting the namespace.
	if err := cluster.RemoveRBDNamespace(ctx, namespace); err != nil {
		panic(err)
	}
}
