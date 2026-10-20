package fault_test

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

func ExampleTemporaryNetworkDelay() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	lease, err := fault.TemporaryNetworkDelay(ctx, cluster, cluster.OSDs()[0], fault.NetworkDelay{
		HelperImage: fault.NetemHelperImage, Plane: ceph.PublicNetworkPlane, Delay: 100 * time.Millisecond,
	})
	if lease != nil {
		defer func() { _ = lease.Restore(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	// Run a bounded native workload while the delay is installed.
}

func ExampleInterruptNetwork() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithSeparateClusterNetwork())
	if cluster != nil {
		defer func() { _ = cluster.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	cut, err := fault.InterruptNetwork(ctx, cluster, cluster.OSDs()[0], ceph.ClusterNetworkPlane)
	if cut != nil {
		defer func() { _ = cut.Restore(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	// Restore reconnects the endpoint; it does not restart a daemon that exited.
}
