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

func ExamplePause() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	pause, err := fault.Pause(ctx, cluster, cluster.OSDs()[0])
	if pause != nil {
		defer func() { _ = pause.Resume(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	// The process stays alive but silent. Resume does not restart it.
}

func ExampleTemporaryBlocklist() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	// Replace this example with the exact address from a connected native client,
	// such as librados GetAddrs. Bare IPs and zero nonces are refused.
	address := "192.0.2.7:0/1234"
	fence, err := fault.TemporaryBlocklist(ctx, cluster, address, time.Minute)
	if fence != nil {
		defer func() { _ = fence.Restore(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	// The selected session is fenced; other sessions on the host remain admitted.
}
