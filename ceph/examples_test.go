package ceph_test

import (
	"context"
	"log"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		panic(err)
	}
	if err := osd.Remove(ctx); err != nil {
		panic(err)
	}
}

// Options from several service packages start one cluster that serves RBD,
// CephFS and S3 clients together.
func ExampleRun_services() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithOSDCount(3),
		rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
		cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
		rgw.WithGateways(rgw.Config{Name: "s3"}),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	_ = cephfs.Filesystems(cluster)
	_, _ = rgw.Gateways(context.Background(), cluster)
}

// Role images are one ImageOptions value, passed to Run once through the grouped
// option function WithImageOptions. The image argument is empty because the set
// is the one image configuration. This example is compiled, so the snippet it
// shows cannot drift from the API; it is not run.
func ExampleWithImageOptions() {
	ctx := context.Background()
	images := ceph.ImageSet{Essential: "registry.example.com/ceph-core:20.2.4", Client: "registry.example.com/ceph-client:20.2.4"}
	cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), ceph.WithOSDCount(1))
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}

// OSD storage is one OSDStorage value, passed to Run once through the grouped
// option function WithOSDStorage. Compiled, not run.
func ExampleWithOSDStorage() {
	ctx := context.Background()
	storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage), ceph.WithOSDCount(1))
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}

// Network and Messenger settings are one NetworkConfig value, passed to Run once
// through the grouped option function WithNetworkConfig. Compiled, not run.
func ExampleWithNetworkConfig() {
	ctx := context.Background()
	network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network), ceph.WithOSDCount(1))
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}
