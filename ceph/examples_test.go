package ceph_test

import (
	"context"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		panic(err)
	}
	if err := cluster.RemoveOSD(ctx, osd.ID); err != nil {
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
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	_ = cephfs.Filesystems(cluster)
	_ = rgw.Gateways(cluster)
}

func ExampleContainer_TemporaryConfig() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	change, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
		Section: "osd", Name: "osd_max_backfills", Value: "1",
	})
	if change != nil {
		// A failed apply can still have persisted; retain its restoration handle.
		defer func() {
			if err := change.Restore(ctx); err != nil {
				panic(err)
			}
		}()
	}
	if err != nil {
		panic(err)
	}
	// Exercise the native client, then Restore removes an originally absent
	// entry instead of pinning the daemon's inherited/default value.
}
