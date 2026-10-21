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
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	_ = cephfs.Filesystems(cluster)
	_ = rgw.Gateways(cluster)
}

func ExampleContainer_OverrideConfig() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{
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

func ExampleContainer_OverrideFullRatios() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	change, err := cluster.OverrideFullRatios(ctx, ceph.FullRatios{
		NearFull: .01, BackfillFull: .02, Full: .03,
	})
	if change != nil {
		// The caller schedules restoration; the override itself has no expiry.
		defer func() { _ = change.Restore(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	// The OSDMap policy is confirmed; observe HealthDetails for actual OSD reports.
}
