package ceph_test

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func ExampleOSDStorage_WithStartupBenchmark() {
	ctx := context.Background()
	storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).
		WithStartupBenchmark(ceph.OSDBenchmarkSkip)
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	// Initial and later OSDs retain mClock with their configured capacity
	// assumptions, without automatic startup calibration of the small store.
	_, err = cluster.AddOSD(ctx)
	if err != nil {
		panic(err)
	}
}
