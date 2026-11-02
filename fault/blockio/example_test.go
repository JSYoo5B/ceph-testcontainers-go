package blockio_test

import (
	"context"
	"os"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault/blockio"
)

func ExampleRun() {
	ctx := context.Background()
	// Select an image containing the documented device-mapper helper tools.
	backend, err := blockio.Run(ctx, os.Getenv("CEPH_TEST_BLOCKIO_HELPER_IMAGE"), blockio.WithConfig(blockio.Config{Size: 1 << 30}))
	if backend != nil {
		defer func() { _ = backend.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip)))
	if cluster != nil {
		// This defer runs before backend cleanup: the OSD consumes its device.
		defer func() { _ = cluster.Terminate(ctx) }()
	}
	if err != nil {
		panic(err)
	}
	if _, err := backend.AttachOSD(ctx, cluster); err != nil {
		panic(err)
	}
	if err := backend.Apply(ctx, blockio.Profile{Delay: 250 * time.Millisecond}); err != nil {
		panic(err)
	}
	// Exercise the SUT and observe its I/O independently of the requested table.
	if _, err := backend.Status(ctx); err != nil {
		panic(err)
	}
	if err := backend.Restore(ctx); err != nil {
		panic(err)
	}
	// Restore changes the device table; restarting an exited OSD is separate.
}
