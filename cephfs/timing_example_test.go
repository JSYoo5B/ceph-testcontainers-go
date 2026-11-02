package cephfs_test

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

func ExampleFilesystem_OverrideTiming() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage, cephfs.WithFilesystems(cephfs.Config{Name: "pins", ActiveMDS: 2}))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	changes, err := fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})
	// Keep every returned handle even when only part of the profile was applied.
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		var restoreErr error
		for i := len(changes) - 1; i >= 0; i-- {
			restoreErr = errors.Join(restoreErr, changes[i].Restore(restore))
		}
		if restoreErr != nil {
			panic(restoreErr)
		}
	}()
	if err != nil {
		panic(err)
	}
	// Prepare several directory-pin transitions, then exercise the client SUT.
	// This condition covers the current active MDSs; it is not a failover profile.
}
