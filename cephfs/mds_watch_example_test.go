package cephfs_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// The baseline is buffered before WatchMDSs returns, so the fault starts
// only after the watch has a known starting topology. The takeover event
// then advances the test while the SUT keeps its mount. This example is
// compiled, not run.
func ExampleFilesystem_WatchMDSs() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	watch, err := fs.WatchMDSs(ctx, ceph.WatchConfig{Interval: time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	baseline := <-watch.Events()
	active := baseline.Status.Active[0]
	for _, daemon := range fs.MDSs() {
		if daemon.ID == active.Name {
			kill := time.Duration(0)
			if err := daemon.Stop(ctx, &kill); err != nil {
				panic(err)
			}
		}
	}
	// ... the SUT runs ...
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		for _, rank := range event.Status.Active {
			if rank.Rank == 0 && rank.Owned && rank.Name != active.Name {
				fmt.Printf("mds.%s took rank 0 from mds.%s at FSMap epoch %d (baseline %d)\n", rank.Name, active.Name, event.Epoch, baseline.Epoch)
				return
			}
		}
	}
	panic(watch.Stop()) // the watch ended before a standby took the rank
}
