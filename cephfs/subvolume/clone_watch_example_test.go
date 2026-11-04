package subvolume_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
)

// The baseline is buffered before Watch returns, so the test acts on a known
// clone state: here it cancels the clone once the watch has seen it pending
// or in progress, and the terminal event then carries the native outcome. A
// clone that completed first ends the watch with a complete status instead.
// This example is compiled, not run.
func ExampleClone_Watch() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	source, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "source", SizeBytes: 64 << 20})
	if err != nil {
		panic(err)
	}
	// ... the SUT writes its data under source.Path ...
	snapshot, err := source.CreateSnapshot(ctx, "checkpoint")
	if err != nil {
		panic(err)
	}
	clone, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "restore"})
	if err != nil {
		panic(err)
	}
	watch, err := clone.Watch(ctx, ceph.WatchConfig{Interval: time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	canceled := false
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		switch event.Status.State {
		case "pending", "in-progress":
			if !canceled {
				// A clone that completes first refuses the cancellation;
				// its terminal event still follows.
				if err := clone.Cancel(ctx); err != nil {
					fmt.Println("cancel refused:", err)
				}
				canceled = true
			}
		default:
			fmt.Printf("clone %s (errno %d) at event %d\n", event.Status.State, event.Status.FailureErrno, event.Sequence)
		}
	}
	if err := watch.Stop(); err != nil {
		panic(err) // the watch ended before a terminal status
	}
}
