package quiesce_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/quiesce"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
)

// Check the baseline before copying and monitor the set between copy steps.
// Release verifies consistency even when no change was observed.
func ExampleSet_Watch() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "data"})
	if err != nil {
		panic(err)
	}
	cfg := quiesce.Config{Timeout: 20 * time.Second, Expiration: time.Minute}
	set, err := quiesce.Start(ctx, fs, []*subvolume.Subvolume{volume}, cfg)
	if set != nil {
		defer func() {
			// A lost create reply may still have a confirmed QUIESCED version.
			// If readback failed, Release refuses the inspection-only handle
			// and the deferred cluster termination disposes of the set.
			releaseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := set.Release(releaseCtx); err != nil {
				fmt.Printf("checkpoint not confirmed for set %s: %v\n", set.ID(), err)
			}
		}()
	}
	if err != nil {
		panic(err)
	}
	watch, err := set.Watch(ctx, ceph.WatchConfig{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the set's Release
	baseline := <-watch.Events()
	if baseline.State.State != "QUIESCED" {
		fmt.Println("checkpoint abandoned: the set is", baseline.State.State)
		return
	}
	// ... the SUT copies a checkpoint chunk ...
	// Check for a change between chunks. Release verifies the final checkpoint.
	select {
	case event, ok := <-watch.Events():
		if !ok {
			fmt.Println("checkpoint abandoned: watch ended", watch.Stop())
			return
		}
		if event.Kind == ceph.WatchGap {
			fmt.Println("checkpoint abandoned: observation lost", event.Err)
			return
		}
		if event.State.State != "QUIESCED" {
			fmt.Println("checkpoint abandoned: the set is", event.State.State)
			return
		}
	default:
	}
}
