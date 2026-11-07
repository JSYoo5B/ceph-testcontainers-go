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

// The baseline is buffered before Watch returns, so the checkpoint starts on
// a known QUIESCED state. Any later event means the set changed: here the
// checkpoint is abandoned once the set left QUIESCED, for example because its
// expiration passed, and Release reports that it is not consistent. This
// example is compiled, not run.
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
	<-watch.Events()                    // the QUIESCED baseline
	lost := make(chan string, 1)
	go func() {
		for event := range watch.Events() {
			if event.Kind != ceph.WatchGap && event.State.State != "QUIESCED" {
				lost <- event.State.State
				return
			}
		}
	}()
	// ... the SUT copies its checkpoint, checking lost between steps ...
	select {
	case state := <-lost:
		fmt.Println("checkpoint abandoned: the set is", state)
	default:
	}
}
