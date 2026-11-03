package mirror_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

// The baseline is buffered before WatchImage returns and pins the image pair.
// Stopping the receiver that replays the image moves the replay to the other
// owned daemon; the event that reports it replay-ready again advances the
// test. The event does not attest that a write arrived: compare the
// destination data afterwards. This example is compiled, not run.
func ExampleMirror_WatchImage() {
	ctx := context.Background()
	source, err := rbd.Run(ctx, ceph.DefaultImage)
	if source != nil {
		defer func() { _ = source.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	destination, err := rbd.Run(ctx, ceph.DefaultImage)
	if destination != nil {
		defer func() { _ = destination.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	link, err := rbdmirror.Run(ctx, ceph.DefaultImage, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source.Container, Destination: destination.Container, Pool: "rbd", DaemonCount: 2,
	}))
	if link != nil {
		defer func() { _ = link.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	// Create "volume" on the source and enable it with link.EnableImage.
	if _, err := link.WaitReplayReady(ctx, "volume"); err != nil {
		panic(err)
	}
	watch, err := link.WatchImage(ctx, ceph.WatchConfig{Interval: 2 * time.Second}, "volume")
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the mirror's Terminate
	baseline := <-watch.Events()
	for _, daemon := range link.Daemons() {
		if daemon.DaemonName == baseline.Status.DaemonName {
			grace := 2 * time.Second
			if err := daemon.Stop(ctx, &grace); err != nil {
				panic(err)
			}
		}
	}
	// ... the SUT keeps writing to the source image ...
	for event := range watch.Events() {
		if event.Kind != ceph.WatchGap && event.Status.ReplayReady && event.Status.DaemonName != baseline.Status.DaemonName {
			fmt.Printf("%s replays volume after %s stopped (event %d)\n", event.Status.DaemonName, baseline.Status.DaemonName, event.Sequence)
			return
		}
	}
	panic(watch.Stop()) // the watch ended before the replay moved
}
