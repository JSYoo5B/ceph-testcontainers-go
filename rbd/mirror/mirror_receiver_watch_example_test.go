package mirror_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

// The baseline is buffered before WatchReceivers returns and pins both
// owned daemons, so stopping the elected one keeps it in the watched cohort
// as a stopped member. The event that names the survivor as leader advances
// the test; it proves election only, not that any image replays. This example
// is compiled, not run.
func ExampleContainer_WatchReceivers() {
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
	ready, err := link.WaitReceiverReady(ctx)
	if err != nil {
		panic(err)
	}
	watch, err := link.WatchReceivers(ctx, ceph.WatchConfig{Interval: 2 * time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the mirror's Terminate
	<-watch.Events()                    // the baseline
	for _, daemon := range link.Daemons() {
		if daemon.DaemonName == ready.LeaderDaemonName {
			grace := 2 * time.Second
			if err := daemon.Stop(ctx, &grace); err != nil {
				panic(err)
			}
		}
	}
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		if leader := event.Status.LeaderDaemonName; leader != "" && leader != ready.LeaderDaemonName {
			fmt.Printf("%s took over from %s (event %d)\n", leader, ready.LeaderDaemonName, event.Sequence)
			return
		}
	}
	panic(watch.Stop()) // the watch ended before a survivor was elected
}
