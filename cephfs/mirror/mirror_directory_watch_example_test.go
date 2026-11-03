package mirror_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
)

// The baseline is buffered before WatchDirectory returns, so the receiver is
// stopped only after the watch has a known starting state. One event then
// advances the test while the SUT keeps writing checkpoints. The destination
// bytes are still compared separately. This example is compiled, not run.
func ExampleMirror_WatchDirectory() {
	ctx := context.Background()
	var clusters [2]*cephfs.Container
	for i := range clusters {
		cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
		if cluster != nil {
			defer func() { _ = cluster.Terminate(context.Background()) }()
		}
		if err != nil {
			panic(err)
		}
		clusters[i] = cluster
	}
	facts, err := ceph.FactsOf(clusters[0].Container)
	if err != nil {
		panic(err)
	}
	mirror, err := cephfsmirror.Run(ctx, facts.Images.CephFS, cephfsmirror.WithConfig(cephfsmirror.Config{
		Source:                clusters[0].Container,
		Destination:           clusters[1].Container,
		SourceFilesystem:      "tc-cephfs",
		DestinationFilesystem: "tc-cephfs",
		Directories:           []string{"/data"},
	}))
	if mirror != nil {
		defer func() { _ = mirror.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	if _, err := mirror.WaitDirectoryReady(ctx, "/data"); err != nil {
		panic(err)
	}
	watch, err := mirror.WatchDirectory(ctx, ceph.WatchConfig{}, "/data")
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the mirror's Terminate
	baseline := <-watch.Events()
	grace := 5 * time.Second
	if err := mirror.Stop(ctx, &grace); err != nil {
		panic(err)
	}
	// ... the SUT runs ...
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		if !event.Status.Ready {
			fmt.Printf("directory %s lost its owner %s (instance %s); problems: %v\n", event.Status.Directory, baseline.Status.DaemonName, baseline.Status.InstanceID, event.Status.DaemonProblems)
			return
		}
	}
	panic(watch.Stop()) // the watch ended before the loss was reported
}
