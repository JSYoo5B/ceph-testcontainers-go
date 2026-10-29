package mirror_test

import (
	"context"
	"log"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// Run derives the daemon image from the source cluster's cephfs role.
// Compiled, not run.
func ExampleRun() {
	ctx := context.Background()
	var source, destination *ceph.Container // two running clusters from ceph.Run
	link, err := cephfsmirror.Run(ctx, cephfsmirror.Config{Source: source, Destination: destination, SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}})
	defer func() {
		if err := testcontainers.TerminateContainer(link); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}

// An explicit daemon image is one ImageOptions value, passed to Run once through
// the grouped option function WithImageOptions. Compiled, not run.
func ExampleWithImageOptions() {
	ctx := context.Background()
	var source, destination *ceph.Container // two running clusters from ceph.Run
	images := cephfsmirror.ImageOptions{}.WithDaemonImage("registry.example.com/ceph-cephfs:20.2.4")
	link, err := cephfsmirror.Run(ctx, cephfsmirror.Config{Source: source, Destination: destination, SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}}, cephfsmirror.WithImageOptions(images))
	defer func() {
		if err := testcontainers.TerminateContainer(link); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}
