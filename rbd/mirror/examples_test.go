package mirror_test

import (
	"context"
	"log"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// Run derives the daemon image from the destination cluster's rbd-aux role.
// Compiled, not run.
func ExampleRun() {
	ctx := context.Background()
	var source, destination *ceph.Container // two running clusters from ceph.Run
	link, err := rbdmirror.Run(ctx, rbdmirror.Config{Source: source, Destination: destination, Pool: "images"})
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
	images := rbdmirror.ImageOptions{}.WithDaemonImage("registry.example.com/ceph-rbd-aux:20.2.4")
	link, err := rbdmirror.Run(ctx, rbdmirror.Config{Source: source, Destination: destination, Pool: "images"}, rbdmirror.WithImageOptions(images))
	defer func() {
		if err := testcontainers.TerminateContainer(link); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
}
