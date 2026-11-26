//go:build all || (integration && topology && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_control_admission))))

//ci: timeout=30m job-timeout=40 image-roles=essential

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterControlAdmissionInspectsLiveDocker(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
			defer cancel()
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3)))
			if mode.Host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, err := ceph.Run(ctx, image, opts...)
			if source != nil {
				testcontainers.CleanupContainer(t, source)
			}
			if err != nil {
				t.Fatal(err)
			}
			destination, err := ceph.Run(ctx, image, opts...)
			if destination != nil {
				testcontainers.CleanupContainer(t, destination)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := multicluster.ValidatePair(ctx, image, source, destination); err != nil {
				t.Fatalf("running independent pair was refused: %v", err)
			}
			var owned multicluster.Resources
			client, err := multicluster.RunClient(ctx, "invalid image reference", destination, "", &owned)
			if err == nil || client != nil || owned.Len() != 0 {
				t.Fatalf("pre-create client failure published a handle or cleanup action: %v %v %d", client, err, owned.Len())
			}
			control, err := source.ControlContainer(ctx)
			if err != nil || control == nil || control.GetContainerID() == source.GetContainerID() {
				t.Fatalf("multi-MON source lacks its independent control: %v", err)
			}
			docker, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = docker.Close() }()
			if _, err := docker.ContainerStop(ctx, control.GetContainerID(), mobycl.ContainerStopOptions{}); err != nil {
				t.Fatal(err)
			}
			if !control.IsRunning() {
				t.Fatal("control handle did not retain the cached running state needed by this regression")
			}
			if err := multicluster.ValidatePair(ctx, image, source, destination); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("stopped native control was admitted: %v", err)
			}
		})
	}
}
