//go:build integration && ci_optional && ci_batch_block_io

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/preflight"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault/blockio"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// preflightNegativeImage has sh, sleep, losetup and blockdev but neither
// dmsetup nor python3: a helper image that misses the contract.
const preflightNegativeImage = "busybox:1.37.0"

const preflightOwnerLabel = "org.testcontainers.ceph.preflight-owner"

var bothBlockIO = []preflight.Capability{preflight.CapabilityBlockIODelay, preflight.CapabilityBlockIOEIO}

func logPreflight(t *testing.T, name string, report *preflight.Report) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s preflight:\n%s%s", name, report, data)
}

// engineInventory lists the engine's containers and images.
func engineInventory(ctx context.Context, t *testing.T, docker *testcontainers.DockerClient) (containers, images []string) {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range list.Items {
		containers = append(containers, c.ID+" "+string(c.State))
	}
	imageList, err := docker.ImageList(ctx, mobycl.ImageListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range imageList.Items {
		images = append(images, image.ID)
	}
	slices.Sort(containers)
	slices.Sort(images)
	return containers, images
}

func requireNoPreflightHelpers(ctx context.Context, t *testing.T, docker *testcontainers.DockerClient) {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", preflightOwnerLabel)})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("preflight left %d helper containers", len(list.Items))
	}
}

// requireOperationMatches applies profile on backend and checks that the
// result agrees with verdict: observed prerequisites apply, unavailable
// ones are refused as an unavailable environment. A Linux engine's helper
// probe must be conclusive.
func requireOperationMatches(ctx context.Context, t *testing.T, backend *blockio.Container, profile blockio.Profile, verdict preflight.State) {
	t.Helper()
	err := backend.Apply(ctx, profile)
	switch verdict {
	case preflight.StateObserved:
		if err != nil {
			t.Fatalf("prerequisites of %+v were observed, but Apply failed: %v", profile, err)
		}
		if err := backend.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	case preflight.StateUnavailable:
		if !errors.Is(err, ceph.ErrEnvironmentUnavailable) {
			t.Fatalf("prerequisites of %+v were unavailable, but Apply returned %v", profile, err)
		}
	default:
		t.Fatalf("the helper probe of a Linux engine left %+v %s", profile, verdict)
	}
}

func testPreflightBlockIOMatchesOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	helperImage := blockIOHelperImage(t)

	// Inspect starts, pulls and removes nothing.
	beforeContainers, beforeImages := engineInventory(ctx, t, docker)
	inspected, err := preflight.Observe(ctx, preflight.Request{Capabilities: bothBlockIO})
	if err != nil {
		t.Fatal(err)
	}
	logPreflight(t, "inspect", inspected)
	afterContainers, afterImages := engineInventory(ctx, t, docker)
	if !slices.Equal(beforeContainers, afterContainers) || !slices.Equal(beforeImages, afterImages) {
		t.Fatalf("inspect changed the engine: containers %v -> %v, images %v -> %v", beforeContainers, afterContainers, beforeImages, afterImages)
	}
	if inspected.Helper != nil || inspected.Verdict(preflight.CapabilityEngine) != preflight.StateObserved || inspected.Verdict(preflight.CapabilityBlockIODelay) != preflight.StateUnknown {
		t.Fatalf("inspect verdicts:\n%s", inspected)
	}

	// The negative control: the operation refuses the image the helper
	// probe found lacking.
	negative, err := preflight.Observe(ctx, preflight.Request{Capabilities: bothBlockIO, Level: preflight.LevelHelper, HelperImage: preflightNegativeImage})
	if err != nil {
		t.Fatal(err)
	}
	logPreflight(t, "negative control", negative)
	if check, _ := negative.Lookup("blockio.image-tools"); check.State != preflight.StateUnavailable || check.Kind != preflight.KindImage {
		t.Fatalf("negative control tools check: %+v", check)
	}
	if negative.Verdict(preflight.CapabilityBlockIODelay) != preflight.StateUnavailable || negative.Verdict(preflight.CapabilityBlockIOEIO) != preflight.StateUnavailable || !negative.Helper.Released {
		t.Fatalf("negative control verdicts:\n%s", negative)
	}
	refused, err := blockio.Run(ctx, preflightNegativeImage, blockio.WithConfig(blockio.Config{Size: blockio.MinSize}))
	if refused != nil {
		if terminateErr := refused.Terminate(context.WithoutCancel(ctx)); terminateErr != nil {
			t.Errorf("terminate refused backend: %v", terminateErr)
		}
	}
	if !errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("blockio.Run accepted the image preflight found lacking: %v", err)
	}

	// The helper level report agrees with the operations on this engine.
	report, err := preflight.Observe(ctx, preflight.Request{Capabilities: bothBlockIO, Level: preflight.LevelHelper, HelperImage: helperImage})
	if err != nil {
		t.Fatal(err)
	}
	logPreflight(t, "helper", report)
	if !report.Helper.Started || !report.Helper.Released {
		t.Fatalf("helper run: %+v", report.Helper)
	}
	requireNoPreflightHelpers(ctx, t, docker)
	delay, eio := report.Verdict(preflight.CapabilityBlockIODelay), report.Verdict(preflight.CapabilityBlockIOEIO)
	backend, err := blockio.Run(ctx, helperImage, blockio.WithConfig(blockio.Config{Size: blockio.MinSize}))
	if backend != nil {
		defer func() {
			if err := backend.Terminate(context.WithoutCancel(ctx)); err != nil {
				t.Errorf("terminate backend: %v", err)
			}
		}()
	}
	if err != nil {
		if !errors.Is(err, ceph.ErrEnvironmentUnavailable) || delay != preflight.StateUnavailable || eio != preflight.StateUnavailable {
			t.Fatalf("blockio.Run failed with delay %s and EIO %s: %v", delay, eio, err)
		}
		return
	}
	requireOperationMatches(ctx, t, backend, blockio.Profile{Delay: 10 * time.Millisecond}, delay)
	requireOperationMatches(ctx, t, backend, blockio.Profile{EIO: true}, eio)
}
