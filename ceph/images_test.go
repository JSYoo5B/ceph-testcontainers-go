package ceph

import (
	"context"
	"errors"
	"testing"
)

func TestImageSetSelectionDoesNotFallback(t *testing.T) {
	s, err := (ImageSet{Essential: "core"}).normalized()
	if err != nil || s.Client != "" || s.CephFS != "" || s.RBDAux != "" || s.RGW != "" {
		t.Fatalf("implicit optional fallback: %+v %v", s, err)
	}
	s, err = (ImageSet{All: "complete"}).normalized()
	if err != nil || s.Essential != "complete" || s.Client != "complete" || s.RGW != "complete" || s.RBDAux != "complete" {
		t.Fatalf("all expansion: %+v %v", s, err)
	}
	for _, images := range []ImageSet{{}, {All: "all", Client: "other"}, {Essential: " core"}} {
		if _, err := images.normalized(); err == nil {
			t.Fatalf("accepted ambiguous set: %+v", images)
		}
	}
}

func TestRoleImagesRejectLegacyOverridesBeforeAllocation(t *testing.T) {
	c, err := RunWithImages(t.Context(), ImageSet{Essential: "core"}, WithOSDImage("legacy"))
	if c != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("allocated or wrong refusal: %v %v", c, err)
	}
}

func TestClientExecutorMissingAndClosedDoNotAllocate(t *testing.T) {
	c := &Container{settings: options{imageSet: &ImageSet{Essential: "core"}}}
	if ctr, err := c.ClientContainerContext(t.Context()); ctr != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing client: %v %v", ctr, err)
	}
	c.settings.imageSet.Client = "toolkit"
	c.nativeClosed = true
	if ctr, err := c.ClientContainerContext(context.Background()); ctr != nil || !errors.Is(err, errClusterTerminated) {
		t.Fatalf("closed client: %v %v", ctr, err)
	}
}

func TestLegacyClientExecutorUsesCurrentControl(t *testing.T) {
	mon, control := &monitorLifecycleDaemon{}, &monitorLifecycleDaemon{}
	c := &Container{Container: mon}
	if got, err := c.ClientContainerContext(t.Context()); err != nil || got != mon {
		t.Fatalf("legacy MON: %v %v", got, err)
	}
	c.controlPlane = control
	if got, err := c.ClientContainerContext(t.Context()); err != nil || got != control {
		t.Fatalf("legacy stable CLI: %v %v", got, err)
	}
}

func TestExpandedAllSelectionIsIdempotent(t *testing.T) {
	s, err := (ImageSet{All: "complete"}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.normalized()
	if err != nil || again != s {
		t.Fatalf("expanded all cannot be selected: %+v %v", again, err)
	}
}

func TestSingleImageToolkitReusesCoreWithoutAllocation(t *testing.T) {
	mon := &monitorLifecycleDaemon{}
	c := &Container{Container: mon, settings: options{imageSet: &ImageSet{Essential: "complete", Client: "complete"}}}
	got, err := c.ClientContainerContext(t.Context())
	if err != nil || got != mon || c.featureClient != nil {
		t.Fatalf("unnecessary all-image executor: %v %v", got, err)
	}
}

func TestTerminationCancelsToolkitStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := &Container{clientStartupCancel: cancel}
	c.cancelNativeInvocations()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("startup was left running during termination")
	}
}

func TestUnconfirmedToolkitStartIsNotRepeated(t *testing.T) {
	c := &Container{settings: options{imageSet: &ImageSet{Essential: "core", Client: "toolkit"}}, featureClientAttempted: true}
	if ctr, err := c.ClientContainerContext(t.Context()); ctr != nil || err == nil {
		t.Fatalf("unconfirmed startup retried: %v %v", ctr, err)
	}
}

func TestToolkitPublicationWaitHonorsCancellation(t *testing.T) {
	c := &Container{}
	c.clientPublishMu.Lock()
	defer c.clientPublishMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.featureClientSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("publication ignored canceled caller: %v", err)
	}
}
