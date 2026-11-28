package finalize

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	mobycl "github.com/moby/moby/client"
)

// baselineVisibilityDocker releases synthetic metadata after the activation
// directory was copied but before the first engine-clock sample.
type baselineVisibilityDocker struct {
	*fakeVerifyDocker
	released, sampledAfterRelease bool
}

func (d *baselineVisibilityDocker) CopyFromContainer(ctx context.Context, id string, opts mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	result, err := d.fakeVerifyDocker.CopyFromContainer(ctx, id, opts)
	if id == "osd0" && !d.released && err == nil {
		d.released = true
		d.containers[id].crashes = append(d.containers[id].crashes, "visible-before-start")
	}
	return result, err
}
func (d *baselineVisibilityDocker) Info(ctx context.Context, opts mobycl.InfoOptions) (mobycl.SystemInfoResult, error) {
	d.sampledAfterRelease = d.released
	return d.fakeVerifyDocker.Info(ctx, opts)
}
func TestVerificationCrashVisibleBetweenBaselineAndStartSample(t *testing.T) {
	fx := newVerifyFixture()
	fx.snapshot.Containers = []diagnostics.Target{crashContainer("osd", "0", "osd0")}
	fx.config.Crashes = []CrashExpectation{{Name: "new-metadata", Process: "ceph-osd", Required: true, MinCount: 1, MaxCount: 1}}
	fx.docker.clock = verifyStart.Add(5 * time.Minute)
	v, err := newVerification(fx.config)
	if err != nil {
		t.Fatal(err)
	}
	fx.wire(v)
	d := &baselineVisibilityDocker{fakeVerifyDocker: fx.docker}
	v.docker = func(context.Context) (verificationDocker, func(), error) { return d, func() {}, nil }
	if err := v.activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !d.sampledAfterRelease || v.baseline.targets[0].crashes["visible-before-start"] {
		t.Fatal("synthetic metadata did not become visible between the two baseline operations")
	}
	fx.docker.clock = verifyStart.Add(10 * time.Minute)
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationClean || len(report.Crashes.Findings) != 1 || report.Crashes.Findings[0].CrashID != "visible-before-start" {
		t.Fatalf("identity-difference selection lost the pre-start visibility gap: %+v", report)
	}
	stamp, err := time.Parse(time.RFC3339Nano, report.Crashes.Findings[0].Time)
	if err != nil || !stamp.Before(report.Window.StartedAt) {
		t.Fatal("control did not distinguish metadata time from engine interval", err)
	}
}
