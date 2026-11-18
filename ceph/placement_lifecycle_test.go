package ceph

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// classFailureControl preserves the startup fake's OSDMap and fails only the
// requested class write, after the daemon has successfully entered that map.
type classFailureControl struct {
	*initialOSDMon
}

func (c classFailureControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if strings.Contains(strings.Join(args, " "), "osd crush set-device-class") {
		return 1, noInitialOSDStream("class assignment failed"), nil
	}
	return c.initialOSDMon.Exec(ctx, args, opts...)
}

func TestPoolPlacementExcludesFailedOSDBringUp(t *testing.T) {
	for _, stage := range []string{"container", "class"} {
		t.Run(stage, func(t *testing.T) {
			c, mon, runner := newInitialOSDCluster(options{})
			if stage == "container" {
				runner.fail = map[int]error{0: errors.New("startup failed")}
			} else {
				c.Container = classFailureControl{mon}
			}
			osd, err := c.AddOSD(t.Context(), OSDOptions{}.WithPlacement(OSDConfig{DeviceClass: "fast"}))
			if err == nil || osd == nil || c.fleet.osdHandles.get(osd.ID) != osd {
				t.Fatalf("failed bring-up did not retain its descriptor: %v", err)
			}
			if stage == "class" && !mon.native[osd.ID].up {
				t.Fatal("class failure did not follow successful daemon startup")
			}
			if osd.placementReady {
				t.Fatal("failed bring-up published eligible placement")
			}
			if pool, err := c.CreatePool(t.Context(), PoolConfig{Name: "unavailable", Replicas: 1, DeviceClass: "fast"}); pool != nil || err == nil || !strings.Contains(err.Error(), "cluster owns 0") {
				t.Fatalf("cleanup descriptor admitted a pool: %v, %v", pool, err)
			}
		})
	}
}

func TestPoolPlacementExcludesInterruptedRemoval(t *testing.T) {
	c, control, _ := newOSDLifecycleFixture()
	for _, osd := range c.fleet.osdHandles.each() {
		osd.placementReady = true
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control.afterAction = func(action string) {
		if action == "osd crush reweight osd.3 0" {
			cancel()
		}
	}
	osd := osdHandle(t, t.Context(), c, 3)
	if err := osd.Remove(ctx); !errors.Is(err, context.Canceled) || c.fleet.osdHandles.get(3) != osd {
		t.Fatalf("interrupted reweight did not retain ownership: %v", err)
	}
	if pool, err := c.CreatePool(t.Context(), PoolConfig{Name: "retiring", Replicas: 2}); pool != nil || err == nil || !strings.Contains(err.Error(), "cluster owns 1") {
		t.Fatalf("retiring descriptor admitted a pool: %v, %v", pool, err)
	}
}

func TestDetachedPlacementRemainsReserved(t *testing.T) {
	for _, candidate := range []OSDConfig{
		{Host: "retained-host", Rack: "other-rack", Root: "default"},
		{Host: "other-host", Rack: "retained-rack", Root: "other-root"},
		{Host: "other-host", Rack: "retained-host", Root: "default"},
		{Host: "other-host", Rack: "retained-rack", Root: "default"},
	} {
		t.Run(candidate.Host+"/"+candidate.Rack+"/"+candidate.Root, func(t *testing.T) {
			f := newRetainedFixture(t)
			osd := osdHandle(t, t.Context(), f.cluster, 3)
			osd.placement = OSDConfig{Host: "retained-host", Rack: "retained-rack", Root: "default"}
			detached, err := osd.Detach(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			err = f.cluster.osdFleet().validateOSDPlacement(candidate)
			sameParent := candidate.Host == "other-host" && candidate.Rack == "retained-rack" && candidate.Root == "default"
			if (err == nil) != sameParent {
				t.Fatalf("detached placement admission: %v", err)
			}
			if f.cluster.fleet.osdHandles.detachedOf(3) != detached {
				t.Fatal("placement validation consumed retained ownership")
			}
		})
	}
}

func TestReopenedPlacementRequiresDeviceClassConfirmation(t *testing.T) {
	f := newRetainedFixture(t)
	osd := osdHandle(t, t.Context(), f.cluster, 3)
	osd.placement.DeviceClass = "fast"
	detached, err := osd.Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The lifecycle control refuses the class command. A started reopened
	// daemon still cannot publish a class that this fixture has not confirmed.
	reopened, err := detached.Reopen(t.Context())
	if err == nil || reopened == nil || reopened.Container == nil || reopened.placementReady {
		t.Fatalf("unconfirmed reopened class became eligible: %v, %v", reopened, err)
	}
}
