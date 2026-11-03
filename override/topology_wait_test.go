package override

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// restorer is the Restore of one override handle.
type restorer interface{ Restore(context.Context) error }

// topologyWaitCases applies one override of each kind that waits for the
// cluster mutex on a fresh fixture cluster.
func topologyWaitCases(t *testing.T) map[string]func() (*ceph.Container, func(context.Context) (restorer, error)) {
	return map[string]func() (*ceph.Container, func(context.Context) (restorer, error)){
		"OSD flag": func() (*ceph.Container, func(context.Context) (restorer, error)) {
			c, _ := newOSDFlagFixture(t)
			return c, func(ctx context.Context) (restorer, error) { return SetOSDFlag(ctx, c, ceph.OSDFlagNoOut, true) }
		},
		"blocklist": func() (*ceph.Container, func(context.Context) (restorer, error)) {
			c, _ := newBlocklistFixture(t)
			return c, func(ctx context.Context) (restorer, error) {
				return AddBlocklist(ctx, c, blocklistTestAddress, time.Minute)
			}
		},
		"MGR module": func() (*ceph.Container, func(context.Context) (restorer, error)) {
			c := mgrModuleCluster(newMGRModuleFixture())
			return c, func(ctx context.Context) (restorer, error) { return SetMGRModule(ctx, c, "mirroring", true) }
		},
		"config": func() (*ceph.Container, func(context.Context) (restorer, error)) {
			c := newConfigFixture(&configFixture{})
			return c, func(ctx context.Context) (restorer, error) {
				return SetConfig(ctx, c, ceph.ConfigSetting{Section: "global", Name: "debug_ms", Value: "1"})
			}
		},
	}
}

// An apply and a Restore wait for the cluster mutex only until the caller's
// context ends: the apply returns a not-applied handle and the Restore keeps
// the override for a retry.
func TestOverridesWaitForTopologyOnlyUntilTheContextEnds(t *testing.T) {
	for name, setup := range topologyWaitCases(t) {
		t.Run(name, func(t *testing.T) {
			c, apply := setup()
			servicehook.Lock(c)
			short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
			handle, err := apply(short)
			stop()
			servicehook.Unlock(c)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("apply ignored the caller deadline: %v", err)
			}
			if err := handle.Restore(t.Context()); err != nil {
				t.Fatalf("the not-applied handle owns something: %v", err)
			}

			if handle, err = apply(t.Context()); err != nil {
				t.Fatal(err)
			}
			servicehook.Lock(c)
			short, stop = context.WithTimeout(t.Context(), 20*time.Millisecond)
			err = handle.Restore(short)
			stop()
			servicehook.Unlock(c)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Restore ignored the caller deadline: %v", err)
			}
			if err := handle.Restore(t.Context()); err != nil {
				t.Fatalf("retry after the deadline: %v", err)
			}
		})
	}
}
