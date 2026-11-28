package fault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// TestNilClusterIsRefusedByEveryEntryPoint calls every cluster-bound entry
// point with a nil cluster and otherwise valid arguments: each refuses it
// with ceph.ErrInvalidRequest before it locks anything, instead of
// panicking on the nil cluster's mutex.
func TestNilClusterIsRefusedByEveryEntryPoint(t *testing.T) {
	target := pauseTestContainer{id: "osd"}
	profile := NetworkProfile{HelperImage: NetemHelperImage, Plane: ceph.PublicNetworkPlane, Delay: 10 * time.Millisecond}
	for name, call := range map[string]func(context.Context) (any, error){
		"InterruptNetwork": func(ctx context.Context) (any, error) {
			return InterruptNetwork(ctx, nil, target, ceph.PublicNetworkPlane)
		},
		"Pause": func(ctx context.Context) (any, error) { return Pause(ctx, nil, target) },
		"ApplyCPULimit": func(ctx context.Context) (any, error) {
			return ApplyCPULimit(ctx, nil, target, CPULimit{NanoCPUs: 5e8})
		},
		"ApplyClockOffset": func(ctx context.Context) (any, error) {
			return ApplyClockOffset(ctx, nil, target, ClockOffset{Offset: time.Minute})
		},
		"ApplyNetworkProfile": func(ctx context.Context) (any, error) {
			return ApplyNetworkProfile(ctx, nil, target, profile)
		},
		"ApplyZoneProfile": func(ctx context.Context) (any, error) {
			return ApplyZoneProfile(ctx, nil, "a", profile)
		},
		"InjectObjectDataError": func(ctx context.Context) (any, error) {
			return nil, InjectObjectDataError(ctx, nil, "rbd", "object", 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			handle, err := call(t.Context())
			if !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("nil cluster: %v", err)
			}
			switch h := handle.(type) {
			case nil:
			case *NetworkInterruption:
				if h != nil {
					t.Fatalf("nil cluster returned a handle %+v", h)
				}
			case *PauseLease:
				if h != nil {
					t.Fatalf("nil cluster returned a handle %+v", h)
				}
			case *CPULimitLease:
				if h != nil {
					t.Fatalf("nil cluster returned a handle %+v", h)
				}
			case *ClockOffsetLease:
				if h != nil {
					t.Fatalf("nil cluster returned a handle %+v", h)
				}
			case *NetworkProfileLease:
				if h != nil {
					t.Fatalf("nil cluster returned a handle %+v", h)
				}
			default:
				t.Fatalf("unexpected handle %T", handle)
			}
		})
	}
}

// TestNilHandlesFollowOneRule calls every exported method of each lease and
// interruption type on a nil receiver: restoring methods return nil, and
// observation methods refuse with ceph.ErrInvalidRequest, as fault.go
// documents.
func TestNilHandlesFollowOneRule(t *testing.T) {
	var (
		interruption *NetworkInterruption
		pause        *PauseLease
		cpu          *CPULimitLease
		clock        *ClockOffsetLease
		profile      *NetworkProfileLease
	)
	for name, restore := range map[string]func(context.Context) error{
		"NetworkInterruption.Restore": interruption.Restore,
		"PauseLease.Resume":           pause.Resume,
		"CPULimitLease.Restore":       cpu.Restore,
		"ClockOffsetLease.Restore":    clock.Restore,
		"NetworkProfileLease.Restore": profile.Restore,
	} {
		t.Run(name, func(t *testing.T) {
			if err := restore(t.Context()); err != nil {
				t.Fatalf("nil handle: %v", err)
			}
		})
	}
	for name, observe := range map[string]func(context.Context) error{
		"CPULimitLease.Status": func(ctx context.Context) error {
			status, err := cpu.Status(ctx)
			if status != (CPULimitStatus{}) {
				t.Errorf("status %+v", status)
			}
			return err
		},
		"ClockOffsetLease.Status": func(ctx context.Context) error {
			status, err := clock.Status(ctx)
			if status != (ClockOffsetStatus{}) {
				t.Errorf("status %+v", status)
			}
			return err
		},
		"NetworkProfileLease.Status": func(ctx context.Context) error {
			status, err := profile.Status(ctx)
			if status.Applied || status.Gone || status.Qdiscs != nil {
				t.Errorf("status %+v", status)
			}
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := observe(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("nil handle: %v", err)
			}
		})
	}
}
