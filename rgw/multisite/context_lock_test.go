package multisite

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

func TestSerializedMultisiteMutationQueuedDeadline(t *testing.T) {
	probe := &clustertest.NativeCallCounter{}
	f := &Fixture{realm: rgwRealmAccess{sourceClient: probe, destinationClient: probe}}
	group := &SyncGroup{owner: f, state: &rgwSyncGroupState{id: "queued"}}
	operations := map[string]func(context.Context) error{
		"AddZone": func(ctx context.Context) error {
			_, err := f.AddZone(ctx, "unused", ZoneConfig{Name: "queued"})
			return err
		},
		"AddZonegroup": func(ctx context.Context) error {
			_, err := f.AddZonegroup(ctx, "unused", ZonegroupConfig{Name: "queued", Zones: []ZoneConfig{{Name: "queued"}}})
			return err
		},
		"RemoveZone": func(ctx context.Context) error { return f.RemoveZone(ctx, "queued") },
		"ZoneAdmin":  func(ctx context.Context) error { _, err := f.ZoneAdmin(ctx, "queued", "user", "create"); return err },
		"CreateSyncGroup": func(ctx context.Context) error {
			_, err := f.CreateSyncGroup(ctx, SyncPolicyScope{}, SyncGroupConfig{ID: "queued", Status: SyncAllowed})
			return err
		},
		"Remove":            func(ctx context.Context) error { return group.Remove(ctx) },
		"Apply":             func(ctx context.Context) error { return group.Apply(ctx) },
		"SetStatus":         func(ctx context.Context) error { return group.SetStatus(ctx, SyncEnabled) },
		"InterruptZoneLink": func(ctx context.Context) error { _, err := f.InterruptZoneLink(ctx, "queued"); return err },
		"ApplyZoneLinkProfile": func(ctx context.Context) error {
			_, err := f.ApplyZoneLinkProfile(ctx, "queued", "peer", fault.NetworkProfile{HelperImage: "netem-helper:test", Loss: 100})
			return err
		},
		"Terminate": func(ctx context.Context) error { return f.Terminate(ctx) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			clustertest.AssertContextLockQueued(t, &f.topologyMu, operation)
			if probe.Execs.Load() != 0 || probe.Stops.Load() != 0 || probe.Terminations.Load() != 0 || f.closed {
				t.Fatal("queued caller changed native runtime, membership or fixture lifecycle")
			}
		})
	}
	// A new context must enter the same fixture and reach real cleanup.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	f.owned.AddContainer(probe)
	if err := f.Terminate(ctx); err != nil || !f.closed {
		t.Fatalf("fresh RGW cleanup: %v", err)
	}
	if probe.Terminations.Load() != 1 || probe.Execs.Load() != 0 || probe.Stops.Load() != 0 {
		t.Fatal("fresh cleanup did not terminate exactly its owned runtime")
	}
}
