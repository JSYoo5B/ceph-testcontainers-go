package multisite

import (
	"context"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// TestRGWTopologyRefusalsBeforeChangesAreInvalidRequests runs each refusal
// of AddZone, AddZonegroup and RemoveZone that comes before any native
// command and requires ceph.ErrInvalidRequest and no command.
func TestRGWTopologyRefusalsBeforeChangesAreInvalidRequests(t *testing.T) {
	for name, refuse := range map[string]func(*Fixture) error{
		"AddZone name": func(f *Fixture) error { _, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "-d"}); return err },
		"AddZone zonegroup name": func(f *Fixture) error {
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d", Zonegroup: "a\tb"})
			return err
		},
		"AddZone detached name": func(f *Fixture) error {
			f.topology.removed = map[string]*rgwZoneRemoval{"d": {state: &rgwZoneState{}}}
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d"})
			return err
		},
		"AddZone pending removal": func(f *Fixture) error {
			f.topology.removed = map[string]*rgwZoneRemoval{"x": {state: &rgwZoneState{}}}
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d"})
			return err
		},
		"AddZone detached cluster": func(f *Fixture) error {
			f.topology.removed = map[string]*rgwZoneRemoval{"x": {state: &rgwZoneState{}, converged: true, gatewayRemoved: true, clientRemoved: true}}
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d"})
			return err
		},
		"AddZone unknown zonegroup": func(f *Fixture) error {
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d", Zonegroup: "new"})
			return err
		},
		"AddZone terminated": func(f *Fixture) error {
			f.closed = true
			_, err := f.AddZone(t.Context(), "image", ZoneConfig{Name: "d"})
			return err
		},
		"AddZonegroup name": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: " ap", Zones: []ZoneConfig{{Name: "d"}}})
			return err
		},
		"AddZonegroup no zones": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap"})
			return err
		},
		"AddZonegroup owned group": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "eu", Zones: []ZoneConfig{{Name: "d"}}})
			return err
		},
		"AddZonegroup unknown master": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap", MasterZone: "e", Zones: []ZoneConfig{{Name: "d"}}})
			return err
		},
		"AddZonegroup conflicting zonegroup": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap", Zones: []ZoneConfig{{Name: "d", Zonegroup: "eu"}}})
			return err
		},
		"AddZonegroup zone name": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap", Zones: []ZoneConfig{{Name: "d"}, {Name: "e\x01"}}})
			return err
		},
		"AddZonegroup owned zone": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap", Zones: []ZoneConfig{{Name: "d"}, {Name: "a"}}})
			return err
		},
		"AddZonegroup repeated zone": func(f *Fixture) error {
			_, err := f.AddZonegroup(t.Context(), "image", ZonegroupConfig{Name: "ap", Zones: []ZoneConfig{{Name: "d"}, {Name: "d"}}})
			return err
		},
		"RemoveZone name": func(f *Fixture) error { return f.RemoveZone(t.Context(), "b\n") },
	} {
		f, cli, _, _, _ := rgwRemovalFixture()
		if err := refuse(f); !errors.Is(err, ceph.ErrInvalidRequest) || len(cli.calls) != 0 {
			t.Errorf("%s returned %v after %d commands", name, err, len(cli.calls))
		}
	}
}

// TestRGWSyncPolicyRefusalsBeforeChangesAreInvalidRequests does the same for
// the sync group operations whose input is refused before any command.
func TestRGWSyncPolicyRefusalsBeforeChangesAreInvalidRequests(t *testing.T) {
	for name, refuse := range map[string]func(context.Context, *SyncGroup) error{
		"RemovePipe": func(ctx context.Context, g *SyncGroup) error { return g.RemovePipe(ctx, "bad id") },
		"SetStatus":  func(ctx context.Context, g *SyncGroup) error { return g.SetStatus(ctx, "paused") },
		"WaitBucketSyncReady pipe": func(ctx context.Context, g *SyncGroup) error {
			_, err := g.WaitBucketSyncReady(ctx, "bad id", "source", "destination")
			return err
		},
		"WaitBucketSyncReady zones": func(ctx context.Context, g *SyncGroup) error {
			_, err := g.WaitBucketSyncReady(ctx, "selected", "source", "source")
			return err
		},
	} {
		_, cli, g := newRGWSyncBucketStatusFixture(t)
		before := len(cli.calls)
		if err := refuse(t.Context(), g); !errors.Is(err, ceph.ErrInvalidRequest) || len(cli.calls) != before {
			t.Errorf("%s returned %v after %d commands", name, err, len(cli.calls)-before)
		}
	}
}
