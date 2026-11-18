//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_retained_reopen))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestOSDPlacementLifecycle checks native retained hierarchy and the pool
// admission of a descriptor left by a failed fresh-store check.
func TestOSDPlacementLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()), ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume()))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	location := ceph.OSDConfig{Host: "retained-host", Rack: "retained-rack", Root: "default", DeviceClass: "fixture-fast"}
	original, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(location))
	if err != nil {
		t.Fatal(err)
	}
	neighbor, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(ceph.OSDConfig{Host: "neighbor", Rack: "neighbor-rack"}))
	if err != nil {
		t.Fatal(err)
	}
	find := func(id int) map[string]string {
		t.Helper()
		data, err := cluster.Ceph(ctx, "osd", "find", strconv.Itoa(id), "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Location map[string]string `json:"crush_location"`
		}
		if err := json.Unmarshal(data, &state); err != nil || state.Location["host"] == "" || state.Location["root"] == "" {
			t.Fatalf("incomplete native CRUSH location: %s, %v", data, err)
		}
		return state.Location
	}
	oldLocation, neighborLocation := find(original.ID), find(neighbor.ID)
	identities := osdMemoryIdentities(t, ctx, cluster)
	detached, err := original.Detach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, conflict := range []ceph.OSDConfig{
		{Host: location.Host, Rack: "other-rack"},
		{Host: "other-host", Rack: location.Rack, Root: "other-root"},
		{Host: "other-host", Rack: location.Host},
	} {
		if osd, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(conflict)); osd != nil || err == nil {
			t.Fatalf("conflicting retained hierarchy accepted: %v, %v", osd, err)
		}
		if !maps.Equal(oldLocation, find(original.ID)) || !maps.Equal(neighborLocation, find(neighbor.ID)) {
			t.Fatal("rejected placement moved a retained or unrelated OSD")
		}
	}
	shared, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithPlacement(location))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := detached.Reopen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.ID != original.ID || !maps.Equal(oldLocation, find(reopened.ID)) || !maps.Equal(oldLocation, find(shared.ID)) || !maps.Equal(neighborLocation, find(neighbor.ID)) {
		t.Fatal("same-parent sharing or reopen changed the native hierarchy")
	}
	after := osdMemoryIdentities(t, ctx, cluster)
	if after[original.ID] != identities[original.ID] || after[neighbor.ID] != identities[neighbor.ID] || after[shared.ID] == "" {
		t.Fatal("retained or neighboring native identity changed")
	}
	// The fixture's shared volume makes this an ordinary fresh-store refusal,
	// after native registration but before any daemon can use the directory.
	next := shared.ID + 1
	harness.ExecCommand(t, ctx, neighbor, "mkdir", "/var/lib/ceph/osd/ceph-"+strconv.Itoa(next))
	partial, err := cluster.AddOSD(ctx)
	if err == nil || partial == nil || partial.ID != next || partial.Container != nil {
		t.Fatalf("fresh-store failure did not retain the partial registration: %v, %v", partial, err)
	}
	if pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "incomplete-placement", Replicas: 4}); pool != nil || err == nil {
		t.Fatalf("incomplete placement admitted a pool: %v, %v", pool, err)
	}
	pools, err := cluster.Ceph(ctx, "osd", "pool", "ls", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := json.Unmarshal(pools, &names); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name == "incomplete-placement" {
			t.Fatal("rejected pool exists natively")
		}
	}
	t.Log("OSD placement verified: retained parents, same-parent sharing, reopen, incomplete registration refusal")
}
