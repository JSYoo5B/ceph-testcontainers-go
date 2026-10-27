//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_peer_network))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package integration_test

import (
	"bytes"
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// This cuts only a gateway's HTTP peer bridge. Both storage clusters and local
// S3 endpoints remain running; restoring the endpoint resumes native RGW sync.
func TestMultiClusterRGWPeerNetworkTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	_, rgwGateway, clusters := rgwTopologyClusters(t, ctx, 2, false)
	fixture, err := multisite.RunTopology(ctx, rgwGateway, multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}},
	})
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	zones := fixture.Zones()
	if len(zones) != 2 {
		t.Fatal("two-zone fixture was not constructed")
	}
	a, b := rgwTopologyS3(t, ctx, zones[0]), rgwTopologyS3(t, ctx, zones[1])
	const bucket = "/tc-rgw-peer-network"
	createMultisiteBucket(t, ctx, fixture, a, bucket, nil)
	seed := bytes.Repeat([]byte("same native zone and local storage across a peer network cut\n"), 64)
	a.request(t, ctx, http.MethodPut, bucket+"/seed", seed, http.StatusOK)
	waitMultisiteObject(t, ctx, b, bucket+"/seed", http.StatusOK, seed)
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	beforePeriod, err := fixture.ZoneAdmin(ctx, "a", "period", "get", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Error(err)
		}
	})
	gateway := zones[1].Gateway
	before, err := docker.ContainerInspect(ctx, gateway.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	local := before.Container.NetworkSettings.Networks[staticFacts(clusters[1]).NetworkName]
	if local == nil || !local.IPAddress.IsValid() || !before.Container.State.Running {
		t.Fatal("gateway local Ceph endpoint/process unavailable")
	}
	cut, err := fixture.InterruptZoneLink(ctx, "b")
	if cut != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := cut.Restore(cleanupCtx); err != nil {
				t.Errorf("restore RGW HTTP peer endpoint: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	peer := before.Container.NetworkSettings.Networks[cut.NetworkName]
	if peer == nil || peer.IPAddress != cut.OriginalAddress || len(peer.Aliases) == 0 || cut.NetworkName == staticFacts(clusters[1]).NetworkName {
		t.Fatal("interruption did not identify the original peer-only endpoint")
	}
	isolated, err := docker.ContainerInspect(ctx, gateway.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kept := isolated.Container.NetworkSettings.Networks[staticFacts(clusters[1]).NetworkName]
	if isolated.Container.NetworkSettings.Networks[cut.NetworkName] != nil || kept == nil || kept.IPAddress != local.IPAddress || !isolated.Container.State.Running || isolated.Container.State.Pid != before.Container.State.Pid {
		t.Fatal("peer cut did not retain the local endpoint and running process")
	}
	if endpoint, err := gateway.S3Endpoint(ctx); err != nil || endpoint != b.endpoint {
		t.Fatalf("peer cut changed application endpoint: %q %v", endpoint, err)
	}
	if got := b.request(t, ctx, http.MethodGet, bucket+"/seed", nil, http.StatusOK); !bytes.Equal(got, seed) {
		t.Fatal("isolated zone did not read its retained stored object")
	}
	fromA, fromB := []byte("new source object while HTTP peer link is cut"), []byte("new secondary object while HTTP peer link is cut")
	a.request(t, ctx, http.MethodPut, bucket+"/from-a", fromA, http.StatusOK)
	b.request(t, ctx, http.MethodPut, bucket+"/from-b", fromB, http.StatusOK)
	if got := b.request(t, ctx, http.MethodGet, bucket+"/from-b", nil, http.StatusOK); !bytes.Equal(got, fromB) {
		t.Fatal("isolated zone did not accept a local object write")
	}
	// Require absence throughout an interval, rather than interpreting one
	// immediate GET as evidence that the native synchronization path is cut.
	until := time.Now().Add(12 * time.Second)
	for {
		b.request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusNotFound)
		a.request(t, ctx, http.MethodGet, bucket+"/from-b", nil, http.StatusNotFound)
		if time.Now().After(until) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cut.Restore(ctx); err != nil {
		t.Fatalf("endpoint restore not idempotent: %v", err)
	}
	restored, err := docker.ContainerInspect(ctx, gateway.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := restored.Container.NetworkSettings.Networks[cut.NetworkName]
	if endpoint == nil || endpoint.IPAddress != peer.IPAddress || !slices.Equal(endpoint.Aliases, peer.Aliases) || !restored.Container.State.Running || restored.Container.State.Pid != before.Container.State.Pid {
		t.Fatal("restoration changed the original HTTP peer IP/aliases or restarted the gateway")
	}
	waitMultisiteObject(t, ctx, b, bucket+"/from-a", http.StatusOK, fromA)
	waitMultisiteObject(t, ctx, a, bucket+"/from-b", http.StatusOK, fromB)
	for _, zone := range fixture.Zones() {
		after, err := fixture.ZoneAdmin(ctx, zone.Name, "period", "get", "--format", "json")
		if err != nil || !bytes.Equal(after, beforePeriod) {
			t.Fatalf("peer endpoint cut changed the native committed period in %s: %v", zone.Name, err)
		}
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	t.Log("RGW peer HTTP bridge removed/restored with original IP/aliases and unchanged process/period: local reads+writes continued, cross-zone updates stayed absent during isolation, bidirectional native catch-up passed")
}

// One direction of the link between two zones' gateways is cut below RGW:
// zone b's packets to zone a's gateway port on the multisite bridge, with
// the netem helper. b's own sync reads of a stall, while a keeps reading
// from b; the zones' local endpoints and both clusters keep serving. After
// Restore b catches up with a's object byte for byte, with the same
// gateway processes and period. It needs the netem helper
// (CEPH_TEST_NETEM_IMAGE, by default fault.NetemHelperImage) and an engine
// that grants it NET_ADMIN.
func TestMultiClusterRGWZoneLinkPathCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	netem := netemImage(t)
	_, rgwGateway, clusters := rgwTopologyClusters(t, ctx, 2, false)
	fixture, err := multisite.RunTopology(ctx, rgwGateway, multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}},
	})
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	zones := fixture.Zones()
	if len(zones) != 2 {
		t.Fatal("two-zone fixture was not constructed")
	}
	a, b := rgwTopologyS3(t, ctx, zones[0]), rgwTopologyS3(t, ctx, zones[1])
	const bucket = "/tc-rgw-zone-link-path"
	createMultisiteBucket(t, ctx, fixture, a, bucket, nil)
	seed := bytes.Repeat([]byte("synced in both directions before one direction is cut\n"), 64)
	a.request(t, ctx, http.MethodPut, bucket+"/seed", seed, http.StatusOK)
	waitMultisiteObject(t, ctx, b, bucket+"/seed", http.StatusOK, seed)
	beforePeriod, err := fixture.ZoneAdmin(ctx, "a", "period", "get", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	pids := map[string]int{}
	for _, zone := range zones {
		state, err := zone.Gateway.State(ctx)
		if err != nil || !state.Running {
			t.Fatalf("gateway of zone %s: %+v, %v", zone.Name, state, err)
		}
		pids[zone.Name] = state.Pid
	}

	lease, err := fixture.ApplyZoneLinkProfile(ctx, "b", "a", fault.NetworkProfile{HelperImage: netem, Loss: 100})
	if lease != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := lease.Restore(cleanupCtx); err != nil {
				t.Errorf("restore the zone link delay: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied || status.Loss != 100 || len(lease.Peers) != 1 || lease.Peers[0].Port != 7480 ||
		lease.ContainerID != zones[1].Gateway.GetContainerID() || lease.NetworkName == staticFacts(clusters[1]).NetworkName {
		t.Fatalf("zone link delay %+v: status %+v, %v", lease, status, err)
	}
	fromA, fromB := []byte("written in zone a while b cannot read from a"), []byte("written in zone b while a still reads from b")
	a.request(t, ctx, http.MethodPut, bucket+"/from-a", fromA, http.StatusOK)
	b.request(t, ctx, http.MethodPut, bucket+"/from-b", fromB, http.StatusOK)
	if got := b.request(t, ctx, http.MethodGet, bucket+"/seed", nil, http.StatusOK); !bytes.Equal(got, seed) {
		t.Fatal("zone b did not read its retained object during the cut")
	}
	// The unselected direction: a reads b's new object from b's gateway.
	waitMultisiteObject(t, ctx, a, bucket+"/from-b", http.StatusOK, fromB)
	readBack := time.Since(cutAt).Round(time.Millisecond)
	// The selected direction: b's native sync status, read by its CLI
	// client, which is not cut, compares a's log with b's markers and finds
	// shards behind, while a has caught up with b; and b does not get the
	// object.
	behind, err := observe(ctx, 3*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) (zoneLinkSync, bool, error) {
		var seen zoneLinkSync
		var err error
		if seen.b, err = fixture.SyncStatus(probeCtx, "b"); err != nil {
			return seen, false, err
		}
		if seen.a, err = fixture.SyncStatus(probeCtx, "a"); err != nil {
			return seen, false, err
		}
		fromA, fromB := dataSource(seen.b, "a"), dataSource(seen.a, "b")
		return seen, fromA.BehindShards > 0 && !fromA.CaughtUp && fromB.CaughtUp, nil
	})
	if err != nil {
		t.Fatalf("sync status during the cut: b %+v, a %+v: %v", behind.Value.b.DataSources, behind.Value.a.DataSources, err)
	}
	until := time.Now().Add(12 * time.Second)
	for {
		b.request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusNotFound)
		if time.Now().After(until) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	window := time.Since(cutAt).Round(time.Millisecond)
	restored := time.Now()
	waitMultisiteObject(t, ctx, b, bucket+"/from-a", http.StatusOK, fromA)
	caughtUp := time.Since(restored).Round(time.Millisecond)
	synced, err := fixture.WaitSyncReady(ctx, "b", "a")
	if err != nil {
		t.Fatalf("zone b's sync status after Restore: %+v: %v", synced.DataSources, err)
	}
	for _, zone := range fixture.Zones() {
		state, err := zone.Gateway.State(ctx)
		if err != nil || !state.Running || state.Pid != pids[zone.Name] {
			t.Fatalf("gateway of zone %s after the cut: %+v, %v (pid before %d)", zone.Name, state, err, pids[zone.Name])
		}
		after, err := fixture.ZoneAdmin(ctx, zone.Name, "period", "get", "--format", "json")
		if err != nil || !bytes.Equal(after, beforePeriod) {
			t.Fatalf("the zone link cut changed the committed period in %s: %v", zone.Name, err)
		}
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	if got := a.request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusOK); !bytes.Equal(got, fromA) {
		t.Fatal("zone a lost its own object")
	}
	t.Logf("RGW_ZONE_LINK_PATH_RECEIPT zone b's gateway %.12s on %s dropped %d packets to %v for %s through filters %s: zone a read b's %d bytes within %s of the cut and its data sync from b was caught up, while b's data sync from a had %d of %d shards behind and b lacked a's object for 12 s; after Restore b read a's %d bytes after %s, its data sync from a was caught up (%+v), and both gateways kept their processes and the period",
		lease.ContainerID, lease.NetworkName, cut.Drops, lease.Peers, window, cut.Filters, len(fromB), readBack,
		dataSource(behind.Value.b, "a").BehindShards, dataSource(behind.Value.b, "a").Shards, len(fromA), caughtUp, dataSource(synced, "a"))
}

// zoneLinkSync is one look at both zones' sync status.
type zoneLinkSync struct {
	a, b multisite.SyncStatus
}

// dataSource returns status's data sync from the zone named source.
func dataSource(status multisite.SyncStatus, source string) multisite.DataSyncStatus {
	for _, data := range status.DataSources {
		if data.SourceZone == source {
			return data
		}
	}
	return multisite.DataSyncStatus{}
}
