//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// This cuts only a gateway's HTTP peer bridge. Both storage clusters and local
// S3 endpoints remain running; restoring the endpoint resumes native RGW sync.
func TestMultiClusterRGWPeerNetworkTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	control, rgw, clusters := rgwTopologyClusters(t, ctx, 2, false)
	fixture, err := multicluster.RunRGWTopology(ctx, rgw, multicluster.RGWTopologyConfig{
		ControlImage: control, Zonegroup: "us", MetadataMaster: "a",
		Zones: []multicluster.RGWZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}},
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
	a.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
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
	local := before.Container.NetworkSettings.Networks[clusters[1].NetworkName()]
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
	if peer == nil || peer.IPAddress != cut.OriginalAddress || len(peer.Aliases) == 0 || cut.NetworkName == clusters[1].NetworkName() {
		t.Fatal("interruption did not identify the original peer-only endpoint")
	}
	isolated, err := docker.ContainerInspect(ctx, gateway.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kept := isolated.Container.NetworkSettings.Networks[clusters[1].NetworkName()]
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
