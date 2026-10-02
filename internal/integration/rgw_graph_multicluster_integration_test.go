//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterRGWThreeZoneTopology(t *testing.T) {
	testRGWThreeZoneTopology(t, false)
}

func testRGWThreeZoneTopology(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	controlImage, options := integrationImages(t)
	options = append(options, ceph.WithOSDCount(1))
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	clusters := make([]*ceph.Container, 3)
	fsids := make(map[string]bool)
	for i := range clusters {
		clusterOptions := append([]testcontainers.ContainerCustomizer(nil), options...)
		if i == 0 {
			clusterOptions = append(clusterOptions, ceph.WithMonitorCount(3))
		}
		cluster, err := ceph.Run(ctx, controlImage, clusterOptions...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cluster
		status, err := cluster.Status(ctx)
		if err != nil || status.FSID == "" || fsids[status.FSID] {
			t.Fatalf("cluster identity not independent: status=%+v error=%v", status, err)
		}
		fsids[status.FSID] = true
	}
	// Multicluster construction must use the live control/quorum rather than
	// requiring the original embedded MON container to remain present.
	if err := clusters[0].RemoveMonitor(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if status, err := clusters[0].QuorumStatus(ctx); err != nil || len(status.QuorumNames) != 2 {
		t.Fatalf("source quorum did not survive primary removal: status=%+v error=%v", status, err)
	}
	t.Log("source primary MON a removed before multisite construction; surviving quorum b,c and independent CLI remain available")
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = controlImage
	}
	// Deliberately declare the master last. Run chooses roles by name rather
	// than assuming that the first caller-provided storage cluster is master.
	fixture, err := multicluster.RunRGWTopology(ctx, rgwImage, multicluster.RGWTopologyConfig{
		Zones:          []multicluster.RGWZoneConfig{{Name: "b", Cluster: clusters[1]}, {Name: "c", Cluster: clusters[2]}, {Name: "a", Cluster: clusters[0]}},
		MetadataMaster: "a", ControlImage: controlImage,
	})
	if fixture != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, zone := range fixture.Zones() {
					for _, args := range [][]string{{"metadata", "sync", "status", "--format", "json"}, {"user", "list"}} {
						logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						status, err := fixture.ZoneAdmin(logCtx, zone.Name, args...)
						cancel()
						t.Logf("zone=%s diagnostic=%v error=%v status=%s", zone.Name, args, err, status)
					}
					if zone.Gateway != nil {
						logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						multiClusterLogContainer(t, logCtx, zone.Gateway.Container)
						cancel()
					}
				}
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := fixture.Terminate(cleanupCtx); err != nil {
				t.Errorf("cleanup RGW topology: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	zones := fixture.Zones()
	if len(zones) != 3 || zones[0].Name != "a" || zones[1].Name != "b" || zones[2].Name != "c" || fixture.Source != zones[0].Gateway {
		t.Fatalf("declared three-zone topology was not produced: %+v", zones)
	}
	ids, peers := make(map[string]bool), make(map[string]bool)
	clients := make([]s3HTTPClient, len(zones))
	for i, zone := range zones {
		if zone.ID == "" || ids[zone.ID] || zone.PeerEndpoint == "" || peers[zone.PeerEndpoint] {
			t.Fatalf("zone identity/listener is not distinct: %+v", zone)
		}
		ids[zone.ID], peers[zone.PeerEndpoint] = true, true
		rgwAssertThreeZonePeriod(t, ctx, fixture, zone.Name, zones)
		endpoint, err := zone.Gateway.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = s3HTTPClient{endpoint: endpoint, accessKey: fixture.Source.AccessKey, secretKey: fixture.Source.SecretKey, region: zone.Gateway.Region, http: &http.Client{Timeout: 20 * time.Second}}
		t.Logf("zone=%s native ID=%s peer=%s application=%s", zone.Name, zone.ID, zone.PeerEndpoint, endpoint)
	}
	const bucket = "/tc-three-zone-topology"
	clients[0].request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	seed := bytes.Repeat([]byte("three independent RGW storage clusters\n"), 128)
	clients[0].request(t, ctx, http.MethodPut, bucket+"/retained", seed, http.StatusOK)
	for _, client := range clients[1:] {
		waitMultisiteObject(t, ctx, client, bucket+"/retained", http.StatusOK, seed)
	}
	stop := 2 * time.Second
	if err := zones[1].Gateway.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if state, err := zones[1].Gateway.State(ctx); err != nil || state == nil || state.Running {
		t.Fatalf("zone b gateway did not stop: state=%+v error=%v", state, err)
	}
	fromC := []byte("zone c remains active while b is stopped")
	clients[2].request(t, ctx, http.MethodPut, bucket+"/from-c", fromC, http.StatusOK)
	waitMultisiteObject(t, ctx, clients[0], bucket+"/from-c", http.StatusOK, fromC)
	if err := zones[1].Gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	clients[1].endpoint, err = zones[1].Gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitMultisiteObject(t, ctx, clients[1], bucket+"/from-c", http.StatusOK, fromC)
	fromB := []byte("zone b rejoins the three-zone graph")
	clients[1].request(t, ctx, http.MethodPut, bucket+"/from-b", fromB, http.StatusOK)
	for _, i := range []int{0, 2} {
		waitMultisiteObject(t, ctx, clients[i], bucket+"/from-b", http.StatusOK, fromB)
	}
	// Fence only the metadata master gateway. Existing object requests at the
	// two secondary sites must use their own independent stored copies.
	if err := zones[0].Gateway.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	for _, client := range clients[1:] {
		if got := client.request(t, ctx, http.MethodGet, bucket+"/retained", nil, http.StatusOK); !bytes.Equal(got, seed) {
			t.Fatal("secondary data differs while the metadata master gateway is stopped")
		}
	}
	if err := zones[0].Gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	clients[0].endpoint, err = zones[0].Gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, zone := range zones {
		rgwAssertThreeZonePeriod(t, ctx, fixture, zone.Name, zones)
		if got := clients[i].request(t, ctx, http.MethodGet, bucket+"/retained", nil, http.StatusOK); !bytes.Equal(got, seed) {
			t.Fatal("retained zone data changed after gateway restarts")
		}
	}
	t.Log("three independent Ceph clusters: one realm/zonegroup, named metadata master, three native zone IDs/endpoints, secondary outage/rejoin and retained cross-zone communication passed")
}

func rgwAssertThreeZonePeriod(t *testing.T, ctx context.Context, fixture *multicluster.RGWMultisite, name string, expected []multicluster.RGWZone) {
	t.Helper()
	data, err := fixture.ZoneAdmin(ctx, name, "period", "get", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var period struct {
		RealmID    string `json:"realm_id"`
		MasterZone string `json:"master_zone"`
		PeriodMap  struct {
			Zonegroups []struct {
				MasterZone string `json:"master_zone"`
				Zones      []struct {
					Name, ID  string
					Endpoints []string `json:"endpoints"`
				} `json:"zones"`
			} `json:"zonegroups"`
		} `json:"period_map"`
	}
	if err := json.Unmarshal(data, &period); err != nil || period.RealmID != fixture.RealmID || period.MasterZone != expected[0].ID || len(period.PeriodMap.Zonegroups) != 1 {
		t.Fatalf("native realm/master/group mismatch in %s: error=%v output=%s", name, err, data)
	}
	group := period.PeriodMap.Zonegroups[0]
	if group.MasterZone != expected[0].ID || len(group.Zones) != len(expected) {
		t.Fatalf("native zone count/master mismatch in %s", name)
	}
	for _, want := range expected {
		found := false
		for _, actual := range group.Zones {
			if actual.Name == want.Name && actual.ID == want.ID && len(actual.Endpoints) == 1 && actual.Endpoints[0] == want.PeerEndpoint {
				found = true
			}
		}
		if !found {
			t.Fatalf("zone %s period omits native identity/endpoint for %s", name, want.Name)
		}
	}
}
