//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterRGWZonegroupsAndRemovalTopology(t *testing.T) {
	testRGWZonegroupsAndRemovalTopology(t, false)
}
func TestMultiClusterRGWInitialZonegroupsTopology(t *testing.T) {
	testRGWInitialZonegroupsTopology(t, false)
}

func rgwTopologyClusters(t *testing.T, ctx context.Context, count int, host bool) (string, string, []*ceph.Container) {
	t.Helper()
	control, options := integrationImages(t)
	options = append(options, ceph.WithOSDCount(1))
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	clusters := make([]*ceph.Container, count)
	identities := make(map[string]bool)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, control, options...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cluster
		status, err := cluster.Status(ctx)
		if err != nil || status.FSID == "" || identities[status.FSID] {
			t.Fatalf("independent storage cluster missing: %+v %v", status, err)
		}
		identities[status.FSID] = true
	}
	rgw := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgw == "" {
		rgw = control
	}
	return control, rgw, clusters
}

func rgwTopologyCleanup(t *testing.T, fixture *multicluster.RGWMultisite) {
	t.Helper()
	if fixture == nil {
		return
	}
	t.Cleanup(func() {
		if t.Failed() {
			for _, zone := range fixture.Zones() {
				logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				for _, args := range [][]string{{"period", "get"}, {"metadata", "sync", "status", "--format", "json"}} {
					output, err := fixture.ZoneAdmin(logCtx, zone.Name, args...)
					t.Logf("RGW zone=%s native=%v error=%v output=%s", zone.Name, args, err, output)
				}
				if zone.Gateway != nil {
					multiClusterLogContainer(t, logCtx, zone.Gateway.Container)
				}
				cancel()
			}
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := fixture.Terminate(cleanupCtx); err != nil {
			t.Errorf("cleanup RGW topology: %v", err)
		}
	})
}

func rgwTopologyS3(t *testing.T, ctx context.Context, zone multicluster.RGWZone) s3HTTPClient {
	t.Helper()
	endpoint, err := zone.Gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return s3HTTPClient{endpoint: endpoint, accessKey: zone.Gateway.AccessKey, secretKey: zone.Gateway.SecretKey, region: zone.Gateway.Region, http: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func rgwAssertZonegroupPeriod(t *testing.T, ctx context.Context, fixture *multicluster.RGWMultisite) {
	t.Helper()
	expected := fixture.Zonegroups()
	for _, local := range fixture.Zones() {
		data, err := fixture.ZoneAdmin(ctx, local.Name, "period", "get", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var period struct {
			RealmID         string `json:"realm_id"`
			MasterZonegroup string `json:"master_zonegroup"`
			PeriodMap       struct {
				Zonegroups []struct {
					Name, ID   string
					MasterZone string   `json:"master_zone"`
					Endpoints  []string `json:"endpoints"`
					Zones      []struct {
						Name, ID  string
						Endpoints []string `json:"endpoints"`
					} `json:"zones"`
				} `json:"zonegroups"`
			} `json:"period_map"`
		}
		if err := json.Unmarshal(data, &period); err != nil || period.RealmID != fixture.RealmID || len(period.PeriodMap.Zonegroups) != len(expected) {
			t.Fatalf("native multi-region realm/group mismatch in %s: %v %s", local.Name, err, data)
		}
		for _, want := range expected {
			found := false
			for _, group := range period.PeriodMap.Zonegroups {
				if group.Name != want.Name {
					continue
				}
				found = true
				if group.ID != want.ID || len(group.Zones) != len(want.Zones) {
					t.Fatalf("native group identity/membership differs: %s", want.Name)
				}
				for _, zone := range want.Zones {
					if zone.Name == want.MasterZone && (group.MasterZone != zone.ID || !slices.Equal(group.Endpoints, []string{zone.PeerEndpoint})) {
						t.Fatal("native local group master/endpoint differs")
					}
					if !slices.ContainsFunc(group.Zones, func(actual struct {
						Name, ID  string
						Endpoints []string `json:"endpoints"`
					}) bool { return actual.Name == zone.Name && actual.ID == zone.ID && slices.Equal(actual.Endpoints, []string{zone.PeerEndpoint}) }) {
						t.Fatalf("native period omitted zone %s", zone.Name)
					}
				}
			}
			if !found {
				t.Fatalf("native period omitted group %s", want.Name)
			}
		}
	}
}

func testRGWInitialZonegroupsTopology(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	control, rgw, clusters := rgwTopologyClusters(t, ctx, 2, host)
	fixture, err := multicluster.RunRGWTopology(ctx, rgw, multicluster.RGWTopologyConfig{ControlImage: control, MasterZonegroup: "us", Zonegroups: []multicluster.RGWZonegroupConfig{
		{Name: "eu", MasterZone: "c", Zones: []multicluster.RGWZoneConfig{{Name: "c", Cluster: clusters[1]}}},
		{Name: "us", MasterZone: "a", Zones: []multicluster.RGWZoneConfig{{Name: "a", Cluster: clusters[0]}}},
	}})
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	zones := fixture.Zones()
	if len(zones) != 2 || zones[0].Name != "a" || fixture.Source != zones[0].Gateway || zones[0].Zonegroup != "us" || zones[1].Zonegroup != "eu" {
		t.Fatalf("named realm/local masters were lost: %+v", zones)
	}
	for _, zone := range zones {
		if err := fixture.RemoveZone(ctx, zone.Name); err == nil {
			t.Fatalf("last/master zone removed: %s", zone.Name)
		}
	}
	t.Log("initial two-region realm: named master zonegroup declared last, one local master per independent storage cluster, exact periods and removal guards passed")
}

func testRGWZonegroupsAndRemovalTopology(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	control, rgw, clusters := rgwTopologyClusters(t, ctx, 3, host)
	fixture, err := multicluster.RunRGWTopology(ctx, rgw, multicluster.RGWTopologyConfig{ControlImage: control, Zonegroup: "us", MetadataMaster: "a", Zones: []multicluster.RGWZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}}})
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	group, err := fixture.AddZonegroup(ctx, rgw, multicluster.RGWZonegroupConfig{Name: "eu", MasterZone: "c", Zones: []multicluster.RGWZoneConfig{{Name: "c", Cluster: clusters[2]}}})
	if err != nil {
		t.Fatal(err)
	}
	if group.ID == "" || len(group.Zones) != 1 || group.Zones[0].Zonegroup != "eu" {
		t.Fatalf("new local-master region missing: %+v", group)
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	zones := fixture.Zones()
	clients := make(map[string]s3HTTPClient)
	for _, zone := range zones {
		clients[zone.Name] = rgwTopologyS3(t, ctx, zone)
	}
	const bucket = "/tc-zonegroup-us"
	clients["a"].request(t, ctx, http.MethodPut, bucket, []byte(`<CreateBucketConfiguration><LocationConstraint>us</LocationConstraint></CreateBucketConfiguration>`), http.StatusOK)
	seed := bytes.Repeat([]byte("owned region storage survives zone detachment\n"), 40)
	clients["a"].request(t, ctx, http.MethodPut, bucket+"/retained", seed, http.StatusOK)
	waitMultisiteObject(t, ctx, clients["b"], bucket+"/retained", http.StatusOK, seed)
	// The other zonegroup shares bucket metadata and redirects requests to
	// its owner. It is not expected to hold a replicated copy of object data.
	waitMultisiteObject(t, ctx, clients["c"], bucket+"/retained", http.StatusMovedPermanently, nil)
	const euBucket = "/tc-zonegroup-eu"
	clients["c"].request(t, ctx, http.MethodPut, euBucket, []byte(`<CreateBucketConfiguration><LocationConstraint>eu</LocationConstraint></CreateBucketConfiguration>`), http.StatusOK)
	euData := []byte("data is local to the eu region")
	clients["c"].request(t, ctx, http.MethodPut, euBucket+"/eu", euData, http.StatusOK)
	waitMultisiteObject(t, ctx, clients["a"], euBucket+"/eu", http.StatusMovedPermanently, nil)
	zoneData, err := fixture.ZoneAdmin(ctx, "b", "zone", "get")
	if err != nil {
		t.Fatal(err)
	}
	var native struct {
		PlacementPools []struct {
			Val struct {
				StorageClasses map[string]struct {
					DataPool string `json:"data_pool"`
				} `json:"storage_classes"`
				DataPool string `json:"data_pool"`
			} `json:"val"`
		} `json:"placement_pools"`
	}
	if err := json.Unmarshal(zoneData, &native); err != nil || len(native.PlacementPools) == 0 {
		t.Fatalf("native zone data pools missing: %v %s", err, zoneData)
	}
	pool := native.PlacementPools[0].Val.DataPool
	if pool == "" {
		pool = native.PlacementPools[0].Val.StorageClasses["STANDARD"].DataPool
	}
	if pool == "" {
		t.Fatal("native zone data pool missing")
	}
	objectNames := strings.Fields(string(multiClusterExecOutput(t, ctx, clusters[1].ControlContainer(), "rados", "-p", pool, "ls")))
	key := ""
	for _, name := range objectNames {
		if strings.Contains(name, "retained") {
			key = name
			break
		}
	}
	if key == "" {
		t.Fatalf("native replicated object missing: %v", objectNames)
	}
	before := multiClusterExecOutput(t, ctx, clusters[1].ControlContainer(), "rados", "-p", pool, "get", key, "-")
	if !bytes.Equal(before, seed) {
		t.Fatal("native stored payload differs from S3 data")
	}
	for _, name := range []string{"a", "c", "foreign"} {
		if err := fixture.RemoveZone(ctx, name); err == nil {
			t.Fatalf("unsafe region/master removal accepted: %s", name)
		}
	}
	if err := fixture.RemoveZone(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.RemoveZone(ctx, "b"); err != nil {
		t.Fatalf("completed zone detach not idempotent: %v", err)
	}
	if len(fixture.Zones()) != 2 || len(clusters[1].Gateways()) != 0 {
		t.Fatal("leaving zone/gateway remained attached")
	}
	if _, err := fixture.ZoneAdmin(ctx, "b", "period", "get"); err == nil {
		t.Fatal("detached CLI still accessible")
	}
	if _, err := fixture.AddZone(ctx, rgw, multicluster.RGWZoneConfig{Name: "b", Cluster: clusters[1]}); err == nil {
		t.Fatal("detached storage adopted as fresh")
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture)
	after := multiClusterExecOutput(t, ctx, clusters[1].ControlContainer(), "rados", "-p", pool, "get", key, "-")
	if !bytes.Equal(after, before) {
		t.Fatal("zone detachment removed or changed retained native object data")
	}
	for _, zone := range fixture.Zones() {
		clients[zone.Name] = rgwTopologyS3(t, ctx, zone)
	}
	if got := clients["a"].request(t, ctx, http.MethodGet, bucket+"/retained", nil, http.StatusOK); !bytes.Equal(got, seed) {
		t.Fatal("remaining us group data changed")
	}
	if got := clients["c"].request(t, ctx, http.MethodGet, euBucket+"/eu", nil, http.StatusOK); !bytes.Equal(got, euData) {
		t.Fatal("eu group data changed")
	}
	for _, cluster := range clusters {
		if _, err := cluster.Status(ctx); err != nil {
			t.Fatalf("fixture mutation terminated storage cluster: %v", err)
		}
	}
	t.Log("one realm/two zonegroups on three independent clusters: dynamic group creation, local region data replication/redirect, master guards, non-master detach, exact period propagation, idempotent cleanup and retained native object bytes passed")
}
