package multicluster

import (
	"slices"
	"strings"
	"testing"
)

type rgwControlImageStub struct {
	image string
	reads int
}

func (cluster *rgwControlImageStub) ControlImage() string {
	cluster.reads++
	return cluster.image
}

func TestRGWControlImageUsesEachClusterAndRespectsOverride(t *testing.T) {
	for _, zone := range []string{"source", "destination", "added-zone"} {
		t.Run(zone, func(t *testing.T) {
			cluster := &rgwControlImageStub{image: "ceph-control:" + zone}
			image, err := rgwControlImage("", cluster)
			if err != nil || image != cluster.image || cluster.reads != 1 {
				t.Fatalf("setup client did not select its own cluster's control image: image=%q reads=%d error=%v", image, cluster.reads, err)
			}
			cluster.reads = 0
			const override = "compatible-control:explicit"
			image, err = rgwControlImage(override, cluster)
			if err != nil || image != override || cluster.reads != 0 {
				t.Fatalf("explicit shared control image was not preserved: image=%q reads=%d error=%v", image, cluster.reads, err)
			}
		})
	}
}

func TestRGWControlImageRejectsMissingOrBlankSelection(t *testing.T) {
	if image, err := rgwControlImage("", nil); err == nil || image != "" {
		t.Fatal("missing control image was accepted")
	}
	for _, image := range []string{"", " \t\n"} {
		if selected, err := rgwControlImage("", &rgwControlImageStub{image: image}); err == nil || selected != "" {
			t.Fatal("empty cluster control image was accepted")
		}
	}
	cluster := &rgwControlImageStub{image: "valid-control:local"}
	if image, err := rgwControlImage(" \t", cluster); err == nil || image != "" || cluster.reads != 0 {
		t.Fatal("blank explicit override silently fell back to the cluster image")
	}
}

func TestRGWCommittedPeriodRequiresActualPeerEndpoints(t *testing.T) {
	fixture := &RGWMultisite{
		RealmID: "realm-id", SourceZoneID: "source-id", DestinationZoneID: "destination-id",
		config:    RGWMultisiteConfig{Zonegroup: "us-east-1", SourceZone: "primary", DestinationZone: "secondary"},
		sourceURL: "http://127.0.0.1:42345", destinationURL: "http://127.0.0.1:42346",
	}
	const committed = `{"realm_id":"realm-id","master_zone":"source-id","period_map":{"zonegroups":[{"name":"us-east-1","master_zone":"source-id","endpoints":["http://127.0.0.1:42345"],"zones":[{"id":"source-id","name":"primary","endpoints":["http://127.0.0.1:42345"]},{"id":"destination-id","name":"secondary","endpoints":["http://127.0.0.1:42346"]}]}]}}`
	if err := fixture.validatePeriodEndpoints([]byte(committed)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(committed, `"realm_id":"realm-id"`, `"realm_id":"another-realm"`, 1),
		strings.Replace(committed, `"master_zone":"source-id"`, `"master_zone":"destination-id"`, 1),
		strings.ReplaceAll(committed, `"endpoints":["http://127.0.0.1:42345"]`, `"endpoints":[]`),
		strings.Replace(committed, `"endpoints":["http://127.0.0.1:42346"]`, `"endpoints":[]`, 1),
		strings.Replace(committed, `http://127.0.0.1:42346`, `http://127.0.0.1:7480`, 1),
		strings.Replace(committed, `"id":"destination-id"`, `"id":"source-id"`, 1),
	} {
		if err := fixture.validatePeriodEndpoints([]byte(invalid)); err == nil {
			t.Fatal("bootstrap, stale or mismatched committed endpoints were accepted")
		}
	}
}

func TestRGWTopologyRequiresEveryZoneIdentityAndEndpoint(t *testing.T) {
	fixture := &RGWMultisite{RealmID: "realm", config: RGWMultisiteConfig{Zonegroup: "group"}}
	states := []*rgwZoneState{
		{RGWZone: RGWZone{Name: "a", ID: "id-a", PeerEndpoint: "http://a:7480"}},
		{RGWZone: RGWZone{Name: "b", ID: "id-b", PeerEndpoint: "http://b:7480"}},
		{RGWZone: RGWZone{Name: "c", ID: "id-c", PeerEndpoint: "http://c:7480"}},
	}
	const committed = `{"realm_id":"realm","master_zone":"id-c","period_map":{"zonegroups":[{"name":"group","master_zone":"id-c","endpoints":["http://c:7480"],"zones":[{"id":"id-a","name":"a","endpoints":["http://a:7480"]},{"id":"id-b","name":"b","endpoints":["http://b:7480"]},{"id":"id-c","name":"c","endpoints":["http://c:7480"]}]}]}}`
	period, err := decodeRGWTopologyPeriod([]byte(committed))
	if err != nil || fixture.checkZonePeriod(period, states[2], states) != nil {
		t.Fatal("three-zone graph with non-original metadata master was rejected")
	}
	for _, invalid := range []string{
		strings.Replace(committed, `"realm_id":"realm"`, `"realm_id":"foreign"`, 1),
		strings.Replace(committed, `"id":"id-b"`, `"id":"id-a"`, 1),
		strings.Replace(committed, `"name":"b"`, `"name":"foreign"`, 1),
		strings.Replace(committed, `http://b:7480`, `http://b:wrong-port`, 1),
		strings.Replace(committed, `"master_zone":"id-c"`, `"master_zone":"id-a"`, 1),
	} {
		period, err := decodeRGWTopologyPeriod([]byte(invalid))
		if err == nil && fixture.checkZonePeriod(period, states[2], states) == nil {
			t.Fatal("stale or incomplete three-zone native graph was accepted")
		}
	}
}

func TestRGWZonePreflightAndTerminationRetainOwnership(t *testing.T) {
	fixture := &RGWMultisite{config: RGWMultisiteConfig{SourceZone: "a", DestinationZone: "b"},
		additionalZones: map[string]*rgwZoneState{"c": {RGWZone: RGWZone{Name: "c", ID: "id-c"}}}}
	if zone, err := fixture.AddZone(t.Context(), "image", RGWZoneConfig{Name: "--invalid"}); err == nil || zone != nil {
		t.Fatal("unsafe zone name was accepted")
	}
	if err := fixture.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if zone, err := fixture.AddZone(t.Context(), "image", RGWZoneConfig{Name: "d"}); err == nil || zone != nil {
		t.Fatal("terminated fixture accepted a new zone")
	}
	zones := fixture.Zones()
	if !slices.Equal([]string{zones[0].Name, zones[1].Name, zones[2].Name}, []string{"a", "b", "c"}) || zones[2].ID != "id-c" {
		t.Fatal("partial zone descriptors were lost or unordered")
	}
	if _, err := fixture.ZoneAdmin(t.Context(), "c", "zone", "get"); err == nil {
		t.Fatal("terminated zone attempted a CLI operation")
	}
}

func TestRGWMetadataBootstrapRequiresEveryIncrementalShard(t *testing.T) {
	const ready = `{"sync_status":{"info":{"status":"sync","num_shards":2},"markers":[{"key":0,"val":{"state":1}},{"key":1,"val":{"state":1}}]},"full_sync":{"total":0,"complete":0}}`
	if err := rgwMetadataBootstrapped([]byte(ready)); err != nil {
		t.Fatal(err)
	}
	for _, pending := range []string{
		// Equal zero entry counts must not hide a missing full-sync index.
		strings.Replace(ready, `"state":1`, `"state":0`, 1),
		strings.Replace(ready, `"status":"sync"`, `"status":"building-full-sync-maps"`, 1),
		strings.Replace(ready, `"num_shards":2`, `"num_shards":3`, 1),
		strings.Replace(ready, `"key":1`, `"key":0`, 1),
		strings.Replace(ready, `"key":1`, `"key":2`, 1),
		`{"sync_status":{"info":{"status":"sync","num_shards":0},"markers":[]}}`,
		`{}`, `invalid json`,
	} {
		if err := rgwMetadataBootstrapped([]byte(pending)); err == nil {
			t.Fatal("incomplete or malformed metadata bootstrap was accepted")
		}
	}
}
