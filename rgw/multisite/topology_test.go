package multisite

import (
	"slices"
	"strings"
	"testing"
)

func TestRGWSetupImageUsesEachClusterAndRespectsOverride(t *testing.T) {
	for _, zone := range []string{"source", "destination", "added-zone"} {
		t.Run(zone, func(t *testing.T) {
			clusterImage := "ceph-rgw:" + zone
			image, err := rgwSetupImage("", clusterImage)
			if err != nil || image != clusterImage {
				t.Fatalf("setup client did not select its own cluster's RGW image: image=%q error=%v", image, err)
			}
			const override = "compatible-rgw:explicit"
			image, err = rgwSetupImage(override, clusterImage)
			if err != nil || image != override {
				t.Fatalf("explicit shared RGW image was not preserved: image=%q error=%v", image, err)
			}
		})
	}
}

func TestRGWSetupImageRejectsMissingOrBlankSelection(t *testing.T) {
	for _, image := range []string{"", " \t\n"} {
		if selected, err := rgwSetupImage("", image); err == nil || selected != "" {
			t.Fatal("empty cluster RGW image was accepted")
		}
	}
	if image, err := rgwSetupImage(" \t", "valid-rgw:local"); err == nil || image != "" {
		t.Fatal("blank explicit override silently fell back to the cluster image")
	}
}

func TestRGWCommittedPeriodRequiresActualPeerEndpoints(t *testing.T) {
	fixture := &Fixture{
		RealmID: "realm-id", SourceZoneID: "source-id", DestinationZoneID: "destination-id",
		realm: rgwRealmAccess{config: Config{Zonegroup: "us-east-1", SourceZone: "primary", DestinationZone: "secondary"}, sourceURL: "http://127.0.0.1:42345", destinationURL: "http://127.0.0.1:42346"}}
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
	fixture := &Fixture{RealmID: "realm", realm: rgwRealmAccess{config: Config{Zonegroup: "group"}}}
	states := []*rgwZoneState{
		{Zone: Zone{Name: "a", ID: "id-a", PeerEndpoint: "http://a:7480"}},
		{Zone: Zone{Name: "b", ID: "id-b", PeerEndpoint: "http://b:7480"}},
		{Zone: Zone{Name: "c", ID: "id-c", PeerEndpoint: "http://c:7480"}},
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
		// Members are read by exact name: a case alias does not stand in for
		// the native member, and the last of repeated keys wins.
		strings.Replace(committed, `"id":"id-b","name":"b"`, `"ID":"id-b","name":"b"`, 1),
		strings.Replace(committed, `"realm_id":"realm"`, `"realm_id":"foreign","REALM_ID":"realm"`, 1),
		strings.Replace(committed, `"id":"id-b"`, `"id":"id-b","id":"id-a"`, 1),
	} {
		period, err := decodeRGWTopologyPeriod([]byte(invalid))
		if err == nil && fixture.checkZonePeriod(period, states[2], states) == nil {
			t.Fatal("stale or incomplete three-zone native graph was accepted")
		}
	}
}

// A committed period whose members have the wrong shape is invalid before any
// topology comparison; missing optional members read as empty.
func TestRGWTopologyPeriodRefusesMalformedMembers(t *testing.T) {
	const base = `{"realm_id":"realm","master_zone":"id-a","period_map":{"zonegroups":[{"id":"g","name":"group","master_zone":"id-a","endpoints":["http://a:7480"],"zones":[{"id":"id-a","name":"a","endpoints":["http://a:7480"]}]}]}}`
	period, err := decodeRGWTopologyPeriod([]byte(base))
	if err != nil || len(period.PeriodMap.Zonegroups) != 1 || period.PeriodMap.Zonegroups[0].Zones[0].ID != "id-a" || period.MasterZonegroup != "" {
		t.Fatalf("well-formed period: %+v, %v", period, err)
	}
	if period, err := decodeRGWTopologyPeriod([]byte(strings.Replace(base, `,"zones":[{"id":"id-a","name":"a","endpoints":["http://a:7480"]}]`, `,"zones":null`, 1))); err != nil || len(period.PeriodMap.Zonegroups[0].Zones) != 0 {
		t.Fatalf("null zones: %+v, %v", period, err)
	}
	for name, malformed := range map[string]string{
		"no realm":             strings.Replace(base, `"realm_id":"realm",`, ``, 1),
		"numeric master":       strings.Replace(base, `"master_zone":"id-a","period_map"`, `"master_zone":7,"period_map"`, 1),
		"no zonegroup list":    `{"realm_id":"realm","master_zone":"id-a","period_map":{}}`,
		"zonegroup not object": `{"realm_id":"realm","master_zone":"id-a","period_map":{"zonegroups":["g"]}}`,
		"endpoint not string":  strings.Replace(base, `"endpoints":["http://a:7480"],"zones"`, `"endpoints":[7],"zones"`, 1),
		"zones not list":       strings.Replace(base, `"zones":[{"id":"id-a","name":"a","endpoints":["http://a:7480"]}]`, `"zones":{}`, 1),
		"zone not object":      strings.Replace(base, `"zones":[{"id":"id-a","name":"a","endpoints":["http://a:7480"]}]`, `"zones":["a"]`, 1),
		"zone id not string":   strings.Replace(base, `{"id":"id-a","name":"a"`, `{"id":1,"name":"a"`, 1),
		"trailing output":      base + "{}",
	} {
		if _, err := decodeRGWTopologyPeriod([]byte(malformed)); err == nil {
			t.Errorf("%s: malformed period was decoded", name)
		}
	}
}

func TestRGWZonePreflightAndTerminationRetainOwnership(t *testing.T) {
	fixture := &Fixture{realm: rgwRealmAccess{config: Config{SourceZone: "a", DestinationZone: "b"}}, topology: rgwZoneTopology{additional: map[string]*rgwZoneState{"c": {Zone: Zone{Name: "c", ID: "id-c"}}}}}
	if zone, err := fixture.AddZone(t.Context(), "image", ZoneConfig{Name: "--invalid"}); err == nil || zone != nil {
		t.Fatal("unsafe zone name was accepted")
	}
	if err := fixture.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if zone, err := fixture.AddZone(t.Context(), "image", ZoneConfig{Name: "d"}); err == nil || zone != nil {
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
