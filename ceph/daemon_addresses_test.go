package ceph

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The test data was captured from clusters with two MONs, two MGRs and one
// OSD on the fixture's bridge network.
func TestParseDaemonAddressesFromCapturedMaps(t *testing.T) {
	for _, release := range []string{"20.2.4", "19.2.5"} {
		t.Run(release, func(t *testing.T) {
			read := func(name string) []byte {
				data, err := os.ReadFile(filepath.Join("testdata", "daemon-addresses", release, name))
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			var got DaemonAddresses
			if err := parseMonitorAddresses(read("mon-dump.json"), &got); err != nil {
				t.Fatal(err)
			}
			if err := parseManagerAddresses(read("mgr-dump.json"), &got); err != nil {
				t.Fatal(err)
			}
			if err := parseOSDAddresses(read("osd-dump.json"), &got); err != nil {
				t.Fatal(err)
			}
			wantMons := []DaemonAddress{
				{Name: "a", Addrs: []EntityAddress{{"v2", "172.20.0.2:3300", 0}, {"v1", "172.20.0.2:6789", 0}}},
				{Name: "b", Addrs: []EntityAddress{{"v2", "172.20.0.4:3300", 0}, {"v1", "172.20.0.4:6789", 0}}},
			}
			if got.MonMapEpoch < 1 || !reflect.DeepEqual(got.Monitors, wantMons) {
				t.Fatalf("monitors: epoch %d %+v", got.MonMapEpoch, got.Monitors)
			}
			active := got.ActiveManager
			if got.MgrMapEpoch < 1 || active == nil || active.Name != "a" || len(active.Addrs) != 2 ||
				active.Addrs[0].Type != "v2" || active.Addrs[0].Addr != "172.20.0.5:6800" || active.Addrs[0].Nonce == 0 ||
				!got.ManagerAvailable || !reflect.DeepEqual(got.StandbyManagers, []string{"b"}) {
				t.Fatalf("managers: epoch %d active %+v standbys %v", got.MgrMapEpoch, active, got.StandbyManagers)
			}
			if got.OSDMapEpoch < 1 || len(got.OSDs) != 1 {
				t.Fatalf("osds: epoch %d %+v", got.OSDMapEpoch, got.OSDs)
			}
			osd := got.OSDs[0]
			for name, vector := range map[string][]EntityAddress{"public": osd.Public, "cluster": osd.Cluster, "front": osd.HeartbeatFront, "back": osd.HeartbeatBack} {
				if len(vector) != 2 || vector[0].Type != "v2" || vector[1].Type != "v1" || !strings.HasPrefix(vector[0].Addr, "172.20.0.7:") {
					t.Fatalf("osd.0 %s addresses %+v", name, vector)
				}
			}
			if osd.ID != 0 || !osd.Up || !osd.In || osd.Public[0].Addr == osd.Cluster[0].Addr {
				t.Fatalf("osd.0 %+v", osd)
			}
		})
	}
}

func TestParseDaemonAddressesRejectsIncompleteMaps(t *testing.T) {
	var result DaemonAddresses
	for name, test := range map[string]struct {
		parse func([]byte, *DaemonAddresses) error
		data  string
	}{
		"monmap without members":   {parseMonitorAddresses, `{"epoch":1}`},
		"monmap out of rank order": {parseMonitorAddresses, `{"epoch":1,"mons":[{"rank":1,"name":"b","public_addrs":{"addrvec":[]}}]}`},
		"monmap invalid address":   {parseMonitorAddresses, `{"epoch":1,"mons":[{"rank":0,"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"not-an-address","nonce":0}]}}]}`},
		"monmap unknown type":      {parseMonitorAddresses, `{"epoch":1,"mons":[{"rank":0,"name":"a","public_addrs":{"addrvec":[{"type":"v3","addr":"10.0.0.1:3300","nonce":0}]}}]}`},
		"monmap missing vector":    {parseMonitorAddresses, `{"epoch":1,"mons":[{"rank":0,"name":"a"}]}`},
		"mgrmap without standbys":  {parseManagerAddresses, `{"epoch":1,"active_name":"a","active_addrs":{"addrvec":[]},"available":true}`},
		"mgrmap without available": {parseManagerAddresses, `{"epoch":1,"active_name":"a","active_addrs":{"addrvec":[]},"standbys":[]}`},
		"mgrmap unnamed standby":   {parseManagerAddresses, `{"epoch":1,"active_name":"","available":false,"standbys":[{}]}`},
		"osdmap incomplete entry":  {parseOSDAddresses, `{"epoch":1,"osds":[{"osd":0,"up":1}]}`},
		"osdmap missing vector":    {parseOSDAddresses, `{"epoch":1,"osds":[{"osd":0,"up":1,"in":1,"public_addrs":{"addrvec":[]}}]}`},
	} {
		if err := test.parse([]byte(test.data), &result); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// An MGRMap without an active MGR and an OSD that never booted are valid.
	result = DaemonAddresses{}
	if err := parseManagerAddresses([]byte(`{"epoch":2,"active_name":"","active_addrs":{"addrvec":[]},"available":false,"standbys":[]}`), &result); err != nil || result.ActiveManager != nil {
		t.Fatalf("no active MGR: %v %+v", err, result.ActiveManager)
	}
	empty := `{"addrvec":[]}`
	if err := parseOSDAddresses([]byte(`{"epoch":3,"osds":[{"osd":4,"up":0,"in":0,"public_addrs":`+empty+`,"cluster_addrs":`+empty+`,"heartbeat_front_addrs":`+empty+`,"heartbeat_back_addrs":`+empty+`}]}`), &result); err != nil || len(result.OSDs) != 1 || result.OSDs[0].Up {
		t.Fatalf("unbooted OSD: %v %+v", err, result.OSDs)
	}
}
