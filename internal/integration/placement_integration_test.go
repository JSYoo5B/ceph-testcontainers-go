//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostFailureDomainPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	layout := []ceph.OSDConfig{
		{Host: "host-a", Rack: "rack-a", Root: "tc-storage", DeviceClass: "fast"},
		{Host: "host-b", Rack: "rack-b", Root: "tc-storage", DeviceClass: "fast"},
		{Host: "host-c", Rack: "rack-c", Root: "tc-storage", DeviceClass: "fast"},
	}
	cluster, client := newServiceCluster(t, ceph.WithInitialOSDs(layout...), ceph.WithPoolDefaults(3, 2), ceph.WithDefaultCRUSHRoot("tc-storage"))
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
		Name: "tc-host-replicated", PGNum: 8, Application: "rados",
		FailureDomain: "host", CRUSHRoot: "tc-storage", DeviceClass: "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	if pool.Replicas != 3 || pool.MinSize != 2 {
		t.Fatalf("cluster pool defaults were not applied: %+v", pool)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// Inspect Ceph's CRUSH hierarchy independently of the Go descriptors. Logical
	// hosts/racks model placement on one Docker engine; stopping an OSD here
	// removes the only daemon in that host's failure domain.
	hostByOSD := make(map[int]string)
	for _, osd := range cluster.OSDs() {
		output, err := cluster.Ceph(ctx, "osd", "find", strconv.Itoa(osd.ID), "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var location struct {
			OSD   int `json:"osd"`
			CRUSH struct {
				Host string `json:"host"`
				Rack string `json:"rack"`
				Root string `json:"root"`
			} `json:"crush_location"`
		}
		if err := json.Unmarshal(output, &location); err != nil {
			t.Fatal(err)
		}
		placement := osd.Placement()
		if location.OSD != osd.ID || location.CRUSH.Host != placement.Host || location.CRUSH.Rack != placement.Rack || location.CRUSH.Root != placement.Root {
			t.Fatalf("osd.%d CRUSH location differs from its requested topology: %s", osd.ID, output)
		}
		hostByOSD[osd.ID] = location.CRUSH.Host
	}
	output, err := cluster.Ceph(ctx, "osd", "crush", "class", "ls-osd", "fast", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var classOSDs []int
	if err := json.Unmarshal(output, &classOSDs); err != nil || len(classOSDs) != 3 {
		t.Fatalf("requested device class did not select the three OSDs: %s error=%v", output, err)
	}
	for id := range hostByOSD {
		if !slices.Contains(classOSDs, id) {
			t.Fatalf("osd.%d is absent from the requested device class", id)
		}
	}
	for index := range 16 {
		output, err := cluster.Ceph(ctx, "osd", "map", pool.Name, fmt.Sprintf("object-%02d", index), "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var mapping struct {
			Acting []int `json:"acting"`
		}
		if err := json.Unmarshal(output, &mapping); err != nil {
			t.Fatal(err)
		}
		hosts := make(map[string]struct{})
		for _, id := range mapping.Acting {
			host := hostByOSD[id]
			if host == "" {
				t.Fatalf("acting set selected an OSD outside the requested root/class: %s", output)
			}
			hosts[host] = struct{}{}
		}
		if len(mapping.Acting) != 3 || len(hosts) != 3 {
			t.Fatalf("acting set did not span three distinct hosts: %s", output)
		}
	}
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, pool.Name, "seed")
	failed := cluster.OSDs()[0]
	stopTimeout := 5 * time.Second
	if err := failed.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	waitPlacementOSDDown(t, ctx, cluster, failed.ID)
	// min_size=2 permits writes with the third host unavailable. Each phase is
	// a fresh native librados session, and verify retains the outage writes.
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, pool.Name, "outage")
	if err := failed.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, pool.Name, "verify")
	t.Logf("three host domains with replicas=3/min_size=2: acting sets span hosts; native reads/writes survive host %s down and recover after restart", failed.Placement().Host)
}

func waitPlacementOSDDown(t *testing.T, parent context.Context, cluster *ceph.Container, id int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		output, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var dump struct {
			OSDs []struct {
				ID int `json:"osd"`
				Up int `json:"up"`
			} `json:"osds"`
		}
		if err := json.Unmarshal(output, &dump); err != nil {
			t.Fatal(err)
		}
		for _, osd := range dump.OSDs {
			if osd.ID == id && osd.Up == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("osd.%d did not become down in the OSD map: %s", id, output)
		case <-ticker.C:
		}
	}
}

const hostPlacementClientScript = `import rados, sys
pool, phase = sys.argv[1:]
original = bytes(range(256)) * 256
patch = b"write-while-one-host-is-down" * 23
offset = 4093
expected = original[:offset] + patch + original[offset + len(patch):]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "30", "rados_osd_op_timeout": "30"}) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        for index in range(16):
            name = "object-%02d" % index
            if phase == "seed":
                ioctx.write_full(name, original)
            elif phase == "outage":
                assert ioctx.read(name, len(original) + 1) == original, "outage read lost seeded bytes"
                ioctx.write(name, patch, offset)
                ioctx.write_full("outage-%02d" % index, patch)
            wanted = original if phase == "seed" else expected
            assert ioctx.read(name, len(wanted) + 1) == wanted, "host-placement object bytes differ"
            if phase != "seed":
                assert ioctx.read("outage-%02d" % index, len(patch) + 1) == patch, "outage-created bytes differ"
            if phase == "verify":
                ioctx.remove_object(name)
                ioctx.remove_object("outage-%02d" % index)
        if phase == "verify":
            assert list(ioctx.list_objects()) == [], "host-placement objects remain after cleanup"
`
