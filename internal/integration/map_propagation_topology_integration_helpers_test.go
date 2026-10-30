//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

// mapPropagationReceipt is what TestMapPropagationReceipt logs.
type mapPropagationReceipt struct {
	OSDMapBefore   uint64       `json:"osdmap_before"`
	OSDMapAfter    uint64       `json:"osdmap_after"`
	OSDArrivals    []mapArrival `json:"osd_arrivals"`
	MonmapBefore   uint64       `json:"monmap_before"`
	MonmapAdded    uint64       `json:"monmap_added"`
	AddArrivals    []mapArrival `json:"add_arrivals"`
	MonmapRemoved  uint64       `json:"monmap_removed"`
	RemoveArrivals []mapArrival `json:"remove_arrivals"`
}

// osdMapEpoch is the MONs' current OSDMap epoch.
func osdMapEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container) uint64 {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "stat", "--format", "json")
	var stat struct {
		Epoch *uint64 `json:"epoch"`
	}
	if err != nil || json.Unmarshal(out, &stat) != nil || stat.Epoch == nil {
		t.Fatalf("osd stat: %v: %.256s", err, out)
	}
	return *stat.Epoch
}

func readMonmapView(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) (monmapView, []byte) {
	t.Helper()
	out, err := harness.MonitorHandle(t, ctx, cluster, name).AdminSocket(ctx, "mon_status")
	if err != nil {
		t.Fatalf("mon_status of mon.%s: %v", name, err)
	}
	view, err := parseMonmapView(out)
	if err != nil {
		t.Fatalf("mon_status of mon.%s: %v: %.512s", name, err, out)
	}
	return view, out
}

// awaitMonmap reads every named MON's own mon_status until each holds a
// MonMap newer than after with exactly members and is in a quorum of all of
// them, and records when each first held that map.
func awaitMonmap(t *testing.T, ctx context.Context, cluster *ceph.Container, names []string, after uint64, members []string, changed time.Time) (uint64, []mapArrival) {
	t.Helper()
	arrivals := map[string]mapArrival{}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		settled, epoch := true, uint64(0)
		var election uint64
		for _, name := range names {
			view, _ := readMonmapView(t, ctx, cluster, name)
			if view.Epoch > after && slices.Equal(view.Members, members) {
				if _, seen := arrivals[name]; !seen {
					compact, _ := json.Marshal(view)
					arrivals[name] = mapArrival{Daemon: "mon." + name, Epoch: view.Epoch, AfterM: time.Since(changed).Milliseconds(), View: compact}
				}
			} else {
				settled = false
			}
			inQuorum := (view.State == "leader" || view.State == "peon") && len(view.Quorum) == len(members)
			if !inQuorum || (epoch != 0 && view.Epoch != epoch) || (election != 0 && view.ElectionEpoch != election) {
				settled = false
			}
			epoch, election = view.Epoch, view.ElectionEpoch
		}
		if settled {
			result := make([]mapArrival, 0, len(names))
			for _, name := range names {
				result = append(result, arrivals[name])
			}
			return epoch, result
		}
		if time.Now().After(deadline) {
			t.Fatalf("MONs %v did not all hold a MonMap after e%d with %v in one quorum; arrivals %+v", names, after, members, arrivals)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// testMapPropagationReceipt records which daemon held which map, from each
// daemon's own admin socket. Creating a pool makes a new OSDMap epoch, and
// each OSD's status must reach it. Adding MON d makes a new MonMap epoch,
// and MONs a, b, c and d must each hold it, with all four members and in one
// quorum with the same election epoch; removing d makes another, which a, b
// and c must each hold with three members. The receipt keeps when each daemon
// first held each map.
func testMapPropagationReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3)))
	if err := cluster.WaitForQuorum(ctx); err != nil {
		t.Fatal(err)
	}
	var receipt mapPropagationReceipt

	receipt.OSDMapBefore = osdMapEpoch(t, ctx, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-maps", PGNum: 1, Replicas: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	changed := time.Now()
	receipt.OSDMapAfter = osdMapEpoch(t, ctx, cluster)
	if receipt.OSDMapAfter <= receipt.OSDMapBefore {
		t.Fatalf("pool creation left the OSDMap at e%d", receipt.OSDMapAfter)
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		deadline := time.Now().Add(time.Minute)
		for {
			out, err := harness.OSDHandle(t, ctx, cluster, osd.ID).AdminSocket(ctx, "status")
			if err != nil {
				t.Fatalf("status of osd.%d: %v", osd.ID, err)
			}
			view, err := parseOSDMapView(out)
			if err != nil || view.WhoAmI != osd.ID {
				t.Fatalf("status of osd.%d: %+v %v: %.512s", osd.ID, view, err, out)
			}
			if view.NewestMap >= receipt.OSDMapAfter {
				compact, _ := json.Marshal(view)
				receipt.OSDArrivals = append(receipt.OSDArrivals, mapArrival{Daemon: fmt.Sprintf("osd.%d", osd.ID), Epoch: view.NewestMap, AfterM: time.Since(changed).Milliseconds(), View: compact})
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("osd.%d holds e%d, not e%d", osd.ID, view.NewestMap, receipt.OSDMapAfter)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	names := []string{}
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		names = append(names, mon.DaemonName)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"a", "b", "c"}) {
		t.Fatalf("monitors %v", names)
	}
	before, _ := readMonmapView(t, ctx, cluster, "a")
	receipt.MonmapBefore = before.Epoch
	if _, err := cluster.AddMonitor(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	changed = time.Now()
	receipt.MonmapAdded, receipt.AddArrivals = awaitMonmap(t, ctx, cluster, []string{"a", "b", "c", "d"}, receipt.MonmapBefore, []string{"a", "b", "c", "d"}, changed)
	if err := harness.MonitorHandle(t, ctx, cluster, "d").Remove(ctx); err != nil {
		t.Fatal(err)
	}
	changed = time.Now()
	receipt.MonmapRemoved, receipt.RemoveArrivals = awaitMonmap(t, ctx, cluster, names, receipt.MonmapAdded, names, changed)
	if receipt.MonmapAdded != receipt.MonmapBefore+1 || receipt.MonmapRemoved != receipt.MonmapAdded+1 {
		t.Fatalf("MonMap epochs e%d -> e%d -> e%d, want one per change", receipt.MonmapBefore, receipt.MonmapAdded, receipt.MonmapRemoved)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MAP_PROPAGATION_RECEIPT %s", encoded)
}
