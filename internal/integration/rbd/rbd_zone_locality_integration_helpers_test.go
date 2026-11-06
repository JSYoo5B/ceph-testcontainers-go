//go:build all || integration

package rbd_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

const (
	localityPool   = "tc-locality"
	localityImage  = "locality"
	localityOneWay = 20 * time.Millisecond
	localitySize   = 4 << 20
)

// localityMap is `ceph osd map` for one object.
type localityMap struct {
	Epoch         int    `json:"epoch"`
	PGID          string `json:"pgid"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}

// localityOp is one entry of an OSD's dump_historic_ops.
type localityOp struct {
	Description string `json:"description"`
	InitiatedAt string `json:"initiated_at"`
}

// localityRead is one data read probe: a fresh rbd process with a policy and
// a declared CRUSH location, and what it did.
type localityRead struct {
	Policy, Location string
	Effective        string
	Elapsed          time.Duration
	SHA256           string
	Served           map[int][]string
}

// testRBDZoneLocality answers whether a consumer can show which OSD serves
// a librbd data read when crossing the zone boundary costs more. One cluster
// spans zones a and b with one OSD each and a zone-failure-domain pool of
// size 2; one image object has its primary in b and its replica in a; the
// client is in a. The boundary delays only the client's packets to and from
// the zone b OSD. Each read is a fresh rbd process without cache, and the
// OSD that served it is the one whose op history gained a read of that data
// object; the bytes, the acting set and the shaping are checked around it.
func testRBDZoneLocality(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	helper := harness.NetemImage(t)
	cluster := harness.NewRegionalCluster(t, ceph.Run,
		ceph.WithZoneConfig(harness.RegionalZoneConfig(t)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
			ceph.OSDConfig{Zone: "a", Host: "zone-a-host-0"},
			ceph.OSDConfig{Zone: "b", Host: "zone-b-host-0"},
		).WithPoolDefaults(2, 1)))
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("release %s, images %+v", facts.CephVersion, facts.Images)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: localityPool, PGNum: 1, Replicas: 2, MinSize: 1, Application: "rbd", FailureDomain: "zone"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, localityPool); err != nil {
		t.Fatal(err)
	}
	for _, setting := range [][2]string{{"osd_op_history_size", "1000"}, {"osd_op_history_duration", "3600"}} {
		if _, err := cluster.Ceph(ctx, "config", "set", "osd", setting[0], setting[1]); err != nil {
			t.Fatal(err)
		}
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client := harness.RegionalClient(t, ctx, cluster, "a")
	layout, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientMember, clientZone := harness.ZoneMemberByContainer(t, layout, client.GetContainerID())
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byZone := map[string]*ceph.OSDContainer{}
	for _, osd := range osds {
		byZone[osd.Placement().Zone] = osd
	}
	osdA, osdB := byZone["a"], byZone["b"]
	if clientZone != "a" || osdA == nil || osdB == nil {
		t.Fatalf("client in zone %q, OSDs by zone %v", clientZone, byZone)
	}

	// Known bytes in one data object whose primary is in zone b.
	want := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c",
		fmt.Sprintf("head -c %d /dev/urandom > /tmp/locality.img && sha256sum /tmp/locality.img", localitySize)))[0]
	harness.ExecOutput(t, ctx, client, "rbd", "import", "--object-size", "4M", "/tmp/locality.img", localityPool+"/"+localityImage)
	var info struct {
		Prefix string `json:"block_name_prefix"`
	}
	if err := json.Unmarshal([]byte(harness.ExecOutput(t, ctx, client, "rbd", "info", "--format", "json", localityPool+"/"+localityImage)), &info); err != nil || info.Prefix == "" {
		t.Fatalf("rbd info: %+v %v", info, err)
	}
	object := info.Prefix + ".0000000000000000"
	placement := localityObjectMap(t, ctx, cluster, object)
	if placement.ActingPrimary != osdB.ID {
		pgid, err := ceph.ParsePGID(placement.PGID)
		if err != nil {
			t.Fatal(err)
		}
		handle, err := pool.OverridePGPrimary(ctx, ceph.PGPrimary{PG: pgid, OSD: osdB, RaiseMinCompatClient: true})
		if handle != nil {
			t.Cleanup(func() {
				if err := handle.Restore(context.Background()); err != nil {
					t.Errorf("restore the PG primary: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		placement = localityObjectMap(t, ctx, cluster, object)
	}
	if placement.ActingPrimary != osdB.ID || !slices.Equal(slices.Sorted(slices.Values(placement.Acting)), slices.Sorted(slices.Values([]int{osdA.ID, osdB.ID}))) {
		t.Fatalf("object %s maps to %+v, want primary osd.%d (zone b) and replica osd.%d (zone a)", object, placement, osdB.ID, osdA.ID)
	}
	t.Logf("data object %s: PG %s acting %v primary osd.%d (zone b), replica osd.%d (zone a), epoch %d; client %s in zone a at %v",
		object, placement.PGID, placement.Acting, placement.ActingPrimary, osdA.ID, placement.Epoch, clientMember.Name, clientMember.PublicAddress)

	// The boundary delays only the client's packets to and from osd.B.
	native, err := observer.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	address := func(id int) netip.AddrPort {
		for _, entry := range native.OSDs {
			if entry.ID == id {
				addr, err := harness.EntityAddrPort(entry.Public, "v2")
				if err != nil {
					t.Fatal(err)
				}
				return addr
			}
		}
		t.Fatalf("osd.%d is not in the OSD map", id)
		return netip.AddrPort{}
	}
	local, remote := address(osdA.ID), address(osdB.ID)
	probe := harness.NewZoneProbe(t, ctx, clientMember.Name, client.GetContainerID())
	rtt := func(phase string) (harness.ZoneRTT, harness.ZoneRTT) {
		t.Helper()
		l, err := probe.ConnectRTT(ctx, local, 20, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		r, err := probe.ConnectRTT(ctx, remote, 20, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RTT %-10s client to local osd.%d %s: %s; to remote osd.%d %s: %s", phase, osdA.ID, local, l, osdB.ID, remote, r)
		return l, r
	}
	baseLocal, baseRemote := rtt("baseline")
	var leases []*fault.NetworkProfileLease
	for zone, peer := range map[string]netip.Addr{"a": remote.Addr(), "b": clientMember.PublicAddress} {
		lease, err := fault.ApplyZoneProfile(ctx, cluster, zone, fault.NetworkProfile{HelperImage: helper, Plane: ceph.PublicNetworkPlane, Delay: localityOneWay, Peer: peer})
		if lease != nil {
			leases = append(leases, lease)
			t.Cleanup(func() {
				if err := lease.Restore(context.Background()); err != nil {
					t.Errorf("restore the zone %s profile: %v", zone, err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("zone %s egress to %s delayed %v on %.12s %s", zone, peer, localityOneWay, lease.ContainerID, lease.Interface)
	}
	shapedLocal, shapedRemote := rtt("shaped")
	if err := harness.CheckZoneControl(baseLocal, shapedLocal); err != nil {
		t.Fatalf("the local path changed under shaping: %v", err)
	}
	if err := harness.CheckZoneShaped(baseRemote, shapedRemote, 2*localityOneWay); err != nil {
		t.Fatalf("the remote path did not gain the configured delay once: %v", err)
	}

	// Policy comparison with routing, delay and placement fixed.
	reads := []struct {
		policy, location string
		served           int
	}{
		{"default", "", osdB.ID},
		{"localize", "zone=a", osdA.ID},
		{"localize", "zone=b", osdB.ID},
		{"default", "", osdB.ID},
	}
	targets := []*ceph.OSDContainer{osdA, osdB}
	for i, read := range reads {
		got := localityReadOnce(t, ctx, client, targets, object, read.policy, read.location, i)
		served := slices.Sorted(func(yield func(int) bool) {
			for id, ops := range got.Served {
				if len(ops) > 0 && !yield(id) {
					return
				}
			}
		})
		t.Logf("read %d: policy %s, declared location %q, effective %s: %v, sha256 %s; data reads by OSD %v",
			i, read.policy, read.location, got.Effective, got.Elapsed.Round(time.Millisecond), got.SHA256, got.Served)
		if got.SHA256 != want {
			t.Fatalf("read %d returned sha256 %s, want %s", i, got.SHA256, want)
		}
		if !slices.Equal(served, []int{read.served}) {
			t.Fatalf("read %d with policy %s at %q was served by OSDs %v, want osd.%d", i, read.policy, read.location, served, read.served)
		}
	}
	after := localityObjectMap(t, ctx, cluster, object)
	if after.PGID != placement.PGID || !slices.Equal(after.Acting, placement.Acting) || after.ActingPrimary != placement.ActingPrimary {
		t.Fatalf("the acting set moved during the reads: %+v, then %+v", placement, after)
	}
	for i := len(leases) - 1; i >= 0; i-- {
		if err := leases[i].Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	restoredLocal, restoredRemote := rtt("restored")
	for name, pair := range map[string][2]harness.ZoneRTT{"local": {baseLocal, restoredLocal}, "remote": {baseRemote, restoredRemote}} {
		if err := harness.CheckZoneControl(pair[0], pair[1]); err != nil {
			t.Fatalf("the %s path after restoration: %v", name, err)
		}
	}
	t.Logf("acting set fixed at %v primary osd.%d from epoch %d to %d", after.Acting, after.ActingPrimary, placement.Epoch, after.Epoch)
}

func localityObjectMap(t *testing.T, ctx context.Context, cluster *ceph.Container, object string) localityMap {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "map", localityPool, object, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var m localityMap
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("osd map %s: %v", out, err)
	}
	return m
}

// localityHistory returns the data reads of object in an OSD's op history,
// keyed by description and start, which identify one request.
func localityHistory(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, object string) map[string]string {
	t.Helper()
	out, err := osd.AdminSocket(ctx, "dump_historic_ops")
	if err != nil {
		t.Fatal(err)
	}
	var history struct {
		Ops []localityOp `json:"ops"`
	}
	if err := json.Unmarshal(out, &history); err != nil {
		t.Fatalf("osd.%d dump_historic_ops: %v", osd.ID, err)
	}
	reads := map[string]string{}
	for _, op := range history.Ops {
		if strings.HasPrefix(op.Description, "osd_op(") && strings.Contains(op.Description, object) &&
			(strings.Contains(op.Description, "[read ") || strings.Contains(op.Description, "sparse-read")) {
			reads[op.Description+" @"+op.InitiatedAt] = op.Description
		}
	}
	return reads
}

// localityReadOnce reads the image with a fresh rbd process without cache,
// with policy and the declared CRUSH location, and attributes its data reads
// to the OSDs whose op history gained them.
func localityReadOnce(t *testing.T, ctx context.Context, client testcontainers.Container, osds []*ceph.OSDContainer, object, policy, location string, n int) localityRead {
	t.Helper()
	args := []string{"--rbd_cache=false", "--rbd_read_from_replica_policy=" + policy}
	if location != "" {
		args = append(args, "--crush_location="+location)
	}
	effective := []string{}
	for _, key := range []string{"rbd_read_from_replica_policy", "crush_location", "rbd_cache"} {
		value := harness.ExecOutput(t, ctx, client, append(append([]string{"rbd"}, args...), "--show-config-value", key)...)
		effective = append(effective, key+"="+strings.TrimSpace(value))
	}
	before := map[int]map[string]string{}
	for _, osd := range osds {
		before[osd.ID] = localityHistory(t, ctx, osd, object)
	}
	path := fmt.Sprintf("/tmp/locality-read-%d", n)
	start := time.Now()
	harness.ExecOutput(t, ctx, client, append(append([]string{"rbd"}, args...), "export", "--no-progress", localityPool+"/"+localityImage, path)...)
	elapsed := time.Since(start)
	sum := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c", "sha256sum "+path+" && rm -f "+path))[0]
	read := localityRead{Policy: policy, Location: location, Effective: strings.Join(effective, " "), Elapsed: elapsed, SHA256: sum, Served: map[int][]string{}}
	for _, osd := range osds {
		for key, description := range localityHistory(t, ctx, osd, object) {
			if _, seen := before[osd.ID][key]; !seen {
				read.Served[osd.ID] = append(read.Served[osd.ID], description)
			}
		}
	}
	return read
}
