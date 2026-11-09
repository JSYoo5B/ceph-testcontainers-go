//go:build all || integration

package rbd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// localityRead is one data read probe: a fresh rbd process with a policy and
// a declared CRUSH location, and what it did.
type localityRead struct {
	Policy, Location string
	Effective        string
	Elapsed          time.Duration
	SHA256           string
	// Evidence is each OSD's op history, replies and read counter change
	// around the read, and Servers the OSDs it attributes the read to.
	Evidence map[int]harness.RBDReadEvidence
	Servers  []int
}

// testRBDZoneLocality answers whether a consumer can show which OSD serves
// a librbd data read when crossing the zone boundary costs more. One cluster
// spans zones a and b with one OSD each and a zone-failure-domain pool of
// size 2; one image object has its primary in b and its replica in a; the
// client is in a. The boundary delays only the client's packets to and from
// the zone b OSD. Each read is a fresh rbd process without cache, and the
// OSD that served it is the one whose op history gained a read of that data
// object or whose debug_ms 1 log shows a reply to a read request of it
// (issue 650: on some runs the serving replica recorded no history entry).
// The OSD read counters only bound that evidence, and a replica read must
// also show in the replica's replica_read_served counter where the release
// reports it. The bytes, the acting set and the shaping are checked around
// it.
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
	controlClient := harness.RegionalClient(t, ctx, cluster, "b")
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
	controlMember, controlZone := harness.ZoneMemberByContainer(t, layout, controlClient.GetContainerID())
	if controlZone != "b" || controlMember.PublicAddress == remote.Addr() || local.Addr() == clientMember.PublicAddress {
		t.Fatal("cross-zone control overlaps a selected peer")
	}
	controlProbe := harness.NewZoneProbe(t, ctx, controlMember.Name, controlClient.GetContainerID())
	rtt := func(phase string) (harness.ZoneRTT, harness.ZoneRTT, harness.ZoneRTT) {
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
		c, err := controlProbe.ConnectRTT(ctx, local, 20, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RTT %s: unselected client %s in b to osd.%d in a: %s", phase, controlMember.PublicAddress, osdA.ID, c)
		return l, r, c
	}
	baseLocal, baseRemote, baseControl := rtt("baseline")
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
	shapedLocal, shapedRemote, shapedControl := rtt("shaped")
	if err := harness.CheckZoneControl(baseControl, shapedControl); err != nil {
		t.Fatalf("unselected cross-zone path gained delay: %v", err)
	}
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
		got := localityReadOnce(t, ctx, client, clientMember.PublicAddress, targets, object, read.policy, read.location, i)
		t.Logf("read %d: policy %s, declared location %q, effective %s: %v, sha256 %s; served by OSDs %v",
			i, read.policy, read.location, got.Effective, got.Elapsed.Round(time.Millisecond), got.SHA256, got.Servers)
		if got.SHA256 != want {
			t.Fatalf("read %d returned sha256 %s, want %s", i, got.SHA256, want)
		}
		if !slices.Equal(got.Servers, []int{read.served}) {
			t.Fatalf("read %d with policy %s at %q was served by OSDs %v, want osd.%d", i, read.policy, read.location, got.Servers, read.served)
		}
		for id, e := range got.Evidence {
			served, ok := e.ReplicaServed()
			if !ok {
				continue
			}
			if want := id == read.served && id != osdB.ID; (served > 0) != want {
				t.Fatalf("read %d: osd.%d served %d replica reads, want a replica read %v", i, id, served, want)
			}
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
	restoredLocal, restoredRemote, restoredControl := rtt("restored")
	for name, pair := range map[string][2]harness.ZoneRTT{"unselected": {baseControl, restoredControl}, "local": {baseLocal, restoredLocal}, "remote": {baseRemote, restoredRemote}} {
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

// localityOSDView is one observation of an OSD: the data reads of the object
// in its op history, keyed by description and start, which identify one
// request; every history entry that names the object, as the OSD reported
// it; and its read counters.
type localityOSDView struct {
	Reads    map[string]string
	Raw      []string
	Counters harness.OSDReadCounters
}

func localityObserve(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, object string) localityOSDView {
	t.Helper()
	out, err := osd.AdminSocket(ctx, "dump_historic_ops")
	if err != nil {
		t.Fatal(err)
	}
	reads, err := harness.RBDDataReadsInHistory(out, map[string]bool{object: true})
	if err != nil {
		t.Fatalf("osd.%d history: %v", osd.ID, err)
	}
	var history struct {
		Ops []json.RawMessage `json:"ops"`
	}
	if err := json.Unmarshal(out, &history); err != nil {
		t.Fatalf("osd.%d history: %v", osd.ID, err)
	}
	view := localityOSDView{Reads: reads}
	for _, op := range history.Ops {
		if strings.Contains(string(op), object) {
			view.Raw = append(view.Raw, string(op))
		}
	}
	perf, err := osd.AdminSocket(ctx, "perf", "dump", "osd")
	if err != nil {
		t.Fatal(err)
	}
	if view.Counters, err = harness.ParseOSDReadCounters(perf); err != nil {
		t.Fatalf("osd.%d: %v", osd.ID, err)
	}
	return view
}

// newReads returns the reads in after that are not in before, sorted.
func newReads(before, after map[string]string) []string {
	var added []string
	for key := range after {
		if _, seen := before[key]; !seen {
			added = append(added, key)
		}
	}
	slices.Sort(added)
	return added
}

// localityLog is an OSD's whole container log.
func localityLog(t *testing.T, ctx context.Context, osd *ceph.OSDContainer) []byte {
	t.Helper()
	logs, err := osd.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(logs)
	if err := errors.Join(err, logs.Close()); err != nil {
		t.Fatalf("osd.%d log: %v", osd.ID, err)
	}
	return data
}

// localityDebugMS sets the OSD's debug_ms through its admin socket and
// returns the value it had.
func localityDebugMS(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, value string) string {
	t.Helper()
	out, err := osd.AdminSocket(ctx, "config", "get", "debug_ms")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil || got["debug_ms"] == "" {
		t.Fatalf("osd.%d debug_ms: %s %v", osd.ID, out, err)
	}
	if _, err := osd.AdminSocket(ctx, "config", "set", "debug_ms", value); err != nil {
		t.Fatal(err)
	}
	return got["debug_ms"]
}

// localityReadOnce reads the image with a fresh rbd process without cache,
// with policy and the declared CRUSH location, and attributes its data read
// with harness.RBDReadServers. Each OSD logs its messages at debug_ms 1 for
// the read, and the log window after the read gives the replies to the
// client's reads of the object. When an OSD replied without a history entry, or no OSD has
// either, the history is observed again for a bounded time; entries that
// appear then count, and the raw entries that name the object are logged
// from before, after and the later observation, so that a delayed,
// unrecorded or differently described request can be told apart.
func localityReadOnce(t *testing.T, ctx context.Context, client testcontainers.Container, clientAddr netip.Addr, osds []*ceph.OSDContainer, object, policy, location string, n int) localityRead {
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
	before := map[int]localityOSDView{}
	logs := map[int][]byte{}
	for _, osd := range osds {
		previous := localityDebugMS(t, ctx, osd, "1/5")
		restored := false
		restore := func(ctx context.Context) {
			if !restored {
				restored = true
				if _, err := osd.AdminSocket(ctx, "config", "set", "debug_ms", previous); err != nil {
					t.Errorf("osd.%d: restore debug_ms %s: %v", osd.ID, previous, err)
				}
			}
		}
		t.Cleanup(func() { restore(context.Background()) })
		defer restore(ctx)
		logs[osd.ID] = localityLog(t, ctx, osd)
		before[osd.ID] = localityObserve(t, ctx, osd, object)
	}
	path := fmt.Sprintf("/tmp/locality-read-%d", n)
	start := time.Now()
	harness.ExecOutput(t, ctx, client, append(append([]string{"rbd"}, args...), "export", "--no-progress", localityPool+"/"+localityImage, path)...)
	elapsed := time.Since(start)
	sum := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c", "sha256sum "+path+" && rm -f "+path))[0]
	read := localityRead{Policy: policy, Location: location, Effective: strings.Join(effective, " "), Elapsed: elapsed, SHA256: sum, Evidence: map[int]harness.RBDReadEvidence{}}
	after := map[int]localityOSDView{}
	for _, osd := range osds {
		after[osd.ID] = localityObserve(t, ctx, osd, object)
		full := localityLog(t, ctx, osd)
		if !bytes.HasPrefix(full, logs[osd.ID]) {
			t.Fatalf("read %d: osd.%d log of %d bytes does not continue the %d bytes before the read", n, osd.ID, len(full), len(logs[osd.ID]))
		}
		window := full[len(logs[osd.ID]):]
		replies, err := harness.RBDDataReadReplies(window, object, clientAddr)
		if err != nil {
			t.Fatalf("read %d: osd.%d log: %v", n, osd.ID, err)
		}
		for line := range strings.SplitSeq(string(window), "\n") {
			if strings.Contains(line, object) && strings.Contains(line, "osd_op_reply(") {
				t.Logf("read %d: osd.%d log: %s", n, osd.ID, line)
			}
		}
		read.Evidence[osd.ID] = harness.RBDReadEvidence{History: newReads(before[osd.ID].Reads, after[osd.ID].Reads), Replies: replies, Before: before[osd.ID].Counters, After: after[osd.ID].Counters}
	}
	none := true
	for _, e := range read.Evidence {
		none = none && len(e.History) == 0 && len(e.Replies) == 0
	}
	for _, osd := range osds {
		e := read.Evidence[osd.ID]
		if len(e.History) > 0 || (len(e.Replies) == 0 && !none) {
			continue
		}
		late, err := harness.Observe(ctx, 10*time.Second, 5*time.Second, time.Second, func(probeCtx context.Context) (localityOSDView, bool, error) {
			view := localityObserve(t, probeCtx, osd, object)
			return view, len(newReads(before[osd.ID].Reads, view.Reads)) > 0, nil
		})
		e.History = newReads(before[osd.ID].Reads, late.Value.Reads)
		read.Evidence[osd.ID] = e
		t.Logf("read %d: osd.%d had no history entry after the read (replies %+v); history entries naming %s before %q, after %q; after %v more, %q (a read appeared: %v)",
			n, osd.ID, e.Replies, object, before[osd.ID].Raw, after[osd.ID].Raw, late.Elapsed.Round(time.Second), late.Value.Raw, err == nil)
	}
	for _, osd := range osds {
		e := read.Evidence[osd.ID]
		replica, reported := e.ReplicaServed()
		t.Logf("read %d: osd.%d history gained %v; replied %+v; returned %d bytes in %d reads; replica reads served %d (reported %v)",
			n, osd.ID, e.History, e.Replies, e.OutBytes(), e.After.OpR-e.Before.OpR, replica, reported)
	}
	var err error
	if read.Servers, err = harness.RBDReadServers(read.Evidence, localitySize); err != nil {
		t.Fatalf("read %d: %v", n, err)
	}
	return read
}
