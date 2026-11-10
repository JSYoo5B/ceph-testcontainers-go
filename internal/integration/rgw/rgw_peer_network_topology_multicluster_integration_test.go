//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_peer_network))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This cuts only a gateway's HTTP peer bridge. Both storage clusters and local
// S3 endpoints remain running; restoring the endpoint resumes native RGW sync.
func TestMultiClusterRGWPeerNetworkTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	_, rgwGateway, clusters := rgwTopologyClusters(t, ctx, 2, false)
	fixture, err := multisite.Run(ctx, rgwGateway, multisite.WithTopology(multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}},
	}))
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
	createMultisiteBucket(t, ctx, fixture, a, bucket, nil)
	seed := bytes.Repeat([]byte("same native zone and local storage across a peer network cut\n"), 64)
	a.Request(t, ctx, http.MethodPut, bucket+"/seed", seed, http.StatusOK)
	waitMultisiteObject(t, ctx, b, bucket+"/seed", http.StatusOK, seed)
	rgwAssertZonegroupPeriod(t, ctx, fixture, false)
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
	local := before.Container.NetworkSettings.Networks[harness.StaticFacts(clusters[1]).NetworkName]
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
	if peer == nil || peer.IPAddress != cut.OriginalAddress || len(peer.Aliases) == 0 || cut.NetworkName == harness.StaticFacts(clusters[1]).NetworkName {
		t.Fatal("interruption did not identify the original peer-only endpoint")
	}
	isolated, err := docker.ContainerInspect(ctx, gateway.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kept := isolated.Container.NetworkSettings.Networks[harness.StaticFacts(clusters[1]).NetworkName]
	if isolated.Container.NetworkSettings.Networks[cut.NetworkName] != nil || kept == nil || kept.IPAddress != local.IPAddress || !isolated.Container.State.Running || isolated.Container.State.Pid != before.Container.State.Pid {
		t.Fatal("peer cut did not retain the local endpoint and running process")
	}
	if endpoint, err := gateway.S3Endpoint(ctx); err != nil || endpoint != b.Endpoint {
		t.Fatalf("peer cut changed application endpoint: %q %v", endpoint, err)
	}
	if got := b.Request(t, ctx, http.MethodGet, bucket+"/seed", nil, http.StatusOK); !bytes.Equal(got, seed) {
		t.Fatal("isolated zone did not read its retained stored object")
	}
	fromA, fromB := []byte("new source object while HTTP peer link is cut"), []byte("new secondary object while HTTP peer link is cut")
	a.Request(t, ctx, http.MethodPut, bucket+"/from-a", fromA, http.StatusOK)
	b.Request(t, ctx, http.MethodPut, bucket+"/from-b", fromB, http.StatusOK)
	if got := b.Request(t, ctx, http.MethodGet, bucket+"/from-b", nil, http.StatusOK); !bytes.Equal(got, fromB) {
		t.Fatal("isolated zone did not accept a local object write")
	}
	// Require absence throughout an interval, rather than interpreting one
	// immediate GET as evidence that the native synchronization path is cut.
	until := time.Now().Add(12 * time.Second)
	for {
		b.Request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusNotFound)
		a.Request(t, ctx, http.MethodGet, bucket+"/from-b", nil, http.StatusNotFound)
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
	rgwAssertZonegroupPeriod(t, ctx, fixture, false)
	t.Log("RGW peer HTTP bridge removed/restored with original IP/aliases and unchanged process/period: local reads+writes continued, cross-zone updates stayed absent during isolation, bidirectional native catch-up passed")
}

// One direction of the link between two zones' gateways is cut below RGW:
// zone b's packets to zone a's gateway port on the multisite bridge, with
// the netem helper. b's own sync reads of a stall, while a keeps reading
// from b; the zones' local endpoints and both clusters keep serving. After
// Restore, b's gateway namespace has no shaping left and connects to a's
// endpoint, and b catches up with a's object byte for byte, with the same
// gateway processes and period. While b catches up, every change in its
// answer for the object, its sync status, the data log shards on which its
// markers differ from a's data log, its bucket sync status and its sync
// errors is logged, so that a catch-up that does not finish shows where it
// stopped, and which shards lost their sync leases during the cut (issue
// 659); a lost lease is reported, not accepted. It needs the netem helper
// (CEPH_TEST_NETEM_IMAGE, by default fault.NetemHelperImage) and an engine
// that grants it NET_ADMIN.
func TestMultiClusterRGWZoneLinkPathCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	_, rgwGateway, clusters := rgwTopologyClusters(t, ctx, 2, false)
	fixture, err := multisite.Run(ctx, rgwGateway, multisite.WithTopology(multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: clusters[0]}, {Name: "b", Cluster: clusters[1]}},
	}))
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	zones := fixture.Zones()
	if len(zones) != 2 {
		t.Fatal("two-zone fixture was not constructed")
	}
	a, b := rgwTopologyS3(t, ctx, zones[0]), rgwTopologyS3(t, ctx, zones[1])
	const bucket = "/tc-rgw-zone-link-path"
	createMultisiteBucket(t, ctx, fixture, a, bucket, nil)
	seed := bytes.Repeat([]byte("synced in both directions before one direction is cut\n"), 64)
	a.Request(t, ctx, http.MethodPut, bucket+"/seed", seed, http.StatusOK)
	waitMultisiteObject(t, ctx, b, bucket+"/seed", http.StatusOK, seed)
	beforePeriod, err := fixture.ZoneAdmin(ctx, "a", "period", "get", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	pids := map[string]int{}
	for _, zone := range zones {
		state, err := zone.Gateway.State(ctx)
		if err != nil || !state.Running {
			t.Fatalf("gateway of zone %s: %+v, %v", zone.Name, state, err)
		}
		pids[zone.Name] = state.Pid
	}

	// zone b's gateway logs its sync at level 20, read when it does not
	// catch up.
	debug, err := zoneLinkGatewayExec(ctx, zones[1].Gateway, `for s in /var/run/ceph/*.asok; do ceph --admin-daemon "$s" config set debug_rgw_sync 20/20; done`)
	t.Logf("zone b's gateway debug_rgw_sync 20: %s, %v", debug, err)
	// A probe in zone b's gateway namespace reads its qdiscs and connects
	// to a's endpoint after Restore.
	probe := harness.NewZoneProbe(t, ctx, "zone b gateway", zones[1].Gateway.GetContainerID())
	lease, err := fixture.ApplyZoneLinkProfile(ctx, "b", "a", fault.NetworkProfile{HelperImage: netem, Loss: 100})
	if lease != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := lease.Restore(cleanupCtx); err != nil {
				t.Errorf("restore the zone link delay: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied || status.Loss != 100 || len(lease.Peers) != 1 || lease.Peers[0].Port != 7480 ||
		lease.ContainerID != zones[1].Gateway.GetContainerID() || lease.NetworkName == harness.StaticFacts(clusters[1]).NetworkName {
		t.Fatalf("zone link delay %+v: status %+v, %v", lease, status, err)
	}
	fromA, fromB := []byte("written in zone a while b cannot read from a"), []byte("written in zone b while a still reads from b")
	a.Request(t, ctx, http.MethodPut, bucket+"/from-a", fromA, http.StatusOK)
	b.Request(t, ctx, http.MethodPut, bucket+"/from-b", fromB, http.StatusOK)
	if got := b.Request(t, ctx, http.MethodGet, bucket+"/seed", nil, http.StatusOK); !bytes.Equal(got, seed) {
		t.Fatal("zone b did not read its retained object during the cut")
	}
	// The unselected direction: a reads b's new object from b's gateway,
	// while b, every second, still lacks a's object. The absence is timed
	// from b's first 404 after a's PUT to its last one before Restore, and
	// lasts at least 12 seconds while the other checks run; the cut ends as
	// soon as both directions are shown and that time is reached.
	missing := 0
	var firstAbsent, lastAbsent time.Time
	lacks := func() {
		t.Helper()
		b.Request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusNotFound)
		lastAbsent = time.Now()
		if missing == 0 {
			firstAbsent = lastAbsent
		}
		missing++
	}
	for {
		lacks()
		status, body, err := zoneLinkGet(ctx, a, bucket+"/from-b")
		if err == nil && status == http.StatusOK && bytes.Equal(body, fromB) {
			break
		}
		if time.Since(cutAt) > 3*time.Minute {
			t.Fatalf("zone a did not read b's object during the cut: HTTP %d, %d bytes, %v", status, len(body), err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	readBack := time.Since(cutAt).Round(time.Millisecond)
	// The selected direction: b's native sync status, read by its CLI
	// client, which is not cut, compares a's log with b's markers and finds
	// shards behind, while a has caught up with b; and b still does not
	// get the object.
	behind, err := harness.Observe(ctx, 3*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) (zoneLinkSync, bool, error) {
		var seen zoneLinkSync
		var err error
		lacks()
		if seen.b, err = fixture.SyncStatus(probeCtx, "b"); err != nil {
			return seen, false, err
		}
		if seen.a, err = fixture.SyncStatus(probeCtx, "a"); err != nil {
			return seen, false, err
		}
		fromA, fromB := dataSource(seen.b, "a"), dataSource(seen.a, "b")
		return seen, fromA.BehindShards > 0 && !fromA.CaughtUp && fromB.CaughtUp, nil
	})
	if err != nil {
		t.Fatalf("sync status during the cut: b %+v, a %+v: %v", behind.Value.b.DataSources, behind.Value.a.DataSources, err)
	}
	for time.Since(firstAbsent) < 12*time.Second {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
		lacks()
	}
	// The last 404, right before Restore.
	lacks()
	absent := lastAbsent.Sub(firstAbsent).Round(time.Millisecond)
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	window := time.Since(cutAt).Round(time.Millisecond)
	restored := time.Now()
	qdiscs, err := probe.Exec(ctx, "tc", "qdisc", "show", "dev", lease.Interface)
	if err != nil {
		t.Fatal(err)
	}
	filters, err := probe.Exec(ctx, "tc", "filter", "show", "dev", lease.Interface)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(qdiscs, "netem") || filters != "" {
		t.Fatalf("Restore left shaping on %s: qdiscs %q, filters %q", lease.Interface, qdiscs, filters)
	}
	peer := netip.AddrPortFrom(lease.Peers[0].Addr, lease.Peers[0].Port)
	reach, err := probe.ConnectRTT(ctx, peer, 5, 2*time.Second)
	if err != nil || reach.OK != reach.Requested {
		t.Fatalf("zone b's gateway cannot reach a's endpoint %s after Restore: %s, %v", peer, reach, err)
	}
	t.Logf("after Restore: %s qdiscs %q, no filters; zone b's gateway connects to %s: %s", lease.Interface, qdiscs, peer, reach)
	gatewayState := func(shards []int) string {
		return zoneLinkGatewayState(ctx, probe, zones[1].Gateway, peer, cutAt, strings.TrimPrefix(bucket, "/"), shards)
	}
	if last, ok := zoneLinkCatchUp(t, ctx, fixture, b, strings.TrimPrefix(bucket, "/"), "from-a", fromA, 3*time.Minute, gatewayState); !ok {
		// While 100 of zone b's reads of a's data log wait for an answer,
		// its gateway renews no sync lease. After more than the 30-second
		// lease period a data log shard that finds its lease lost in the
		// middle of a batch moves its marker past entries it did not sync
		// (experiment case rgw_sync_lease_lapse). A lost lease is reported
		// to help attribute the failure; it is not accepted as a cause.
		t.Fatalf("zone b did not catch up with a within 3m0s of Restore: object %s; %s; data log shards behind a: %s; shards that lost their sync lease since the cut: %v",
			last.Object, last.Status, last.Lag, zoneLinkLostLeases(ctx, zones[1].Gateway, cutAt))
	}
	caughtUp := time.Since(restored).Round(time.Millisecond)
	t.Logf("zone b's gateway sync leases since the cut: %s", zoneLinkLeaseLog(ctx, zones[1].Gateway, cutAt))
	synced, err := fixture.WaitSyncReady(ctx, "b", "a")
	if err != nil {
		t.Fatalf("zone b's sync status after Restore: %+v: %v", synced.DataSources, err)
	}
	for _, zone := range fixture.Zones() {
		state, err := zone.Gateway.State(ctx)
		if err != nil || !state.Running || state.Pid != pids[zone.Name] {
			t.Fatalf("gateway of zone %s after the cut: %+v, %v (pid before %d)", zone.Name, state, err, pids[zone.Name])
		}
		after, err := fixture.ZoneAdmin(ctx, zone.Name, "period", "get", "--format", "json")
		if err != nil || !bytes.Equal(after, beforePeriod) {
			t.Fatalf("the zone link cut changed the committed period in %s: %v", zone.Name, err)
		}
	}
	rgwAssertZonegroupPeriod(t, ctx, fixture, false)
	if got := a.Request(t, ctx, http.MethodGet, bucket+"/from-a", nil, http.StatusOK); !bytes.Equal(got, fromA) {
		t.Fatal("zone a lost its own object")
	}
	t.Logf("RGW_ZONE_LINK_PATH_RECEIPT zone b's gateway %.12s on %s dropped %d packets to %v for %s through filters %s: zone a read b's %d bytes within %s of the cut and its data sync from b was caught up, while b's data sync from a had %d of %d shards behind and b lacked a's object in %d reads over %s from its first 404 to its last before Restore; after Restore b read a's %d bytes after %s, its data sync from a was caught up (%+v), and both gateways kept their processes and the period",
		lease.ContainerID, lease.NetworkName, cut.Drops, lease.Peers, window, cut.Filters, len(fromB), readBack,
		dataSource(behind.Value.b, "a").BehindShards, dataSource(behind.Value.b, "a").Shards, missing, absent, len(fromA), caughtUp, dataSource(synced, "a"))
}

// zoneLinkSync is one look at both zones' sync status.
type zoneLinkSync struct {
	a, b multisite.SyncStatus
}

// dataSource returns status's data sync from the zone named source.
func dataSource(status multisite.SyncStatus, source string) multisite.DataSyncStatus {
	for _, data := range status.DataSources {
		if data.SourceZone == source {
			return data
		}
	}
	return multisite.DataSyncStatus{}
}

// zoneLinkObservation is one look at zone b catching up with zone a after
// the cut: b's answer for the object, b's sync status, the data log shards on
// which b's sync markers for a differ from a's data log, b's sync of the
// bucket and b's sync errors. Native text is kept as the CLI printed it.
type zoneLinkObservation struct {
	Object, Status, Lag, Bucket, Errors string
	// Shards are the data log shards in Lag.
	Shards string
	// Done means that b served the object and its data sync from a and its
	// metadata sync were caught up.
	Done bool
}

// zoneLinkObserve reads one zoneLinkObservation. A failed read is recorded
// in the observation rather than ending the test.
func zoneLinkObserve(ctx context.Context, fixture *multisite.Fixture, b harness.S3HTTPClient, bucket, key string, want []byte) zoneLinkObservation {
	var seen zoneLinkObservation
	code, body, err := zoneLinkGet(ctx, b, "/"+bucket+"/"+key)
	objectOK := err == nil && code == http.StatusOK && bytes.Equal(body, want)
	seen.Object = fmt.Sprintf("HTTP %d, %d bytes, payload matches %v, error %v", code, len(body), objectOK, err)
	status, err := fixture.SyncStatus(ctx, "b")
	fromA := dataSource(status, "a")
	seen.Status = fmt.Sprintf("metadata caught up %v, data from a %+v, error %v", status.Metadata.CaughtUp, fromA, err)
	native := func(zone string, args ...string) string {
		out, err := fixture.ZoneAdmin(ctx, zone, args...)
		if err != nil {
			return fmt.Sprintf("%v failed: %v", args, err)
		}
		return strings.TrimSpace(string(out))
	}
	datalog, err := fixture.ZoneAdmin(ctx, "a", "datalog", "status")
	if err == nil {
		var markers []byte
		if markers, err = fixture.ZoneAdmin(ctx, "b", "data", "sync", "status", "--source-zone=a"); err == nil {
			var lag []harness.RGWShardLag
			if lag, err = harness.RGWDataSyncLag(datalog, markers); err == nil {
				seen.Lag = fmt.Sprintf("%+v", lag)
				for _, shard := range lag {
					seen.Shards += fmt.Sprintf("%d ", shard.Shard)
				}
			}
		}
	}
	if err != nil {
		seen.Lag = "error: " + err.Error()
	}
	// The bucket status names the time it was read; leave it out so that
	// only a change of state is a new observation.
	var kept []string
	for line := range strings.SplitSeq(native("b", "bucket", "sync", "status", "--bucket="+bucket), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "current time ") {
			kept = append(kept, strings.TrimSpace(line))
		}
	}
	seen.Bucket = strings.Join(kept, "\n")
	seen.Errors = zoneLinkSyncErrors(native("b", "sync", "error", "list"))
	seen.Done = objectOK && err == nil && fromA.CaughtUp && status.Metadata.CaughtUp
	return seen
}

// zoneLinkGet reads path from client without failing the test.
func zoneLinkGet(ctx context.Context, client harness.S3HTTPClient, path string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.Endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	client.Sign(request, nil, time.Now().UTC())
	response, err := client.HTTP.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

// zoneLinkSyncErrors reduces `radosgw-admin sync error list`, one entry list
// per error log shard, to the shards that have entries; other output is kept
// as it is.
func zoneLinkSyncErrors(out string) string {
	var shards []struct {
		Shard   int               `json:"shard_id"`
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal([]byte(out), &shards); err != nil {
		return out
	}
	var found []string
	for _, shard := range shards {
		if len(shard.Entries) > 0 {
			found = append(found, fmt.Sprintf("shard %d: %s", shard.Shard, shard.Entries))
		}
	}
	if len(found) == 0 {
		return fmt.Sprintf("none in %d shards", len(shards))
	}
	return strings.Join(found, "; ")
}

// zoneLinkGatewayState reads, from zone b's gateway, its TCP connections to
// a's endpoint, as the probe in its namespace sees them, and its sync trace
// and coroutine dump from its admin sockets, each bounded.
func zoneLinkGatewayState(ctx context.Context, probe *harness.ZoneProbe, gateway testcontainers.Container, peer netip.AddrPort, since time.Time, bucket string, shards []int) string {
	sockets, err := probe.Exec(ctx, "ss", "-tnoi", "dst", peer.String())
	if err != nil {
		sockets = "ss failed: " + err.Error()
	}
	script := `for s in /var/run/ceph/*.asok; do echo "== $s sync trace show (data)"; ceph --admin-daemon "$s" sync trace show 2>&1 | grep '"status": "data' | head -c 40000; echo; echo "== $s cr dump (data and bucket sync stacks)"; ceph --admin-daemon "$s" cr dump 2>&1 | python3 -c "$1" | head -c 60000; echo; done`
	// The coroutine dump holds every sync stack; keep the stacks with a data
	// or bucket sync coroutine, one line per coroutine with its latest
	// status and history entry.
	summary := `import json, sys
try:
    dump = json.load(sys.stdin)
except Exception as e:
    print("cr dump is not JSON:", e); sys.exit()
for manager in dump.get("coroutine_managers", []):
    for context in manager.get("run_contexts", []):
        for stack in context.get("entries", []):
            ops = stack.get("ops", [])
            if not any("Data" in op.get("type", "") or "Bucket" in op.get("type", "") for op in ops):
                continue
            print("stack", stack.get("stack"), "run_count", stack.get("run_count"))
            for op in ops:
                history = op.get("history") or [{}]
                print("  ", op.get("type"), json.dumps(op.get("status")), json.dumps(history[-1]), "spawned", len(op.get("spawned", [])))`
	daemon, err := zoneLinkGatewayExec(ctx, gateway, script, summary)
	if err != nil {
		daemon += " (" + err.Error() + ")"
	}
	return fmt.Sprintf("TCP to %s:\n%s\n%s\n%s\nsync leases since the cut: %s", peer, sockets, daemon, zoneLinkSyncLog(ctx, gateway, since, bucket, shards), zoneLinkLeaseLog(ctx, gateway, since))
}

// zoneLinkGatewayExec runs a shell script in the gateway container with
// args and returns its exit status and output.
func zoneLinkGatewayExec(ctx context.Context, gateway testcontainers.Container, script string, args ...string) (string, error) {
	code, reader, err := gateway.Exec(ctx, append([]string{"sh", "-c", script, "sh"}, args...), tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	out, err := io.ReadAll(reader)
	return fmt.Sprintf("exit %d:\n%s", code, strings.TrimSpace(string(out))), err
}

// A daemon log line is "<time> <thread> <level> <text>".
var zoneLinkLogThread = regexp.MustCompile(`([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+[+-][0-9]{4}) ([0-9a-f]+) +-?[0-9]+ `)

var zoneLinkLogStamp = regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}`)

// zoneLinkSyncLog returns the gateway's log lines from since on that name
// one of the data log shards or the bucket, report an ERROR or a WARNING, or
// are about a sync lease or lock: the first and the last 200 of them.
func zoneLinkSyncLog(ctx context.Context, gateway testcontainers.Container, since time.Time, bucket string, shards []int) string {
	reader, err := gateway.Logs(ctx)
	if err != nil {
		return "gateway log: " + err.Error()
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return "gateway log: " + err.Error()
	}
	stamp := since.UTC().Add(-time.Second).Format("2006-01-02T15:04:05")
	var kept []string
	for line := range strings.SplitSeq(string(data), "\n") {
		// A log frame may carry a stream header before the timestamp.
		if at := zoneLinkLogStamp.FindString(line); at == "" || at < stamp || strings.Contains(line, " beast: ") {
			continue
		}
		match := strings.Contains(line, "ERROR") || strings.Contains(line, "WARNING") || strings.Contains(line, "lease") || strings.Contains(line, "lock ") || strings.Contains(line, bucket)
		for _, shard := range shards {
			match = match || strings.Contains(line, fmt.Sprintf("shard[%d]", shard)) || strings.Contains(line, fmt.Sprintf("shard_id=%d ", shard))
		}
		if match {
			kept = append(kept, line)
		}
	}
	total := len(kept)
	if total > 400 {
		kept = append(kept[:200:200], append([]string{fmt.Sprintf("... %d lines left out ...", total-400)}, kept[total-200:]...)...)
	}
	return fmt.Sprintf("gateway log lines since %s naming bucket %s, shards %v, an ERROR, a WARNING or a lease (%d):\n%s", stamp, bucket, shards, total, strings.Join(kept, "\n"))
}

// zoneLinkLeaseLog returns the gateway's log lines from since on in which a
// sync shard found its lease lost or a lease renewal came late, at most 20.
// When a shard found its lease lost, it also returns the lines of that
// shard's thread around the longest pause of the thread, which shows what
// the thread did before it stopped renewing the leases.
func zoneLinkLeaseLog(ctx context.Context, gateway testcontainers.Container, since time.Time) string {
	reader, err := gateway.Logs(ctx)
	if err != nil {
		return "gateway log: " + err.Error()
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return "gateway log: " + err.Error()
	}
	stamp := since.UTC().Add(-time.Second).Format("2006-01-02T15:04:05")
	var lines, kept []string
	thread := ""
	for line := range strings.SplitSeq(string(data), "\n") {
		if at := zoneLinkLogStamp.FindString(line); at == "" || at < stamp {
			continue
		}
		lines = append(lines, line)
		if strings.Contains(line, "lease is lost") || strings.Contains(line, "did not renew lock") {
			kept = append(kept, line)
			if m := zoneLinkLogThread.FindStringSubmatch(line); m != nil && thread == "" && strings.Contains(line, "lease is lost") {
				thread = m[2]
			}
		}
	}
	total := len(kept)
	out := fmt.Sprintf("%d lines\n%s", total, strings.Join(kept[:min(total, 20)], "\n"))
	if thread == "" {
		return out
	}
	var own []string
	var times []time.Time
	for _, line := range lines {
		m := zoneLinkLogThread.FindStringSubmatch(line)
		if m == nil || m[2] != thread {
			continue
		}
		at, err := time.Parse("2006-01-02T15:04:05.000-0700", m[1])
		if err != nil {
			continue
		}
		own, times = append(own, line), append(times, at)
	}
	gap := 0
	for i := 1; i < len(times); i++ {
		if times[i].Sub(times[i-1]) > times[gap].Sub(times[max(gap-1, 0)]) {
			gap = i
		}
	}
	if gap == 0 {
		return out
	}
	return fmt.Sprintf("%s\nthread %s paused %v; its lines before and after:\n%s", out, thread, times[gap].Sub(times[gap-1]),
		strings.Join(own[max(gap-30, 0):min(gap+5, len(own))], "\n"))
}

// zoneLinkLostLeases returns the data log shards whose sync found its lease
// lost in the gateway's log from since on.
func zoneLinkLostLeases(ctx context.Context, gateway testcontainers.Container, since time.Time) map[int]bool {
	lost := map[int]bool{}
	reader, err := gateway.Logs(ctx)
	if err != nil {
		return lost
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return lost
	}
	stamp := since.UTC().Add(-time.Second).Format("2006-01-02T15:04:05")
	for line := range strings.SplitSeq(string(data), "\n") {
		if at := zoneLinkLogStamp.FindString(line); at == "" || at < stamp {
			continue
		}
		if m := zoneLinkLostLease.FindStringSubmatch(line); m != nil {
			var shard int
			if _, err := fmt.Sscan(m[1], &shard); err == nil {
				lost[shard] = true
			}
		}
	}
	return lost
}

var zoneLinkLostLease = regexp.MustCompile(`RGW-SYNC:data:sync:shard\[([0-9]+)\]: lease is lost, abort`)

// zoneLinkCatchUp observes zone b every five seconds until it serves the
// object written in zone a during the cut and its sync from a is caught up,
// logging every observation that differs from the one before, and fails
// and returns the last observation and false after limit. gateway reads the
// gateway's own state for the lagging data log shards, which is logged with
// the first observation and when the limit passes.
func zoneLinkCatchUp(t *testing.T, ctx context.Context, fixture *multisite.Fixture, b harness.S3HTTPClient, bucket, key string, want []byte, limit time.Duration, gateway func(shards []int) string) (zoneLinkObservation, bool) {
	t.Helper()
	started := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var last zoneLinkObservation
	for n := 0; ; n++ {
		probeCtx, probeCancel := context.WithTimeout(waitCtx, time.Minute)
		seen := zoneLinkObserve(probeCtx, fixture, b, bucket, key, want)
		probeCancel()
		elapsed := time.Since(started).Round(time.Second)
		if n == 0 && !seen.Done {
			t.Logf("zone b's gateway %s after Restore: %s", elapsed, gateway(zoneLinkShards(seen)))
		}
		if n == 0 || seen != last {
			t.Logf("zone b %s after Restore: object %s; %s; data log shards behind a: %s; bucket sync status:\n%s\nsync errors: %s", elapsed, seen.Object, seen.Status, seen.Lag, seen.Bucket, seen.Errors)
		}
		last = seen
		if seen.Done {
			return seen, true
		}
		select {
		case <-waitCtx.Done():
			t.Logf("zone b's gateway %v after Restore: %s", time.Since(started).Round(time.Second), gateway(zoneLinkShards(last)))
			return last, false
		case <-time.After(5 * time.Second):
		}
	}
}

// zoneLinkShards returns the data log shards of an observation's lag.
func zoneLinkShards(seen zoneLinkObservation) []int {
	var shards []int
	for field := range strings.FieldsSeq(seen.Shards) {
		var shard int
		if _, err := fmt.Sscan(field, &shard); err == nil {
			shards = append(shards, shard)
		}
	}
	return shards
}
