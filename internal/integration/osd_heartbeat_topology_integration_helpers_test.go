//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

func readOSDMap(t *testing.T, ctx context.Context, cluster *ceph.Container) osdMap {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal("osd dump:", err)
	}
	dump, err := parseOSDMap(out)
	if err != nil {
		t.Fatalf("osd dump: %v: %.512s", err, out)
	}
	return dump
}

func monConfigValue(t *testing.T, ctx context.Context, cluster *ceph.Container, who, name string) string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "config", "get", who, name)
	if err != nil {
		t.Fatalf("config get %s %s: %v", who, name, err)
	}
	return strings.TrimSpace(string(out))
}

// osdHeartbeatReceipt is what TestOSDHeartbeatFailure logs.
type osdHeartbeatReceipt struct {
	Target        int         `json:"target"`
	Peers         []int       `json:"peers"`
	Grace         string      `json:"osd_heartbeat_grace"`
	MinReporters  string      `json:"mon_osd_min_down_reporters"`
	SubtreeLevel  string      `json:"mon_osd_reporter_subtree_level"`
	Before        osdMapState `json:"before"`
	Down          osdMapState `json:"down"`
	DownEpoch     int         `json:"down_epoch"`
	MarkedAfterS  float64     `json:"marked_after_s"`
	Failure       osdFailure  `json:"failure"`
	DeadEpoch     int         `json:"dead_epoch"`
	ExitCode      int         `json:"exit_code"`
	RebindFailure string      `json:"rebind_failure"`
	After         osdMapState `json:"after"`
}

// testOSDHeartbeatFailure cuts one OSD's cluster network in a cluster of
// three OSDs on separate hosts. Its heartbeats to the other two fail on the
// back interface, so both report it, and after osd_heartbeat_grace the MON
// marks it down with two reporters from different hosts; the other OSDs stay
// up from the same epoch, and a client keeps reading and writing. The marked
// OSD sees the new map, rebinds its cluster messenger to restart its boot,
// and cannot, because its cluster address is gone: it logs "rebind
// cluster_messenger failed", tells the MONs it is dead as of that epoch or a
// later one, and exits. After the endpoint is restored, the same OSD
// starts again in its container and comes up with the same UUID from a later
// epoch, and the cluster is clean.
func testOSDHeartbeatFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithSeparateClusterNetwork(), ceph.WithOSDCount(3))
	// Three replicas with min_size 2 keep the pool writable on the two OSDs
	// that stay up.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-topology", PGNum: 8, Replicas: 3, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "seed")
	osds := cluster.OSDs()
	if len(osds) != 3 {
		t.Fatalf("want three OSDs, have %d", len(osds))
	}
	target := osds[2]
	receipt := osdHeartbeatReceipt{Target: target.ID, Peers: []int{osds[0].ID, osds[1].ID},
		Grace:        monConfigValue(t, ctx, cluster, "osd", "osd_heartbeat_grace"),
		MinReporters: monConfigValue(t, ctx, cluster, "mon", "mon_osd_min_down_reporters"),
		SubtreeLevel: monConfigValue(t, ctx, cluster, "mon", "mon_osd_reporter_subtree_level")}
	before := readOSDMap(t, ctx, cluster)
	for _, osd := range osds {
		state, ok := before.osd(osd.ID)
		if !ok || state.Up != 1 {
			t.Fatalf("osd.%d is not up before the cut: %+v", osd.ID, before.OSDs)
		}
	}
	receipt.Before, _ = before.osd(target.ID)

	cut, err := fault.InterruptNetwork(ctx, cluster, target, ceph.ClusterNetworkPlane)
	if cut != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := cut.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	var down osdMap
	for {
		down = readOSDMap(t, ctx, cluster)
		if state, _ := down.osd(target.ID); state.Up == 0 {
			break
		}
		if time.Since(cutAt) > 3*time.Minute {
			t.Fatalf("osd.%d was not marked down: %+v", target.ID, down.OSDs)
		}
		time.Sleep(time.Second)
	}
	receipt.MarkedAfterS = time.Since(cutAt).Round(time.Second).Seconds()
	receipt.Down, _ = down.osd(target.ID)
	receipt.DownEpoch = receipt.Down.DownAt
	for _, peer := range receipt.Peers {
		was, _ := before.osd(peer)
		now, _ := down.osd(peer)
		if now.Up != 1 || now.UpFrom != was.UpFrom {
			t.Fatalf("peer osd.%d changed while osd.%d was cut: %+v, before %+v", peer, target.ID, now, was)
		}
	}
	if receipt.Down.UUID != receipt.Before.UUID || receipt.DownEpoch <= receipt.Before.UpFrom {
		t.Fatalf("osd.%d down %+v, before %+v", target.ID, receipt.Down, receipt.Before)
	}
	topologyRados(t, ctx, client, "osd-marked-down")

	// The marked OSD exits on its own; nothing in the test stops it.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		state, err := target.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			receipt.ExitCode = state.ExitCode
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("osd.%d still runs after it was marked down", target.ID)
		}
		time.Sleep(time.Second)
	}
	reader, err := target.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logs, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(logs), "\n") {
		if strings.Contains(line, "rebind cluster_messenger failed") {
			receipt.RebindFailure = strings.TrimSpace(line)
		}
	}
	if receipt.RebindFailure == "" {
		t.Fatalf("osd.%d exited without logging the failed rebind: %.4096s", target.ID, logs)
	}
	clusterLog, err := cluster.Ceph(ctx, "log", "last", "300", "info", "cluster")
	if err != nil {
		t.Fatal("log last:", err)
	}
	failure, ok := parseOSDFailure(string(clusterLog), target.ID)
	grace, _ := strconv.ParseFloat(receipt.Grace, 64)
	minReporters, _ := strconv.Atoi(receipt.MinReporters)
	if !ok || failure.Reporters < minReporters || failure.Subtree != receipt.SubtreeLevel || failure.After < grace || failure.Grace != grace {
		t.Fatalf("no failure record of osd.%d with %d reporters from different %s after the %v s grace: %+v in %.4096s",
			target.ID, minReporters, receipt.SubtreeLevel, grace, failure, clusterLog)
	}
	receipt.Failure = failure
	dead, stillRunning, ok := parseOSDMarkedDead(string(clusterLog), target.ID)
	// The OSD names the epoch of the map it is handling, which can already
	// be later than the one that marked it down.
	if !ok || !stillRunning || dead < receipt.DownEpoch {
		t.Fatalf("osd.%d dead as of e%d (found=%t, still running=%t), marked down at e%d: %.4096s", target.ID, dead, ok, stillRunning, receipt.DownEpoch, clusterLog)
	}
	receipt.DeadEpoch = dead

	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := target.Start(ctx); err != nil {
		t.Fatal("start osd:", err)
	}
	for {
		after := readOSDMap(t, ctx, cluster)
		if state, _ := after.osd(target.ID); state.Up == 1 {
			receipt.After = state
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("osd.%d did not come back: %+v", target.ID, after.OSDs)
		}
		time.Sleep(time.Second)
	}
	if receipt.After.UUID != receipt.Before.UUID || receipt.After.UpFrom <= receipt.DownEpoch {
		t.Fatalf("osd.%d came back as %+v, down at e%d, before %+v", target.ID, receipt.After, receipt.DownEpoch, receipt.Before)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "osd-restarted")
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_HEARTBEAT_RECEIPT %s", encoded)
}
