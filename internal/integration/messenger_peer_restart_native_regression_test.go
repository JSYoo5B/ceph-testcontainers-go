//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_messenger_peer_restart)))

//ci: timeout=30m job-timeout=45 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// Messenger lines that tell which branch of the v2 accept path handled a
// connection from a peer address that already has a connection. The restart
// branch drops the existing connection, with whatever it still queues, when
// the incoming peer presents a new client cookie and a lower global sequence
// and the existing connection is neither READY nor STANDBY; releases without
// it reject the incoming connection as stale or reset the session instead.
var peerRestartMarkers = []string{"client has clearly restarted", "this is a stale connection", "peer must have reset"}

// TestMessengerPeerRestartAtSameAddress records which branch of the
// ProtocolV2 accept path a daemon restart at the same messenger address
// reaches, and whether the cluster state that rides on the dropped or reset
// connection still converges.
//
// Three MONs and three OSDs start with debug_ms 1 on every daemon, and seeded
// objects are written. Replace restarts a peon MON on its store and address
// (nonce 0), so the surviving MONs' existing connections to that address meet
// a new instance. Detach and Reopen then restart an OSD; its public and
// cluster address nonces before and after decide whether its peers can
// confuse the new instance with the old one. After each restart the quorum
// and every PG must recover, the seeded objects must read back exactly and a
// new write must be acknowledged.
func TestMessengerPeerRestartAtSameAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	config := filepath.Join(t.TempDir(), "messenger.conf")
	if err := os.WriteFile(config, []byte("[mon]\ndebug_ms = 1\n[osd]\ndebug_ms = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts = append(opts, ceph.WithConfigFile(config),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3)),
		ceph.WithMonitorStorage(ceph.MonitorStorage{}.WithVolume()),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume().WithStartupBenchmark(ceph.OSDBenchmarkSkip)))
	cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
	t.Logf("PEER_RESTART ceph=%s", harness.StaticFacts(cluster).CephVersion)
	const pool = "tc-peer-restart"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", PGNum: 16, Replicas: 3, MinSize: 2}); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf(`set -e
for i in $(seq 1 16); do head -c 65536 /dev/urandom > /tmp/seed.$i; rados -p %s put seed.$i /tmp/seed.$i; done
cd /tmp && sha256sum seed.* > /tmp/seed.sums`, pool))
	verify := func(t *testing.T, stage string) {
		t.Helper()
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		out := harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf(`set -e
cd /tmp && for i in $(seq 1 16); do rados -p %s get seed.$i /tmp/read.$i; cmp -s /tmp/read.$i /tmp/seed.$i || { echo "seed.$i differs"; exit 1; }; done
head -c 4096 /dev/urandom > /tmp/after; rados -p %s put %s /tmp/after; rados -p %s get %s /tmp/after.read; cmp /tmp/after /tmp/after.read && echo exact`,
			pool, pool, stage, pool, stage))
		if !strings.HasSuffix(out, "exact") {
			t.Fatalf("%s: %s", stage, out)
		}
		var quorum struct {
			Names []string `json:"quorum_names"`
		}
		if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "quorum_status", "--format", "json"), &quorum); err != nil || len(quorum.Names) != 3 {
			t.Fatalf("%s: quorum %v: %v", stage, quorum.Names, err)
		}
	}
	verify(t, "before")

	mons := clusteraccess.Monitors(t, cluster)
	var status struct {
		Leader string `json:"quorum_leader_name"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "quorum_status", "--format", "json"), &status); err != nil {
		t.Fatal(err)
	}
	var peon *ceph.MonitorContainer
	for _, m := range mons {
		if m.DaemonName != status.Leader && m.DaemonName != "a" {
			peon = m
		}
	}
	if peon == nil {
		t.Fatalf("no peon other than mon.a: leader mon.%s", status.Leader)
	}
	survivors := map[string]int{}
	for _, m := range mons {
		if m != peon {
			survivors["mon."+m.DaemonName] = len(harness.ContainerLogLines(t, ctx, m))
		}
	}
	start := time.Now()
	replaced, err := peon.Replace(ctx)
	if err != nil {
		t.Fatalf("replace mon.%s: %v", peon.DaemonName, err)
	}
	t.Logf("PEER_RESTART mon.%s replaced (leader mon.%s) in %s", replaced.DaemonName, status.Leader, time.Since(start).Round(time.Millisecond))
	for _, m := range mons {
		if m == peon {
			continue
		}
		peerRestartReport(t, ctx, "mon."+m.DaemonName, harness.ContainerLogLines(t, ctx, m)[survivors["mon."+m.DaemonName]:], "mon."+peon.DaemonName)
	}
	verify(t, "after-mon-replace")

	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = noout.Restore(context.WithoutCancel(ctx)) }()
	osds := clusteraccess.OSDs(t, cluster)
	victim := osds[0]
	addrsBefore := osdMessengerAddrs(t, ctx, cluster, victim.ID)
	peers := map[int]int{}
	for _, o := range osds[1:] {
		peers[o.ID] = len(harness.ContainerLogLines(t, ctx, o))
	}
	detached, err := victim.Detach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := detached.Reopen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	addrsAfter := osdMessengerAddrs(t, ctx, cluster, reopened.ID)
	t.Logf("PEER_RESTART osd.%d addrs before=%v after=%v", victim.ID, addrsBefore, addrsAfter)
	for _, o := range osds[1:] {
		peerRestartReport(t, ctx, fmt.Sprintf("osd.%d", o.ID), harness.ContainerLogLines(t, ctx, o)[peers[o.ID]:], fmt.Sprintf("osd.%d", victim.ID))
	}
	for _, before := range addrsBefore {
		if slices.Contains(addrsAfter, before) {
			t.Errorf("osd.%d reopened with an address of its previous instance, %s: its peers' existing connections can meet the new instance", victim.ID, before)
		}
	}
	verify(t, "after-osd-reopen")
}

// peerRestartReport logs how often each accept-path marker appears in lines
// and the first matching line of each.
func peerRestartReport(t *testing.T, ctx context.Context, daemon string, lines []string, restarted string) {
	t.Helper()
	for _, marker := range peerRestartMarkers {
		count, first := 0, ""
		for _, line := range lines {
			if strings.Contains(line, marker) {
				count++
				if first == "" {
					first = line
				}
			}
		}
		t.Logf("PEER_RESTART observer=%s restarted=%s marker=%q count=%d first=%q", daemon, restarted, marker, count, first)
	}
}

// osdMessengerAddrs returns the OSD's public and cluster address vectors from
// the OSD map, each entry as type:addr/nonce.
func osdMessengerAddrs(t *testing.T, ctx context.Context, cluster *ceph.Container, id int) []string {
	t.Helper()
	type vector struct {
		Addrs []struct {
			Type  string `json:"type"`
			Addr  string `json:"addr"`
			Nonce uint64 `json:"nonce"`
		} `json:"addrvec"`
	}
	var dump struct {
		OSDs []struct {
			ID      int    `json:"osd"`
			Public  vector `json:"public_addrs"`
			Cluster vector `json:"cluster_addrs"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "osd", "dump", "--format", "json"), &dump); err != nil {
		t.Fatal(err)
	}
	for _, o := range dump.OSDs {
		if o.ID != id {
			continue
		}
		var out []string
		for _, v := range []vector{o.Public, o.Cluster} {
			for _, a := range v.Addrs {
				out = append(out, fmt.Sprintf("%s:%s/%d", a.Type, a.Addr, a.Nonce))
			}
		}
		if len(out) == 0 {
			t.Fatalf("osd.%d has no messenger addresses", id)
		}
		return out
	}
	t.Fatalf("osd.%d is not in the OSD map", id)
	return nil
}
