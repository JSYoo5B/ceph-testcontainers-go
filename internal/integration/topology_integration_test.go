//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMonitorManagerTopology(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2)), ceph.WithStartupTimeout(3*time.Minute))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, err := ceph.Run(ctx, image, opts...)
			if cluster != nil {
				testcontainers.CleanupContainer(t, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(clusteraccess.Monitors(t, cluster)) != 3 || len(clusteraccess.Managers(t, cluster)) != 2 || clusteraccess.ControlContainer(t, cluster).GetContainerID() == cluster.GetContainerID() {
				t.Fatal("HA fixture lacks separate control or daemon counts")
			}
			cc, err := cluster.ConnectionConfig(ctx)
			config := cc.Conf
			if err != nil || strings.Count(string(config), "[v2:") != 3 {
				t.Fatalf("fresh client lacks three monitor addrvecs: %v: %s", err, config)
			}
			harness.CephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-topology", "8")
			harness.CephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-topology", "rados")
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(),
				ceph.WithIdleEntrypoint(),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
			if client != nil {
				testcontainers.CleanupContainer(t, client)
			}
			if err != nil {
				t.Fatal(err)
			}
			session := harness.StartTopologyRadosSession(t, ctx, client)
			harness.TopologyFreshConnection(t, ctx, client, false)
			monitors := clusteraccess.Monitors(t, cluster)
			primary := monitors[0]
			stop := 3 * time.Second
			if err := primary.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			harness.TopologyWait(t, ctx, func() bool {
				q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 2
			})
			if err := cluster.WaitForQuorum(ctx); err != nil {
				t.Fatal(err)
			}
			harness.TopologyRados(t, ctx, client, "verify")
			session.Check(t, ctx, client, "one-monitor-down")
			t.Log("MON a stopped: CLI control, the retained native librados session, and fresh authenticated I/O survived on quorum b,c")
			if err := primary.Start(ctx); err != nil {
				t.Fatal(err)
			}
			harness.TopologyWait(t, ctx, func() bool {
				q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			session.Check(t, ctx, client, "one-monitor-restarted")
			// Two stopped members remove the majority. Inspect the surviving
			// daemon's local admin socket: a quorum-dependent CLI would block.
			for _, mon := range monitors[:2] {
				if err := mon.Stop(ctx, &stop); err != nil {
					t.Fatal(err)
				}
			}
			harness.TopologyWait(t, ctx, func() bool {
				data := harness.TopologyExecOutput(t, ctx, monitors[2], "ceph", "--admin-daemon", "/var/run/ceph/ceph-mon."+monitors[2].DaemonName+".asok", "mon_status")
				var status struct {
					State  string `json:"state"`
					Quorum []int  `json:"quorum"`
				}
				if err := json.Unmarshal(data, &status); err != nil || status.Quorum == nil {
					t.Fatalf("decode surviving MON's local status: %v: %s", err, data)
				}
				return len(status.Quorum) == 0 && status.State != "leader" && status.State != "peon"
			})
			harness.TopologyFreshConnection(t, ctx, client, true)
			t.Log("Two of three MONs stopped: the surviving daemon has no quorum and a fresh native session cannot authenticate within the bounded probe; retained sessions are left idle")
			for _, mon := range monitors[:2] {
				if err := mon.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			harness.TopologyWait(t, ctx, func() bool {
				q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			harness.TopologyFreshConnection(t, ctx, client, false)
			session.Check(t, ctx, client, "quorum-recovered")
			harness.TopologyQuorumStall(t, ctx, cluster, clusteraccess.Monitors(t, cluster))
			if _, err := cluster.AddMonitor(ctx, "replacement"); err != nil {
				t.Fatal(err)
			}
			if err := harness.MonitorHandle(t, ctx, cluster, "a").Remove(ctx); err != nil {
				t.Fatal(err)
			}
			if len(clusteraccess.Monitors(t, cluster)) != 3 {
				t.Fatal("replacement did not preserve three owned monitors")
			}
			harness.TopologyRados(t, ctx, client, "verify")
			session.Check(t, ctx, client, "monitor-replaced")
			t.Log("MON replacement: 3 -> 4 -> 3, primary a removed, both retained and fresh client sessions retained data")
			harness.CephCommand(t, ctx, cluster, "mgr", "module", "enable", "volumes")
			fs, err := cephfs.Start(ctx, cluster, cephfs.Config{})
			if err != nil {
				t.Fatal(err)
			}
			// The MGR learns a new filesystem from its own FSMap subscription,
			// which can trail the MON's commit; after the MON replacement it
			// did, and a raw volumes command right after Start returned
			// ENOENT. CreateSubvolumeGroup first waits until the active MGR
			// lists the filesystem, so each create runs against a MGR that
			// knows it, before and after the failover.
			if _, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{Name: "before-failover"}); err != nil {
				t.Fatal(err)
			}
			mgrStatus, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			previous := mgrStatus.ActiveName
			for _, mgr := range clusteraccess.Managers(t, cluster) {
				if mgr.DaemonName == previous {
					if err := mgr.Stop(ctx, &stop); err != nil {
						t.Fatal(err)
					}
				}
			}
			harness.TopologyWait(t, ctx, func() bool {
				status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
				return err == nil && status.Available && status.ActiveName != previous
			})
			if _, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{Name: "after-failover"}); err != nil {
				t.Fatal(err)
			}
			data, err := cluster.Ceph(ctx, "fs", "subvolumegroup", "ls", fs.FilesystemName, "--format", "json")
			var groups []struct{ Name string }
			if err != nil || json.Unmarshal(data, &groups) != nil || len(groups) != 2 {
				t.Fatalf("MGR volumes operation lost state after failover: %v: %s", err, data)
			}
			harness.TopologyRados(t, ctx, client, "verify")
			session.Check(t, ctx, client, "manager-failed-over")
			session.Finish(t, ctx, client)
			t.Log("MGR failover: active identity changed, native volumes commands created and listed durable subvolume groups")
		})
	}
}

// Each daemon's own admin socket shows which OSDMap and MonMap epoch it holds
// after a pool creation and a MON addition and removal.
func TestMapPropagationReceipt(t *testing.T) {
	testMapPropagationReceipt(t)
}
