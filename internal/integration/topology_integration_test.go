//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMonitorManagerTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithStartupTimeout(3*time.Minute))
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
			if len(cluster.Monitors()) != 3 || len(cluster.Managers()) != 2 || cluster.ControlContainer().GetContainerID() == cluster.GetContainerID() {
				t.Fatal("HA fixture lacks separate control or daemon counts")
			}
			config, _, err := cluster.ConnectionConfig()
			if err != nil || strings.Count(string(config), "[v2:") != 3 {
				t.Fatalf("fresh client lacks three monitor addrvecs: %v: %s", err, config)
			}
			cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-topology", "8")
			cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-topology", "rados")
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
				testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
			if client != nil {
				testcontainers.CleanupContainer(t, client)
			}
			if err != nil {
				t.Fatal(err)
			}
			session := startTopologyRadosSession(t, ctx, client)
			topologyFreshConnection(t, ctx, client, false)
			monitors := cluster.Monitors()
			primary := monitors[0]
			stop := 3 * time.Second
			if err := primary.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 2
			})
			if err := cluster.WaitForQuorum(ctx); err != nil {
				t.Fatal(err)
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "one-monitor-down")
			t.Log("MON a stopped: CLI control, the retained native librados session, and fresh authenticated I/O survived on quorum b,c")
			if err := primary.Start(ctx); err != nil {
				t.Fatal(err)
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			session.check(t, ctx, client, "one-monitor-restarted")
			// Two stopped members remove the majority. Inspect the surviving
			// daemon's local admin socket: a quorum-dependent CLI would block.
			for _, mon := range monitors[:2] {
				if err := mon.Stop(ctx, &stop); err != nil {
					t.Fatal(err)
				}
			}
			topologyWait(t, ctx, func() bool {
				data := topologyExecOutput(t, ctx, monitors[2], "ceph", "--admin-daemon", "/var/run/ceph/ceph-mon."+monitors[2].DaemonName+".asok", "mon_status")
				var status struct {
					State  string `json:"state"`
					Quorum []int  `json:"quorum"`
				}
				if err := json.Unmarshal(data, &status); err != nil || status.Quorum == nil {
					t.Fatalf("decode surviving MON's local status: %v: %s", err, data)
				}
				return len(status.Quorum) == 0 && status.State != "leader" && status.State != "peon"
			})
			topologyFreshConnection(t, ctx, client, true)
			t.Log("Two of three MONs stopped: the surviving daemon has no quorum and a fresh native session cannot authenticate within the bounded probe; retained sessions are left idle")
			for _, mon := range monitors[:2] {
				if err := mon.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			topologyFreshConnection(t, ctx, client, false)
			session.check(t, ctx, client, "quorum-recovered")
			if _, err := cluster.AddMonitor(ctx, "replacement"); err != nil {
				t.Fatal(err)
			}
			if err := cluster.RemoveMonitor(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			if len(cluster.Monitors()) != 3 {
				t.Fatal("replacement did not preserve three owned monitors")
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "monitor-replaced")
			t.Log("MON replacement: 3 -> 4 -> 3, primary a removed, both retained and fresh client sessions retained data")
			cephCommand(t, ctx, cluster, "mgr", "module", "enable", "volumes")
			fs, err := cluster.StartCephFS(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cephCommand(t, ctx, cluster, "fs", "subvolumegroup", "create", fs.FilesystemName, "before-failover")
			mgrStatus, err := cluster.ManagerStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			previous := mgrStatus.ActiveName
			for _, mgr := range cluster.Managers() {
				if mgr.DaemonName == previous {
					if err := mgr.Stop(ctx, &stop); err != nil {
						t.Fatal(err)
					}
				}
			}
			topologyWait(t, ctx, func() bool {
				status, err := cluster.ManagerStatus(ctx)
				return err == nil && status.Available && status.ActiveName != previous
			})
			cephCommand(t, ctx, cluster, "fs", "subvolumegroup", "create", fs.FilesystemName, "after-failover")
			data, err := cluster.Ceph(ctx, "fs", "subvolumegroup", "ls", fs.FilesystemName, "--format", "json")
			var groups []struct{ Name string }
			if err != nil || json.Unmarshal(data, &groups) != nil || len(groups) != 2 {
				t.Fatalf("MGR volumes operation lost state after failover: %v: %s", err, data)
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "manager-failed-over")
			session.finish(t, ctx, client)
			t.Log("MGR failover: active identity changed, native volumes command created and listed durable subvolume groups")
		})
	}
}
