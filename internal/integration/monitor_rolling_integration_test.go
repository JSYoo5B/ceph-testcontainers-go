//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// Warm daemon sessions learn the current monmap without rereading ceph.conf.
// Replace every original MON and cold-start original stopped services to prove
// that their persistent bootstrap addresses also follow the live membership.
func TestMonitorRollingReplacement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{
				ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(1).WithOSDCount(2).WithPools(ceph.PoolConfig{Name: "tc-mon-rolling", Application: "rados"})),
				cephfs.WithFilesystems(cephfs.Config{Name: "tc-mon-rolling-fs"}), rgw.WithGateways(),
				ceph.WithStartupTimeout(3 * time.Minute),
			}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, caller := harness.NewServiceCluster(t, opts...)
			if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			if len(clusteraccess.Monitors(t, cluster)) != 3 || len(clusteraccess.Managers(t, cluster)) != 1 || len(clusteraccess.OSDs(t, cluster)) != 2 || len(clusteraccess.Gateways(t, cluster)) != 1 || len(cephfs.Filesystems(cluster)) != 1 {
				t.Fatal("initial MON/MGR/OSD/RGW/MDS topology differs")
			}
			manager, gateway, filesystem := clusteraccess.Managers(t, cluster)[0], clusteraccess.Gateways(t, cluster)[0], cephfs.Filesystems(cluster)[0]
			if len(filesystem.MDSs()) != 1 {
				t.Fatal("initial filesystem lacks its one owned metadata daemon")
			}
			mds := filesystem.MDSs()[0]
			managerID, gatewayID, mdsID := manager.GetContainerID(), gateway.GetContainerID(), mds.GetContainerID()
			initialManager, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
			if err != nil || !initialManager.Available || initialManager.ActiveName != manager.DaemonName || initialManager.ActiveGID == 0 {
				t.Fatalf("initial native MGR identity is unavailable: %v", err)
			}
			initialMDS, err := filesystem.MDSStatus(ctx)
			if err != nil || len(initialMDS.Active) != 1 || initialMDS.Active[0].Name != mds.ID || initialMDS.Active[0].GID == 0 {
				t.Fatalf("initial native MDS identity is unavailable: %v", err)
			}
			callerConfig := monitorRollingFile(t, ctx, caller, "/etc/ceph/ceph.conf")
			nodes := make(map[string]monitorRollingNode)
			record := func(name, keyringPath string, ctr testcontainers.Container) {
				t.Helper()
				config := monitorRollingFile(t, ctx, ctr, "/etc/ceph/ceph.conf")
				// Distinct node-local settings and comments must survive; using
				// the shared client template as a replacement would erase both.
				config = append(config, []byte(fmt.Sprintf("\n# tc-mon-rolling-private-%s\n[client]\nclient_mount_timeout = %d\n", name, 20+len(nodes)))...)
				if err := ctr.CopyToContainer(ctx, config, "/etc/ceph/ceph.conf", 0o644); err != nil {
					t.Fatalf("seed %s private configuration: %v", name, err)
				}
				nodes[name] = monitorRollingNode{container: ctr, config: config, keyringPath: keyringPath, keyring: monitorRollingFile(t, ctx, ctr, keyringPath)}
			}
			record("control", "/etc/ceph/ceph.client.admin.keyring", clusteraccess.ControlContainer(t, cluster))
			for _, monitor := range clusteraccess.Monitors(t, cluster) {
				record("mon."+monitor.DaemonName, "/etc/ceph/mon.keyring", monitor)
			}
			record("mgr."+manager.DaemonName, "/etc/ceph/mgr.keyring", manager)
			for _, osd := range clusteraccess.OSDs(t, cluster) {
				record(fmt.Sprintf("osd.%d", osd.ID), "/etc/ceph/osd.keyring", osd)
			}
			record("mds."+mds.ID, "/etc/ceph/mds.keyring", mds)
			record("rgw."+gateway.GatewayName, "/etc/ceph/ceph.client.admin.keyring", gateway)
			payload := make([]byte, 64<<10)
			for i := range payload {
				payload[i] = byte(i)
			}
			if err := harness.ToolkitContainer(t, cluster).CopyToContainer(ctx, payload, "/tmp/tc-mon-rolling-input", 0o600); err != nil {
				t.Fatal(err)
			}
			harness.TopologyExecOutput(t, ctx, harness.ToolkitContainer(t, cluster), "rados", "-p", "tc-mon-rolling", "put", "retained", "/tmp/tc-mon-rolling-input")
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s3 := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
			const bucket = "/tc-mon-rolling"
			s3.Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			s3.Request(t, ctx, http.MethodPut, bucket+"/retained", payload, http.StatusOK)
			// Observe warm daemons past the MON's effective grace windows and a tick.
			mon := harness.MonitorHandle(t, ctx, cluster, "a")
			readSeconds := func(option string) time.Duration {
				t.Helper()
				raw, err := mon.AdminSocket(ctx, "config", "get", option)
				var value map[string]string
				if err != nil || json.Unmarshal(raw, &value) != nil {
					t.Fatalf("read %s: %q, %v", option, raw, err)
				}
				n, err := strconv.ParseFloat(value[option], 64)
				if err != nil || n <= 0 {
					t.Fatalf("unexpected %s=%q: %v", option, value[option], err)
				}
				return time.Duration(n * float64(time.Second))
			}
			beaconWindow := max(readSeconds("mon_mgr_beacon_grace"), readSeconds("mds_beacon_grace")) + readSeconds("mon_tick_interval")
			stop := 3 * time.Second
			for i, pair := range [][2]string{{"a", "d"}, {"b", "e"}, {"c", "f"}} {
				if i == 2 {
					// Copy archive APIs work with stopped containers. An Exec
					// based config refresh cannot handle this ordinary fixture.
					for _, daemon := range []testcontainers.Container{manager, mds, gateway} {
						if err := daemon.Stop(ctx, &stop); err != nil {
							t.Fatal(err)
						}
					}
				}
				added, err := cluster.AddMonitor(ctx, pair[1])
				if err != nil {
					t.Fatal(err)
				}
				monitorRollingQuorum(t, ctx, cluster, 4)
				record("mon."+added.DaemonName, "/etc/ceph/mon.keyring", added)
				if err := harness.MonitorHandle(t, ctx, cluster, pair[0]).Remove(ctx); err != nil {
					t.Fatal(err)
				}
				delete(nodes, "mon."+pair[0])
				monitorRollingQuorum(t, ctx, cluster, 3)
				monitorRollingRADOS(t, ctx, harness.ToolkitContainer(t, cluster), payload)
				if i != 2 {
					monitorRollingS3(t, ctx, s3, bucket+"/retained", payload)
					select {
					case <-time.After(beaconWindow):
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					observer := harness.ObserveCluster(cluster)
					now, err := observer.ManagerStatus(ctx)
					if err != nil || !now.Available || now.ActiveGID != initialManager.ActiveGID {
						t.Fatalf("warm MGR after mon.%s removal: %+v, %v", pair[0], now, err)
					}
					health, err := observer.HealthDetails(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if check, down := health.Checks["MGR_DOWN"]; down {
						t.Fatalf("warm MGR stopped beaconing: %+v", check)
					}
					raw, err := cluster.Ceph(ctx, "fs", "get", filesystem.FilesystemName, "--format", "json")
					var fs struct {
						MDSMap struct {
							Info map[string]struct {
								GID   uint64          `json:"gid"`
								State string          `json:"state"`
								Laggy json.RawMessage `json:"laggy_since"`
							} `json:"info"`
						} `json:"mdsmap"`
					}
					if err != nil || json.Unmarshal(raw, &fs) != nil || len(fs.MDSMap.Info) != 1 {
						t.Fatalf("read warm MDS map: %s, %v", raw, err)
					}
					for _, rank := range fs.MDSMap.Info {
						if rank.GID != initialMDS.Active[0].GID || rank.State != "up:active" || len(rank.Laggy) != 0 {
							t.Fatalf("warm MDS after mon.%s removal: %+v", pair[0], rank)
						}
					}
				}
			}
			quorum, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(quorum.QuorumNames)
			if !slices.Equal(quorum.QuorumNames, []string{"d", "e", "f"}) {
				t.Fatalf("final quorum is %v", quorum.QuorumNames)
			}
			cc, err := cluster.ConnectionConfig(ctx)
			currentConfig := cc.Conf
			if err != nil {
				t.Fatal(err)
			}
			bootstrap := monitorRollingBootstrap(currentConfig)
			if strings.Count(bootstrap, "[v2:") != 3 {
				t.Fatal("fresh client configuration lacks the three surviving MON vectors")
			}
			// Assert config freshness independently of I/O. Bridge IP reuse
			// could otherwise hide obsolete bootstrap addresses by accident.
			for name, node := range nodes {
				config := monitorRollingFile(t, ctx, node.container, "/etc/ceph/ceph.conf")
				if monitorRollingBootstrap(config) != bootstrap {
					t.Errorf("owned %s lacks the final live MON bootstrap vectors", name)
				}
				if !bytes.Equal(monitorRollingWithoutBootstrap(node.config), monitorRollingWithoutBootstrap(config)) {
					t.Errorf("owned %s lost node-local config content", name)
				}
				if !bytes.Equal(node.keyring, monitorRollingFile(t, ctx, node.container, node.keyringPath)) {
					t.Errorf("owned %s role keyring changed during MON replacement", name)
				}
			}
			if !bytes.Equal(callerConfig, monitorRollingFile(t, ctx, caller, "/etc/ceph/ceph.conf")) {
				t.Error("MON replacement changed a caller-owned client snapshot")
			}
			// Restore an obsolete bootstrap line in an original stopped MDS
			// without changing any of its private content. Explicit refresh
			// repairs this file without starting the daemon or changing keys.
			mdsNode := nodes["mds."+mds.ID]
			freshMDSConfig := monitorRollingFile(t, ctx, mds, "/etc/ceph/ceph.conf")
			oldBootstrap := monitorRollingBootstrap(mdsNode.config)
			if oldBootstrap == bootstrap {
				t.Fatal("rolling replacement did not change the initial bootstrap vectors")
			}
			staleMDSConfig := []byte(strings.Replace(string(freshMDSConfig), bootstrap, oldBootstrap, 1))
			if err := mds.CopyToContainer(ctx, staleMDSConfig, "/etc/ceph/ceph.conf", 0o644); err != nil {
				t.Fatal(err)
			}
			if err := cluster.RefreshMonitorConfig(ctx); err != nil {
				t.Fatalf("explicit stopped MDS configuration refresh: %v", err)
			}
			if !bytes.Equal(freshMDSConfig, monitorRollingFile(t, ctx, mds, "/etc/ceph/ceph.conf")) {
				t.Error("explicit refresh did not restore the stopped MDS bootstrap while preserving private content")
			}
			if !bytes.Equal(mdsNode.keyring, monitorRollingFile(t, ctx, mds, mdsNode.keyringPath)) {
				t.Error("explicit refresh changed the stopped MDS role keyring")
			}
			monitorRollingQuorum(t, ctx, cluster, 3)
			for _, daemon := range []testcontainers.Container{manager, mds, gateway} {
				state, err := daemon.State(ctx)
				if err != nil || state == nil || state.Running {
					t.Fatalf("MON replacement did not retain an injected stopped daemon: %v", err)
				}
			}
			for _, daemon := range []testcontainers.Container{manager, mds, gateway} {
				if err := daemon.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if manager.GetContainerID() != managerID || gateway.GetContainerID() != gatewayID || mds.GetContainerID() != mdsID {
				t.Fatal("cold restart replaced an original daemon container")
			}
			topologyWait(t, ctx, func() bool {
				status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
				return err == nil && status.Available && status.ActiveName == manager.DaemonName && status.ActiveGID != 0 && status.ActiveGID != initialManager.ActiveGID
			})
			topologyWait(t, ctx, func() bool {
				status, err := filesystem.MDSStatus(ctx)
				if err != nil {
					return false
				}
				restarted, err := expect.MDSs{
					FilesystemID: initialMDS.FilesystemID, Owned: true, Exact: true,
					Ranks: []expect.MDSRank{{Rank: 0, Name: mds.ID, NotGID: initialMDS.Active[0].GID}},
				}.Evaluate(*status)
				return err == nil && restarted.Matched
			})
			// Re-read the published endpoint: Docker can remap an ephemeral
			// host port when the same stopped gateway is started again.
			s3.Endpoint, err = gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			monitorRollingS3(t, ctx, s3, bucket+"/retained", payload)
			monitorRollingRADOS(t, ctx, harness.ToolkitContainer(t, cluster), payload)
			t.Log("MON a,b,c -> d,e,f kept 4 -> 3 quorums; running/stopped owned configs and explicit refresh retained private settings and role keys; original MGR/MDS/RGW cold-started with retained RADOS/S3 bytes")
		})
	}
}
