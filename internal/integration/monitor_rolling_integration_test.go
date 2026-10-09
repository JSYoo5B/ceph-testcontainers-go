//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Warm daemon sessions learn the current monmap without rereading ceph.conf.
// Replace every original MON and cold-start original stopped services to prove
// that their persistent bootstrap addresses also follow the live membership.
func TestMonitorRollingReplacement(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{
				ceph.WithMonitorCount(3), ceph.WithManagerCount(1), ceph.WithOSDCount(2),
				ceph.WithPools(ceph.PoolConfig{Name: "tc-mon-rolling", Application: "rados"}),
				ceph.WithCephFS(ceph.CephFSConfig{Name: "tc-mon-rolling-fs"}), ceph.WithRGW(),
				ceph.WithStartupTimeout(3 * time.Minute),
			}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, caller := newServiceCluster(t, opts...)
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			if len(cluster.Monitors()) != 3 || len(cluster.Managers()) != 1 || len(cluster.OSDs()) != 2 || len(cluster.Gateways()) != 1 || len(cluster.Filesystems()) != 1 {
				t.Fatal("initial MON/MGR/OSD/RGW/MDS topology differs")
			}
			manager, gateway, filesystem := cluster.Managers()[0], cluster.Gateways()[0], cluster.Filesystems()[0]
			if len(filesystem.MDSs()) != 1 {
				t.Fatal("initial filesystem lacks its one owned metadata daemon")
			}
			mds := filesystem.MDSs()[0]
			managerID, gatewayID, mdsID := manager.GetContainerID(), gateway.GetContainerID(), mds.GetContainerID()
			initialManager, err := cluster.ManagerStatus(ctx)
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
			record("control", "/etc/ceph/ceph.client.admin.keyring", cluster.ControlContainer())
			for _, monitor := range cluster.Monitors() {
				record("mon."+monitor.DaemonName, "/etc/ceph/mon.keyring", monitor)
			}
			record("mgr."+manager.DaemonName, "/etc/ceph/mgr.keyring", manager)
			for _, osd := range cluster.OSDs() {
				record(fmt.Sprintf("osd.%d", osd.ID), "/etc/ceph/osd.keyring", osd)
			}
			record("mds."+mds.ID, "/etc/ceph/mds.keyring", mds)
			record("rgw."+gateway.GatewayName, "/etc/ceph/ceph.client.admin.keyring", gateway)
			payload := make([]byte, 64<<10)
			for i := range payload {
				payload[i] = byte(i)
			}
			if err := cluster.ControlContainer().CopyToContainer(ctx, payload, "/tmp/tc-mon-rolling-input", 0o600); err != nil {
				t.Fatal(err)
			}
			topologyExecOutput(t, ctx, cluster.ControlContainer(), "rados", "-p", "tc-mon-rolling", "put", "retained", "/tmp/tc-mon-rolling-input")
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s3 := s3HTTPClient{endpoint: endpoint, accessKey: gateway.AccessKey, secretKey: gateway.SecretKey, region: gateway.Region, http: &http.Client{Timeout: 20 * time.Second}}
			const bucket = "/tc-mon-rolling"
			s3.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			s3.request(t, ctx, http.MethodPut, bucket+"/retained", payload, http.StatusOK)
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
				if err := cluster.RemoveMonitor(ctx, pair[0]); err != nil {
					t.Fatal(err)
				}
				delete(nodes, "mon."+pair[0])
				monitorRollingQuorum(t, ctx, cluster, 3)
				monitorRollingRADOS(t, ctx, cluster.ControlContainer(), payload)
				if i != 2 {
					monitorRollingS3(t, ctx, s3, bucket+"/retained", payload)
				}
			}
			quorum, err := cluster.QuorumStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(quorum.QuorumNames)
			if !slices.Equal(quorum.QuorumNames, []string{"d", "e", "f"}) {
				t.Fatalf("final quorum is %v", quorum.QuorumNames)
			}
			currentConfig, _, err := cluster.ConnectionConfig()
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
				status, err := cluster.ManagerStatus(ctx)
				return err == nil && status.Available && status.ActiveName == manager.DaemonName && status.ActiveGID != 0 && status.ActiveGID != initialManager.ActiveGID
			})
			topologyWait(t, ctx, func() bool {
				status, err := filesystem.MDSStatus(ctx)
				if err != nil || status.FilesystemID != initialMDS.FilesystemID || len(status.Active) != 1 {
					return false
				}
				active := status.Active[0]
				return active.Name == mds.ID && active.Rank == 0 && active.State == "up:active" &&
					active.Owned && active.GID != 0 && active.GID != initialMDS.Active[0].GID
			})
			// Re-read the published endpoint: Docker can remap an ephemeral
			// host port when the same stopped gateway is started again.
			s3.endpoint, err = gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			monitorRollingS3(t, ctx, s3, bucket+"/retained", payload)
			monitorRollingRADOS(t, ctx, cluster.ControlContainer(), payload)
			t.Log("MON a,b,c -> d,e,f kept 4 -> 3 quorums; running/stopped owned configs and explicit refresh retained private settings and role keys; original MGR/MDS/RGW cold-started with retained RADOS/S3 bytes")
		})
	}
}
