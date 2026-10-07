//go:build integration && topology && multicluster

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

// Replace every monitor on both sides, then cold-start the same clients and
// mirror processes. File/peer assertions prevent address reuse or a warm
// librados session from concealing obsolete bootstrap configuration.
func TestMultiClusterMonitorBootstrapRefresh(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithMonitorCount(3), ceph.WithOSDCount(1), ceph.WithStartupTimeout(3 * time.Minute)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
			sourceStatus, err := source.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			destinationStatus, err := destination.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			const pool, image, script = "tc-mon-bootstrap", "volume", "/tmp/cephfs-bootstrap.py"
			rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
			rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
			rbdMirror := rbdJournalRunLink(t, ctx, source, destination, pool, "source", "destination")
			execCommand(t, ctx, sourceClient, "rbd", "create", pool+"/"+image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
			payload := rbdMultiClusterPayload(8<<20, 29)
			rbdJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, image, 0, payload)
			if err := rbdMirror.EnableImage(ctx, image); err != nil {
				t.Fatal(err)
			}
			initialRBD := rbdMirrorReplayReady(t, ctx, rbdMirror, image, multicluster.RBDMirrorModeJournal, "", "")
			originalRBDPeer := rbdScenarioPeer(t, ctx, rbdMirror, pool)
			originalRBDHosts, originalRBDKey := monitorBootstrapRBDPeerConfig(t, ctx, destination, pool, originalRBDPeer)
			rbdJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, image, payload)
			sourceFS, err := source.StartCephFS(ctx)
			if err != nil {
				t.Fatal(err)
			}
			destinationFS, err := destination.StartCephFS(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
				if err := client.CopyToContainer(ctx, []byte(cephFSInterClusterScript), script, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
				t.Helper()
				command := append([]string{"python3", script, filesystem}, args...)
				multiClusterExecOutput(t, ctx, client, command...)
			}
			fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
			mirror, err := multicluster.RunCephFSMirror(ctx, source.ControlImage(), multicluster.CephFSMirrorConfig{
				Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName,
				DestinationFilesystem: destinationFS.FilesystemName, Directories: []string{"/federation"},
			})
			if mirror != nil {
				t.Cleanup(func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
					defer stop()
					if t.Failed() && mirror.Container != nil {
						multiClusterLogContainer(t, cleanupCtx, mirror.Container)
					}
					if err := mirror.Terminate(cleanupCtx); err != nil {
						t.Errorf("terminate bootstrap CephFS link: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := cephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
			peers, err := mirror.PeerIDs(ctx)
			if err != nil || len(peers) != 1 {
				t.Fatalf("initial CephFS peer count: %v error=%v", peers, err)
			}
			expected.PeerID = peers[0]
			snapshot := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, "/federation", "backup-1")
			initialFS := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/federation", snapshot)
			verifySnapshot := func(name string) {
				t.Helper()
				path := "/tmp/cephfs-bootstrap-" + name + ".json"
				fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/"+name, path)
				archive := multiClusterReadFile(t, ctx, sourceClient, path)
				if err := destinationClient.CopyToContainer(ctx, archive, path, 0o600); err != nil {
					t.Fatal(err)
				}
				fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/"+name, path)
			}
			verifySnapshot("backup-1")
			peerBefore := monitorBootstrapPeerConfig(t, ctx, source, mirror.SourceFilesystem, expected.PeerID)
			// A caller-owned extension must survive changing just mon_host.
			peerBefore["testcontainers_extension"] = json.RawMessage(`{"preserve":true}`)
			monitorBootstrapSetPeerConfig(t, ctx, source, mirror.SourceFilesystem, expected.PeerID, peerBefore)
			type node struct {
				cluster     *ceph.Container
				ctr         testcontainers.Container
				config, key []byte
				keyPath, id string
			}
			nodes := []node{
				{cluster: source, ctr: sourceClient, keyPath: "/etc/ceph/ceph.client.admin.keyring"},
				{cluster: destination, ctr: destinationClient, keyPath: "/etc/ceph/ceph.client.admin.keyring"},
				{cluster: destination, ctr: rbdMirror.Container, keyPath: "/etc/ceph/rbd-mirror.keyring"},
				{cluster: source, ctr: mirror.Container, keyPath: "/etc/ceph/ceph." + mirror.SourceClientEntity + ".keyring"},
			}
			stopTimeout := 3 * time.Second
			for i := range nodes {
				n := &nodes[i]
				n.id = n.ctr.GetContainerID()
				n.config = monitorRollingFile(t, ctx, n.ctr, "/etc/ceph/ceph.conf")
				n.config = append(n.config, []byte(fmt.Sprintf("\n# retained-bootstrap-client-%d\n[client]\nclient_mount_timeout = %d\n", i, 30+i))...)
				if err := n.ctr.CopyToContainer(ctx, n.config, "/etc/ceph/ceph.conf", 0o644); err != nil {
					t.Fatal(err)
				}
				n.key = monitorRollingFile(t, ctx, n.ctr, n.keyPath)
				if err := n.ctr.Stop(ctx, &stopTimeout); err != nil {
					t.Fatal(err)
				}
			}
			for _, cluster := range []*ceph.Container{source, destination} {
				for _, pair := range [][2]string{{"a", "d"}, {"b", "e"}, {"c", "f"}} {
					if _, err := cluster.AddMonitor(ctx, pair[1]); err != nil {
						t.Fatal(err)
					}
					monitorRollingQuorum(t, ctx, cluster, 4)
					if err := cluster.RemoveMonitor(ctx, pair[0]); err != nil {
						t.Fatal(err)
					}
					monitorRollingQuorum(t, ctx, cluster, 3)
				}
				quorum, err := cluster.QuorumStatus(ctx)
				if err != nil {
					t.Fatal(err)
				}
				slices.Sort(quorum.QuorumNames)
				if !slices.Equal(quorum.QuorumNames, []string{"d", "e", "f"}) {
					t.Fatal("original monitor survived the rolling replacement")
				}
			}
			for _, n := range nodes {
				if !bytes.Equal(n.config, monitorRollingFile(t, ctx, n.ctr, "/etc/ceph/ceph.conf")) {
					t.Fatal("rolling replacement implicitly rewrote a caller/link snapshot")
				}
			}
			if err := source.RefreshClientMonitorConfig(ctx, sourceClient); err != nil {
				t.Fatal(err)
			}
			if err := destination.RefreshClientMonitorConfig(ctx, destinationClient); err != nil {
				t.Fatal(err)
			}
			if err := rbdMirror.RefreshMonitorConfig(ctx); err != nil {
				t.Fatal(err)
			}
			if err := mirror.RefreshMonitorConfig(ctx); err != nil {
				t.Fatal(err)
			}
			localOnlyHosts, localOnlyKey := monitorBootstrapRBDPeerConfig(t, ctx, destination, pool, originalRBDPeer)
			if localOnlyHosts != originalRBDHosts || localOnlyKey != originalRBDKey {
				t.Fatal("local RBD refresh implicitly changed remote peer bootstrap")
			}
			if err := rbdMirror.Rebootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			if rbdScenarioPeer(t, ctx, rbdMirror, pool) != originalRBDPeer {
				t.Fatal("RBD bootstrap refresh replaced the original receiving peer")
			}
			currentSource, err := source.MonitorBootstrapAddresses(ctx)
			if err != nil {
				t.Fatal(err)
			}
			remoteRBDHosts, remoteRBDKey := monitorBootstrapRBDPeerConfig(t, ctx, destination, pool, originalRBDPeer)
			if !slices.Equal(monitorBootstrapAddressEndpoints(remoteRBDHosts), monitorBootstrapAddressEndpoints(currentSource)) || remoteRBDKey != originalRBDKey {
				t.Fatal("RBD remote peer lacks the exact current source MON endpoints or changed credentials")
			}
			if err := mirror.RefreshPeerMonitorConfig(ctx); err != nil {
				t.Fatal(err)
			}
			currentDestination, err := destination.MonitorBootstrapAddresses(ctx)
			if err != nil {
				t.Fatal(err)
			}
			peerAfter := monitorBootstrapPeerConfig(t, ctx, source, mirror.SourceFilesystem, expected.PeerID)
			monitorBootstrapAssertPeerConfig(t, peerBefore, peerAfter, currentDestination)
			for _, n := range nodes {
				config := monitorRollingFile(t, ctx, n.ctr, "/etc/ceph/ceph.conf")
				current, err := n.cluster.MonitorBootstrapAddresses(ctx)
				if err != nil || monitorRollingBootstrap(config) != "mon host = "+current || monitorRollingBootstrap(config) == monitorRollingBootstrap(n.config) {
					t.Fatalf("explicit refresh did not adopt new monitor addresses: container=%s error=%v", n.id[:12], err)
				}
				if !bytes.Equal(monitorRollingWithoutBootstrap(n.config), monitorRollingWithoutBootstrap(config)) || !bytes.Equal(n.key, monitorRollingFile(t, ctx, n.ctr, n.keyPath)) {
					t.Fatal("explicit refresh changed private config or credentials")
				}
				state, err := n.ctr.State(ctx)
				if err != nil || state == nil || state.Running || n.ctr.GetContainerID() != n.id {
					t.Fatalf("refresh changed stopped process or container identity: error=%v", err)
				}
				if err := n.ctr.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for i, client := range []testcontainers.Container{sourceClient, destinationClient} {
				fsid := strings.TrimSpace(string(multiClusterExecOutput(t, ctx, client, "ceph", "fsid")))
				if fsid != []string{sourceStatus.FSID, destinationStatus.FSID}[i] {
					t.Fatal("cold-started client connected to a different cluster")
				}
			}
			restartedRBD := rbdMirrorReplayReady(t, ctx, rbdMirror, image, multicluster.RBDMirrorModeJournal, "", "")
			if rbdScenarioPeer(t, ctx, rbdMirror, pool) != originalRBDPeer {
				t.Fatal("RBD cold restart replaced the original receiving peer")
			}
			if restartedRBD.GlobalID != initialRBD.GlobalID || restartedRBD.SourceImageID != initialRBD.SourceImageID || restartedRBD.DestinationImageID != initialRBD.DestinationImageID || restartedRBD.InstanceID == initialRBD.InstanceID {
				t.Fatal("RBD cold restart changed images or retained the previous process instance")
			}
			restartedFS := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/federation", snapshot)
			if restartedFS.InstanceID == initialFS.InstanceID {
				t.Fatal("CephFS cold restart retained the previous native watcher")
			}
			rbdJournalRead(t, ctx, destinationClient, destinationStatus.FSID, pool, image, payload)
			patch := rbdMultiClusterPayload(1<<20, 79)
			copy(payload[2<<20:], patch)
			rbdJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, image, 2<<20, patch)
			rbdJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, image, payload)
			fsCommand(sourceClient, sourceFS.FilesystemName, "mutate")
			snapshot = cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, "/federation", "backup-2")
			cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/federation", snapshot)
			verifySnapshot("backup-1")
			verifySnapshot("backup-2")
			t.Log("full MON replacement on both clusters: explicit stopped-client/local-link/remote-peer refresh retained cluster, image, filesystem, peer, container and credential identities; same cold-started daemons replayed fresh RBD and CephFS bytes")
			// The original destination MGR remains warm. Recreating a peer must
			// normalize its bootstrap token's stale conf_get(mon_host) value.
			oldPeer := expected.PeerID
			if err := mirror.RemovePeer(ctx, oldPeer); err != nil {
				t.Fatal(err)
			}
			cephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, oldPeer)
			newPeer, err := mirror.RebootstrapPeer(ctx)
			if err != nil || newPeer == "" || newPeer == oldPeer {
				t.Fatalf("warm-MGR rebootstrap did not create a new peer: error=%v", err)
			}
			expected.PeerID = newPeer
			freshPeer := monitorBootstrapPeerConfig(t, ctx, source, mirror.SourceFilesystem, newPeer)
			var freshAddresses string
			if json.Unmarshal(freshPeer["mon_host"], &freshAddresses) != nil || freshAddresses != currentDestination {
				t.Fatal("warm destination MGR bootstrap imported obsolete monitor addresses")
			}
			fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "mon-rebootstrap", "backup-3")
			snapshot = cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, "/federation", "backup-3")
			cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/federation", snapshot)
			verifySnapshot("backup-3")
			t.Logf("warm destination MGR peer rebootstrap: old=%s new=%s; current destination monmap and independent source snapshot %d/%s with exact destination bytes", oldPeer, newPeer, snapshot.ID, snapshot.Name)
		})
	}
}

func monitorBootstrapRBDPeerConfig(t *testing.T, ctx context.Context, destination *ceph.Container, pool, peer string) (string, string) {
	t.Helper()
	policy, err := destination.PoolStatus(ctx, pool)
	if err != nil || policy.ID <= 0 {
		t.Fatalf("read RBD receiving pool identity: error=%v", err)
	}
	raw, err := destination.Ceph(ctx, "config-key", "get", fmt.Sprintf("rbd/mirror/peer/%d/%s", policy.ID, peer))
	if err != nil {
		t.Fatal("read native RBD remote peer bootstrap failed; content withheld")
	}
	var value struct {
		MonHost string `json:"mon_host"`
		Key     string `json:"key"`
	}
	if json.Unmarshal(raw, &value) != nil || value.MonHost == "" || value.Key == "" || len(monitorBootstrapAddressEndpoints(value.MonHost)) != 6 {
		t.Fatal("invalid native RBD remote peer bootstrap; content withheld")
	}
	return value.MonHost, value.Key
}

// Native RBD joins vectors with commas and includes endpoint nonces; the public
// mon_host value joins vectors with spaces and omits nonces. Compare the full
// type/IP/port multiset instead, retaining duplicates rather than hiding them.
func monitorBootstrapAddressEndpoints(addresses string) []string {
	values := regexp.MustCompile(`v[12]:[^,[:space:]]+`).FindAllString(addresses, -1)
	for i, value := range values {
		value = strings.TrimRight(value, "]")
		values[i], _, _ = strings.Cut(value, "/")
	}
	slices.Sort(values)
	return values
}

func monitorBootstrapPeerConfig(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peer string) map[string]json.RawMessage {
	t.Helper()
	raw, err := source.Ceph(ctx, "config-key", "get", "cephfs/mirror/peer/"+filesystem+"/"+peer)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || len(value) == 0 {
		t.Fatal("invalid native CephFS peer config; content withheld")
	}
	return value
}

func monitorBootstrapSetPeerConfig(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peer string, value map[string]json.RawMessage) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/tmp/tc-cephfs-peer-config.json"
	if err := source.ControlContainer().CopyToContainer(ctx, raw, path, 0o600); err != nil {
		t.Fatal(err)
	}
	topologyExecOutput(t, ctx, source.ControlContainer(), "ceph", "config-key", "set", "cephfs/mirror/peer/"+filesystem+"/"+peer, "-i", path)
	topologyExecOutput(t, ctx, source.ControlContainer(), "rm", "-f", path)
}

func monitorBootstrapAssertPeerConfig(t *testing.T, before, after map[string]json.RawMessage, addresses string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("remote refresh lost or added peer config fields")
	}
	for key, value := range before {
		if key == "mon_host" {
			var current string
			if json.Unmarshal(after[key], &current) != nil || current != addresses {
				t.Fatal("remote peer monitor addresses differ from the current destination monmap")
			}
		} else if !bytes.Equal(value, after[key]) {
			t.Fatalf("remote refresh changed peer field %s; values withheld", key)
		}
	}
}
