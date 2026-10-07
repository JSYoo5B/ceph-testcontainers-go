//go:build integration && multicluster

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestMultiClusterRBDReceiverReadiness verifies image-free scope discovery,
// exact owned election, topology changes and independent resumed payload I/O.
func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 35*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
			for index, mapping := range [][2]string{{"", ""}, {"ns-a", "ns-b"}, {"ns-a", ""}, {"", "ns-b"}, {"ns-a", "ns-b"}} {
				t.Run(fmt.Sprintf("scope-%d", index), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
					defer cancel()
					pool := fmt.Sprintf("tc-rbd-receiver-%d", index)
					scope := multicluster.RBDMirrorScopeImage
					mode := "snapshot"
					if index == 4 {
						scope, mode = multicluster.RBDMirrorScopePool, "journal"
					}
					for _, cluster := range []*ceph.Container{source, destination} {
						if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
							t.Fatal(err)
						}
						if err := cluster.InitRBDPool(ctx, pool); err != nil {
							t.Fatal(err)
						}
						for _, ns := range []string{"ns-a", "ns-b"} {
							if _, err := cluster.CreateRBDNamespace(ctx, pool, ns); err != nil {
								t.Fatal(err)
							}
						}
						if err := cluster.WaitForClean(ctx); err != nil {
							t.Fatal(err)
						}
					}
					originalSourcePool, err := source.PoolStatus(ctx, pool)
					if err != nil {
						t.Fatal(err)
					}
					originalDestinationPool, err := destination.PoolStatus(ctx, pool)
					if err != nil {
						t.Fatal(err)
					}
					sourceStatus, err := source.Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					destinationStatus, err := destination.Status(ctx)
					if err != nil {
						t.Fatal(err)
					}
					link, err := multicluster.RunRBDMirror(ctx, source.ControlImage(), multicluster.RBDMirrorConfig{Source: source, Destination: destination, Pool: pool, Scope: scope, SourceNamespace: mapping[0], DestinationNamespace: mapping[1], DaemonCount: 2})
					if link != nil {
						t.Cleanup(func() {
							cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
							defer done()
							if err := link.Terminate(cleanup); err != nil {
								t.Error(err)
							}
						})
					}
					if err != nil {
						t.Fatal(err)
					}
					identity := rbdReceiverNativeIdentity(t, ctx, destinationClient, pool)
					originalPolicies := [4]rbdReceiverIndependentPolicy{
						rbdReceiverNativePolicy(t, ctx, sourceClient, pool, ""),
						rbdReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]),
						rbdReceiverNativePolicy(t, ctx, destinationClient, pool, ""),
						rbdReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1]),
					}
					if originalPolicies[1].mode != string(scope) || originalPolicies[3].mode != string(scope) || originalPolicies[1].remote != mapping[1] || originalPolicies[3].remote != mapping[0] {
						t.Fatal("independent original native selected policy differs from configured scope/mapping")
					}
					rbdReceiverNativePeerMatches(t, identity, identity, originalPolicies[0].mirrorUUID)
					check := func(phase string, names ...string) multicluster.RBDMirrorReceiverStatus {
						t.Helper()
						status, err := link.WaitReceiverReady(ctx, names...)
						if err != nil || !status.Ready {
							t.Fatalf("%s readiness: %+v %v", phase, status, err)
						}
						if status.SourceFSID != sourceStatus.FSID || status.DestinationFSID != destinationStatus.FSID || status.SourcePoolID != originalSourcePool.ID || status.DestinationPoolID != originalDestinationPool.ID || status.PeerID != identity.peerUUID || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] {
							t.Fatalf("%s public identity differs: %+v", phase, status)
						}
						currentPolicies := [4]rbdReceiverIndependentPolicy{
							rbdReceiverNativePolicy(t, ctx, sourceClient, pool, ""),
							rbdReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]),
							rbdReceiverNativePolicy(t, ctx, destinationClient, pool, ""),
							rbdReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1]),
						}
						if currentPolicies != originalPolicies {
							t.Fatalf("%s original native base/selected policies changed: before=%+v after=%+v", phase, originalPolicies, currentPolicies)
						}
						rbdReceiverNativePeerMatches(t, rbdReceiverNativeIdentity(t, ctx, destinationClient, pool), identity, originalPolicies[0].mirrorUUID)
						rbdReceiverNativeOriginalClusterPool(t, ctx, sourceClient, pool, sourceStatus.FSID, originalSourcePool.ID)
						rbdReceiverNativeOriginalClusterPool(t, ctx, destinationClient, pool, destinationStatus.FSID, originalDestinationPool.ID)
						rbdReceiverNativeElection(t, ctx, link, status, identity, mapping)
						payload, _ := json.Marshal(map[string]any{"phase": phase, "pool": pool, "mapping": mapping, "status": status})
						t.Logf("RBD_RECEIVER_READY %s", payload)
						return status
					}
					initial := check("empty-two")
					for _, site := range []struct {
						client testcontainers.Container
						ns     string
					}{{sourceClient, mapping[0]}, {destinationClient, mapping[1]}} {
						var images []string
						if err := json.Unmarshal(rbdOutput(t, ctx, site.client, "ls", rbdReceiverNativeScope(pool, site.ns), "--format", "json"), &images); err != nil || images == nil || len(images) != 0 {
							t.Fatalf("initial readiness used nonempty image namespace: %v %v", images, err)
						}
					}
					var leader, survivor *multicluster.RBDMirrorDaemon
					for _, daemon := range link.Daemons() {
						if daemon.DaemonName == initial.LeaderDaemonName {
							leader = daemon
						} else {
							survivor = daemon
						}
					}
					grace := 2 * time.Second
					if err := leader.Stop(ctx, &grace); err != nil {
						t.Fatal(err)
					}
					if status, err := link.ReceiverStatus(ctx); err != nil || status.Ready {
						t.Fatalf("default cohort blessed stopped member: %+v %v", status, err)
					}
					check("survivor-only", survivor.DaemonName)
					cid, instance := leader.GetContainerID(), initial.Daemons[leader.DaemonName].InstanceID
					if err := leader.Start(ctx); err != nil {
						t.Fatal(err)
					}
					restarted := check("same-cid-restart")
					if leader.GetContainerID() != cid || restarted.Daemons[leader.DaemonName].InstanceID == instance {
						t.Fatal("same container restart did not preserve CID/change native instance")
					}
					if err := link.RemoveDaemon(ctx, restarted.LeaderDaemonName); err != nil {
						t.Fatal(err)
					}
					check("leader-removed")
					if _, err := link.AddDaemon(ctx, "replacement"); err != nil {
						t.Fatal(err)
					}
					check("replacement-two")
					partial, err := link.AddDaemon(ctx, "partial", testcontainers.WithWaitStrategy(wait.ForExec([]string{"sh", "-c", "exit 1"}).WithStartupTimeout(2*time.Second)))
					if partial == nil || err == nil {
						t.Fatalf("partial startup control missing: result=%v error=%v", partial != nil, err)
					}
					if status, _ := link.ReceiverStatus(ctx); status.Ready {
						t.Fatal("partial AddDaemon was adopted as ready")
					}
					if err := link.RemoveDaemon(ctx, "partial"); err != nil {
						t.Fatal(err)
					}
					check("partial-removed")
					for _, daemon := range link.Daemons() {
						if err := link.RemoveDaemon(ctx, daemon.DaemonName); err != nil {
							t.Fatal(err)
						}
					}
					zeroCtx, zeroCancel := context.WithTimeout(ctx, 3*time.Second)
					zero, err := link.WaitReceiverReady(zeroCtx)
					zeroCancel()
					if !errors.Is(err, context.DeadlineExceeded) || zero.Ready || len(link.Daemons()) != 0 || zero.PeerID != identity.peerUUID {
						t.Fatalf("zero receiver deadline: %+v %v", zero, err)
					}
					payload := rbdMultiClusterPayload(2<<20, 43+index)
					rbdScopeCreate(t, ctx, sourceClient, pool, mapping[0], "backlog", scope == multicluster.RBDMirrorScopePool)
					if scope == multicluster.RBDMirrorScopeImage {
						if err := link.EnableImage(ctx, "backlog"); err != nil {
							t.Fatal(err)
						}
					}
					rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
					var checkpointID uint64
					if scope == multicluster.RBDMirrorScopeImage {
						// Enrollment's initial snapshot predates payload; this explicit
						// checkpoint causally includes the write made with zero receivers.
						checkpointID = rbdReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "backlog")
						t.Logf("RBD_RECEIVER_CHECKPOINT pool=%s namespace=%q snapshot_id=%d", pool, mapping[0], checkpointID)
					}
					if _, err := link.AddDaemon(ctx, "resumed"); err != nil {
						t.Fatal(err)
					}
					check("zero-resumed")
					if _, err := link.WaitReplayReady(ctx, "backlog"); err != nil {
						t.Fatal(err)
					}
					rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "backlog", payload)
					rbdScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
					rbdScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, mapping[0], mapping[1], "backlog", mode)
					t.Logf("RBD_RECEIVER_BYTES pool=%s bytes=%d sha256=%x mapping=%q mode=%s checkpoint_id=%d", pool, len(payload), sha256.Sum256(payload), mapping, mode, checkpointID)
				})
			}
		})
	}
}

func rbdReceiverNativeScope(pool, namespace string) string {
	if namespace == "" {
		return pool
	}
	return pool + "/" + namespace
}

type rbdReceiverIndependentIdentity struct{ mirrorUUID, peerUUID, site, client, remoteMirrorUUID string }

func rbdReceiverNativeIdentity(t *testing.T, ctx context.Context, client testcontainers.Container, pool string) rbdReceiverIndependentIdentity {
	t.Helper()
	var info struct {
		MirrorUUID *string `json:"mirror_uuid"`
		Peers      []struct {
			UUID             *string `json:"uuid"`
			Site             *string `json:"site_name"`
			Client           *string `json:"client_name"`
			Direction        *string `json:"direction"`
			RemoteMirrorUUID *string `json:"mirror_uuid"`
		} `json:"peers"`
	}
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "mirror", "pool", "info", pool, "--format", "json"), &info); err != nil || info.MirrorUUID == nil || *info.MirrorUUID == "" || len(info.Peers) != 1 {
		t.Fatalf("independent receiving pool/peer identity missing: %+v %v", info, err)
	}
	p := info.Peers[0]
	if p.UUID == nil || *p.UUID == "" || p.Site == nil || *p.Site == "" || p.Client == nil || *p.Client == "" || p.RemoteMirrorUUID == nil || p.Direction == nil || !slices.Contains([]string{"rx-only", "rx-tx"}, *p.Direction) {
		t.Fatal("independent receiving peer identity/receiving fields missing or invalid")
	}
	return rbdReceiverIndependentIdentity{*info.MirrorUUID, *p.UUID, *p.Site, *p.Client, *p.RemoteMirrorUUID}
}

func rbdReceiverNativePeerMatches(t *testing.T, current, original rbdReceiverIndependentIdentity, sourceMirrorUUID string) {
	t.Helper()
	if current.remoteMirrorUUID != "" && current.remoteMirrorUUID != sourceMirrorUUID || original.remoteMirrorUUID != "" && original.remoteMirrorUUID != sourceMirrorUUID {
		t.Fatal("independent receiving remote mirror UUID contradicts original source base UUID")
	}
	// An explicitly present empty UUID is async learning, not a new identity.
	current.remoteMirrorUUID = original.remoteMirrorUUID
	if current != original {
		t.Fatalf("independent original native receiving tuple changed: before=%+v after=%+v", original, current)
	}
}

type rbdReceiverIndependentPolicy struct{ mode, mirrorUUID, remote, site string }

func rbdReceiverNativePolicy(t *testing.T, ctx context.Context, client testcontainers.Container, pool, namespace string) rbdReceiverIndependentPolicy {
	t.Helper()
	var native struct {
		Mode       *string `json:"mode"`
		MirrorUUID *string `json:"mirror_uuid"`
		Remote     *string `json:"remote_namespace"`
		Site       *string `json:"site_name"`
	}
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "mirror", "pool", "info", rbdReceiverNativeScope(pool, namespace), "--format", "json"), &native); err != nil || native.Mode == nil || !slices.Contains([]string{"init-only", "image", "pool"}, *native.Mode) || native.MirrorUUID == nil || *native.MirrorUUID == "" || native.Remote == nil || namespace == "" && (native.Site == nil || *native.Site == "") {
		t.Fatalf("independent native original policy identity missing/invalid: namespace=%q error=%v", namespace, err)
	}
	site := ""
	if native.Site != nil {
		site = *native.Site
	}
	return rbdReceiverIndependentPolicy{*native.Mode, *native.MirrorUUID, *native.Remote, site}
}

func rbdReceiverNativeElection(t *testing.T, ctx context.Context, link *multicluster.RBDMirror, status multicluster.RBDMirrorReceiverStatus, identity rbdReceiverIndependentIdentity, mapping [2]string) {
	t.Helper()
	peer := "uuid: " + identity.peerUUID + " cluster: " + identity.site + " client: " + identity.client
	var actualIDs, leaderMembers []string
	var leaderID, leaderName string
	for _, daemon := range link.Daemons() {
		if !slices.Contains(status.ExpectedDaemons, daemon.DaemonName) {
			continue
		}
		code, data, err := rbdMultiClusterExec(ctx, daemon.Container, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
		if err != nil || code != 0 {
			t.Fatalf("fresh native socket query: exit=%d error=%v", code, err)
		}
		var native struct {
			Pools []struct {
				Pool       string   `json:"pool"`
				Peer       string   `json:"peer"`
				State      string   `json:"state"`
				Instance   string   `json:"instance_id"`
				LeaderID   string   `json:"leader_instance_id"`
				Leader     bool     `json:"leader"`
				Members    []string `json:"instances"`
				Namespaces []struct {
					Local  *string           `json:"local_namespace"`
					Remote *string           `json:"remote_namespace"`
					Images []json.RawMessage `json:"image_replayers"`
				} `json:"namespace_replayers"`
			} `json:"pool_replayers"`
		}
		if err := json.Unmarshal(data, &native); err != nil {
			t.Fatal(err)
		}
		selected := 0
		for _, row := range native.Pools {
			if row.Pool != status.Pool || row.Peer != peer {
				continue
			}
			selected++
			foundNamespace := false
			for _, ns := range row.Namespaces {
				if ns.Local == nil || ns.Remote == nil || ns.Images == nil {
					t.Fatal("independent namespace identity fields missing/null")
				}
				if *ns.Local == mapping[1] && *ns.Remote == mapping[0] {
					foundNamespace = true
				}
			}
			observed := status.Daemons[daemon.DaemonName]
			if row.State != "running" || !foundNamespace || row.Instance == "" || observed.InstanceID != row.Instance || observed.Leader != row.Leader || observed.LeaderInstanceID != row.LeaderID || observed.ContainerID != daemon.GetContainerID() {
				t.Fatalf("public observation differs from independent socket: report=%+v native=%+v", observed, row)
			}
			actualIDs = append(actualIDs, row.Instance)
			if row.Leader {
				if leaderName != "" {
					t.Fatal("independent socket saw multiple leaders")
				}
				leaderName, leaderID, leaderMembers = daemon.DaemonName, row.Instance, slices.Clone(row.Members)
			}
		}
		if selected != 1 {
			t.Fatal("independent native scope was ambiguous or missing")
		}
	}
	slices.Sort(actualIDs)
	slices.Sort(leaderMembers)
	if leaderName != status.LeaderDaemonName || leaderID != status.LeaderInstanceID || !slices.Equal(actualIDs, leaderMembers) || !slices.Equal(leaderMembers, status.Instances) || len(slices.Compact(slices.Clone(actualIDs))) != len(status.ExpectedDaemons) {
		t.Fatalf("independent exact cohort differs: leader=%s/%s members=%v public=%+v", leaderName, leaderID, leaderMembers, status)
	}
	if strings.TrimSpace(peer) != peer {
		t.Fatal("independent peer identity unexpectedly padded")
	}
}

func rbdReceiverNativeOriginalClusterPool(t *testing.T, ctx context.Context, client testcontainers.Container, pool, fsid string, poolID int64) {
	t.Helper()
	code, data, err := rbdMultiClusterExec(ctx, client, "ceph", "fsid")
	if err != nil || code != 0 || strings.TrimSpace(string(data)) != fsid {
		t.Fatalf("independent original FSID differs: exit=%d error=%v", code, err)
	}
	code, data, err = rbdMultiClusterExec(ctx, client, "ceph", "osd", "pool", "ls", "detail", "--format", "json")
	var pools []struct {
		Name string `json:"pool_name"`
		ID   int64  `json:"pool_id"`
		Type int    `json:"type"`
	}
	if err != nil || code != 0 || json.Unmarshal(data, &pools) != nil || pools == nil {
		t.Fatalf("independent native pool query failed: exit=%d error=%v", code, err)
	}
	found := 0
	for _, current := range pools {
		if current.Name == pool {
			found++
			if current.ID != poolID || current.Type != 1 {
				t.Fatal("independent original replicated pool identity changed")
			}
		}
	}
	if found != 1 {
		t.Fatal("independent original pool missing/ambiguous")
	}
}

func rbdReceiverNativeSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, pool, namespace, image string) uint64 {
	t.Helper()
	spec := rbdScopeImage(pool, namespace, image)
	output := strings.TrimSpace(string(rbdOutput(t, ctx, client, "mirror", "image", "snapshot", spec)))
	value, valid := strings.CutPrefix(output, "Snapshot ID: ")
	id, err := strconv.ParseUint(value, 10, 64)
	if !valid || err != nil || id == 0 || strconv.FormatUint(id, 10) != value {
		t.Fatalf("explicit native mirror checkpoint ID invalid: %q", output)
	}
	var status struct {
		Snapshots []struct {
			ID *uint64 `json:"id"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "mirror", "image", "status", spec, "--format", "json"), &status); err != nil {
		t.Fatal(err)
	}
	for _, snap := range status.Snapshots {
		if snap.ID != nil && *snap.ID == id {
			return id
		}
	}
	t.Fatal("explicit checkpoint ID absent from independent source mirror status")
	return 0
}
