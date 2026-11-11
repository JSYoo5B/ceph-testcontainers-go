//go:build all || (integration && multicluster)

package rbd_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdexpect "github.com/JSYoo5B/ceph-testcontainers-go/rbd/expect"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func rbdReceiverNativeScope(pool, namespace string) string {
	if namespace == "" {
		return pool
	}
	return pool + "/" + namespace
}

func runRBDReceiverReadiness(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
	for index, mapping := range [][2]string{{"", ""}, {"ns-a", "ns-b"}, {"ns-a", ""}, {"", "ns-b"}, {"ns-a", "ns-b"}} {
		t.Run(fmt.Sprintf("scope-%d", index), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			pool := fmt.Sprintf("tc-rbd-receiver-%d", index)
			scope := rbdmirror.ScopeImage
			mode := "snapshot"
			if index == 4 {
				scope, mode = rbdmirror.ScopePool, "journal"
			}
			for _, cluster := range []*ceph.Container{source, destination} {
				if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
					t.Fatal(err)
				}
				if err := rbd.InitPool(ctx, cluster, pool); err != nil {
					t.Fatal(err)
				}
				for _, ns := range []string{"ns-a", "ns-b"} {
					if _, err := rbd.CreateNamespace(ctx, cluster, pool, ns); err != nil {
						t.Fatal(err)
					}
				}
				if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
			}
			originalSourcePool, err := harness.PoolHandle(t, ctx, source, pool).Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			originalDestinationPool, err := harness.PoolHandle(t, ctx, destination, pool).Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sourceStatus, err := harness.ObserveCluster(source).Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			destinationStatus, err := harness.ObserveCluster(destination).Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool, Scope: scope, SourceNamespace: mapping[0], DestinationNamespace: mapping[1], DaemonCount: 2}))
			if link != nil {
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
					defer done()
					if err := link.Terminate(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if mapping[0] != mapping[1] && harness.RBDNamespaceMappingUnsupported(t, ctx, source) {
				requireRBDNamespaceMappingRefused(t, err, source, mapping)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			identity := harness.RBDReceiverNativeIdentity(t, ctx, destinationClient, pool)
			originalPolicies := [4]harness.RBDReceiverIndependentPolicy{
				harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""),
				harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]),
				harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""),
				harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1]),
			}
			if originalPolicies[1].Mode != string(scope) || originalPolicies[3].Mode != string(scope) || originalPolicies[1].Remote != mapping[1] || originalPolicies[3].Remote != mapping[0] {
				t.Fatal("independent original native selected policy differs from configured scope/mapping")
			}
			harness.RBDReceiverNativePeerMatches(t, identity, identity, originalPolicies[0].MirrorUUID)
			check := func(phase string, names ...string) rbdmirror.ReceiverStatus {
				t.Helper()
				status, err := link.WaitReceiverReady(ctx, names...)
				if err != nil || !status.Ready {
					t.Fatalf("%s readiness: %+v %v", phase, status, err)
				}
				if status.SourceFSID != sourceStatus.FSID || status.DestinationFSID != destinationStatus.FSID || status.SourcePoolID != originalSourcePool.ID || status.DestinationPoolID != originalDestinationPool.ID || status.PeerID != identity.PeerUUID || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] {
					t.Fatalf("%s public identity differs: %+v", phase, status)
				}
				currentPolicies := [4]harness.RBDReceiverIndependentPolicy{
					harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""),
					harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]),
					harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""),
					harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1]),
				}
				if currentPolicies != originalPolicies {
					t.Fatalf("%s original native base/selected policies changed: before=%+v after=%+v", phase, originalPolicies, currentPolicies)
				}
				harness.RBDReceiverNativePeerMatches(t, harness.RBDReceiverNativeIdentity(t, ctx, destinationClient, pool), identity, originalPolicies[0].MirrorUUID)
				harness.RBDReceiverNativeOriginalClusterPool(t, ctx, sourceClient, pool, sourceStatus.FSID, originalSourcePool.ID)
				harness.RBDReceiverNativeOriginalClusterPool(t, ctx, destinationClient, pool, destinationStatus.FSID, originalDestinationPool.ID)
				harness.RBDReceiverNativeElection(t, ctx, link, status, identity, mapping)
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
				if err := json.Unmarshal(harness.RBDOutput(t, ctx, site.client, "ls", rbdReceiverNativeScope(pool, site.ns), "--format", "json"), &images); err != nil || images == nil || len(images) != 0 {
					t.Fatalf("initial readiness used nonempty image namespace: %v %v", images, err)
				}
			}
			var leader, survivor *rbdmirror.Daemon
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
			cid := leader.GetContainerID()
			if err := leader.Start(ctx); err != nil {
				t.Fatal(err)
			}
			restarted := check("same-cid-restart")
			restart := rbdexpect.Receivers{Select: []rbdexpect.ReceiverDaemon{{Name: leader.DaemonName, Baseline: initial.Daemons[leader.DaemonName], Running: true, NewInstance: true}}}
			if result, err := restart.Evaluate(restarted); leader.GetContainerID() != cid || err != nil || !result.Matched {
				t.Fatalf("same container restart did not preserve CID/change native instance: %v %v", result, err)
			}
			removed := false
			for _, daemon := range link.Daemons() {
				if daemon.DaemonName == restarted.LeaderDaemonName {
					if err := daemon.Remove(ctx); err != nil {
						t.Fatal(err)
					}
					removed = true
				}
			}
			if !removed {
				t.Fatalf("restarted leader %s is not owned", restarted.LeaderDaemonName)
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
			if err := partial.Remove(ctx); err != nil {
				t.Fatal(err)
			}
			check("partial-removed")
			for _, daemon := range link.Daemons() {
				if err := daemon.Remove(ctx); err != nil {
					t.Fatal(err)
				}
			}
			zeroCtx, zeroCancel := context.WithTimeout(ctx, 3*time.Second)
			zero, err := link.WaitReceiverReady(zeroCtx)
			zeroCancel()
			if !errors.Is(err, context.DeadlineExceeded) || zero.Ready || len(link.Daemons()) != 0 || zero.PeerID != identity.PeerUUID {
				t.Fatalf("zero receiver deadline: %+v %v", zero, err)
			}
			payload := harness.RBDMultiClusterPayload(2<<20, 43+index)
			harness.RBDScopeCreate(t, ctx, sourceClient, pool, mapping[0], "backlog", scope == rbdmirror.ScopePool)
			if scope == rbdmirror.ScopeImage {
				if err := link.EnableImage(ctx, "backlog"); err != nil {
					t.Fatal(err)
				}
			}
			harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
			var checkpointID uint64
			if scope == rbdmirror.ScopeImage {
				// Enrollment's initial snapshot predates payload; this explicit
				// checkpoint causally includes the write made with zero receivers.
				checkpointID = harness.RBDReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "backlog")
				t.Logf("RBD_RECEIVER_CHECKPOINT pool=%s namespace=%q snapshot_id=%d", pool, mapping[0], checkpointID)
			}
			if _, err := link.AddDaemon(ctx, "resumed"); err != nil {
				t.Fatal(err)
			}
			check("zero-resumed")
			if _, err := link.WaitReplayReady(ctx, "backlog"); err != nil {
				t.Fatal(err)
			}
			harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "backlog", payload)
			harness.RBDScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
			harness.RBDScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, mapping[0], mapping[1], "backlog", mode)
			t.Logf("RBD_RECEIVER_BYTES pool=%s bytes=%d sha256=%x mapping=%q mode=%s checkpoint_id=%d", pool, len(payload), sha256.Sum256(payload), mapping, mode, checkpointID)
		})
	}
}
