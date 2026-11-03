//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func runMirrorInitialRBD(t *testing.T, host, journal bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.MirrorInitialPair(t, host)
	const pool = "tc-zero-rbd"
	mapping := [2]string{}
	scope, mode := rbdmirror.ScopeImage, "snapshot"
	if journal {
		mapping, scope, mode = [2]string{"ns-a", "ns-b"}, rbdmirror.ScopePool, "journal"
		if harness.RBDNamespaceMappingUnsupported(t, ctx, source) {
			mapping[1] = mapping[0] // Ceph 19 pairs only same-named namespaces.
		}
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
			t.Fatal(err)
		}
		if err := rbd.InitPool(ctx, cluster, pool); err != nil {
			t.Fatal(err)
		}
		if journal {
			for _, ns := range slices.Compact(slices.Clone(mapping[:])) {
				if _, err := rbd.CreateNamespace(ctx, cluster, pool, ns); err != nil {
					t.Fatal(err)
				}
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
	oracle := harness.MirrorInitialNewDockerOracle(t, ctx)
	var customizers atomic.Int32
	link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool, Scope: scope, SourceNamespace: mapping[0], DestinationNamespace: mapping[1], NoInitialDaemons: true}), testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
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
	if link.Container != nil || len(link.Daemons()) != 0 || customizers.Load() != 0 {
		t.Fatal("policy-only RBD construction allocated an owned process")
	}
	identity := harness.RBDReceiverNativeIdentity(t, ctx, destinationClient, pool)
	policies := [4]harness.RBDReceiverIndependentPolicy{harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""), harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""), harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
	checkPolicy := func() {
		t.Helper()
		current := [4]harness.RBDReceiverIndependentPolicy{harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""), harness.RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""), harness.RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
		if current != policies || policies[1].Mode != string(scope) || policies[3].Mode != string(scope) || policies[1].Remote != mapping[1] || policies[3].Remote != mapping[0] {
			t.Fatal("original native mirror policy/mapping changed")
		}
		harness.RBDReceiverNativeOriginalClusterPool(t, ctx, sourceClient, pool, sourceStatus.FSID, originalSourcePool.ID)
		harness.RBDReceiverNativeOriginalClusterPool(t, ctx, destinationClient, pool, destinationStatus.FSID, originalDestinationPool.ID)
		harness.RBDReceiverNativePeerMatches(t, harness.RBDReceiverNativeIdentity(t, ctx, destinationClient, pool), identity, policies[0].MirrorUUID)
	}
	checkPolicy()
	zero, err := link.ReceiverStatus(ctx)
	if err != nil || zero.Ready || zero.PeerID != identity.PeerUUID || len(zero.Daemons) != 0 {
		t.Fatalf("successful zero construction lacked confirmed original observer capability: %+v %v", zero, err)
	}
	zeroCtx, zeroCancel := context.WithTimeout(ctx, 2*time.Second)
	zero, err = link.WaitReceiverReady(zeroCtx)
	zeroCancel()
	if !errors.Is(err, context.DeadlineExceeded) || zero.Ready || zero.PeerID != identity.PeerUUID || customizers.Load() != 0 {
		t.Fatal("zero wait started/adopted a receiver", err)
	}
	oracle.AssertPolicyOnly(t, ctx, 2, "rbd-"+mode)
	payload := harness.RBDMultiClusterPayload(2<<20, 71)
	harness.RBDScopeCreate(t, ctx, sourceClient, pool, mapping[0], "backlog", journal)
	harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
	if !journal {
		harness.RBDScopeAssertUnmirrored(t, ctx, sourceClient, harness.RBDScopeImage(pool, mapping[0], "backlog"))
	}
	oracle.AssertPolicyOnly(t, ctx, 2, "rbd-backlog-"+mode)
	if journal {
		partial, err := link.AddDaemon(ctx, "partial", testcontainers.WithWaitStrategy(wait.ForExec([]string{"sh", "-c", "exit 1"}).WithStartupTimeout(2*time.Second)))
		if partial == nil || err == nil || link.Container != partial || len(link.Daemons()) != 1 || customizers.Load() != 1 {
			t.Fatal("first partial Add lost owned/embedded handle", err)
		}
		if status, _ := link.ReceiverStatus(ctx); status.Ready {
			t.Fatal("partial startup blessed as Ready")
		}
		if err := partial.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		if link.Container != nil || len(link.Daemons()) != 0 {
			t.Fatal("partial removal did not clear owned initial process")
		}
	}
	receiver, err := link.AddDaemon(ctx, "receiver")
	if err != nil || receiver == nil || len(link.Daemons()) != 1 {
		t.Fatal("explicit delayed receiver failed", err)
	}
	wantCustomizers := int32(1)
	if journal {
		wantCustomizers = 2
		if link.Container != nil {
			t.Fatal("later Add rebound removed initial partial embedded handle")
		}
	} else if link.Container != receiver {
		t.Fatal("normal first Add did not expose original embedded handle")
	}
	if customizers.Load() != wantCustomizers {
		t.Fatal("daemon customizer ran outside explicit Add")
	}
	ready, err := link.WaitReceiverReady(ctx)
	if err != nil || !ready.Ready || ready.PeerID != identity.PeerUUID || ready.SourceFSID != sourceStatus.FSID || ready.DestinationFSID != destinationStatus.FSID || ready.SourcePoolID != originalSourcePool.ID || ready.DestinationPoolID != originalDestinationPool.ID {
		t.Fatalf("delayed original receiver readiness: %+v %v", ready, err)
	}
	checkPolicy()
	harness.RBDReceiverNativeElection(t, ctx, link, ready, identity, mapping)
	harness.RBDScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	var checkpoint uint64
	if !journal {
		// A fresh zero Run has no hidden transmitter discovery. Enroll only after
		// the explicit first receiver; checkpoint a subsequent ordinary write.
		if err := link.EnableImage(ctx, "backlog"); err != nil {
			t.Fatal(err)
		}
		payload = harness.RBDMultiClusterPayload(2<<20, 83)
		harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
		checkpoint = harness.RBDReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "backlog")
	}
	if _, err := link.WaitReplayReady(ctx, "backlog"); err != nil {
		t.Fatal(err)
	}
	harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "backlog", payload)
	harness.RBDScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
	harness.RBDScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, mapping[0], mapping[1], "backlog", mode)
	checkPolicy()
	t.Logf("MIRROR_INITIAL_RBD_BYTES mode=%s bytes=%d sha256=%x checkpoint_id=%d normal_first=%t customizers=%d", mode, len(payload), sha256.Sum256(payload), checkpoint, !journal, customizers.Load())
}

func runMirrorInitialCephFS(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.MirrorInitialPair(t, host)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	const seed, added, script, site = "/daemon-a", "/zero-added", "/tmp/cephfs-zero-initial-probe.py", "zero-initial-target"
	probe := strings.Replace(harness.CephFSOriginalProcessProbeScript, "assert directory == '/daemon-a'", "assert directory in ('/daemon-a', '/zero-added')", 1)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(probe), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(directory, name string) cephfsmirror.Snapshot {
		t.Helper()
		harness.MultiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	seedBacklog := checkpoint(seed, "seed-policy-only-backlog")
	expected := harness.CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	oracle := harness.MirrorInitialNewDockerOracle(t, ctx)
	var customizers, factoryCalls atomic.Int32
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName, DestinationSite: site, Directories: []string{seed}, NoInitialDaemons: true, OriginalProcessClientFactory: func(ctx context.Context) (*mobycl.Client, error) {
		factoryCalls.Add(1)
		client, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			return nil, err
		}
		return client.Client, nil
	}}), testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	if mirror != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if err := mirror.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if mirror.Container != nil || len(mirror.Daemons()) != 0 || customizers.Load() != 0 || factoryCalls.Load() != 0 {
		t.Fatal("CephFS policy-only Run allocated process/observer client")
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatal("zero construction lacked original peer", err)
	}
	expected.PeerID = peers[0]
	policy := harness.CephFSOriginalProcessPolicy{Source: source, Destination: destination, SourceFS: sourceFS, DestinationFS: destinationFS, Expected: expected, Directory: seed, Client: mirror.DestinationClientEntity, Site: site}
	if err := policy.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	harness.CephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed})
	oracle.AssertPolicyOnly(t, ctx, 0, "cephfs-seed")
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, seed, seedBacklog.Name, "zero-initial-seed", 5*time.Second)
	addedBacklog := checkpoint(added, "typed-policy-only-backlog")
	addition, err := mirror.BeginDirectoryAddition(ctx, added)
	if addition == nil {
		t.Fatal("zero typed addition lost retained intent", err)
	}
	harness.CephFSDirectoryAdditionReconcile(t, ctx, mirror, added, addition)
	status, err := addition.Status(ctx)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
	if err != nil || !slices.Equal(mirror.Directories, []string{seed, added}) || len(mirror.Daemons()) != 0 || customizers.Load() != 0 || factoryCalls.Load() != 0 {
		t.Fatal("zero typed addition started or adopted process", err)
	}
	harness.CephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, added})
	oracle.AssertPolicyOnly(t, ctx, 0, "cephfs-typed-addition")
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, addedBacklog.Name, "zero-initial-added", 5*time.Second)
	zeroCtx, zeroCancel := context.WithTimeout(ctx, 2*time.Second)
	zero, err := mirror.WaitSnapshotSynced(zeroCtx, added, addedBacklog)
	zeroCancel()
	if !errors.Is(err, context.DeadlineExceeded) || zero.Ready || customizers.Load() != 0 || factoryCalls.Load() != 0 {
		t.Fatalf("zero wait created receiver: %+v %v", zero, err)
	}
	receiver, err := mirror.AddDaemon(ctx, "receiver")
	if err != nil || receiver == nil || mirror.Container != receiver || len(mirror.Daemons()) != 1 || customizers.Load() != 1 || factoryCalls.Load() != 1 || !receiver.ProcessObserverBindingStatus().Available {
		t.Fatal("normal first explicit Add lost process/binding", err)
	}
	verify := func(directory string, snapshot cephfsmirror.Snapshot, stage string) {
		t.Helper()
		harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, snapshot)
		for _, role := range []string{"source", "destination"} {
			client, fs := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, fs = destinationClient, destinationFS.FilesystemName
			}
			harness.CephFSWaitOriginalProcessBytes(t, ctx, client, fs, script, directory, snapshot.Name, role, stage)
		}
		if current := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, snapshot.Name); current != snapshot {
			t.Fatal("original independent source snapshot changed")
		}
	}
	verify(seed, seedBacklog, "zero-initial-seed-replayed")
	verify(added, addedBacklog, "zero-initial-typed-replayed")
	verify(added, checkpoint(added, "normal-first-fresh"), "zero-initial-fresh")
	if err := receiver.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	harness.CephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	partial, err := mirror.AddDaemon(ctx, "partial", testcontainers.WithWaitStrategy(wait.ForExec([]string{"sh", "-c", "exit 1"}).WithStartupTimeout(2*time.Second)))
	if partial == nil || err == nil || len(mirror.Daemons()) != 1 || mirror.Container != nil || customizers.Load() != 2 || factoryCalls.Load() != 1 || partial.ProcessObserverBindingStatus().State != "partial-startup" {
		t.Fatal("explicit partial startup lost cleanup/factory contract", err)
	}
	if err := partial.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	harness.CephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	resumed, err := mirror.AddDaemon(ctx, "resumed")
	if err != nil || resumed == nil || mirror.Container != nil || len(mirror.Daemons()) != 1 || customizers.Load() != 3 || factoryCalls.Load() != 2 || !resumed.ProcessObserverBindingStatus().Available {
		t.Fatal("explicit resumed process rebound original embedded handle/factory", err)
	}
	verify(added, checkpoint(added, "after-explicit-partial"), "zero-initial-partial-resumed")
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, added}); err != nil {
		t.Fatal(err)
	}
	t.Logf("MIRROR_INITIAL_CEPHFS_COMPLETE source_fsid=%s destination_fsid=%s peer=%s customizers=%d raw_factory_calls=%d directories=%q", policy.FSIDs[0], policy.FSIDs[1], expected.PeerID, customizers.Load(), factoryCalls.Load(), mirror.Directories)
}
