//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"crypto/sha256"
	"errors"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type MirrorInitialDockerOracle struct {
	Client *mobycl.Client
	Engine string
	Before map[string]bool
}

func MirrorInitialNewDockerOracle(t *testing.T, ctx context.Context) MirrorInitialDockerOracle {
	t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	info, err := client.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID == "" {
		t.Fatal("uncached original Docker engine identity unavailable", err)
	}
	listed, err := client.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]bool{}
	for _, item := range listed.Items {
		if item.ID == "" || before[item.ID] {
			t.Fatal("invalid original Docker full container inventory")
		}
		before[item.ID] = true
	}
	return MirrorInitialDockerOracle{Client: client.Client, Engine: info.Info.ID, Before: before}
}

// This test owns a fresh pair and runs exclusively. The uncached engine/list/
// exact-CID oracle counts new containers, including stopped partial processes.
// It is a local resource proof, not a promise that a shared pool has no peers.
func (o MirrorInitialDockerOracle) AssertPolicyOnly(t *testing.T, ctx context.Context, cliCount int, kind string) {
	t.Helper()
	info, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != o.Engine {
		t.Fatal("original Docker engine changed", err)
	}
	listed, err := o.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, item := range listed.Items {
		if item.ID == "" {
			t.Fatal("empty native full container ID")
		}
		if o.Before[item.ID] {
			continue
		}
		created++
		inspected, err := o.Client.ContainerInspect(ctx, item.ID, mobycl.ContainerInspectOptions{})
		if err != nil || inspected.Container.ID != item.ID || inspected.Container.Config == nil || inspected.Container.State == nil {
			t.Fatal("fresh exact-CID construction inspection unavailable", err)
		}
		config := inspected.Container.Config
		for _, entry := range config.Entrypoint {
			if path.Base(entry) == "rbd-mirror" || path.Base(entry) == "cephfs-mirror" {
				t.Fatal("zero construction launched a hidden mirror process")
			}
		}
		if !slices.Equal(config.Entrypoint, []string{"sleep"}) || !slices.Equal(config.Cmd, []string{"infinity"}) || !inspected.Container.State.Running {
			t.Fatal("unexpected construction container outside retained setup CLIs")
		}
	}
	after, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.Engine || created != cliCount {
		t.Fatalf("zero construction resource graph: kind=%s new=%d want=%d err=%v", kind, created, cliCount, err)
	}
	t.Logf("MIRROR_INITIAL_ZERO kind=%s engine=%s new_cli_containers=%d owned_daemons=0", kind, o.Engine, created)
}

func MirrorInitialPair(t *testing.T, host bool) (*ceph.Container, *ceph.Container, testcontainers.Container, testcontainers.Container) {
	t.Helper()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	return NewMultiClusterPair(t, opts...)
}

func RunMirrorInitialRBD(t *testing.T, host, journal bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := MirrorInitialPair(t, host)
	const pool = "tc-zero-rbd"
	mapping := [2]string{}
	scope, mode := rbdmirror.ScopeImage, "snapshot"
	if journal {
		mapping, scope, mode = [2]string{"ns-a", "ns-b"}, rbdmirror.ScopePool, "journal"
		if RBDNamespaceMappingUnsupported(t, ctx, source) {
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
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	originalSourcePool, err := PoolHandle(t, ctx, source, pool).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	originalDestinationPool, err := PoolHandle(t, ctx, destination, pool).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sourceStatus, err := ObserveCluster(source).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := ObserveCluster(destination).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oracle := MirrorInitialNewDockerOracle(t, ctx)
	var customizers atomic.Int32
	link, err := rbdmirror.Run(ctx, NativeMirrorImage(t, "rbd-aux", StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool, Scope: scope, SourceNamespace: mapping[0], DestinationNamespace: mapping[1], NoInitialDaemons: true}), testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
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
	identity := RBDReceiverNativeIdentity(t, ctx, destinationClient, pool)
	policies := [4]RBDReceiverIndependentPolicy{RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""), RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""), RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
	checkPolicy := func() {
		t.Helper()
		current := [4]RBDReceiverIndependentPolicy{RBDReceiverNativePolicy(t, ctx, sourceClient, pool, ""), RBDReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), RBDReceiverNativePolicy(t, ctx, destinationClient, pool, ""), RBDReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
		if current != policies || policies[1].Mode != string(scope) || policies[3].Mode != string(scope) || policies[1].Remote != mapping[1] || policies[3].Remote != mapping[0] {
			t.Fatal("original native mirror policy/mapping changed")
		}
		RBDReceiverNativeOriginalClusterPool(t, ctx, sourceClient, pool, sourceStatus.FSID, originalSourcePool.ID)
		RBDReceiverNativeOriginalClusterPool(t, ctx, destinationClient, pool, destinationStatus.FSID, originalDestinationPool.ID)
		RBDReceiverNativePeerMatches(t, RBDReceiverNativeIdentity(t, ctx, destinationClient, pool), identity, policies[0].MirrorUUID)
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
	payload := RBDMultiClusterPayload(2<<20, 71)
	RBDScopeCreate(t, ctx, sourceClient, pool, mapping[0], "backlog", journal)
	RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
	if !journal {
		RBDScopeAssertUnmirrored(t, ctx, sourceClient, RBDScopeImage(pool, mapping[0], "backlog"))
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
	RBDReceiverNativeElection(t, ctx, link, ready, identity, mapping)
	RBDScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	var checkpoint uint64
	if !journal {
		// A fresh zero Run has no hidden transmitter discovery. Enroll only after
		// the explicit first receiver; checkpoint a subsequent ordinary write.
		if err := link.EnableImage(ctx, "backlog"); err != nil {
			t.Fatal(err)
		}
		payload = RBDMultiClusterPayload(2<<20, 83)
		RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
		checkpoint = RBDReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "backlog")
	}
	if _, err := link.WaitReplayReady(ctx, "backlog"); err != nil {
		t.Fatal(err)
	}
	RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "backlog", payload)
	RBDScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "backlog", 0, payload)
	RBDScopeAssertReplicaIdentity(t, ctx, sourceClient, destinationClient, pool, mapping[0], mapping[1], "backlog", mode)
	checkPolicy()
	t.Logf("MIRROR_INITIAL_RBD_BYTES mode=%s bytes=%d sha256=%x checkpoint_id=%d normal_first=%t customizers=%d", mode, len(payload), sha256.Sum256(payload), checkpoint, !journal, customizers.Load())
}

func RunMirrorInitialCephFS(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := MirrorInitialPair(t, host)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	const seed, added, script, site = "/daemon-a", "/zero-added", "/tmp/cephfs-zero-initial-probe.py", "zero-initial-target"
	probe := strings.Replace(CephFSOriginalProcessProbeScript, "assert directory == '/daemon-a'", "assert directory in ('/daemon-a', '/zero-added')", 1)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(probe), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(directory, name string) cephfsmirror.Snapshot {
		t.Helper()
		MultiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	seedBacklog := checkpoint(seed, "seed-policy-only-backlog")
	expected := CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	oracle := MirrorInitialNewDockerOracle(t, ctx)
	var customizers, factoryCalls atomic.Int32
	mirror, err := cephfsmirror.Run(ctx, NativeMirrorImage(t, "cephfs", StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName, DestinationSite: site, Directories: []string{seed}, NoInitialDaemons: true, OriginalProcessClientFactory: func(ctx context.Context) (*mobycl.Client, error) {
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
	policy := CephFSOriginalProcessPolicy{Source: source, Destination: destination, SourceFS: sourceFS, DestinationFS: destinationFS, Expected: expected, Directory: seed, Client: mirror.DestinationClientEntity, Site: site}
	if err := policy.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed})
	oracle.AssertPolicyOnly(t, ctx, 0, "cephfs-seed")
	CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, seed, seedBacklog.Name, "zero-initial-seed", 5*time.Second)
	addedBacklog := checkpoint(added, "typed-policy-only-backlog")
	addition, err := mirror.BeginDirectoryAddition(ctx, added)
	if addition == nil {
		t.Fatal("zero typed addition lost retained intent", err)
	}
	cephFSDirectoryAdditionReconcile(t, ctx, mirror, added, addition)
	status, err := addition.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
	if err != nil || !slices.Equal(mirror.Directories, []string{seed, added}) || len(mirror.Daemons()) != 0 || customizers.Load() != 0 || factoryCalls.Load() != 0 {
		t.Fatal("zero typed addition started or adopted process", err)
	}
	cephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, added})
	oracle.AssertPolicyOnly(t, ctx, 0, "cephfs-typed-addition")
	CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, addedBacklog.Name, "zero-initial-added", 5*time.Second)
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
		CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, snapshot)
		for _, role := range []string{"source", "destination"} {
			client, fs := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, fs = destinationClient, destinationFS.FilesystemName
			}
			CephFSWaitOriginalProcessBytes(t, ctx, client, fs, script, directory, snapshot.Name, role, stage)
		}
		if current := CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, snapshot.Name); current != snapshot {
			t.Fatal("original independent source snapshot changed")
		}
	}
	verify(seed, seedBacklog, "zero-initial-seed-replayed")
	verify(added, addedBacklog, "zero-initial-typed-replayed")
	verify(added, checkpoint(added, "normal-first-fresh"), "zero-initial-fresh")
	if err := receiver.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	partial, err := mirror.AddDaemon(ctx, "partial", testcontainers.WithWaitStrategy(wait.ForExec([]string{"sh", "-c", "exit 1"}).WithStartupTimeout(2*time.Second)))
	if partial == nil || err == nil || len(mirror.Daemons()) != 1 || mirror.Container != nil || customizers.Load() != 2 || factoryCalls.Load() != 1 || partial.ProcessObserverBindingStatus().State != "partial-startup" {
		t.Fatal("explicit partial startup lost cleanup/factory contract", err)
	}
	if err := partial.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	resumed, err := mirror.AddDaemon(ctx, "resumed")
	if err != nil || resumed == nil || mirror.Container != nil || len(mirror.Daemons()) != 1 || customizers.Load() != 3 || factoryCalls.Load() != 2 || !resumed.ProcessObserverBindingStatus().Available {
		t.Fatal("explicit resumed process rebound original embedded handle/factory", err)
	}
	verify(added, checkpoint(added, "after-explicit-partial"), "zero-initial-partial-resumed")
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, added}); err != nil {
		t.Fatal(err)
	}
	t.Logf("MIRROR_INITIAL_CEPHFS_COMPLETE source_fsid=%s destination_fsid=%s peer=%s customizers=%d raw_factory_calls=%d directories=%q", policy.fsids[0], policy.fsids[1], expected.PeerID, customizers.Load(), factoryCalls.Load(), mirror.Directories)
}
