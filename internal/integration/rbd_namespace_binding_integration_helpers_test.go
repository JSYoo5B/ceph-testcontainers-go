//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

type rbdNamespaceBindingNativeImage struct {
	sourceID, destinationID, globalID string
}

func rbdNamespaceBindingNativeIdentity(t *testing.T, ctx context.Context, source, destination testcontainers.Container, pool string, mapping [2]string, mode string) rbdNamespaceBindingNativeImage {
	t.Helper()
	rbdScopeAssertReplicaIdentity(t, ctx, source, destination, pool, mapping[0], mapping[1], "volume", mode)
	var result rbdNamespaceBindingNativeImage
	for index, site := range []struct {
		client testcontainers.Container
		ns     string
	}{{source, mapping[0]}, {destination, mapping[1]}} {
		var info struct {
			ID        *string `json:"id"`
			Mirroring *struct {
				GlobalID *string `json:"global_id"`
			} `json:"mirroring"`
		}
		if err := json.Unmarshal(rbdOutput(t, ctx, site.client, "info", rbdScopeImage(pool, site.ns, "volume"), "--format", "json"), &info); err != nil || info.ID == nil || *info.ID == "" || info.Mirroring == nil || info.Mirroring.GlobalID == nil || *info.Mirroring.GlobalID == "" {
			t.Fatal("independent native original image IDs missing", err)
		}
		global, err := uuid.Parse(*info.Mirroring.GlobalID)
		if err != nil || global == uuid.Nil || global.String() != *info.Mirroring.GlobalID {
			t.Fatal("independent mirrored global image identity is not a positive canonical UUID")
		}
		if index == 0 {
			result.sourceID, result.globalID = *info.ID, *info.Mirroring.GlobalID
		} else {
			result.destinationID = *info.ID
			if *info.Mirroring.GlobalID != result.globalID {
				t.Fatal("namespace binding delivered another image's replica")
			}
		}
	}
	return result
}

func testRBDNamespaceBinding(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := mirrorInitialPair(t, host)
	const pool = "tc-shared-binding"
	mappings := [][2]string{{"ns-a", "ns-b"}, {"ns-c", "ns-d"}, {"ns-e", "ns-f"}}
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
			t.Fatal(err)
		}
		if err := rbd.InitPool(ctx, cluster, pool); err != nil {
			t.Fatal(err)
		}
		for _, ns := range []string{"ns-a", "ns-b", "ns-c", "ns-d", "ns-e", "ns-f", "isolated"} {
			if _, err := rbd.CreateNamespace(ctx, cluster, pool, ns); err != nil {
				t.Fatal(err)
			}
		}
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sourcePool, err := source.PoolStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := destination.PoolStatus(ctx, pool)
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
	controlPayloads := [2][]byte{rbdMultiClusterPayload(2<<20, 119), rbdMultiClusterPayload(2<<20, 137)}
	for role, site := range []struct {
		client testcontainers.Container
		fsid   string
	}{{sourceClient, sourceStatus.FSID}, {destinationClient, destinationStatus.FSID}} {
		for _, ns := range []string{"", "isolated"} {
			rbdScopeCreate(t, ctx, site.client, pool, ns, "volume", true)
			rbdScopeIO(t, ctx, site.client, "write", site.fsid, pool, ns, "volume", 0, controlPayloads[role])
			rbdScopeAssertUnmirrored(t, ctx, site.client, rbdScopeImage(pool, ns, "volume"))
		}
	}
	payloads := make([][]byte, len(mappings))
	for index, mapping := range mappings {
		payloads[index] = rbdMultiClusterPayload(2<<20, 23+31*index)
		rbdScopeCreate(t, ctx, sourceClient, pool, mapping[0], "volume", index == 2)
		rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
		rbdScopeAssertUnmirrored(t, ctx, sourceClient, rbdScopeImage(pool, mapping[0], "volume"))
	}
	oracle := mirrorInitialNewDockerOracle(t, ctx)
	var customizers atomic.Int32
	owner, err := rbd.RunMirror(ctx, source.ControlImage(), rbd.MirrorConfig{Source: source, Destination: destination, Pool: pool, SourceNamespace: mappings[0][0], DestinationNamespace: mappings[0][1], NoInitialDaemons: true}, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	if owner != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if err := owner.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if rbdNamespaceMappingUnsupported(source) {
		requireRBDNamespaceMappingRefused(t, err, mappings[0])
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	base := [2]rbdReceiverIndependentPolicy{rbdReceiverNativePolicy(t, ctx, sourceClient, pool, ""), rbdReceiverNativePolicy(t, ctx, destinationClient, pool, "")}
	if base[0].mode != "init-only" || base[1].mode != "init-only" {
		t.Fatal("named owner activated an unrequested default policy")
	}
	identity := rbdReceiverNativeIdentity(t, ctx, destinationClient, pool)
	// The caller configures existing sibling namespace policies explicitly. Bind
	// performs only readback; no second Run or peer/daemon bootstrap is involved.
	for index, mapping := range mappings {
		if index == 0 {
			continue
		}
		scope := "image"
		if index == 2 {
			scope = "pool"
		}
		for _, site := range []struct {
			command          func(context.Context, ...string) ([]byte, error)
			ns, remote, site string
		}{{owner.SourceRBD, mapping[0], mapping[1], "source"}, {owner.DestinationRBD, mapping[1], mapping[0], "destination"}} {
			if _, err := site.command(ctx, "mirror", "pool", "enable", "--site-name", site.site, rbdReceiverNativeScope(pool, site.ns), scope, "--remote-namespace", site.remote); err != nil {
				t.Fatal(err)
			}
		}
	}
	views := make([]*rbd.MirrorNamespace, len(mappings))
	policies := make([][2]rbdReceiverIndependentPolicy, len(mappings))
	for index, mapping := range mappings {
		views[index], err = owner.BindNamespace(ctx, mapping[0], mapping[1])
		if err != nil || views[index] == nil {
			t.Fatal("bind explicit native sibling", err)
		}
		policies[index] = [2]rbdReceiverIndependentPolicy{rbdReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), rbdReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
		wantScope := "image"
		if index == 2 {
			wantScope = "pool"
		}
		if policies[index][0].mode != wantScope || policies[index][1].mode != wantScope || policies[index][0].remote != mapping[1] || policies[index][1].remote != mapping[0] {
			t.Fatal("independent sibling original policy is incompatible")
		}
		zero, err := views[index].ReceiverStatus(ctx)
		if err != nil || zero.Ready || len(zero.Daemons) != 0 || zero.PeerID != identity.peerUUID {
			t.Fatalf("zero shared owner binding: %+v %v", zero, err)
		}
	}
	if len(owner.Daemons()) != 0 || owner.Container != nil || customizers.Load() != 0 {
		t.Fatal("binding allocated daemon resources or customized receiver")
	}
	oracle.assertPolicyOnly(t, ctx, 2, "rbd-shared-bindings")
	checkPolicies := func() {
		t.Helper()
		if current := [2]rbdReceiverIndependentPolicy{rbdReceiverNativePolicy(t, ctx, sourceClient, pool, ""), rbdReceiverNativePolicy(t, ctx, destinationClient, pool, "")}; current != base {
			t.Fatal("binding silently rebased original default policies")
		}
		for index, mapping := range mappings {
			current := [2]rbdReceiverIndependentPolicy{rbdReceiverNativePolicy(t, ctx, sourceClient, pool, mapping[0]), rbdReceiverNativePolicy(t, ctx, destinationClient, pool, mapping[1])}
			if current != policies[index] {
				t.Fatalf("original sibling policy changed: mapping=%q before=%+v after=%+v", mapping, policies[index], current)
			}
		}
		rbdReceiverNativeOriginalClusterPool(t, ctx, sourceClient, pool, sourceStatus.FSID, sourcePool.ID)
		rbdReceiverNativeOriginalClusterPool(t, ctx, destinationClient, pool, destinationStatus.FSID, destinationPool.ID)
		rbdReceiverNativePeerMatches(t, rbdReceiverNativeIdentity(t, ctx, destinationClient, pool), identity, base[0].mirrorUUID)
	}
	check := func(phase string) []rbd.MirrorReceiverStatus {
		t.Helper()
		checkPolicies()
		statuses := make([]rbd.MirrorReceiverStatus, len(views))
		for index, view := range views {
			status, err := view.WaitReceiverReady(ctx)
			if err != nil || !status.Ready || status.SourceNamespace != mappings[index][0] || status.DestinationNamespace != mappings[index][1] || status.PeerID != identity.peerUUID || status.SourceFSID != sourceStatus.FSID || status.DestinationFSID != destinationStatus.FSID || status.SourcePoolID != sourcePool.ID || status.DestinationPoolID != destinationPool.ID {
				t.Fatalf("%s bound scope readiness: %+v %v", phase, status, err)
			}
			rbdReceiverNativeElection(t, ctx, owner, status, identity, mappings[index])
			if index > 0 {
				previous := statuses[0]
				if status.LeaderInstanceID != previous.LeaderInstanceID || status.LeaderDaemonName != previous.LeaderDaemonName || !slices.Equal(status.Instances, previous.Instances) || !slices.Equal(status.ExpectedDaemons, previous.ExpectedDaemons) {
					t.Fatal("namespace views elected separate owner cohorts")
				}
				for name, daemon := range status.Daemons {
					if daemon.ContainerID != previous.Daemons[name].ContainerID || daemon.ClientName != previous.Daemons[name].ClientName || daemon.InstanceID != previous.Daemons[name].InstanceID {
						t.Fatal("bound views adopted separate actual processes")
					}
				}
			}
			statuses[index] = status
			encoded, _ := json.Marshal(map[string]any{"phase": phase, "mapping": mappings[index], "status": status})
			t.Logf("RBD_NAMESPACE_BINDING_READY %s", encoded)
		}
		checkPolicies()
		return statuses
	}
	receiver, err := owner.AddDaemon(ctx, "receiver")
	if err != nil || receiver == nil || owner.Container != receiver || customizers.Load() != 1 {
		t.Fatal("one explicit shared cohort failed", err)
	}
	initial := check("one-receiver")
	rbdScenarioWaitTransmitPeer(t, ctx, owner.SourceRBD, pool, "destination")
	checkpoints := make([]uint64, len(mappings))
	for index, mapping := range mappings {
		if index == 2 {
			continue
		}
		if _, err := owner.SourceRBD(ctx, "mirror", "image", "enable", rbdScopeImage(pool, mapping[0], "volume"), "snapshot"); err != nil {
			t.Fatal(err)
		}
		payloads[index] = rbdMultiClusterPayload(2<<20, 41+43*index)
		rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
		checkpoints[index] = rbdReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "volume")
	}
	images := make([]rbdNamespaceBindingNativeImage, len(mappings))
	verify := func(phase string) {
		t.Helper()
		seen := make(map[string]bool)
		for index, mapping := range mappings {
			mode := "snapshot"
			if index == 2 {
				mode = "journal"
			}
			rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "volume", payloads[index])
			rbdScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
			image := rbdNamespaceBindingNativeIdentity(t, ctx, sourceClient, destinationClient, pool, mapping, mode)
			if seen[image.globalID] {
				t.Fatal("distinct namespace image names shared a native global ID")
			}
			seen[image.globalID] = true
			if images[index].globalID != "" && images[index] != image {
				t.Fatal("process topology change replaced original native images")
			}
			images[index] = image
			t.Logf("RBD_NAMESPACE_BINDING_BYTES phase=%s mapping=%q mode=%s bytes=%d sha256=%x checkpoint_id=%d source_id=%s destination_id=%s global_id=%s", phase, mapping, mode, len(payloads[index]), sha256.Sum256(payloads[index]), checkpoints[index], image.sourceID, image.destinationID, image.globalID)
		}
		for role, site := range []struct {
			client testcontainers.Container
			fsid   string
		}{{sourceClient, sourceStatus.FSID}, {destinationClient, destinationStatus.FSID}} {
			for _, ns := range []string{"", "isolated"} {
				rbdScopeIO(t, ctx, site.client, "read", site.fsid, pool, ns, "volume", 0, controlPayloads[role])
				rbdScopeAssertUnmirrored(t, ctx, site.client, rbdScopeImage(pool, ns, "volume"))
			}
		}
		checkPolicies()
	}
	verify("initial-distinct-payloads")
	grace := 2 * time.Second
	if err := receiver.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	for _, view := range views {
		status, err := view.ReceiverStatus(ctx)
		if err != nil || status.Ready {
			t.Fatal("shared stopped receiver remained ready", err)
		}
	}
	cid, instance := receiver.GetContainerID(), initial[0].Daemons["receiver"].InstanceID
	if err := receiver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := check("same-cid-restart")
	if receiver.GetContainerID() != cid || restarted[0].Daemons["receiver"].InstanceID == instance {
		t.Fatal("shared same-CID restart did not create fresh native instance")
	}
	if err := owner.RemoveDaemon(ctx, "receiver"); err != nil {
		t.Fatal(err)
	}
	for _, view := range views {
		zero, err := view.ReceiverStatus(ctx)
		if err != nil || zero.Ready || len(zero.Daemons) != 0 {
			t.Fatal("shared zero cohort adopted stale native member", err)
		}
	}
	if owner.Container != nil {
		t.Fatal("shared removal retained compatibility handle")
	}
	for index, mapping := range mappings {
		payloads[index] = rbdMultiClusterPayload(2<<20, 101+19*index)
		rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
		if index != 2 {
			checkpoints[index] = rbdReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "volume")
		}
	}
	replacement, err := owner.AddDaemon(ctx, "replacement")
	if err != nil || replacement == nil || replacement.GetContainerID() == cid || owner.Container != nil || customizers.Load() != 2 {
		t.Fatal("explicit shared replacement changed ownership/customizer semantics", err)
	}
	check("explicit-replacement")
	verify("zero-backlog-replaced-receiver")
	if _, err := owner.BindNamespace(ctx, "", " "); err == nil {
		t.Fatal("invalid sibling input reached binding authority")
	}
	if view, err := owner.BindNamespace(ctx, "", ""); err == nil || view != nil {
		t.Fatal("read-only binding activated init-only default namespace")
	}
	if current := [2]rbdReceiverIndependentPolicy{rbdReceiverNativePolicy(t, ctx, sourceClient, pool, ""), rbdReceiverNativePolicy(t, ctx, destinationClient, pool, "")}; current != base {
		t.Fatal("rejected default binding changed original base")
	}
	t.Logf("RBD_NAMESPACE_BINDING_COMPLETE customizers=%d owned_daemons=%d views=%d same_pool=%s", customizers.Load(), len(owner.Daemons()), len(views), pool)
}
