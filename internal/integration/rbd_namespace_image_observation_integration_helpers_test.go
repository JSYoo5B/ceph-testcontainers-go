//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func testRBDNamespaceImageObservation(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(2)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := newMultiClusterPairWithContext(t, ctx, opts...)
	const pool = "tc-scoped-image"
	mappings := [][2]string{{"ns-a", "ns-b"}, {"ns-c", "ns-d"}, {"ns-e", "ns-f"}}
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 2, MinSize: 1}); err != nil {
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
	for role, site := range []struct {
		cluster *ceph.Container
		client  testcontainers.Container
	}{{source, sourceClient}, {destination, destinationClient}} {
		rbdScopedImageHealth(t, ctx, site.cluster, site.client, []string{"source", "destination"}[role], "after-initial-pools")
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
		payloads[index] = rbdScopedImagePayload(index, "prepared")
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

	checkPolicies()
	setupCIDs := rbdScopedImageSetupCIDs(t, ctx, oracle)
	for index, mapping := range mappings {
		if index < 2 {
			if _, err := owner.SourceRBD(ctx, "mirror", "image", "enable", rbdScopeImage(pool, mapping[0], "volume"), "snapshot"); err != nil {
				t.Fatal(err)
			}
		}
	}
	sources := make([]rbdScopedImageInfo, len(mappings))
	for index, mapping := range mappings {
		mode := "snapshot"
		if index == 2 {
			mode = "journal"
		}
		sources[index] = rbdScopedImageReadInfo(t, ctx, sourceClient, pool, mapping[0], mode)
		zero, queryErr := views[index].ImageStatus(ctx, "volume")
		// Destination info may still return ENOENT. A useful source observation
		// with a retryable query error is valid; it cannot claim replay readiness.
		if zero.ReplayReady || zero.SourceImageID != sources[index].id || zero.GlobalID != sources[index].globalID || string(zero.Mode) != mode || zero.SourceNamespace != mapping[0] || zero.DestinationNamespace != mapping[1] {
			t.Fatalf("zero scoped image observation lost original source: %+v %v", zero, queryErr)
		}
		encoded, _ := json.Marshal(map[string]any{"phase": "zero-original-cohort", "mapping": mapping, "report": zero, "query_error": queryErr != nil})
		t.Logf("RBD_NAMESPACE_IMAGE_PENDING %s", encoded)
	}
	// This wait completes before Add. It proves bounded non-readiness, not an
	// in-flight first-witness barrier or permission to adopt a later cohort.
	zeroCtx, stopZero := context.WithTimeout(ctx, 10*time.Second)
	zero, zeroErr := views[0].WaitReplayReady(zeroCtx, "volume")
	stopZero()
	if !errors.Is(zeroErr, context.DeadlineExceeded) || zero.ReplayReady || zero.SourceImageID != sources[0].id || zero.GlobalID != sources[0].globalID || string(zero.Mode) != "snapshot" {
		t.Fatalf("completed zero-cohort wait differs: %+v %v", zero, zeroErr)
	}
	oracle.assertPolicyOnly(t, ctx, 2, "rbd-scoped-image-zero-observation")
	if len(owner.Daemons()) != 0 || customizers.Load() != 0 {
		t.Fatal("read-only image observation launched a receiver")
	}
	receiver, err := owner.AddDaemon(ctx, "receiver")
	if err != nil || receiver == nil || owner.Container != receiver || customizers.Load() != 1 {
		t.Fatal("explicit original receiver failed", err)
	}
	rbdScenarioWaitTransmitPeer(t, ctx, owner.SourceRBD, pool, "destination")
	checkpoints := make([]uint64, len(mappings))
	images := make([]rbdNamespaceBindingNativeImage, len(mappings))
	write := func(phase string) {
		t.Helper()
		for index, mapping := range mappings {
			payloads[index] = rbdScopedImagePayload(index, phase)
			rbdScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
			if index < 2 {
				checkpoints[index] = rbdReceiverNativeSnapshot(t, ctx, sourceClient, pool, mapping[0], "volume")
				t.Logf("RBD_NAMESPACE_IMAGE_CHECKPOINT phase=%s mapping=%q source_snapshot_id=%d", phase, mapping, checkpoints[index])
			}
		}
	}
	check := func(phase string, worker *rbd.MirrorDaemon) []rbd.MirrorImageStatus {
		t.Helper()
		checkPolicies()
		if members := owner.Daemons(); len(members) != 1 || members[0] != worker {
			t.Fatal("scoped readiness changed the exact owned current cohort")
		}
		seenSource, seenDestination, seenGlobal := map[string]bool{}, map[string]bool{}, map[string]bool{}
		reports := make([]rbd.MirrorImageStatus, len(views))
		for index, view := range views {
			mode := "snapshot"
			if index == 2 {
				mode = "journal"
			}
			waited, err := view.WaitReplayReady(ctx, "volume")
			if err != nil || !waited.ReplayReady {
				t.Fatalf("%s scoped wait: %+v %v", phase, waited, err)
			}
			report, err := view.ImageStatus(ctx, "volume")
			if err != nil || !report.ReplayReady {
				t.Fatalf("%s scoped status: %+v %v", phase, report, err)
			}
			image := rbdNamespaceBindingNativeIdentity(t, ctx, sourceClient, destinationClient, pool, mappings[index], mode)
			if image.sourceID != sources[index].id || image.globalID != sources[index].globalID || images[index].globalID != "" && images[index] != image {
				t.Fatal("scoped observer adopted replacement images")
			}
			if seenSource[image.sourceID] || seenDestination[image.destinationID] || seenGlobal[image.globalID] {
				t.Fatal("same-name namespace images share local/global identities")
			}
			seenSource[image.sourceID], seenDestination[image.destinationID], seenGlobal[image.globalID] = true, true, true
			images[index] = image
			for _, observed := range []rbd.MirrorImageStatus{waited, report} {
				if observed.Pool != pool || observed.Name != "volume" || observed.SourceNamespace != mappings[index][0] || observed.DestinationNamespace != mappings[index][1] || string(observed.Mode) != mode || observed.SourceImageID != image.sourceID || observed.DestinationImageID != image.destinationID || observed.GlobalID != image.globalID || !observed.SourcePrimary || observed.DestinationPrimary || observed.SourceMirrorState != "enabled" || observed.DestinationMirrorState != "enabled" || observed.State != "up+replaying" || observed.DaemonName != worker.DaemonName || observed.InstanceID == "" {
					t.Fatalf("%s scoped report differs from native identities: %+v", phase, observed)
				}
			}
			if waited.InstanceID != report.InstanceID {
				t.Fatal("stable scoped wait and status attribute different receiving instances")
			}
			process := rbdScopedImageAttribution(t, ctx, oracle, worker, destinationClient, pool, mappings[index], identity, report)
			if index > 0 && report.InstanceID != reports[0].InstanceID {
				t.Fatal("scoped views adopted separate receiving processes")
			}
			reports[index] = report
			encoded, _ := json.Marshal(map[string]any{"phase": phase, "mapping": mappings[index], "report": report, "worker_cid": worker.GetContainerID(), "worker_started_at": process.startedAt, "native_instance": report.InstanceID, "source_fsid": sourceStatus.FSID, "destination_fsid": destinationStatus.FSID, "source_pool_id": sourcePool.ID, "destination_pool_id": destinationPool.ID, "source_client_cid": sourceClient.GetContainerID(), "destination_client_cid": destinationClient.GetContainerID()})
			t.Logf("RBD_NAMESPACE_IMAGE_READY %s", encoded)
		}
		legacy, err := owner.ImageStatus(ctx, "volume")
		first := reports[0]
		if err != nil || !legacy.ReplayReady || legacy.SourceNamespace != mappings[0][0] || legacy.DestinationNamespace != mappings[0][1] || string(legacy.Mode) != "snapshot" || legacy.SourceImageID != first.SourceImageID || legacy.DestinationImageID != first.DestinationImageID || legacy.GlobalID != first.GlobalID {
			t.Fatalf("legacy owner escaped original mapping/mode: %+v %v", legacy, err)
		}
		checkPolicies()
		return reports
	}
	verify := func(phase string) {
		t.Helper()
		for index, mapping := range mappings {
			rbdScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, mapping[1], "volume", payloads[index])
			rbdScopeIO(t, ctx, sourceClient, "read", sourceStatus.FSID, pool, mapping[0], "volume", 0, payloads[index])
			mode := "snapshot"
			if index == 2 {
				mode = "journal"
			}
			current := rbdNamespaceBindingNativeIdentity(t, ctx, sourceClient, destinationClient, pool, mapping, mode)
			if current != images[index] || index < 2 && checkpoints[index] == 0 || index == 2 && checkpoints[index] != 0 {
				t.Fatal("data/checkpoint check changed original image identities or modes")
			}
			// The source ID is positively retained by rbdReceiverNativeSnapshot;
			// exact fresh replica bytes prove its nonce checkpoint effect. Do not
			// invent a destination snapshot-ID equality schema.
			t.Logf("RBD_NAMESPACE_IMAGE_BYTES phase=%s mapping=%q mode=%s bytes=%d sha256=%x source_snapshot_id=%d source_id=%s destination_id=%s global_id=%s", phase, mapping, mode, len(payloads[index]), sha256.Sum256(payloads[index]), checkpoints[index], current.sourceID, current.destinationID, current.globalID)
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
	nonReady := func(phase string) {
		t.Helper()
		for index, view := range views {
			report, err := view.ImageStatus(ctx, "volume")
			expected := images[index]
			if err != nil || report.ReplayReady || report.SourceImageID != expected.sourceID || report.DestinationImageID != expected.destinationID || report.GlobalID != expected.globalID {
				t.Fatalf("%s stale native report became ready/adopted images: %+v %v", phase, report, err)
			}
			encoded, _ := json.Marshal(map[string]any{"phase": phase, "mapping": mappings[index], "report": report})
			t.Logf("RBD_NAMESPACE_IMAGE_PENDING %s", encoded)
		}
		waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		last, err := views[0].WaitReplayReady(waitCtx, "volume")
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || last.ReplayReady || last.SourceImageID != images[0].sourceID || last.GlobalID != images[0].globalID {
			t.Fatalf("%s non-ready wait lost identity/deadline: %+v %v", phase, last, err)
		}
		checkPolicies()
	}
	write("initial")
	initial := check("initial", receiver)
	verify("initial")
	cid := receiver.GetContainerID()
	originalProcess := rbdScopedImageRawProcess(t, ctx, oracle, receiver, true)
	grace := 2 * time.Second
	if err := receiver.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	rbdScopedImageRawProcess(t, ctx, oracle, receiver, false)
	nonReady("stopped-original")
	write("same-cid-restart")
	if err := receiver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := check("same-cid-restart", receiver)
	currentProcess := rbdScopedImageRawProcess(t, ctx, oracle, receiver, true)
	before, beforeErr := time.Parse(time.RFC3339Nano, originalProcess.startedAt)
	after, afterErr := time.Parse(time.RFC3339Nano, currentProcess.startedAt)
	if receiver.GetContainerID() != cid || restarted[0].InstanceID == initial[0].InstanceID || beforeErr != nil || afterErr != nil || !after.After(before) {
		t.Fatal("same-CID restart did not retain container and create fresh task/instance")
	}
	verify("same-cid-restart")
	if err := owner.RemoveDaemon(ctx, "receiver"); err != nil {
		t.Fatal(err)
	}
	rbdScopedImageRemoved(t, ctx, oracle, cid)
	if owner.Container != nil || len(owner.Daemons()) != 0 {
		t.Fatal("removed original member remains owned")
	}
	nonReady("removed-original")
	write("explicit-replacement")
	replacement, err := owner.AddDaemon(ctx, "replacement")
	if err != nil || replacement == nil || replacement.GetContainerID() == cid || owner.Container != nil || customizers.Load() != 2 {
		t.Fatal("explicit replacement changed original owner/customizer semantics", err)
	}
	replaced := check("explicit-replacement", replacement)
	if replaced[0].InstanceID == restarted[0].InstanceID {
		t.Fatal("new container adopted the retired original native instance")
	}
	verify("explicit-replacement")
	for role, site := range []struct {
		cluster *ceph.Container
		client  testcontainers.Container
	}{{source, sourceClient}, {destination, destinationClient}} {
		rbdScopedImageHealth(t, ctx, site.cluster, site.client, []string{"source", "destination"}[role], "after-final-data")
	}
	cleanup, finish := context.WithTimeout(context.Background(), 2*time.Minute)
	defer finish()
	if err := owner.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	for _, original := range append(setupCIDs, cid, replacement.GetContainerID()) {
		rbdScopedImageRemoved(t, cleanup, oracle, original)
	}
	oracle.assertPolicyOnly(t, cleanup, 0, "rbd-scoped-image-owner-cleanup")
	t.Logf("RBD_NAMESPACE_IMAGE_CLEANUP engine=%s owner_containers=%d remaining=0", oracle.engine, len(setupCIDs)+2)
	t.Logf("RBD_NAMESPACE_IMAGE_COMPLETE views=%d customizers=%d pool=%s", len(views), customizers.Load(), pool)
}

func rbdScopedImagePayload(index int, phase string) []byte {
	payload := rbdMultiClusterPayload(2<<20, 31+47*index)
	nonce := []byte("rbd-scoped-image/" + phase + "/" + uuid.NewString())
	copy(payload, nonce)
	copy(payload[len(payload)-len(nonce):], nonce)
	return payload
}

type rbdScopedImageInfo struct{ id, globalID string }

func rbdScopedImageReadInfo(t *testing.T, parent context.Context, client testcontainers.Container, pool, namespace, mode string) rbdScopedImageInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	var info struct {
		ID        *string `json:"id"`
		Name      *string `json:"name"`
		Mirroring *struct {
			GlobalID *string `json:"global_id"`
			Mode     *string `json:"mode"`
			State    *string `json:"state"`
			Primary  *bool   `json:"primary"`
		} `json:"mirroring"`
	}
	data := rbdOutput(t, ctx, client, "info", rbdScopeImage(pool, namespace, "volume"), "--format", "json")
	if err := json.Unmarshal(data, &info); err != nil || info.ID == nil || *info.ID == "" || info.Name == nil || *info.Name != "volume" || info.Mirroring == nil || info.Mirroring.GlobalID == nil || *info.Mirroring.GlobalID == "" || info.Mirroring.Mode == nil || *info.Mirroring.Mode != mode || info.Mirroring.State == nil || *info.Mirroring.State != "enabled" || info.Mirroring.Primary == nil || !*info.Mirroring.Primary {
		t.Fatal("strict independent source mirror info unavailable", err)
	}
	id, err := uuid.Parse(*info.Mirroring.GlobalID)
	if err != nil || id == uuid.Nil || id.String() != *info.Mirroring.GlobalID {
		t.Fatal("source global ID not canonical positive UUID")
	}
	return rbdScopedImageInfo{*info.ID, *info.Mirroring.GlobalID}
}

type rbdScopedImageProcess struct {
	startedAt string
	pid       int
}

func rbdScopedImageRawProcess(t *testing.T, parent context.Context, oracle mirrorInitialDockerOracle, worker *rbd.MirrorDaemon, running bool) rbdScopedImageProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != oracle.engine {
		t.Fatal("original raw engine unavailable", err)
	}
	cid := worker.GetContainerID()
	if !rbdScopedImageFullCID(cid) {
		t.Fatal("original returned worker CID is not exact full native identity")
	}
	inspected, err := oracle.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
	if err != nil || inspected.Container.ID != cid || inspected.Container.State == nil || inspected.Container.Config == nil {
		t.Fatal("exact original worker CID unavailable", err)
	}
	state := inspected.Container.State
	start, err := time.Parse(time.RFC3339Nano, state.StartedAt)
	if err != nil || start.IsZero() || state.Paused || state.Restarting || state.Dead || state.Error != "" || state.Running != running || running && (state.Status != "running" || state.Pid <= 0) || !running && state.Status != "exited" {
		t.Fatal("raw original worker state is not coherent")
	}
	actual, err := worker.State(ctx)
	if err != nil || actual == nil || actual.StartedAt != state.StartedAt || actual.Pid != state.Pid || actual.Running != state.Running {
		t.Fatal("returned original handle differs from fresh raw CID", err)
	}
	after, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != oracle.engine {
		t.Fatal("original raw engine changed", err)
	}
	return rbdScopedImageProcess{state.StartedAt, state.Pid}
}

func rbdScopedImageAttribution(t *testing.T, parent context.Context, oracle mirrorInitialDockerOracle, worker *rbd.MirrorDaemon, destination testcontainers.Container, pool string, mapping [2]string, identity rbdReceiverIndependentIdentity, report rbd.MirrorImageStatus) rbdScopedImageProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before := rbdScopedImageRawProcess(t, ctx, oracle, worker, true)
	var status struct {
		Name     *string `json:"name"`
		GlobalID *string `json:"global_id"`
		State    *string `json:"state"`
		Service  *struct {
			ServiceID  *string `json:"service_id"`
			DaemonID   *string `json:"daemon_id"`
			InstanceID *string `json:"instance_id"`
		} `json:"daemon_service"`
	}
	data := rbdOutput(t, ctx, destination, "mirror", "image", "status", rbdScopeImage(pool, mapping[1], "volume"), "--format", "json")
	daemonID, valid := strings.CutPrefix(worker.ClientName, "client.rbd-mirror.")
	if err := json.Unmarshal(data, &status); err != nil || status.Name == nil || *status.Name != "volume" || status.GlobalID == nil || *status.GlobalID != report.GlobalID || status.State == nil || *status.State != "up+replaying" || status.Service == nil || status.Service.ServiceID == nil || *status.Service.ServiceID == "" || status.Service.DaemonID == nil || *status.Service.DaemonID != daemonID || !valid || daemonID == "" || status.Service.InstanceID == nil || *status.Service.InstanceID != report.InstanceID {
		t.Fatal("independent native image service does not attribute public ready to owned receiver", err)
	}
	var socket struct {
		Pools []struct {
			Pool       *string `json:"pool"`
			Peer       *string `json:"peer"`
			State      *string `json:"state"`
			Instance   *string `json:"instance_id"`
			Namespaces []struct {
				Local  *string           `json:"local_namespace"`
				Remote *string           `json:"remote_namespace"`
				Images []json.RawMessage `json:"image_replayers"`
			} `json:"namespace_replayers"`
		} `json:"pool_replayers"`
	}
	code, data, err := rbdMultiClusterExec(ctx, worker.Container, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil || code != 0 || json.Unmarshal(data, &socket) != nil || socket.Pools == nil {
		t.Fatal("independent owned admin socket unavailable", err)
	}
	selected := 0
	for _, row := range socket.Pools {
		if row.Pool == nil || row.Peer == nil || row.State == nil || row.Namespaces == nil {
			t.Fatal("independent socket scope fields missing/null")
		}
		if *row.Pool != pool || *row.Peer != "uuid: "+identity.peerUUID+" cluster: "+identity.site+" client: "+identity.client {
			continue
		}
		selected++
		if *row.State != "running" || row.Instance == nil || *row.Instance != report.InstanceID {
			t.Fatal("independent socket disagrees with live image service instance")
		}
		id, err := strconv.ParseUint(*row.Instance, 10, 64)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != *row.Instance {
			t.Fatal("native receiver instance is not positive canonical numeric identity")
		}
		found := 0
		for _, ns := range row.Namespaces {
			if ns.Local == nil || ns.Remote == nil || ns.Images == nil {
				t.Fatal("independent socket namespace fields missing/null")
			}
			if *ns.Local == mapping[1] && *ns.Remote == mapping[0] {
				found++
			}
		}
		if found != 1 {
			t.Fatal("independent live receiver does not discover exact reciprocal selected namespace")
		}
	}
	if selected != 1 {
		t.Fatal("independent selected pool/peer socket ambiguous/missing")
	}
	after := rbdScopedImageRawProcess(t, ctx, oracle, worker, true)
	if before != after {
		t.Fatal("original receiving task changed during independent attribution")
	}
	return after
}

func rbdScopedImageSetupCIDs(t *testing.T, parent context.Context, oracle mirrorInitialDockerOracle) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	info, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != oracle.engine {
		t.Fatal("raw setup engine unavailable", err)
	}
	listed, err := oracle.client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, item := range listed.Items {
		if !oracle.before[item.ID] {
			result = append(result, item.ID)
		}
	}
	slices.Sort(result)
	if len(result) != 2 || !rbdScopedImageFullCID(result[0]) || !rbdScopedImageFullCID(result[1]) || result[0] == result[1] {
		t.Fatal("original retained setup CLIs not exact two distinct CIDs")
	}
	after, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != oracle.engine {
		t.Fatal("original raw setup engine changed", err)
	}
	return result
}

func rbdScopedImageRemoved(t *testing.T, parent context.Context, oracle mirrorInitialDockerOracle, cid string) {
	t.Helper()
	if !rbdScopedImageFullCID(cid) {
		t.Fatal("removed identity is not the original full CID")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != oracle.engine {
		t.Fatal("original raw engine unavailable before exact removed CID check", err)
	}
	if _, err := oracle.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("retained original raw client cannot prove exact owned CID removed", err)
	}
	after, err := oracle.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != oracle.engine {
		t.Fatal("original raw engine changed after exact removed CID check", err)
	}
}

// One shared 90-second context covers clean readiness, raw health polling and
// required module readback. This pinned-default profile does not waive unused
// control payloads or certify every image component with a new image checker.
func rbdScopedImageHealth(t *testing.T, parent context.Context, cluster *ceph.Container, client testcontainers.Container, role, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatalf("%s %s clean before raw health: %v", role, phase, err)
	}
	for {
		code, data, err := rbdMultiClusterExec(ctx, client, "ceph", "--connect-timeout", "5", "health", "detail", "--format", "json")
		if err != nil || code != 0 {
			t.Fatalf("%s %s raw health query failed: exit=%d error=%v detail=%s", role, phase, code, err, data)
		}
		t.Logf("RBD_NAMESPACE_IMAGE_HEALTH role=%s phase=%s detail=%s", role, phase, data)
		var health struct {
			Status *string                     `json:"status"`
			Checks *map[string]json.RawMessage `json:"checks"`
		}
		if err := json.Unmarshal(data, &health); err != nil || health.Status == nil || health.Checks == nil || !slices.Contains([]string{"HEALTH_OK", "HEALTH_WARN", "HEALTH_ERR"}, *health.Status) {
			t.Fatal("strict raw native health detail unavailable", err)
		}
		for check := range *health.Checks {
			if strings.HasPrefix(check, "MGR_MODULE") {
				t.Fatalf("observed native manager module/dependency failure; stop for fixed image-policy classification: %s detail=%s", check, data)
			}
		}
		if *health.Status == "HEALTH_OK" {
			if len(*health.Checks) != 0 {
				t.Fatal("native health summary contradicts checks")
			}
			break
		}
		// No warning is declared successful or suppressed; all must disappear.
		select {
		case <-ctx.Done():
			t.Fatalf("%s %s did not reach HEALTH_OK: %v detail=%s", role, phase, ctx.Err(), data)
		case <-time.After(time.Second):
		}
	}
	code, data, err := rbdMultiClusterExec(ctx, client, "ceph", "--connect-timeout", "5", "mgr", "dump", "--format", "json")
	if err != nil || code != 0 {
		t.Fatalf("native required manager module read failed: exit=%d error=%v detail=%s", code, err, data)
	}
	var native struct {
		Available        *bool               `json:"available"`
		ActiveGID        *uint64             `json:"active_gid"`
		Modules          *[]string           `json:"modules"`
		AlwaysOn         map[string][]string `json:"always_on_modules"`
		AvailableModules *[]struct {
			Name   *string `json:"name"`
			CanRun *bool   `json:"can_run"`
			Error  *string `json:"error_string"`
		} `json:"available_modules"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Available == nil || !*native.Available || native.ActiveGID == nil || *native.ActiveGID == 0 || native.Modules == nil || native.AlwaysOn == nil || native.AvailableModules == nil {
		t.Fatal("strict native active manager module catalog unavailable", err)
	}
	required := map[string]bool{"volumes": true, "rbd_support": true, "mirroring": true}
	for _, name := range *native.Modules {
		if name == "" {
			t.Fatal("empty enabled manager module name")
		}
		required[name] = true
	}
	// The native map publishes release sets; checking their union is bounded
	// and retains every always-on component published by this pinned release.
	if len(native.AlwaysOn) == 0 {
		t.Fatal("native always-on manager release sets unavailable")
	}
	for _, names := range native.AlwaysOn {
		if names == nil {
			t.Fatal("null native always-on release set")
		}
		for _, name := range names {
			if name == "" {
				t.Fatal("empty native always-on module name")
			}
			required[name] = true
		}
	}
	seen := map[string]bool{}
	selected := map[string]bool{}
	for _, module := range *native.AvailableModules {
		if module.Name == nil || *module.Name == "" || module.CanRun == nil || module.Error == nil || seen[*module.Name] {
			t.Fatal("strict native manager available-module catalog malformed")
		}
		seen[*module.Name] = true
		if required[*module.Name] {
			if !*module.CanRun || *module.Error != "" {
				t.Fatalf("observed required manager module/dependency failure; stop for fixed image-policy classification: module=%s can_run=%t error=%s", *module.Name, *module.CanRun, *module.Error)
			}
			selected[*module.Name] = true
		}
	}
	for name := range required {
		if !selected[name] {
			t.Fatalf("observed required manager module absent from catalog; stop for fixed image-policy classification: %s", name)
		}
	}
	encoded, _ := json.Marshal(selected)
	t.Logf("RBD_NAMESPACE_IMAGE_MODULES role=%s phase=%s active_gid=%d can_run=%s", role, phase, *native.ActiveGID, encoded)
}

func rbdScopedImageFullCID(cid string) bool {
	if len(cid) != 64 {
		return false
	}
	for _, value := range cid {
		if value < '0' || value > '9' && value < 'a' || value > 'f' {
			return false
		}
	}
	return true
}
