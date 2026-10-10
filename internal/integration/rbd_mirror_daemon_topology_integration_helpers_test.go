//go:build all || (integration && multicluster)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

func testMultiClusterRBDMirrorDaemonTopology(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, append(opts, ceph.WithOSDCount(1))...)
	const pool = "tc-rbd-daemon-topology"
	const name = "preserved"
	const image = pool + "/" + name
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	imageName := source.ControlImage()
	link, err := rbd.RunMirror(ctx, imageName, rbd.MirrorConfig{
		Source: source, Destination: destination, Pool: pool, DaemonCount: 2,
	})
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() {
				for _, daemon := range link.Daemons() {
					multiClusterLogContainer(t, cleanupCtx, daemon.Container)
					if status, err := daemon.Status(cleanupCtx); err == nil {
						t.Logf("remaining daemon %s native status: %+v", daemon.DaemonName, status)
					}
				}
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RBD daemon topology: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	daemons := link.Daemons()
	if len(daemons) != 2 || daemons[0].DaemonName != "a" || daemons[1].DaemonName != "b" ||
		daemons[0].ClientName == daemons[1].ClientName || link.Container == nil || link.GetContainerID() != daemons[0].GetContainerID() {
		t.Fatalf("initial daemon composition/unique auth/legacy handle is wrong: %+v", daemons)
	}
	for _, daemon := range daemons {
		keyring, err := destination.Ceph(ctx, "auth", "get", daemon.ClientName)
		if err != nil || !bytes.Contains(keyring, []byte("["+daemon.ClientName+"]")) {
			t.Fatalf("daemon %s does not have its own native auth entity: %v", daemon.DaemonName, err)
		}
	}
	initialPeer := rbdDaemonPoolIdentity(t, ctx, destinationClient, pool)
	leader := rbdDaemonWaitElection(t, ctx, pool, daemons)
	before := rbdMultiClusterPayload(size, 43)
	execCommand(t, ctx, sourceClient, "rbd", "create", image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	var sourceFSID, destinationFSID string
	for cluster, fsid := range map[*ceph.Container]*string{source: &sourceFSID, destination: &destinationFSID} {
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		*fsid = status.FSID
	}
	rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, name, 0, before)
	if err := link.EnableImage(ctx, name); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, name, before)
	globalID := rbdDaemonImageIdentity(t, ctx, sourceClient, destinationClient, image, "")

	// A stopped pool leader must be replaced by the other existing process.
	grace := 2 * time.Second
	if err := leader.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	state, err := leader.State(ctx)
	if err != nil || state.Running {
		t.Fatalf("native leader container did not stop: state=%+v error=%v", state, err)
	}
	var survivor *rbd.MirrorDaemon
	for _, daemon := range daemons {
		if daemon != leader {
			survivor = daemon
		}
	}
	if elected := rbdDaemonWaitElection(t, ctx, pool, []*rbd.MirrorDaemon{survivor}); elected != survivor {
		t.Fatal("surviving daemon did not become native pool leader")
	}
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 79)
	copy(after[2<<20:], patch)
	rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, name, 2<<20, patch)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, name, after)
	if err := leader.Start(ctx); err != nil {
		t.Fatal(err)
	}
	leader = rbdDaemonWaitElection(t, ctx, pool, daemons)
	rbdDaemonImageIdentity(t, ctx, sourceClient, destinationClient, image, globalID)

	// Delete the elected process and replace it with a new named process. This
	// does not rebootstrap the pool or modify the existing mirrored image.
	if err := link.RemoveDaemon(ctx, leader.DaemonName); err != nil {
		t.Fatal(err)
	}
	remaining := link.Daemons()
	if len(remaining) != 1 {
		t.Fatalf("daemon removal lost owned membership: %+v", remaining)
	}
	rbdDaemonWaitElection(t, ctx, pool, remaining)
	replacement, err := link.AddDaemon(ctx, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ClientName == daemons[0].ClientName || replacement.ClientName == daemons[1].ClientName {
		t.Fatal("replacement reused another daemon's auth identity")
	}
	rbdDaemonWaitElection(t, ctx, pool, link.Daemons())
	rbdFanoutRead(t, ctx, destinationClient, destinationFSID, pool, name, after)
	rbdDaemonImageIdentity(t, ctx, sourceClient, destinationClient, image, globalID)
	if got := rbdDaemonPoolIdentity(t, ctx, destinationClient, pool); got != initialPeer {
		t.Fatalf("process replacement changed native pool/peer identity: before=%s after=%s", initialPeer, got)
	}

	// Delivery of a newly enabled image demonstrates pool discovery continues
	// after membership replacement, beyond retaining an already copied image.
	const newName = "after-replacement"
	const newImage = pool + "/" + newName
	execCommand(t, ctx, sourceClient, "rbd", "create", newImage, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	newBytes := rbdMultiClusterPayload(size, 101)
	rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, newName, 0, newBytes)
	if err := link.EnableImage(ctx, newName); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, newName, newBytes)
	rbdDaemonImageIdentity(t, ctx, sourceClient, destinationClient, newImage, "")

	// Explicit 2 -> 0 -> 1 topology pauses all replication and resumes without
	// recreating any peer, pool or image. Existing replicas stay independently
	// readable while no mirror process exists.
	for _, daemon := range link.Daemons() {
		if err := link.RemoveDaemon(ctx, daemon.DaemonName); err != nil {
			t.Fatal(err)
		}
	}
	if len(link.Daemons()) != 0 || link.Container != nil {
		t.Fatal("zero-daemon topology retained runtime or initial handle")
	}
	rbdFanoutRead(t, ctx, destinationClient, destinationFSID, pool, name, after)
	final := bytes.Clone(after)
	lastPatch := rbdMultiClusterPayload(256<<10, 133)
	copy(final[6<<20:], lastPatch)
	rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, name, 6<<20, lastPatch)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	rbdFanoutRead(t, ctx, destinationClient, destinationFSID, pool, name, after)
	resumed, err := link.AddDaemon(ctx, "resumed")
	if err != nil {
		t.Fatal(err)
	}
	rbdDaemonWaitElection(t, ctx, pool, []*rbd.MirrorDaemon{resumed})
	rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, name, final)
	rbdFanoutRead(t, ctx, destinationClient, destinationFSID, pool, newName, newBytes)
	rbdDaemonImageIdentity(t, ctx, sourceClient, destinationClient, image, globalID)
	if got := rbdDaemonPoolIdentity(t, ctx, destinationClient, pool); got != initialPeer {
		t.Fatalf("zero-daemon pause/resume changed peer configuration: before=%s after=%s", initialPeer, got)
	}
	if err := link.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.Status(ctx); err != nil {
			t.Fatalf("mirror cleanup terminated caller-owned Ceph cluster: %v", err)
		}
	}
	t.Log("RBD daemon topology: native 2-member/one-leader pool election, leader stop/restart, leader removal/replacement, new image delivery, zero-daemon pause/resume, unchanged peers/global image identity, exact 8 MiB replicas and runtime-only cleanup verified")
}

func rbdDaemonWaitElection(t *testing.T, parent context.Context, pool string, daemons []*rbd.MirrorDaemon) *rbd.MirrorDaemon {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		valid := true
		var leader *rbd.MirrorDaemon
		var leaderStatus rbd.MirrorPoolReplayerStatus
		statuses := make([]rbd.MirrorPoolReplayerStatus, 0, len(daemons))
		instanceIDs := make([]string, 0, len(daemons))
		for _, daemon := range daemons {
			attempt, stop := context.WithTimeout(ctx, 15*time.Second)
			status, err := daemon.Status(attempt)
			stop()
			if err != nil || len(status.PoolReplayers) != 1 || status.PoolReplayers[0].Pool != pool || status.PoolReplayers[0].State != "running" || status.PoolReplayers[0].InstanceID == "" || status.PoolReplayers[0].Peer == "" {
				last = fmt.Sprintf("daemon %s: status=%+v error=%v", daemon.DaemonName, status, err)
				valid = false
				break
			}
			replayer := status.PoolReplayers[0]
			statuses = append(statuses, replayer)
			instanceIDs = append(instanceIDs, replayer.InstanceID)
			if replayer.Leader {
				if leader != nil {
					valid = false
				}
				leader, leaderStatus = daemon, replayer
			}
		}
		if valid && leader != nil {
			slices.Sort(instanceIDs)
			leaderInstances := slices.Clone(leaderStatus.Instances)
			slices.Sort(leaderInstances)
			if leaderStatus.LeaderInstanceID != leaderStatus.InstanceID || !slices.Equal(instanceIDs, leaderInstances) || len(slices.Compact(slices.Clone(instanceIDs))) != len(daemons) {
				valid = false
			}
			for _, status := range statuses {
				if status.LeaderInstanceID != leaderStatus.InstanceID || status.Peer != leaderStatus.Peer {
					valid = false
				}
			}
			if valid {
				t.Logf("native pool leader=%s instance=%s members=%v", leader.DaemonName, leaderStatus.InstanceID, leaderInstances)
				return leader
			}
		}
		last = fmt.Sprintf("%s; pool election=%+v", last, statuses)
		if len(last) > 5000 {
			last = last[len(last)-5000:]
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native RBD mirror election did not reach %d members/one leader: %v; %s", len(daemons), ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func rbdDaemonPoolIdentity(t *testing.T, ctx context.Context, destinationClient testcontainers.Container, pool string) string {
	t.Helper()
	data, err := rbdMirrorPoolInfo(ctx, destinationClient, pool, "")
	if err != nil {
		t.Fatal(err)
	}
	var info rbdFanoutPoolInfo
	if err := json.Unmarshal(data, &info); err != nil || info.Mode != "image" || info.MirrorUUID == "" || len(info.Peers) != 1 || info.Peers[0].UUID == "" || info.Peers[0].Direction != "rx-only" {
		t.Fatalf("unexpected receiving pool/peer topology: %+v error=%v", info, err)
	}
	// The remote mirror UUID is learned asynchronously. Compare the durable
	// receiving peer identity and direction, which must survive process changes.
	return strings.Join([]string{info.MirrorUUID, info.Peers[0].UUID, info.Peers[0].SiteName, info.Peers[0].Direction}, "/")
}

func rbdDaemonImageIdentity(t *testing.T, ctx context.Context, source, destination testcontainers.Container, image, expected string) string {
	t.Helper()
	for index, client := range []testcontainers.Container{source, destination} {
		rbdMultiClusterAssertPrimary(t, ctx, client, image, index == 0)
		var info struct {
			Mirroring struct {
				GlobalID string `json:"global_id"`
			} `json:"mirroring"`
		}
		if err := json.Unmarshal(rbdOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
			t.Fatal(err)
		}
		if expected == "" {
			expected = info.Mirroring.GlobalID
		}
		if info.Mirroring.GlobalID == "" || info.Mirroring.GlobalID != expected {
			t.Fatalf("image %s was recreated or lost native mirror identity: got=%s expected=%s", image, info.Mirroring.GlobalID, expected)
		}
	}
	return expected
}
