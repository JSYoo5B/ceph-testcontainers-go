//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

// TestMultiClusterRBDFailback moves primary ownership A -> B -> A. Both sites
// run native receivers so B's new writes and snapshots return to A before A is
// promoted, and subsequent A writes are mirrored to B again.
func TestMultiClusterRBDFailback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-failback"
	const image = pool + "/volume"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := rbdScenarioRunLink(t, ctx, destination, source, pool, "destination", "source")
	rbdScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 31)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@on-a")
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image+"@on-a", before)
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	rbdMultiClusterPromote(t, ctx, destinationClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, false)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	// B is the active site. Pause its receiver without interrupting its native
	// sender: A's receiver must obtain B's writes across the cluster boundary.
	rbdScenarioStop(t, ctx, forward)
	onB := bytes.Clone(before)
	patchB := rbdMultiClusterPayload(1<<20, 101)
	copy(onB[4<<20:], patchB)
	rbdMultiClusterWriteRange(t, ctx, destinationClient, image, size, 4<<20, patchB)
	execCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@on-b")
	rbdScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, sourceClient, image, onB)
	rbdScenarioWaitBytes(t, ctx, sourceClient, image+"@on-b", onB)
	verifyRBDBytes(t, ctx, sourceClient, image+"@on-a", before)
	rbdScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "demote", image)
	rbdMultiClusterPromote(t, ctx, sourceClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	verifyRBDBytes(t, ctx, sourceClient, image, onB)

	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(onB)
	patchA := rbdMultiClusterPayload(256<<10, 127)
	copy(after[:len(patchA)], patchA)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, patchA)
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		verifyRBDBytes(t, ctx, client, image+"@on-a", before)
		verifyRBDBytes(t, ctx, client, image+"@on-b", onB)
	}
	t.Logf("RBD planned A -> B -> A failback: B-only writes returned before non-forced A promotion; A resumed replication; both user snapshots retained complete 8 MiB history; final sha256=%x", sha256.Sum256(after))
}

// TestMultiClusterRBDSplitBrainResync deliberately forks a disposable volume.
// Recovery explicitly selects A as authoritative and discards B's divergent
// writes; this is not a data merge or a claim of lossless forced failover.
func TestMultiClusterRBDSplitBrainResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-split-brain"
	const image = pool + "/disposable"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 43)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	// A deliberately forced B primary needs a TX-capable peer to create its
	// own divergent mirror checkpoints. No receiver is started on A.
	peer := rbdScenarioPeer(t, ctx, link, pool)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "set", pool, peer, "direction", "rx-tx")
	rbdScenarioStop(t, ctx, link)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "promote", "--force", image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	authoritative := bytes.Clone(before)
	patchA := rbdMultiClusterPayload(1<<20, 137)
	copy(authoritative[1<<20:], patchA)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 1<<20, patchA)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@authoritative-a")
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	discarded := bytes.Clone(before)
	patchB := rbdMultiClusterPayload(1<<20, 163)
	copy(discarded[3<<20:], patchB)
	rbdMultiClusterWriteRange(t, ctx, destinationClient, image, size, 3<<20, patchB)
	execCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@discarded-b")
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "snapshot", image)
	verifyRBDBytes(t, ctx, sourceClient, image, authoritative)
	verifyRBDBytes(t, ctx, destinationClient, image, discarded)

	// A stays primary. Demoting the losing branch permits the restarted B
	// receiver to discover the incompatible histories and report split-brain.
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "demote", image)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	rbdScenarioWaitSplitBrain(t, ctx, link, image)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, authoritative)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image+"@authoritative-a", authoritative)
	rbdScenarioRequireSnapshotAbsent(t, ctx, destinationClient, image, "discarded-b")
	if bytes.Equal(authoritative[3<<20:4<<20], patchB) {
		t.Fatal("fixture cannot distinguish discarded branch from authoritative content")
	}

	// Confirm that recovery clears the fault and mirroring continues, rather
	// than merely producing a one-time copy of the winning branch.
	after := bytes.Clone(authoritative)
	tail := rbdMultiClusterPayload(256<<10, 181)
	copy(after[7<<20:], tail)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 7<<20, tail)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	t.Logf("RBD native split-brain detected and recovered by choosing A + demoting/resyncing B; discarded B branch sha256=%x, winning A branch sha256=%x; subsequent checkpoint replicated", sha256.Sum256(discarded), sha256.Sum256(authoritative))
}

// TestMultiClusterRBDPeerLifecycle removes the destination's native peer,
// proves that new checkpoints stop arriving, reboots the relationship and
// requests a full resync before verifying that later checkpoints continue.
func TestMultiClusterRBDPeerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-peer-lifecycle"
	const image = pool + "/volume"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 53)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	oldPeer := rbdScenarioPeer(t, ctx, link, pool)
	rbdScenarioStop(t, ctx, link)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "remove", pool, oldPeer)
	rbdScenarioRequireNoPeers(t, ctx, link, pool)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 191)
	copy(after[5<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 5<<20, patch)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	for i := 0; i < 3; i++ {
		verifyRBDBytes(t, ctx, destinationClient, image, before)
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err := link.Rebootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	newPeer := rbdScenarioPeer(t, ctx, link, pool)
	if newPeer == oldPeer {
		t.Fatal("removed peer UUID was reused rather than creating a new relationship")
	}
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	latest := bytes.Clone(after)
	lastPatch := rbdMultiClusterPayload(256<<10, 211)
	copy(latest[:len(lastPatch)], lastPatch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, lastPatch)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, latest)
	t.Logf("RBD peer lifecycle: removed %s, no peer/no new data while daemon ran, rebootstrap created %s; resync and subsequent checkpoint restored native replication", oldPeer, newPeer)
}

func rbdScenarioRunLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *multicluster.RBDMirror {
	t.Helper()
	image := source.ControlImage()
	link, err := multicluster.RunRBDMirror(ctx, image, multicluster.RBDMirrorConfig{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite,
	})
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && link.Container != nil {
				multiClusterLogContainer(t, cleanupCtx, link.Container)
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RBD scenario link: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func rbdScenarioSeed(t *testing.T, ctx context.Context, client testcontainers.Container, image string, seed int) []byte {
	t.Helper()
	data := rbdMultiClusterPayload(8<<20, seed)
	if err := client.CopyToContainer(ctx, data, "/tmp/rbd-scenario-seed", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "rbd", "import", "/tmp/rbd-scenario-seed", image,
		"--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
	return data
}

func rbdScenarioCommand(t *testing.T, ctx context.Context, command func(context.Context, ...string) ([]byte, error), args ...string) []byte {
	t.Helper()
	out, err := command(ctx, args...)
	if err != nil {
		t.Fatalf("RBD %s: %v", strings.Join(args, " "), err)
	}
	return out
}

func rbdScenarioStop(t *testing.T, ctx context.Context, link *multicluster.RBDMirror) {
	t.Helper()
	grace := 3 * time.Second
	if err := link.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
}

func rbdScenarioWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		_, _, _ = rbdMultiClusterExec(attempt, client, "rm", "-f", "/tmp/rbd-scenario-result")
		code, out, err := rbdMultiClusterExec(attempt, client, "rbd", "export", image, "/tmp/rbd-scenario-result", "--no-progress")
		if err == nil && code == 0 {
			reader, readErr := client.CopyFileFromContainer(attempt, "/tmp/rbd-scenario-result")
			if readErr == nil {
				actual, readErr := io.ReadAll(reader)
				reader.Close()
				if readErr == nil && bytes.Equal(actual, expected) {
					t.Logf("RBD %s complete bytes verified: sha256=%x", image, sha256.Sum256(expected))
					stop()
					return
				}
				last = fmt.Sprintf("read error=%v bytes=%d sha256=%x", readErr, len(actual), sha256.Sum256(actual))
			} else {
				last = readErr.Error()
			}
		} else {
			last = fmt.Sprintf("export exit=%d error=%v output=%s", code, err, out)
		}
		stop()
		select {
		case <-ctx.Done():
			t.Fatalf("wait for RBD %s data: %v; %s", image, ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func rbdScenarioWaitSplitBrain(t *testing.T, parent context.Context, link *multicluster.RBDMirror, image string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		data, err := link.DestinationRBD(ctx, "mirror", "image", "status", image, "--format", "json")
		if err == nil {
			var status struct {
				State       string
				Description string
			}
			if err := json.Unmarshal(data, &status); err != nil {
				t.Fatal(err)
			}
			last = string(data)
			description := strings.ToLower(status.Description)
			if strings.Contains(status.State, "error") && (strings.Contains(description, "split-brain") || strings.Contains(description, "split brain")) {
				t.Logf("native RBD split-brain status=%s", data)
				return
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native RBD split-brain was not reported: %v; %s", ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func rbdScenarioWaitTransmitPeer(t *testing.T, parent context.Context, command func(context.Context, ...string) ([]byte, error), pool, site string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		data, err := command(attempt, "mirror", "pool", "info", pool, "--format", "json")
		if err == nil {
			var info struct {
				Peers []struct {
					UUID       string
					Direction  string
					SiteName   string `json:"site_name"`
					MirrorUUID string `json:"mirror_uuid"`
				}
			}
			if err := json.Unmarshal(data, &info); err != nil {
				stop()
				t.Fatal(err)
			}
			last = string(data)
			for _, peer := range info.Peers {
				if peer.SiteName == site && peer.UUID != "" && peer.MirrorUUID != "" && (peer.Direction == "tx-only" || peer.Direction == "rx-tx") {
					stop()
					t.Logf("RBD pool %s native transmit peer %s is ready: direction=%s mirror_uuid=%s", pool, site, peer.Direction, peer.MirrorUUID)
					return
				}
			}
		} else {
			last = err.Error()
		}
		stop()
		select {
		case <-ctx.Done():
			t.Fatalf("wait for RBD transmit peer %s: %v; %s", site, ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func rbdScenarioRequireSnapshotAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, image, snapshot string) {
	t.Helper()
	var snapshots []struct{ Name string }
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "snap", "ls", image, "--format", "json"), &snapshots); err != nil {
		t.Fatal(err)
	}
	for _, snap := range snapshots {
		if snap.Name == snapshot {
			t.Fatalf("losing branch snapshot %s survived authoritative resync", snapshot)
		}
	}
}

func rbdScenarioPeer(t *testing.T, ctx context.Context, link *multicluster.RBDMirror, pool string) string {
	t.Helper()
	var info struct{ Peers []struct{ UUID string } }
	if err := json.Unmarshal(rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "info", pool, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Peers) != 1 || info.Peers[0].UUID == "" {
		t.Fatalf("expected one configured RBD peer: %+v", info.Peers)
	}
	return info.Peers[0].UUID
}

func rbdScenarioRequireNoPeers(t *testing.T, ctx context.Context, link *multicluster.RBDMirror, pool string) {
	t.Helper()
	var info struct{ Peers []json.RawMessage }
	if err := json.Unmarshal(rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "info", pool, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Peers) != 0 {
		t.Fatalf("RBD peer removal left peers: %s", info.Peers)
	}
}
