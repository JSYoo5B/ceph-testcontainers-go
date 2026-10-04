//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestMultiClusterRBDSnapshotFanout composes A -> B and A -> C links. Each
// cluster uses one OSD: this tests independent replication paths and receiver
// recovery, rather than redundancy within a storage cluster. Only rbd-mirror
// copies image data; clients perform native librbd writes and fresh reads.
func TestMultiClusterRBDSnapshotFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	a, b, aClient, bClient := newMultiClusterPair(t, ceph.WithOSDCount(1))
	c, cClient := rbdFanoutThirdCluster(t, ctx, a, b, aClient, bClient)
	clusters := []*ceph.Container{a, b, c}
	clients := []testcontainers.Container{aClient, bClient, cClient}
	var fsids [3]string
	const pool = "tc-rbd-fanout"
	const name = "volume"
	const image = pool + "/" + name
	const size = 8 << 20
	for i, cluster := range clusters {
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fsids[i] = status.FSID
		rbdMultiClusterPool(t, ctx, cluster, clients[i], pool)
	}
	ab := rbdFanoutRunLink(t, ctx, a, b, pool, "b")
	ac := rbdFanoutRunLink(t, ctx, a, c, pool, "c")
	// The receivers register their tx-only entries on A asynchronously. Wait
	// for both before enabling the image, so the initial native mirror snapshot
	// includes both destination peer UUIDs.
	rbdFanoutWaitPeers(t, ctx, aClient, bClient, cClient, pool)
	execCommand(t, ctx, aClient, "rbd", "create", image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	before := rbdMultiClusterPayload(size, 41)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 0, before)
	for _, link := range []*multicluster.RBDMirror{ab, ac} {
		if err := link.EnableImage(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, before)
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, before)
	rbdFanoutAssertImageIdentity(t, ctx, clients, image)

	stopGrace := 2 * time.Second
	if err := ab.Stop(ctx, &stopGrace); err != nil {
		t.Fatal(err)
	}
	if state, err := ab.State(ctx); err != nil || state == nil || state.Running {
		t.Fatalf("B receiver did not stop: state=%+v error=%v", state, err)
	}
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 87)
	copy(after[2<<20:], patch)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 2<<20, patch)
	if _, err := ab.SourceRBD(ctx, "mirror", "image", "snapshot", image); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, after)
	rbdFanoutRead(t, ctx, bClient, fsids[1], pool, name, before)
	t.Log("B receiver is stopped and retains its previous 8 MiB image; C independently received A's new checkpoint")

	if err := ab.Start(ctx); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, after)
	rbdFanoutRead(t, ctx, cClient, fsids[2], pool, name, after)
	final := bytes.Clone(after)
	lastPatch := rbdMultiClusterPayload(256<<10, 119)
	copy(final[6<<20:], lastPatch)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 6<<20, lastPatch)
	if _, err := ac.SourceRBD(ctx, "mirror", "image", "snapshot", image); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, final)
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, final)
	rbdFanoutRead(t, ctx, aClient, fsids[0], pool, name, final)
	rbdFanoutAssertImageIdentity(t, ctx, clients, image)
	for _, cluster := range clusters {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}

	for _, link := range []*multicluster.RBDMirror{ab, ac} {
		if err := link.Stop(ctx, &stopGrace); err != nil {
			t.Fatal(err)
		}
	}
	stopMultiClusterSource(t, ctx, a)
	rbdFanoutRead(t, ctx, bClient, fsids[1], pool, name, final)
	rbdFanoutRead(t, ctx, cClient, fsids[2], pool, name, final)
	t.Logf("snapshot fanout A -> B/C verified independent FSIDs=%v; exact %d-byte replicas sha256=%x survive stopped source MON/OSD and both stopped receivers", fsids, len(final), sha256.Sum256(final))
}

func rbdFanoutThirdCluster(t *testing.T, ctx context.Context, a, b *ceph.Container, aClient, bClient testcontainers.Container) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	image, opts := integrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithOSDCount(1))...)
	if cluster != nil {
		t.Cleanup(func() {
			if t.Failed() {
				ctrs := append([]testcontainers.Container{cluster.Container}, cluster.ServiceContainers()...)
				ctrs = append(ctrs, osdContainers(cluster)...)
				for _, ctr := range ctrs {
					logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
					multiClusterLogContainer(t, logCtx, ctr)
					logCancel()
				}
			}
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cleanupCancel()
			if err := cluster.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate third fanout cluster: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, other := range []*ceph.Container{a, b} {
		if cluster.NetworkName() == other.NetworkName() {
			t.Fatal("third fanout cluster shares an isolated Ceph network")
		}
		current, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := other.Status(ctx)
		if err != nil || current.FSID == "" || current.FSID == previous.FSID {
			t.Fatalf("third fanout cluster is not independent: FSID=%s other=%s error=%v", current.FSID, previous.FSID, err)
		}
	}
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
		testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
	if client != nil {
		cleanupMultiClusterContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	keyring := multiClusterReadFile(t, ctx, client, "/etc/ceph/ceph.client.admin.keyring")
	for _, other := range []testcontainers.Container{aClient, bClient} {
		if bytes.Equal(keyring, multiClusterReadFile(t, ctx, other, "/etc/ceph/ceph.client.admin.keyring")) {
			t.Fatal("third fanout cluster shares another cluster's admin credentials")
		}
	}
	return cluster, client
}

func rbdFanoutRunLink(t *testing.T, ctx context.Context, a, destination *ceph.Container, pool, destinationSite string) *multicluster.RBDMirror {
	t.Helper()
	image := a.ControlImage()
	link, err := multicluster.RunRBDMirror(ctx, image, multicluster.RBDMirrorConfig{
		Source: a, Destination: destination, Pool: pool, SourceSite: "a", DestinationSite: destinationSite,
	})
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() && link.Container != nil {
				multiClusterLogContainer(t, cleanupCtx, link.Container)
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate fanout link A -> %s: %v", destinationSite, err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

type rbdFanoutPoolInfo struct {
	Mode       string `json:"mode"`
	MirrorUUID string `json:"mirror_uuid"`
	Peers      []struct {
		UUID, Direction string
		SiteName        string `json:"site_name"`
		MirrorUUID      string `json:"mirror_uuid"`
	} `json:"peers"`
}

func rbdFanoutWaitPeers(t *testing.T, parent context.Context, a, b, c testcontainers.Container, pool string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		var info [3]rbdFanoutPoolInfo
		valid := true
		last = ""
		for i, client := range []testcontainers.Container{a, b, c} {
			code, output, err := rbdMultiClusterExec(ctx, client, "rbd", "mirror", "pool", "info", pool, "--format", "json")
			if err != nil || code != 0 || json.Unmarshal(output, &info[i]) != nil || info[i].Mode != "image" || info[i].MirrorUUID == "" {
				last = fmt.Sprintf("pool %d: exit=%d error=%v output=%s", i, code, err, output)
				valid = false
				break
			}
		}
		if valid && len(info[0].Peers) == 2 && len(info[1].Peers) == 1 && len(info[2].Peers) == 1 {
			seen := make(map[string]bool)
			for _, peer := range info[0].Peers {
				index := map[string]int{"b": 1, "c": 2}[peer.SiteName]
				if index == 0 || seen[peer.SiteName] || peer.UUID == "" || peer.Direction != "tx-only" || peer.MirrorUUID != info[index].MirrorUUID {
					valid = false
				}
				seen[peer.SiteName] = true
			}
			for _, destination := range info[1:] {
				peer := destination.Peers[0]
				if peer.SiteName != "a" || peer.UUID == "" || peer.Direction != "rx-only" || (peer.MirrorUUID != "" && peer.MirrorUUID != info[0].MirrorUUID) {
					valid = false
				}
			}
			if valid && seen["b"] && seen["c"] && info[0].Peers[0].UUID != info[0].Peers[1].UUID && info[0].MirrorUUID != info[1].MirrorUUID && info[0].MirrorUUID != info[2].MirrorUUID && info[1].MirrorUUID != info[2].MirrorUUID {
				t.Logf("native A pool has distinct B/C tx-only peers; each destination has only A as its rx-only source: %+v", info)
				return
			}
		}
		last = fmt.Sprintf("native fanout peer topology not ready: %+v; %s", info, last)
		select {
		case <-ctx.Done():
			t.Fatalf("wait for RBD fanout peer registration: %v; %s", ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func rbdFanoutAssertImageIdentity(t *testing.T, ctx context.Context, clients []testcontainers.Container, image string) {
	t.Helper()
	var globalID string
	for i, client := range clients {
		rbdMultiClusterAssertPrimary(t, ctx, client, image, i == 0)
		var info struct {
			Mirroring struct {
				GlobalID string `json:"global_id"`
			} `json:"mirroring"`
		}
		if err := json.Unmarshal(rbdOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			globalID = info.Mirroring.GlobalID
		} else if info.Mirroring.GlobalID != globalID {
			t.Fatalf("fanout destination %d has a different mirror image identity: %s instead of %s", i, info.Mirroring.GlobalID, globalID)
		}
	}
}

func rbdFanoutWrite(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, offset int, data []byte) {
	t.Helper()
	rbdFanoutNativeIO(t, ctx, client, "write", fsid, pool, image, offset, data)
}

func rbdFanoutRead(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	rbdFanoutNativeIO(t, ctx, client, "read", fsid, pool, image, 0, expected)
}

func rbdFanoutNativeIO(t *testing.T, ctx context.Context, client testcontainers.Container, mode, fsid, pool, image string, offset int, expected []byte) {
	t.Helper()
	const path = "/tmp/rbd-fanout-fixture"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	// The shared native I/O fixture is independent of the replication mode and
	// validates FSID, fresh connection, disabled cache and exact bytes.
	code, output, err := rbdMultiClusterExec(ctx, client, "python3", "-c", rbdJournalIOScript, mode, fsid, pool, image, fmt.Sprint(offset), path)
	if err != nil || code != 0 {
		t.Fatalf("native fanout %s failed: FSID=%s exit=%d error=%v output=%s", mode, fsid, code, err, output)
	}
}

func rbdFanoutWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	const path = "/tmp/rbd-fanout-fixture"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 35*time.Second)
		code, output, err := rbdMultiClusterExec(attempt, client, "python3", "-c", rbdJournalIOScript, "read", fsid, pool, image, "0", path)
		stop()
		if err == nil && code == 0 {
			t.Logf("fanout destination FSID=%s reached exact %d-byte checkpoint sha256=%x; native evidence=%s", fsid, len(expected), sha256.Sum256(expected), output)
			return
		}
		last = fmt.Sprintf("exit=%d error=%v output=%s", code, err, output)
		select {
		case <-ctx.Done():
			statusCtx, statusCancel := context.WithTimeout(parent, 10*time.Second)
			_, status, _ := rbdMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", pool+"/"+image, "--format", "json")
			statusCancel()
			t.Fatalf("fanout destination %s did not reach expected bytes: %v; %s; status=%s", fsid, ctx.Err(), last, status)
		case <-time.After(2 * time.Second):
		}
	}
}
