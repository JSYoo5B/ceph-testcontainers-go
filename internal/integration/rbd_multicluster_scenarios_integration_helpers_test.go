//go:build all || (integration && multicluster)

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
