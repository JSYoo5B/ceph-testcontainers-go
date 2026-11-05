//go:build all || (integration && multicluster)

package harness

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

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

func RBDScenarioCommand(t *testing.T, ctx context.Context, command func(context.Context, ...string) ([]byte, error), args ...string) []byte {
	t.Helper()
	out, err := command(ctx, args...)
	if err != nil {
		t.Fatalf("RBD %s: %v", strings.Join(args, " "), err)
	}
	return out
}

func RBDScenarioWaitTransmitPeer(t *testing.T, parent context.Context, command func(context.Context, ...string) ([]byte, error), pool, site string) {
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

func RBDScenarioPeer(t *testing.T, ctx context.Context, link *rbdmirror.Container, pool string) string {
	t.Helper()
	var info struct{ Peers []struct{ UUID string } }
	if err := json.Unmarshal(RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "info", pool, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Peers) != 1 || info.Peers[0].UUID == "" {
		t.Fatalf("expected one configured RBD peer: %+v", info.Peers)
	}
	return info.Peers[0].UUID
}

func RBDScenarioRunLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *rbdmirror.Container {
	t.Helper()
	image := StaticFacts(source).Images.RBDAux
	link, err := rbdmirror.Run(ctx, NativeMirrorImage(t, "rbd-aux", image), rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite,
	}))
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && link.Container != nil {
				MultiClusterLogContainer(t, cleanupCtx, link.Container)
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

func RBDScenarioWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		_, _, _ = RBDMultiClusterExec(attempt, client, "rm", "-f", "/tmp/rbd-scenario-result")
		code, out, err := RBDMultiClusterExec(attempt, client, "rbd", "export", image, "/tmp/rbd-scenario-result", "--no-progress")
		if err == nil && code == 0 {
			reader, readErr := client.CopyFileFromContainer(attempt, "/tmp/rbd-scenario-result")
			if readErr == nil {
				actual, readErr := io.ReadAll(reader)
				_ = reader.Close()
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
