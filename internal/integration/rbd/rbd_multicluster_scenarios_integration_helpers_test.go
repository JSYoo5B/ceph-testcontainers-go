//go:build all || (integration && multicluster)

package rbd_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

func rbdScenarioSeed(t *testing.T, ctx context.Context, client testcontainers.Container, image string, seed int) []byte {
	t.Helper()
	data := harness.RBDMultiClusterPayload(8<<20, seed)
	if err := client.CopyToContainer(ctx, data, "/tmp/rbd-scenario-seed", 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "rbd", "import", "/tmp/rbd-scenario-seed", image,
		"--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
	return data
}

func rbdScenarioStop(t *testing.T, ctx context.Context, link *rbdmirror.Mirror) {
	t.Helper()
	grace := 3 * time.Second
	if err := link.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
}

func rbdScenarioWaitSplitBrain(t *testing.T, parent context.Context, link *rbdmirror.Mirror, image string) {
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

func rbdScenarioRequireSnapshotAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, image, snapshot string) {
	t.Helper()
	var snapshots []struct{ Name string }
	if err := json.Unmarshal(harness.RBDOutput(t, ctx, client, "snap", "ls", image, "--format", "json"), &snapshots); err != nil {
		t.Fatal(err)
	}
	for _, snap := range snapshots {
		if snap.Name == snapshot {
			t.Fatalf("losing branch snapshot %s survived authoritative resync", snapshot)
		}
	}
}

func rbdScenarioRequireNoPeers(t *testing.T, ctx context.Context, link *rbdmirror.Mirror, pool string) {
	t.Helper()
	var info struct{ Peers []json.RawMessage }
	if err := json.Unmarshal(harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "info", pool, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Peers) != 0 {
		t.Fatalf("RBD peer removal left peers: %s", info.Peers)
	}
}
