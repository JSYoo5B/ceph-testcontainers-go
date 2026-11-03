//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
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

func RBDScenarioPeer(t *testing.T, ctx context.Context, link *rbdmirror.Mirror, pool string) string {
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
