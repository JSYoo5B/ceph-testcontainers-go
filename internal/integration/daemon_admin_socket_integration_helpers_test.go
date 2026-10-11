//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// testDaemonAdminSocket reads each daemon's own view through its admin socket
// and checks it against the cluster maps and the daemon's lifecycle.
func testDaemonAdminSocket(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)}, opts...)...)

	for _, daemon := range []string{"mon.a", "mgr.a", "osd.0"} {
		out, err := cluster.AdminSocket(ctx, daemon, "version")
		var version struct {
			Version string `json:"version"`
		}
		if err != nil || json.Unmarshal(out, &version) != nil || !strings.HasPrefix(version.Version, cluster.CephVersion()) {
			t.Fatalf("%s admin socket version %q, err=%v, cluster release %s", daemon, out, err, cluster.CephVersion())
		}
	}
	out, err := cluster.AdminSocket(ctx, "mon.a", "mon_status")
	var monStatus struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err != nil || json.Unmarshal(out, &monStatus) != nil || monStatus.Name != "a" || monStatus.State != "leader" {
		t.Fatalf("MON a admin socket status %q, err=%v", out, err)
	}

	// A new pool advances the OSDMap; the OSD's own status reports when it has
	// applied that epoch, which the cluster maps alone cannot show.
	before := adminSocketOSDMapEpoch(t, ctx, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-admin-socket"}); err != nil {
		t.Fatal(err)
	}
	after := adminSocketOSDMapEpoch(t, ctx, cluster)
	if after <= before {
		t.Fatalf("pool creation did not advance the OSDMap: %d -> %d", before, after)
	}
	applied := waitAdminSocketOSDEpoch(t, ctx, cluster, after)
	t.Logf("osd.0 applied OSDMap epoch %d (cluster epoch %d, before pool %d)", applied, after, before)

	// The socket belongs to the running process: a stopped OSD has none, and
	// the restarted process answers again.
	osd := cluster.OSDs()[0]
	stop := 10 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.AdminSocket(ctx, "osd.0", "status"); err == nil {
		t.Fatal("a stopped OSD answered its admin socket")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitAdminSocketOSDEpoch(t, ctx, cluster, after)

	if _, err := cluster.AdminSocket(ctx, "osd.9", "status"); err == nil {
		t.Fatal("an OSD that the cluster does not own was reachable")
	}
	t.Log("MON, MGR and OSD admin sockets: versions match the cluster release, MON a is leader, osd.0 applied the new OSDMap epoch before and after a restart")
}

func adminSocketOSDMapEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container) int {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	var dump struct {
		Epoch int `json:"epoch"`
	}
	if err != nil || json.Unmarshal(out, &dump) != nil || dump.Epoch <= 0 {
		t.Fatalf("osd dump: %q, %v", out, err)
	}
	return dump.Epoch
}

// waitAdminSocketOSDEpoch waits until osd.0 reports an applied OSDMap epoch
// of at least want. A starting OSD may answer before it registers "status".
func waitAdminSocketOSDEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container, want int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	for {
		out, err := cluster.AdminSocket(ctx, "osd.0", "status")
		var status struct {
			State     string `json:"state"`
			NewestMap string `json:"newest_map"`
		}
		if err == nil && json.Unmarshal(out, &status) == nil {
			// OSD::status prints epochs as JSON strings.
			if newest, convErr := strconv.Atoi(status.NewestMap); convErr == nil && newest >= want && status.State == "active" {
				return newest
			}
		}
		last = string(out)
		if err != nil {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("osd.0 did not report OSDMap epoch %d: last %s", want, last)
		case <-time.After(time.Second):
		}
	}
}
