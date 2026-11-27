//go:build all || (integration && diagnostics)

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// testDaemonProcessLog runs one MON and one MGR. It reads the MGR's process
// log and refusals while it is healthy, with a bound smaller than the
// process's log, after a container restart, and after the MONs' key of the
// MGR was replaced and the MGR was failed, so that it respawns in its
// container and authenticates with the key it still holds.
func testDaemonProcessLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	manager := harness.ManagerHandle(t, ctx, cluster, "a")
	read := func(what string, config diagnostics.LogConfig) *diagnostics.ProcessLog {
		t.Helper()
		log, err := diagnostics.CollectProcessLog(ctx, cluster, manager, config)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		t.Logf("%s: %s, container started %s, running %t, start %s %q, %d lines", what, log.Status, log.ContainerStartedAt.Format(time.RFC3339Nano),
			log.Running, log.Start.Time.Format(time.RFC3339Nano), log.Start.Text, len(log.Lines))
		return log
	}
	refusals := func(what string) *diagnostics.ManagerRefusals {
		t.Helper()
		result, err := diagnostics.CollectManagerRefusals(ctx, cluster, manager, diagnostics.LogConfig{})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		data, _ := json.Marshal(result)
		t.Logf("%s: %s", what, data)
		return result
	}

	healthy := read("healthy", diagnostics.LogConfig{})
	if healthy.Status != diagnostics.LogComplete || !healthy.Running || healthy.Start.Text == "" || !strings.Contains(healthy.Start.Text, ", process ceph-mgr, pid ") ||
		healthy.Lines[0] != healthy.Start || healthy.Start.Time.Before(healthy.ContainerStartedAt) {
		t.Fatalf("healthy MGR process log = %+v", healthy.LogSource)
	}
	clean := refusals("healthy refusals")
	if clean.Manager.Status != diagnostics.LogComplete || len(clean.Refusals) != 0 || len(clean.Monitors) != 1 || clean.Monitors[0].Name != "a" ||
		clean.Monitors[0].Status != diagnostics.LogComplete || len(clean.Monitors[0].Refusals) != 0 || !clean.Monitors[0].Since.Equal(healthy.Start.Time) {
		t.Fatalf("healthy MGR refusals = %+v", clean)
	}
	bounded := read("bounded", diagnostics.LogConfig{MaxBytes: len(healthy.Start.Text) + 1})
	if bounded.Status != diagnostics.LogTruncated || bounded.Start != healthy.Start || len(bounded.Lines) != 1 {
		t.Fatalf("bounded MGR process log = %+v", bounded.LogSource)
	}

	grace := 10 * time.Second
	if err := manager.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := awaitProcessLog(t, ctx, "restarted", read, func(log *diagnostics.ProcessLog) bool {
		return log.Status == diagnostics.LogComplete && log.ContainerStartedAt.After(healthy.ContainerStartedAt)
	})
	if !restarted.Start.Time.After(healthy.Lines[len(healthy.Lines)-1].Time) || restarted.Lines[0] != restarted.Start {
		t.Fatalf("the restarted MGR's log merged its earlier run: start %+v after %+v", restarted.Start, healthy.Lines[len(healthy.Lines)-1])
	}

	// The MONs get another key for mgr.a; its keyring keeps the old one.
	data, err := cluster.Ceph(ctx, "auth", "get", "mgr.a", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []struct {
		Caps map[string]string `json:"caps"`
	}
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) != 1 || len(entries[0].Caps) == 0 {
		t.Fatalf("auth get mgr.a = %s, %v", data, err)
	}
	if _, err := cluster.Ceph(ctx, "auth", "rm", "mgr.a"); err != nil {
		t.Fatal(err)
	}
	add := []string{"auth", "add", "mgr.a"}
	for service, caps := range entries[0].Caps {
		add = append(add, service, caps)
	}
	if _, err := cluster.Ceph(ctx, add...); err != nil {
		t.Fatal(err)
	}
	failed := time.Now()
	if _, err := cluster.Ceph(ctx, "mgr", "fail", "a"); err != nil {
		t.Fatal(err)
	}
	var refused *diagnostics.ManagerRefusals
	deadline := time.Now().Add(2 * time.Minute)
	for {
		refused = refusals("refused key")
		if refused.Manager.Status == diagnostics.LogComplete && refused.Start.Time.After(failed.Add(-time.Second)) && len(refused.Refusals) > 0 &&
			len(refused.Monitors) == 1 && refused.Monitors[0].Status == diagnostics.LogComplete && len(refused.Monitors[0].Refusals) > 0 {
			break
		}
		if time.Now().After(deadline) {
			log := read("refused key process log", diagnostics.LogConfig{})
			for _, line := range log.Lines {
				t.Log(line.Time.Format(time.RFC3339Nano), line.Text)
			}
			t.Fatal("no attributed refusal of the respawned MGR")
		}
		time.Sleep(2 * time.Second)
	}
	if !refused.Manager.ContainerStartedAt.Equal(restarted.ContainerStartedAt) || !refused.Start.Time.After(restarted.Start.Time) {
		for _, line := range read("refused key process log", diagnostics.LogConfig{}).Lines {
			t.Log(line.Time.Format(time.RFC3339Nano), line.Text)
		}
		t.Fatalf("the respawn is not a new process in the same container run: %+v start %+v, before %+v", refused.Manager, refused.Start, restarted.LogSource)
	}
	for _, refusal := range refused.Monitors[0].Refusals {
		if refusal.Reason != "unexpected key" || refusal.Line.Time.Before(refused.Start.Time) {
			t.Fatalf("MON refusal = %+v", refusal)
		}
	}
	t.Logf("native process log: restart %s -> %s, respawn start %s, %d MGR refusals, %d MON refusals", healthy.ContainerStartedAt.Format(time.RFC3339Nano),
		restarted.ContainerStartedAt.Format(time.RFC3339Nano), refused.Start.Time.Format(time.RFC3339Nano), len(refused.Refusals), len(refused.Monitors[0].Refusals))
}

// awaitProcessLog reads until accept takes a read or a minute passes.
func awaitProcessLog(t *testing.T, ctx context.Context, what string, read func(string, diagnostics.LogConfig) *diagnostics.ProcessLog, accept func(*diagnostics.ProcessLog) bool) *diagnostics.ProcessLog {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		log := read(what, diagnostics.LogConfig{})
		if accept(log) {
			return log
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: no accepted read", what)
		}
		time.Sleep(time.Second)
	}
}
