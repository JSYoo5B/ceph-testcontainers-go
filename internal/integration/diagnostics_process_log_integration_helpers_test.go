//go:build all || (integration && diagnostics)

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
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
// MGR was replaced and the MGR was failed. The MGR then authenticates with
// the key it still holds, from a respawned process or from the running one,
// depending on the release.
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
	replaced := time.Now()
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
	if _, err := cluster.Ceph(ctx, "mgr", "fail", "a"); err != nil {
		t.Fatal(err)
	}
	// A MON refuses the MGR's old key either after "mgr fail" made it respawn
	// (Tentacle) or right away, when the running process authenticates again
	// (Squid). Either way the refusal lies in the MGR's latest process.
	refusedAfter := func(result *diagnostics.ManagerRefusals) bool {
		return len(result.Monitors) == 1 && result.Monitors[0].Status == diagnostics.LogComplete &&
			slices.ContainsFunc(result.Monitors[0].Refusals, func(r diagnostics.MonitorRefusal) bool { return !r.Line.Time.Before(replaced) })
	}
	refused := awaitRefusals(t, "refused key", refusals, read, func(result *diagnostics.ManagerRefusals) bool {
		return result.Manager.Status == diagnostics.LogComplete && refusedAfter(result)
	})
	if !refused.Manager.ContainerStartedAt.Equal(restarted.ContainerStartedAt) || refused.Start.Time.Before(restarted.Start.Time) {
		t.Fatalf("the refused process is not in the restarted container run: %+v start %+v, before %+v", refused.Manager, refused.Start, restarted.LogSource)
	}
	respawned := refused.Start != restarted.Start
	if respawned {
		// The respawned process logs its refusal as it is refused.
		refused = awaitRefusals(t, "refused respawn", refusals, read, func(result *diagnostics.ManagerRefusals) bool {
			return result.Manager.Status == diagnostics.LogComplete && result.Start == refused.Start && len(result.Refusals) > 0 && refusedAfter(result)
		})
	}
	for _, refusal := range refused.Monitors[0].Refusals {
		if refusal.Reason != "unexpected key" || refusal.Line.Time.Before(refused.Start.Time) {
			t.Fatalf("MON refusal = %+v outside the process from %+v", refusal, refused.Start)
		}
	}
	for _, line := range refused.Refusals {
		if line.Time.Before(refused.Start.Time) {
			t.Fatalf("MGR refusal %+v before its process start %+v", line, refused.Start)
		}
	}
	t.Logf("native process log: restart %s -> %s, refused process from %s (respawned %t), %d MGR refusals, %d MON refusals", healthy.ContainerStartedAt.Format(time.RFC3339Nano),
		restarted.ContainerStartedAt.Format(time.RFC3339Nano), refused.Start.Time.Format(time.RFC3339Nano), respawned, len(refused.Refusals), len(refused.Monitors[0].Refusals))
}

// awaitRefusals reads until accept takes a read or two minutes pass, and
// logs the MGR's process log when none does.
func awaitRefusals(t *testing.T, what string, read func(string) *diagnostics.ManagerRefusals, processLog func(string, diagnostics.LogConfig) *diagnostics.ProcessLog, accept func(*diagnostics.ManagerRefusals) bool) *diagnostics.ManagerRefusals {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		result := read(what)
		if accept(result) {
			return result
		}
		if time.Now().After(deadline) {
			for _, line := range processLog(what+" process log", diagnostics.LogConfig{}).Lines {
				t.Log(line.Time.Format(time.RFC3339Nano), line.Text)
			}
			t.Fatalf("%s: no accepted read", what)
		}
		time.Sleep(2 * time.Second)
	}
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
