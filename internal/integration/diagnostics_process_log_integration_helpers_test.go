//go:build all || (integration && diagnostics)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
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

	restarted = readAcrossRestarts(t, ctx, cluster, manager)
	// The key change below fails the active MGR, so wait until it is active.
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
		if err == nil && status.Available && status.ActiveName == "a" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mgr.a is not active again after the restarts: %+v, %v", status, err)
		}
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

// readAcrossRestarts reads manager's process log back to back while it
// restarts the container, up to ten times, until one read spans a change of
// the container's run and reports LogChanged. Every read reports a status
// that matches what it covered: a read inside one run is complete, or has no
// start yet right after the container started. It returns a complete read
// after the last restart.
func readAcrossRestarts(t *testing.T, ctx context.Context, cluster *ceph.Container, manager *ceph.ManagerContainer) *diagnostics.ProcessLog {
	t.Helper()
	statuses := map[diagnostics.LogStatus]int{}
	for cycle := 1; ; cycle++ {
		done := make(chan struct{})
		reads := make(chan []*diagnostics.ProcessLog, 1)
		go func() {
			var logs []*diagnostics.ProcessLog
			defer func() { reads <- logs }()
			for {
				select {
				case <-done:
					return
				default:
				}
				log, err := diagnostics.CollectProcessLog(ctx, cluster, manager, diagnostics.LogConfig{})
				if err != nil {
					t.Errorf("read during the restart: %v", err)
					return
				}
				logs = append(logs, log)
			}
		}()
		grace := 10 * time.Second
		stopErr := manager.Stop(ctx, &grace)
		startErr := manager.Start(ctx)
		close(done)
		logs := <-reads
		if stopErr != nil || startErr != nil || t.Failed() {
			t.Fatalf("restart %d: stop %v, start %v", cycle, stopErr, startErr)
		}
		changed := 0
		for _, log := range logs {
			statuses[log.Status]++
			switch log.Status {
			case diagnostics.LogChanged:
				changed++
			case diagnostics.LogComplete:
				if log.Start.Time.Before(log.ContainerStartedAt) || log.Lines[0] != log.Start {
					t.Fatalf("restart %d: a complete read outside its run: %+v start %+v", cycle, log.LogSource, log.Start)
				}
			case diagnostics.LogStartNotFound:
				// Only a container that has just started has not logged it yet.
				if !log.Running {
					t.Fatalf("restart %d: a stopped run without a process start: %+v", cycle, log.LogSource)
				}
			default:
				t.Fatalf("restart %d: read status %s: %+v", cycle, log.Status, log.LogSource)
			}
		}
		t.Logf("restart %d: %d reads, %d changed", cycle, len(logs), changed)
		if changed > 0 {
			break
		}
		if cycle == 10 {
			t.Fatalf("no read of %d restarts reported a changed run: %v", cycle, statuses)
		}
	}
	var after *diagnostics.ProcessLog
	deadline := time.Now().Add(time.Minute)
	for {
		log, err := diagnostics.CollectProcessLog(ctx, cluster, manager, diagnostics.LogConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if log.Status == diagnostics.LogComplete && log.Running {
			after = log
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no complete read after the restarts: %+v", log.LogSource)
		}
		time.Sleep(time.Second)
	}
	t.Logf("reads across restarts: %v", statuses)
	return after
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

// testManagerReclaimRefusal freezes the MGR for longer than the MONs keep
// the rotating secret of its last auth ticket, under a short
// auth_mon_ticket_ttl, and resumes it. The MGR's MonClient then renews with
// that ticket and reclaims its global ID; the MON refuses the reclaim and
// names the global ID, the MGR's GID in the MgrMap read before the freeze.
func testManagerReclaimRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	const ttl = 10 * time.Second
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithMonitorCount(2)),
		ceph.WithAuth(ceph.AuthConfig{MonTicketTTL: ttl}))...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	manager := harness.ManagerHandle(t, ctx, cluster, "a")
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || status.ActiveName != "a" || status.ActiveGID == 0 {
		t.Fatalf("manager status = %+v, %v", status, err)
	}
	pause, err := fault.Pause(ctx, cluster, manager)
	if err != nil {
		t.Fatal(err)
	}
	frozen := time.Now()
	select {
	case <-time.After(6 * ttl):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	read := func(what string) *diagnostics.ManagerRefusals {
		t.Helper()
		result, err := diagnostics.CollectManagerRefusals(ctx, cluster, manager, diagnostics.LogConfig{})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		data, _ := json.Marshal(result)
		t.Logf("%s: %s", what, data)
		return result
	}
	processLog := func(what string, config diagnostics.LogConfig) *diagnostics.ProcessLog {
		t.Helper()
		log, err := diagnostics.CollectProcessLog(ctx, cluster, manager, config)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		return log
	}
	reclaim := func(r diagnostics.MonitorRefusal) bool { return r.GlobalID != 0 && !r.Line.Time.Before(frozen) }
	refused := awaitRefusals(t, "reclaim", read, processLog, func(result *diagnostics.ManagerRefusals) bool {
		if result.Manager.Status != diagnostics.LogComplete || len(result.Monitors) != 2 {
			return false
		}
		found := false
		for _, monitor := range result.Monitors {
			if monitor.Status != diagnostics.LogComplete {
				return false
			}
			found = found || slices.ContainsFunc(monitor.Refusals, reclaim)
		}
		return found
	})
	// Every MonClient of the process reclaims its own global ID: the beacon's
	// is the MgrMap GID, the module clients' are others.
	beacon := map[string]bool{}
	var ids []string
	for _, monitor := range refused.Monitors {
		for _, refusal := range monitor.Refusals {
			if !reclaim(refusal) {
				continue
			}
			if refusal.Line.Time.Before(refused.Start.Time) || !slices.Contains([]string{"without presenting ticket", "using bad ticket", "using mismatching ticket"}, refusal.Reason) {
				t.Fatalf("mon.%s reclaim refusal %+v outside mgr.a's process from %+v", monitor.Name, refusal, refused.Start)
			}
			beacon[monitor.Name] = beacon[monitor.Name] || refusal.GlobalID == status.ActiveGID
			ids = append(ids, fmt.Sprintf("mon.%s %s %d", monitor.Name, refusal.Reason, refusal.GlobalID))
		}
	}
	if !beacon["a"] && !beacon["b"] {
		t.Fatalf("no MON refused the reclaim of mgr.a's GID %d: %s", status.ActiveGID, strings.Join(ids, ", "))
	}
	// Tentacle's MonClient logs its refusal at level -1; Squid's only at 10.
	version, err := manager.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "19.") && len(refused.Refusals) == 0 {
		t.Fatalf("mgr.a %s logged no MonClient refusal in its process", version)
	}
	t.Logf("native reclaim refusal: mgr.a %s GID %d, process from %s; MON refusals %s; %d MGR refusal lines",
		version, status.ActiveGID, refused.Start.Time.Format(time.RFC3339Nano), strings.Join(ids, ", "), len(refused.Refusals))
}
