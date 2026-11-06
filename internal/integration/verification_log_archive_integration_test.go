//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// archivedLogLine is one line of a verification-<n>-<name>-logs.jsonl file.
type archivedLogLine struct {
	Source    string `json:"source"`
	Stream    string `json:"stream"`
	Time      string `json:"time"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}

var (
	archivedMonDown   = regexp.MustCompile(`cluster \[WRN\] Health check failed: 1 osds down \(OSD_DOWN\)`)
	archivedMonBoot   = regexp.MustCompile(`log \[INF\] : osd\.0 \S+ boot$`)
	archivedOSDWrong  = regexp.MustCompile(`wrongly marked me down`)
	archivedOSDMarked = regexp.MustCompile(`log \[WRN\] : Monitor daemon marked osd\.0 down, but it is still running`)
)

// TestVerificationLogArchive marks osd.0 down under noup, confirms the down
// and the recovery through the OSDMap, then raises osd.0's debug level until
// its diagnostics log tail no longer reaches back to the interval. A forced check failure takes the failure
// archive. The archived window lines of a verification with ArchiveLogs must
// still hold the MON's and the OSD's own records of the interval after the
// cluster is terminated, where the diagnostics log tail of osd.0 no longer
// does.
func TestVerificationLogArchive(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	recorder := &finalizationRecorder{TB: t}
	archives := t.TempDir()
	var finalization *finalize.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(recorder, cluster, finalize.Config{Archive: true, ArchiveDirectory: archives, Scenario: "verification-log-archive"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
		t.Cleanup(recorder.finalize)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	owned := []string{cluster.GetContainerID(), clusteraccess.ControlContainer(t, cluster).GetContainerID(), clusteraccess.OSDs(t, cluster)[0].GetContainerID()}
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		owned = append(owned, mgr.GetContainerID())
	}

	if _, err := finalization.Verify(ctx, finalize.VerificationConfig{
		Name:        "osd-interval",
		ArchiveLogs: true,
		MaxFindings: 4096,
		Logs: []finalize.LogExpectation{
			{Name: "mon-osd-down", Required: true, Daemon: "mon.a", Event: finalize.LogClusterWarning, Pattern: `OSD_DOWN`},
			{Name: "osd-marked-down", Required: true, Daemon: "osd.0", Event: finalize.LogClusterWarning, Pattern: `Monitor daemon marked osd\.0 down`},
			// The noup flag and the down OSD, its host and its root raise
			// further health warnings, and the MON writes osd.0's entry
			// again as the cluster log.
			{Name: "interval-warnings", Event: finalize.LogClusterWarning, Pattern: `Health check (failed|update): .*\((OSDMAP_FLAGS|OSD_[A-Z_]+|PG_[A-Z_]+)\)$|Monitor daemon marked osd\.0 down, but it is still running$`},
		},
	}); err != nil {
		t.Fatal(err)
	}

	noup, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoUp, true)
	if err != nil {
		t.Fatal(err)
	}
	harness.MustCeph(t, ctx, cluster, "osd", "down", "0")
	verificationWaitOSDUp(t, ctx, cluster, false)
	if err := noup.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	verificationWaitOSDUp(t, ctx, cluster, true)
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	noise, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "20"})
	finalization.TrackDebugLevel(noise)
	if err != nil {
		t.Fatal(err)
	}
	// At debug_osd 20 an idle OSD writes a few KiB a second; wait, bounded,
	// until its diagnostics log tail no longer reaches back to the interval.
	verificationWaitTailPast(t, ctx, cluster, archivedOSDWrong)
	if err := noise.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	finalization.Check("take the failure archive", func(context.Context) error { return errors.New("archive requested by the test") })

	recorder.finalize()
	failures := recorder.failures()
	if len(failures) != 1 || !strings.Contains(failures[0], "archive requested by the test") {
		t.Fatalf("finalization failures %q", failures)
	}
	verificationAssertRemoved(t, owned)

	directories, err := filepath.Glob(filepath.Join(archives, "*", "ceph-*"))
	if err != nil || len(directories) != 1 {
		t.Fatalf("archives %v %v", directories, err)
	}
	directory := directories[0]
	var report finalize.VerificationReport
	verificationReadJSON(t, filepath.Join(directory, "verification-1-osd-interval.json"), &report)
	logVerification(t, "archived", &report)
	if report.Outcome != finalize.VerificationClean {
		t.Fatalf("verification %s", report.Outcome)
	}
	lines := verificationReadLogLines(t, filepath.Join(directory, "verification-1-osd-interval-logs.jsonl"))
	perSource := map[string]int{}
	for _, line := range lines {
		perSource[line.Source]++
	}
	for _, source := range report.Sources {
		if source.LogLines == 0 || perSource[source.Source] != source.LogLines {
			t.Fatalf("source %s: %d archived lines, report %+v", source.Source, perSource[source.Source], source)
		}
	}
	first := func(source string, pattern *regexp.Regexp) archivedLogLine {
		t.Helper()
		index := slices.IndexFunc(lines, func(line archivedLogLine) bool { return line.Source == source && pattern.MatchString(line.Text) })
		if index < 0 {
			t.Fatalf("archived %s lines lack %s", source, pattern)
		}
		t.Logf("archived %s %s %s %.200s", lines[index].Source, lines[index].Stream, lines[index].Time, lines[index].Text)
		return lines[index]
	}
	down, boot := first("mon.a", archivedMonDown), first("mon.a", archivedMonBoot)
	marked, wrong := first("osd.0", archivedOSDMarked), first("osd.0", archivedOSDWrong)
	if !(down.Time < boot.Time && marked.Time <= wrong.Time) {
		t.Fatalf("archived interval out of order: %+v %+v %+v %+v", down, boot, marked, wrong)
	}

	// The diagnostics in the same archive keep only a log tail: after the
	// debug output, osd.0's no longer reaches back to the interval.
	var collected diagnostics.Report
	verificationReadJSON(t, filepath.Join(directory, "diagnostics.json"), &collected)
	tail := slices.IndexFunc(collected.Artifacts, func(a diagnostics.Artifact) bool {
		return a.Kind == "container-logs" && a.Role == "osd" && a.Name == "0"
	})
	if tail < 0 {
		t.Fatal("the archive has no osd.0 log tail")
	}
	t.Logf("diagnostics osd.0 tail: %d bytes, truncated %v; archived window: %d osd.0 lines", len(collected.Artifacts[tail].Data), collected.Artifacts[tail].Truncated, perSource["osd.0"])
	if archivedOSDWrong.MatchString(collected.Artifacts[tail].Data) {
		t.Fatal("the debug output did not move osd.0's log tail past the interval")
	}
}

// verificationWaitTailPast waits until the default diagnostics log tail of
// osd.0 no longer holds a line that pattern matches.
func verificationWaitTailPast(t *testing.T, ctx context.Context, cluster *ceph.Container, pattern *regexp.Regexp) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("osd.0's log tail still reaches back to the interval: %v", ctx.Err())
		case <-time.After(5 * time.Second):
		}
		report, _ := diagnostics.Collect(ctx, cluster, diagnostics.Config{Timeout: 30 * time.Second})
		if report == nil {
			continue
		}
		tail := slices.IndexFunc(report.Artifacts, func(a diagnostics.Artifact) bool {
			return a.Kind == "container-logs" && a.Role == "osd" && a.Name == "0" && a.Error == ""
		})
		if tail >= 0 && !pattern.MatchString(report.Artifacts[tail].Data) {
			return
		}
	}
}

func verificationWaitOSDUp(t *testing.T, ctx context.Context, cluster *ceph.Container, up bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		var dump struct {
			OSDs []struct {
				OSD int `json:"osd"`
				Up  int `json:"up"`
			} `json:"osds"`
		}
		out := harness.MustCeph(t, ctx, cluster, "osd", "dump", "--format", "json")
		if err := json.Unmarshal(out, &dump); err != nil || len(dump.OSDs) != 1 {
			t.Fatalf("osd dump %v: %s", err, out)
		}
		if (dump.OSDs[0].Up == 1) == up {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("osd.0 up=%d, want up=%v: %v", dump.OSDs[0].Up, up, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// verificationAssertRemoved checks that no container of the terminated
// cluster remains, so the archive is all that is left of their logs.
func verificationAssertRemoved(t *testing.T, ids []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	for _, id := range ids {
		if _, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatalf("container %s of the terminated cluster: %v", id, err)
		}
	}
}

func verificationReadJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func verificationReadLogLines(t *testing.T, path string) []archivedLogLine {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []archivedLogLine
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scanner.Scan() {
		var line archivedLogLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}
