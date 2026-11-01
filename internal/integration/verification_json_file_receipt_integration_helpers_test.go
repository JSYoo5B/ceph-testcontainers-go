//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/pkg/stdcopy"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	jsonFileSource     = "mon.a"
	jsonFileTailLines  = 24
	jsonFileNoiseCount = 10
	jsonFileReadLimit  = 64 << 20
)

// jsonFileMonCommands sends raw MON commands, one per line of its argument,
// through librados so that each command reaches the MON byte for byte. It
// prints the return code and status of each and fails on the first nonzero
// return code.
const jsonFileMonCommands = `import rados,sys,json
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_mon_op_timeout':'30'})
client.connect()
results=[]
try:
    for command in sys.argv[1].split('\n'):
        r,out,err=client.mon_command(command,b'')
        results.append({'r':r,'status':err})
        if r!=0:
            break
finally:
    client.shutdown()
print(json.dumps(results))
sys.exit(0 if results and all(x['r']==0 for x in results) else 1)
`

// jsonFileFinalizer is the test of a finalization whose verification failure
// the scenario expects (the quoting INF entry must leave its required warning
// unsatisfied): it records the failures instead of failing the test, and runs
// the finalization once, when the scenario calls finalize or at the latest in
// the test's cleanup.
type jsonFileFinalizer struct {
	testing.TB
	mu      sync.Mutex
	failed  bool
	errors  []string
	cleanup func()
	once    sync.Once
}

func (r *jsonFileFinalizer) Cleanup(cleanup func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanup = cleanup
}

func (r *jsonFileFinalizer) finalize() {
	r.once.Do(func() {
		r.mu.Lock()
		cleanup := r.cleanup
		r.mu.Unlock()
		if cleanup != nil {
			cleanup()
		}
	})
}

func (r *jsonFileFinalizer) Failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed
}

func (r *jsonFileFinalizer) Errorf(format string, args ...any) {
	r.mu.Lock()
	r.failed = true
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	r.TB.Logf("recorded finalization failure: "+format, args...)
}

func (r *jsonFileFinalizer) Error(args ...any) { r.Errorf("%s", fmt.Sprint(args...)) }
func (r *jsonFileFinalizer) Fail()             { r.Errorf("failed") }
func (r *jsonFileFinalizer) FailNow() {
	r.Fail()
	runtime.Goexit()
}
func (r *jsonFileFinalizer) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}
func (r *jsonFileFinalizer) Fatal(args ...any) {
	r.Error(args...)
	runtime.Goexit()
}

func (r *jsonFileFinalizer) failures() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errors)
}

// jsonFileEngineClock reads the engine's clock as the verifier does, with
// the engine's own client (Testcontainers' Info caches its first answer).
func jsonFileEngineClock(ctx context.Context, docker *mobycl.Client) (time.Time, error) {
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return time.Time{}, err
	}
	clock, err := time.Parse(time.RFC3339Nano, info.Info.SystemTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("the engine reported its time as %q", info.Info.SystemTime)
	}
	return clock.UTC(), nil
}

// jsonFileInspect identifies a container generation and its log driver.
func jsonFileInspect(ctx context.Context, docker *mobycl.Client, id string) (jsonFileGeneration, error) {
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return jsonFileGeneration{ContainerID: id}, err
	}
	ctr := inspection.Container
	generation := jsonFileGeneration{ContainerID: id, RestartCount: ctr.RestartCount, Present: true}
	if ctr.State != nil {
		generation.StartedAt = ctr.State.StartedAt
	}
	if ctr.HostConfig != nil {
		generation.LogDriver = ctr.HostConfig.LogConfig.Type
		if size := ctr.HostConfig.LogConfig.Config["max-size"]; size != "" && size != "-1" {
			generation.MaxSize = size
		}
	}
	return generation, nil
}

// jsonFileReadEngineLog reads the whole log of a container from the engine
// with its timestamps, independently of the verifier, and returns its lines
// and the bytes read. A log of more than jsonFileReadLimit bytes is an error.
func jsonFileReadEngineLog(ctx context.Context, docker *mobycl.Client, id string) ([]string, int64, error) {
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, 0, err
	}
	stream, err := docker.ContainerLogs(ctx, id, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true})
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = stream.Close() }()
	var stdout, stderr bytes.Buffer
	counted := &io.LimitedReader{R: stream, N: jsonFileReadLimit + 1}
	if inspection.Container.Config != nil && inspection.Container.Config.Tty {
		// A TTY container's log is not multiplexed.
		_, err = io.Copy(&stdout, counted)
	} else {
		_, err = stdcopy.StdCopy(&stdout, &stderr, counted)
	}
	if err != nil {
		return nil, 0, err
	}
	if counted.N <= 0 {
		return nil, 0, fmt.Errorf("the engine log exceeds %d bytes", jsonFileReadLimit)
	}
	size := int64(stdout.Len() + stderr.Len())
	var lines []string
	for _, buffer := range []*bytes.Buffer{&stdout, &stderr} {
		for _, line := range strings.Split(buffer.String(), "\n") {
			if line = strings.TrimRight(line, "\r"); line != "" {
				lines = append(lines, line)
			}
		}
	}
	return lines, size, nil
}

// testVerificationJSONFileNativeReceipt observes one MON generation on the
// engine's non-rotating json-file driver. It emits, through the MON's own
// `log` command, an early structural WRN sentinel, an INF entry that only
// quotes a WRN marker, a run of INF noise and a tail WRN sentinel; reads the
// MON's engine log independently; collects a diagnostic tail too short to
// hold the early sentinel; and evaluates the verification. The receipt is
// logged on every outcome, before the cleanups destroy the cluster.
func testVerificationJSONFileNativeReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	recorder := &jsonFileFinalizer{TB: t}
	var finalization *finalize.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(recorder, cluster, finalize.Config{Scenario: "verification-json-file-receipt"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
		// Whatever ends the test body, the recorded finalization runs.
		t.Cleanup(recorder.finalize)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	receipt := jsonFileReceipt{Source: jsonFileSource}
	var judged error
	// The receipt is logged on every outcome of the body, with whatever it
	// holds, before the cleanups run.
	defer func() {
		data, err := json.Marshal(receipt.summary())
		if err != nil {
			t.Logf("JSON_FILE_RECEIPT could not be encoded: %v", err)
			return
		}
		verdict := "accepted"
		switch {
		case judged != nil:
			verdict = "refused: " + judged.Error()
		case receipt.Report == nil:
			verdict = "not judged"
		}
		t.Logf("JSON_FILE_RECEIPT %s judge %s", data, verdict)
	}()

	dockerClient, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dockerClient.Close() }()
	docker := dockerClient.Client
	monitors := clusteraccess.Monitors(t, cluster)
	if len(monitors) != 1 || monitors[0].DaemonName != "a" {
		t.Fatalf("expected the single MON a, got %d monitors", len(monitors))
	}
	monID := monitors[0].GetContainerID()
	if receipt.Baseline, err = jsonFileInspect(ctx, docker, monID); err != nil {
		t.Fatal(err)
	}

	nonce := make([]byte, 6)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(nonce)
	early, quote, tail := "jfr-early-"+suffix, "jfr-quote-"+suffix, "jfr-tail-"+suffix
	expectation := func(name, id string) finalize.LogExpectation {
		return finalize.LogExpectation{Name: name, Required: true, Daemon: jsonFileSource, Event: finalize.LogClusterWarning, Pattern: `.*` + regexp.QuoteMeta(id) + `.*`}
	}
	policy, err := finalization.Verify(ctx, finalize.VerificationConfig{
		Name:        "json-file-receipt",
		MaxFindings: 4096,
		Logs:        []finalize.LogExpectation{expectation(jsonFileEarlyName, early), expectation(jsonFileTailName, tail), expectation(jsonFileQuoteName, quote)},
	})
	if err != nil {
		t.Fatal(err)
	}

	// emit sends the commands of one role (one per line) and records the
	// engine's clock around them.
	emit := func(role string, ids []string, commands []string) {
		t.Helper()
		before, err := jsonFileEngineClock(ctx, docker)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		out := harness.ExecOutput(t, ctx, cluster.Container, "python3", "-c", jsonFileMonCommands, strings.Join(commands, "\n"))
		receipt.Elapsed.Emission += time.Since(started)
		after, err := jsonFileEngineClock(ctx, docker)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			receipt.Events = append(receipt.Events, jsonFileEvent{ID: id, Role: role, SampleBefore: before, SampleAfter: after})
		}
		t.Logf("emitted %s %v: %s", role, ids, out)
	}
	logCommand := func(level, text string) string {
		if level == "" {
			return `{"prefix": "log", "logtext": ["` + text + `"]}`
		}
		return `{"prefix": "log", "level": "` + level + `", "logtext": ["` + text + `"]}`
	}
	emit(jsonFileRoleEarly, []string{early}, []string{logCommand("warn", early)})
	// The MON echoes every command it handles to its own output outside any
	// log envelope, so the quoted marker is sent as JSON escapes: the MON
	// decodes them into the message but never prints a literal marker.
	emit(jsonFileRoleQuote, []string{quote}, []string{jsonFileQuoteCommand(quote)})
	var noiseIDs, noiseCommands []string
	for i := range jsonFileNoiseCount {
		id := fmt.Sprintf("jfr-noise-%s-%02d", suffix, i)
		noiseIDs = append(noiseIDs, id)
		noiseCommands = append(noiseCommands, logCommand("", id))
	}
	emit(jsonFileRoleNoise, noiseIDs, noiseCommands)
	emit(jsonFileRoleTail, []string{tail}, []string{logCommand("warn", tail)})

	var ids []string
	for _, ev := range receipt.Events {
		ids = append(ids, ev.ID)
	}
	// Wait until the engine's own log holds every sentinel with the
	// structural priority that it was emitted with.
	if err := jsonFileWaitRaw(ctx, docker, monID, ids, early, quote, tail, noiseIDs[len(noiseIDs)-1]); err != nil {
		t.Fatal(err)
	}

	// The diagnostic tail, which the noise overflowed.
	started := time.Now()
	collected, diagErr := diagnostics.Collect(ctx, cluster, diagnostics.Config{LogTail: jsonFileTailLines})
	receipt.Elapsed.Diagnostics = time.Since(started)
	artifact := diagnostics.Artifact{Error: "no container-logs artifact for the source"}
	if collected != nil {
		for _, candidate := range collected.Artifacts {
			if candidate.Kind == "container-logs" && candidate.ContainerID == monID {
				artifact = candidate
			}
		}
	}
	receipt.Tail = jsonFileTailFrom(artifact, jsonFileTailLines, ids)
	if diagErr != nil {
		t.Logf("diagnostics returned an aggregate error: %v", diagErr)
	}

	if receipt.ClockBefore, err = jsonFileEngineClock(ctx, docker); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	report, err := policy.Evaluate(ctx)
	receipt.Elapsed.Evaluation = time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Report = report
	if receipt.ClockAfter, err = jsonFileEngineClock(ctx, docker); err != nil {
		t.Fatal(err)
	}

	lines, size, err := jsonFileReadEngineLog(ctx, docker, monID)
	if err != nil {
		t.Fatal(err)
	}
	var unstamped int
	receipt.Raw, unstamped = jsonFileRawLines(lines, ids)
	receipt.RawBytes, receipt.RawLines = size, len(lines)
	if unstamped != 0 {
		t.Logf("%d engine log lines carry no parsable stamp", unstamped)
	}
	if receipt.Evaluated, err = jsonFileInspect(ctx, docker, monID); err != nil {
		t.Fatal(err)
	}
	logVerificationReceiptReport(t, report)

	judged = judgeJSONFileReceipt(receipt)
	if judged != nil {
		t.Fatal(judged)
	}

	// The unsatisfied required warning (the quoting INF entry must not
	// satisfy it) fails the finalization's verification. Nothing else may
	// fail it.
	recorder.finalize()
	failures := recorder.failures()
	if len(failures) == 0 {
		t.Fatal("the finalization recorded no failure for the unsatisfied required warning")
	}
	for _, failure := range failures {
		if !strings.Contains(failure, "verification json-file-receipt") {
			t.Errorf("unexpected finalization failure: %s", failure)
		}
	}
}

// jsonFileWaitRaw polls the MON's engine log until the early and the tail
// sentinels appear with the WRN priority, and the quoting entry and the last
// noise entry with the INF priority.
func jsonFileWaitRaw(ctx context.Context, docker *mobycl.Client, id string, ids []string, early, quote, tail, lastNoise string) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		lines, _, err := jsonFileReadEngineLog(ctx, docker, id)
		if err != nil {
			return err
		}
		raw, _ := jsonFileRawLines(lines, ids)
		if raw[early].Priorities["WRN"] > 0 && raw[tail].Priorities["WRN"] > 0 && raw[quote].Priorities["INF"] > 0 && raw[lastNoise].Priorities["INF"] > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the MON output lacks the sentinels after two minutes: early %v, quote %v, tail %v, last noise %v",
				raw[early].Priorities, raw[quote].Priorities, raw[tail].Priorities, raw[lastNoise].Priorities)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// logVerificationReceiptReport logs the check's outcome, counts and gaps; the
// receipt carries the rest.
func logVerificationReceiptReport(t *testing.T, report *finalize.VerificationReport) {
	t.Helper()
	t.Logf("verification %s: window %s to %s, logs %s with %d permitted and %d unexpected, %d gaps",
		report.Outcome, report.Window.StartedAt.Format(time.RFC3339Nano), report.Window.EndedAt.Format(time.RFC3339Nano),
		report.Logs.Outcome, report.Logs.Permitted, report.Logs.Unexpected, len(report.Logs.Gaps))
	for _, gap := range report.Logs.Gaps {
		t.Logf("  log gap %s: %s", gap.Source, gap.Reason)
	}
}
