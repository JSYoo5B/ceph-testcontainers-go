//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestOSDRuntimeFaultComposition composes ordered daemon-local fault settings
// of one OSD from public primitives only: override.SetDebugLevel for the
// debug level and OSDContainer.AdminSocket with a small adapter for the
// settings that are not debug levels. Each step keeps its own prior,
// applied and restored readback and is journaled before and after its
// mutation; restoration runs in the reverse order, observes before it sets,
// sets at most once and refuses a replaced OSD.
//
// Every fault is shown by its effect on one native operation, with a
// negative control before and the restored behavior after it:
// ms_blackhole_client makes a client write time out and the OSD log the
// dropped MOSDOp, osd_copyfrom_max_chunk splits a copy-from into copy-get
// chunks of the lowered size with byte-identical data, and
// osd_debug_inject_copyfrom_error fails the copy. A restarted OSD drops its
// runtime values and the adapter leaves it alone.
func TestOSDRuntimeFaultComposition(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	if cluster != nil {
		if _, registerErr := finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true, Scenario: "osd-runtime-fault-composition"}); registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	const pool = "tc-runtime-faults"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rados := func(timeout time.Duration, script string) (ceph.NativeCommandResult, error) {
		return cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"sh", "-c", script}, Timeout: timeout,
			MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "rados"})
	}
	mustRados := func(script string) string {
		t.Helper()
		result, err := rados(2*time.Minute, script)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("%s: %v %+v %s", script, err, result, result.Stderr)
		}
		return strings.TrimSpace(string(result.Stdout))
	}
	osd := osdHandle(t, ctx, cluster, 0)
	journal := &runtimeJournal{t: t}

	// Writeback-fence order: debug_ms first, so that the first dropped
	// message is logged, then the blackhole. Restore blackhole first.
	put := "rados --rados-osd-op-timeout 5 -p " + pool + " put blackhole-probe /etc/hostname"
	mustRados(put) // negative control
	fence := &runtimeComposition{osd: osd, journal: journal}
	debug, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_ms", Level: "10/10"})
	journal.record("debug_ms", debug.Receipt())
	if err != nil {
		t.Fatal(err)
	}
	mark := osdLogLength(t, ctx, osd)
	if err := fence.apply(ctx, "ms_blackhole_client", "true"); err != nil {
		t.Fatal(err)
	}
	result, err := rados(time.Minute, put)
	if err != nil || result.ExitCode == 0 || !strings.Contains(string(result.Stderr), "(110)") {
		t.Fatalf("a client write succeeded through the blackhole: %v %+v %s", err, result, result.Stderr)
	}
	if !strings.Contains(osdLogSince(t, ctx, osd, mark), ".handle_message blackhole osd_op(") {
		t.Fatal("the OSD logged no dropped MOSDOp at debug_ms 10")
	}
	if err := fence.restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := debug.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	journal.record("debug_ms", debug.Receipt())
	mustRados(put) // restored behavior

	// Copy-from order: lower the chunk first and verify it, then enable the
	// injector. Restore the injector first.
	const size = 96 << 10
	mustRados(fmt.Sprintf("head -c %d /dev/urandom > /tmp/copy-src && rados -p %s put copy-src /tmp/copy-src", size, pool))
	source := mustRados("sha256sum < /tmp/copy-src")
	copied := func(dst string) string {
		t.Helper()
		return mustRados(fmt.Sprintf("rados -p %s get %s - | sha256sum", pool, dst))
	}
	// The operation history keeps osd_op_history_size operations, 20 by
	// default, which the cluster's own traffic fills; widen it for the
	// observation, as one more restored step.
	history := &runtimeComposition{osd: osd, journal: journal}
	if err := history.apply(ctx, "osd_op_history_size", "1000"); err != nil {
		t.Fatal(err)
	}
	copyFrom := &runtimeComposition{osd: osd, journal: journal}
	if err := copyFrom.apply(ctx, "osd_copyfrom_max_chunk", "32768"); err != nil {
		t.Fatal(err)
	}
	mustRados("rados -p " + pool + " cp copy-src copy-chunked")
	if got := copied("copy-chunked"); got != source {
		t.Fatalf("the chunked copy differs: %s, source %s", got, source)
	}
	copyGetChunks(t, ctx, osd, "copy-src", "copy-chunked", 32768, size/32768)
	if err := copyFrom.apply(ctx, "osd_debug_inject_copyfrom_error", "true"); err != nil {
		t.Fatal(err)
	}
	if result, err := rados(2*time.Minute, "rados -p "+pool+" cp copy-src copy-injected"); err != nil || result.ExitCode == 0 {
		t.Fatalf("the copy succeeded with the injector on: %v %+v", err, result)
	}
	if err := copyFrom.restore(ctx); err != nil {
		t.Fatal(err)
	}
	mustRados("rados -p " + pool + " cp copy-src copy-restored")
	if got := copied("copy-restored"); got != source {
		t.Fatalf("the restored copy differs: %s, source %s", got, source)
	}
	if err := history.restore(ctx); err != nil {
		t.Fatal(err)
	}

	// A restarted OSD runs another generation without the runtime value:
	// the adapter refuses to restore into it and sets nothing.
	replaced := &runtimeComposition{osd: osd, journal: journal}
	if err := replaced.apply(ctx, "osd_debug_inject_copyfrom_error", "true"); err != nil {
		t.Fatal(err)
	}
	stop := 10 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	err = replaced.restore(ctx)
	if !errors.Is(err, errRuntimeTargetChanged) || replaced.steps[0].State != "target-changed" || replaced.steps[0].RestoreIssued {
		t.Fatalf("restore into a restarted OSD: %v %+v", err, replaced.steps[0])
	}
	waitInjector(t, ctx, osd, "false")
	journal.dump()
}

var errRuntimeTargetChanged = errors.New("the OSD runs another container generation; its runtime value is gone")

// runtimeStep is the record of one ordered runtime setting.
type runtimeStep struct {
	Option        string `json:"option"`
	Value         string `json:"value"`
	Generation    string `json:"generation"`
	Prior         string `json:"prior"`
	Applied       string `json:"applied,omitempty"`
	Restored      string `json:"restored,omitempty"`
	State         string `json:"state"`
	RestoreIssued bool   `json:"restore_issued"`
	Error         string `json:"error,omitempty"`
}

// runtimeComposition applies steps in call order and restores them in reverse
// order. It is what a consumer writes on OSDContainer.AdminSocket; it is not
// atomic and never replays a mutation.
type runtimeComposition struct {
	osd     *ceph.OSDContainer
	journal *runtimeJournal
	steps   []*runtimeStep
}

func (r *runtimeComposition) generation(ctx context.Context) (string, error) {
	state, err := r.osd.State(ctx)
	if err != nil {
		return "", err
	}
	if !state.Running || state.Paused {
		return "", errors.New("the OSD is not running")
	}
	return r.osd.GetContainerID() + "@" + state.StartedAt, nil
}

func (r *runtimeComposition) get(ctx context.Context, option string) (string, error) {
	out, err := r.osd.AdminSocket(ctx, "config", "get", option)
	if err != nil {
		return "", err
	}
	var value map[string]string
	if err := json.Unmarshal(out, &value); err != nil || value[option] == "" {
		return "", fmt.Errorf("config get %s: %q", option, out)
	}
	return value[option], nil
}

// apply reads the generation and the effective prior, journals the intent,
// sets once and reads back. A failed set or readback is uncertain: the step
// stays for restore, which observes first.
func (r *runtimeComposition) apply(ctx context.Context, option, value string) error {
	generation, err := r.generation(ctx)
	if err != nil {
		return err
	}
	prior, err := r.get(ctx, option)
	if err != nil {
		return err
	}
	step := &runtimeStep{Option: option, Value: value, Generation: generation, Prior: prior, State: "intended"}
	r.steps = append(r.steps, step)
	r.journal.record(option, *step)
	_, err = r.osd.AdminSocket(ctx, "config", "set", option, value)
	if err == nil {
		step.Applied, err = r.get(ctx, option)
	}
	if err == nil && step.Applied != value {
		err = fmt.Errorf("%s reads back %q, not %q", option, step.Applied, value)
	}
	step.State = "applied"
	if err != nil {
		step.State, step.Error = "uncertain", err.Error()
	}
	r.journal.record(option, *step)
	return err
}

// restore walks the steps in reverse order. A step of another generation is
// target-changed, a value that is neither the prior nor the step's own is a
// conflict, and a restoring set already issued is never issued again.
func (r *runtimeComposition) restore(ctx context.Context) error {
	var errs []error
	for i := len(r.steps) - 1; i >= 0; i-- {
		step := r.steps[i]
		if step.State == "restored" || step.State == "conflict" || step.State == "target-changed" {
			continue
		}
		err := r.restoreStep(ctx, step)
		if err != nil {
			step.Error = err.Error()
		}
		r.journal.record(step.Option, *step)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *runtimeComposition) restoreStep(ctx context.Context, step *runtimeStep) error {
	if generation, err := r.generation(ctx); err != nil {
		return err
	} else if generation != step.Generation {
		step.State = "target-changed"
		return errRuntimeTargetChanged
	}
	current, err := r.get(ctx, step.Option)
	if err != nil {
		return err
	}
	switch {
	case current == step.Prior:
		step.State, step.Restored = "restored", current
		return nil
	case current != step.Value:
		step.State = "conflict"
		return fmt.Errorf("%s changed outside the composition to %q", step.Option, current)
	case step.RestoreIssued:
		return fmt.Errorf("%s was already restored once; refusing to replay it", step.Option)
	}
	step.State, step.RestoreIssued = "restore-intended", true
	r.journal.record(step.Option, *step)
	if _, err := r.osd.AdminSocket(ctx, "config", "set", step.Option, step.Prior); err != nil {
		step.State = "restore-uncertain"
		return err
	}
	if step.Restored, err = r.get(ctx, step.Option); err != nil || step.Restored != step.Prior {
		step.State = "restore-uncertain"
		return errors.Join(err, fmt.Errorf("%s reads back %q after its restoration", step.Option, step.Restored))
	}
	if generation, err := r.generation(ctx); err != nil || generation != step.Generation {
		step.State = "restore-uncertain"
		return errors.Join(err, errRuntimeTargetChanged)
	}
	step.State = "restored"
	return nil
}

// runtimeJournal is the consumer's external journal: one JSON line per
// recorded step state, in order.
type runtimeJournal struct {
	t     *testing.T
	lines []string
}

func (j *runtimeJournal) record(name string, record any) {
	data, _ := json.Marshal(record)
	j.lines = append(j.lines, name+" "+string(data))
}

func (j *runtimeJournal) dump() {
	for _, line := range j.lines {
		j.t.Log(line)
	}
}

func osdLog(t *testing.T, ctx context.Context, osd *ceph.OSDContainer) string {
	t.Helper()
	reader, err := osd.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, 256<<20))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func osdLogLength(t *testing.T, ctx context.Context, osd *ceph.OSDContainer) int {
	return len(osdLog(t, ctx, osd))
}

func osdLogSince(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, mark int) string {
	log := osdLog(t, ctx, osd)
	if len(log) < mark {
		t.Fatalf("the OSD log shrank from %d to %d bytes", mark, len(log))
	}
	return log[mark:]
}

// copyGetChunks reads the OSD's operation history until it attributes at
// least want copy-get chunks of chunk bytes to the one copy-from of source to
// destination. It only reads: the copy ran once and its destination is not
// written again. The OSD moves a finished operation into its history
// asynchronously, so a dump taken right after the client's reply can lack the
// whole copy (on Tentacle 20.2.4 the copy appeared up to about 0.4 s after
// it); a history without the copy's entries is read again for a bounded
// time. A history that shows another chunk size or an ambiguous copy fails at
// once. A failure logs the parse counts and every entry relevant to the probe
// objects.
func copyGetChunks(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, source, destination string, chunk, want int) {
	t.Helper()
	var last error
	seen, err := harness.Observe(ctx, 30*time.Second, 15*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (copyGetEvidence, bool, error) {
		raw, err := osd.AdminSocket(probeCtx, "dump_historic_ops")
		if err != nil {
			return copyGetEvidence{}, false, err
		}
		dump, err := parseHistoricOps(raw)
		if err != nil {
			return copyGetEvidence{}, false, err
		}
		evidence, err := attributeCopyGets(dump, source, destination, chunk, want)
		if last = nil; errors.Is(err, errCopyGetIncomplete) {
			last = err
			return evidence, false, nil
		}
		return evidence, err == nil, err
	})
	if err != nil {
		t.Fatalf("copy-get chunks of %s after %d reads in %s: %v: %s", destination, seen.Attempts, seen.Elapsed, errors.Join(err, last), seen.Value)
	}
	t.Logf("OSD_COPY_GET_EVIDENCE reads=%d elapsed=%s %s", seen.Attempts, seen.Elapsed.Round(time.Millisecond), seen.Value)
}

// waitInjector waits until the restarted OSD answers on its admin socket and
// checks its effective injector.
func waitInjector(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, want string) {
	t.Helper()
	seen, err := harness.Observe(ctx, 2*time.Minute, 15*time.Second, time.Second, func(probeCtx context.Context) (string, bool, error) {
		out, err := osd.AdminSocket(probeCtx, "config", "get", "osd_debug_inject_copyfrom_error")
		if err != nil {
			return "", false, nil
		}
		var value map[string]string
		if json.Unmarshal(out, &value) != nil {
			return string(out), false, nil
		}
		return value["osd_debug_inject_copyfrom_error"], true, nil
	})
	if err != nil || seen.Value != want {
		t.Fatalf("the restarted OSD shows osd_debug_inject_copyfrom_error %q, want %q: %v", seen.Value, want, err)
	}
}
