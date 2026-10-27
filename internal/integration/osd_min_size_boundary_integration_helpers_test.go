//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// minSizeSpan is how long the PG must be observed below min_size without a
	// break; minSizeGap bounds the time between two observations, and
	// minSizeCount the number of samples, so that a few late reads cannot stand
	// for a window.
	minSizeSpan  = 20 * time.Second
	minSizeGap   = 10 * time.Second
	minSizeCount = 5
)

// minSizeReceipt is the scenario's record. It is logged on every outcome,
// before cleanup destroys the cluster.
type minSizeReceipt struct {
	PG        string            `json:"pg"`
	Primary   int               `json:"primary"`
	Replica   int               `json:"replica"`
	Peer      int               `json:"peer"`
	Spare     int               `json:"spare"`
	Held      *backfillBaseline `json:"held_backfill,omitempty"`
	Above     *secondFaultMark  `json:"writer_above_min_size,omitempty"`
	Baseline  *minSizeBaseline  `json:"below_min_size_baseline,omitempty"`
	Window    *minSizeJudgement `json:"below_min_size_window,omitempty"`
	Blocked   string            `json:"blocked_submission,omitempty"`
	BlockedAs string            `json:"blocked_submission_outcome,omitempty"`
	After     *secondFaultMark  `json:"writer_after_return,omitempty"`
	Final     *secondFaultMark  `json:"writer_final,omitempty"`
	Done      bool              `json:"completed"`
}

// minSizeLedger reads the writer's ledger without failing the test, so that a
// failed read can become an inconclusive sample.
func minSizeLedger(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, path string) ([]secondFaultEvent, error) {
	result, err := cluster.RunNativeCommand(ctx, client, secondFaultCommand("ledger", time.Minute, "ledger", path))
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		return nil, fmt.Errorf("exit %d complete %t: %v: %.512s", result.ExitCode, result.Complete(), err, result.Stderr)
	}
	var events []secondFaultEvent
	if err := json.Unmarshal(result.Stdout, &events); err != nil {
		return nil, fmt.Errorf("ledger %.256s: %w", result.Stdout, err)
	}
	return events, nil
}

// minSizeAcksAfter requires the retained writer to be acknowledged n more times
// than at the moment this is called, which is after the phase was confirmed. It
// returns that fresh watermark and the later one.
func minSizeAcksAfter(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, path string, n int, what string) (confirmed, after secondFaultMark) {
	t.Helper()
	read := func() (secondFaultMark, error) {
		events, err := minSizeLedger(ctx, cluster, client, path)
		if err != nil {
			return secondFaultMark{}, err
		}
		ops, err := secondFaultOps(events, false)
		if err != nil {
			return secondFaultMark{}, err
		}
		return secondFaultCount(ops), nil
	}
	confirmed, after, err := minSizeProgressAfter(read, n, 180, func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	})
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	return confirmed, after
}

// minSizeSampleNow takes one sample from the primary's own pg query and the
// ledger.
func minSizeSampleNow(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pgid, path string) minSizeSample {
	at := time.Now()
	raw, rawErr := pgQueryRaw(ctx, cluster, pgid)
	events, eventsErr := minSizeLedger(ctx, cluster, client, path)
	return minSizeSampleOf(at, raw, rawErr, events, eventsErr)
}

// testOSDMinSizeBoundary crosses min_size twice with one live writer. Four OSDs
// hold one PG of a size 3, min_size 2 pool. An acting replica A stops and is
// marked out while nobackfill is set, so that the spare D becomes a backfill
// destination that is held at MIN: the recovery is pending and no object has
// been transferred. The PG then acts on two OSDs, exactly min_size, and the
// writer and an independent client are served.
//
// Then the other non-primary acting OSD B stops. The primary alone acts, below
// min_size, and its own pg query must report peered, not active. After that
// fault is confirmed, a new baseline is taken: the writer's acknowledgment
// count and the single submission that has not returned. Over a window of
// samples the PG must stay peered in one interval, the count must not move, and
// that same submission must stay unreturned. Any failed or malformed read fails
// the window.
//
// B then returns. The PG must be active on two OSDs again, with D still at MIN
// (no transfer happened at any time), the waiting submission must return, and
// the writer must be acknowledged again. A returns, the hold is lifted, the PG
// must become clean on three copies, and an independent client must read back
// every acknowledged object. No operation is ever replayed; the outcome of the
// waiting submission is recorded as it came.
func testOSDMinSizeBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 22*time.Minute)
	defer cancel()
	const ledger, stopFile = "/tmp/tc-min-size.jsonl", "/tmp/tc-min-size.stop"
	image, opts := integrationImages(t)
	cluster, client := newServiceClusterWithOptions(t, image, append(opts, ceph.WithNoInitialOSDs())...)
	restore := func(name string, fn func(context.Context) error) {
		t.Helper()
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := fn(cleanup); err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
		})
	}
	// A short, always-trimmed PG log keeps a spare that has no copy of the PG
	// from being brought up to date by log recovery: its empty copy then lies
	// behind the log tail, which needs a backfill.
	for name, value := range map[string]string{"osd_min_pg_log_entries": "10", "osd_max_pg_log_entries": "10", "osd_pg_log_trim_min": "1"} {
		handle, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: name, Value: value})
		if handle != nil {
			restore(name, handle.Restore)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-paths", PGNum: 1, Replicas: 3, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryRun(t, ctx, cluster, client, "write", "seed:20")
	clean := secondFaultWaitPG(t, ctx, cluster, "clean on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, noOSD)
	})
	pgid, primary := clean.PGID.String(), clean.ActingPrimary
	var a, b, d *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		switch {
		case !slices.Contains(clean.Acting, osd.ID):
			d = osd
		case osd.ID != primary && a == nil:
			a = osd
		case osd.ID != primary:
			b = osd
		}
	}
	if a == nil || b == nil || d == nil {
		t.Fatalf("no two non-primary acting OSDs and spare among four OSDs: %+v", clean)
	}
	receipt := &minSizeReceipt{PG: pgid, Primary: primary, Replica: a.ID, Peer: b.ID, Spare: d.ID}
	out, err := cluster.Ceph(ctx, "version")
	if err != nil {
		t.Fatal(err)
	}
	uname, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"uname", "-sm"}, Timeout: time.Minute, MaxStdout: 256, MaxStderr: 256, Label: "min size platform"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_MIN_SIZE_RUNTIME ceph=%q platform=%q pg=%s primary=osd.%d replica=osd.%d peer=osd.%d spare=osd.%d", strings.TrimSpace(string(out)), strings.TrimSpace(string(uname.Stdout)), pgid, primary, a.ID, b.ID, d.ID)

	stopped := map[int]bool{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		restore(fmt.Sprintf("osd.%d", osd.ID), func(cleanup context.Context) error {
			if stopped[osd.ID] {
				stopped[osd.ID] = false
				return osd.Start(cleanup)
			}
			return nil
		})
	}
	// Every OSD is captured as a running instance before any fault; a stop is
	// admitted only for that same instance, and its result is read back apart
	// from the request.
	instances := map[int]secondFaultInstance{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		instances[osd.ID] = secondFaultInstanceOf(t, ctx, osd)
		if !instances[osd.ID].Running {
			t.Fatalf("osd.%d is not running before any fault: %+v", osd.ID, instances[osd.ID])
		}
	}
	stopOSD := func(osd *ceph.OSDContainer) {
		t.Helper()
		now := secondFaultInstanceOf(t, ctx, osd)
		if err := secondFaultAdmitStop(instances[osd.ID], now); err != nil {
			t.Fatalf("osd.%d stop refused: %v", osd.ID, err)
		}
		stopped[osd.ID] = true
		timeout := 10 * time.Second
		if err := osd.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		after := secondFaultInstanceOf(t, ctx, osd)
		if after.Running || after.OOMKilled {
			t.Fatalf("osd.%d after the stop request: %+v", osd.ID, after)
		}
		t.Logf("OSD_MIN_SIZE_STOP osd=%d requested_from=%+v resulting=%+v", osd.ID, now, after)
	}
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	restore("noout", noout.Restore)
	if err != nil {
		t.Fatal(err)
	}
	hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoBackfill, true)
	restore("nobackfill", hold.Restore)
	if err != nil {
		t.Fatal(err)
	}
	restore("osd in", func(cleanup context.Context) error { return a.SetIn(cleanup, true) })

	// The live writer keeps one connection and one ledger for the whole run.
	actor := startNative(ctx, cluster, client, secondFaultCommand("actor", 20*time.Minute, "actor", "tc-paths", ledger, stopFile, "2", "600"))
	actorDone := false
	t.Cleanup(func() {
		if actorDone {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(outcome.err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := unstopped.Stop(stopCtx); err != nil {
					t.Errorf("stop the live writer: %v", err)
				}
			}
		case <-time.After(time.Minute):
			t.Error("the live writer did not stop")
		}
	})
	// The receipt and the ledger are emitted on every outcome, before the
	// restores above destroy anything. A failed read here is reported, not hidden.
	t.Cleanup(func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Errorf("encode the receipt: %v", err)
			return
		}
		t.Logf("OSD_MIN_SIZE_RECEIPT %s", data)
		if receipt.Done {
			return
		}
		readCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		events, err := minSizeLedger(readCtx, cluster, client, ledger)
		if err != nil {
			t.Logf("OSD_MIN_SIZE_LEDGER unavailable at failure: %v", err)
			return
		}
		ledgerJSON, _ := json.Marshal(events)
		t.Logf("OSD_MIN_SIZE_LEDGER at failure: %.65536s", ledgerJSON)
	})
	secondFaultAcks(t, ctx, cluster, client, ledger, secondFaultMark{}, 3, "while healthy")
	secondFaultReceipt(t, ctx, cluster, "healthy", stopped)

	// First fault: one acting replica stops and keeps its store. The PG still
	// acts on min_size OSDs.
	stopOSD(a)
	secondFaultWaitPG(t, ctx, cluster, "active on the two remaining replicas", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active") && strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Acting, a.ID) && len(pg.Acting) == 2
	})
	secondFaultReceipt(t, ctx, cluster, "first-fault", stopped)
	// The watermark is taken now, after the outage is confirmed.
	minSizeAcksAfter(t, ctx, cluster, client, ledger, 2, "with one replica down")
	recoveryRun(t, ctx, cluster, client, "write", "delta:100")

	// Marking the stopped replica out admits the spare as a new member. The
	// held backfill is the baseline: its interval, up and acting sets, and the
	// destination's cursor at MIN.
	if err := a.SetIn(ctx, false); err != nil {
		t.Fatal(err)
	}
	secondFaultWaitPG(t, ctx, cluster, "held backfill to the spare", func(pg ceph.PGState) bool {
		return slices.Contains(pg.Up, d.ID) && !slices.Contains(pg.Acting, d.ID) && strings.Contains(pg.State, "backfill")
	})
	heldBaseline := func(what string) backfillBaseline {
		t.Helper()
		var base backfillBaseline
		var raw []byte
		var err error
		for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
			raw, err = pgQueryRaw(ctx, cluster, pgid)
			if err == nil {
				var held pgProgress
				if held, err = parsePGProgress(raw); err == nil {
					if base, err = backfillHeld(held, d.ID); err == nil {
						return base
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: backfill to osd.%d was never seen held: %v\n%.16384s", what, d.ID, err, raw)
			}
		}
	}
	held := heldBaseline("before min_size is crossed")
	receipt.Held = &held
	t.Logf("OSD_MIN_SIZE_HELD baseline=%+v", held)
	time.Sleep(5 * time.Second)
	if again, err := pgQueryRaw(ctx, cluster, pgid); err != nil {
		t.Fatal(err)
	} else if later, err := parsePGProgress(again); err != nil {
		t.Fatal(err)
	} else if verdict, why := backfillProgress(held, later); verdict != progressWait {
		t.Fatalf("nobackfill is set, but the progress gate says %s: %s", verdict, why)
	}

	// At min_size: the retained writer and an independent client are served.
	// The watermark is taken now, after the held backfill and its hold check were
	// confirmed: acknowledgments accumulated during the setup above do not count.
	aboveFrom, above := minSizeAcksAfter(t, ctx, cluster, client, ledger, 2, "at min_size with the backfill pending")
	receipt.Above = &above
	t.Logf("OSD_MIN_SIZE_AT_MIN_SIZE writer_when_confirmed=%+v writer_later=%+v", aboveFrom, above)
	recoveryRun(t, ctx, cluster, client, "write", "above:10")
	readRecoveryObjects(t, ctx, cluster, "seed:20", "delta:100", "above:10")
	secondFaultReceipt(t, ctx, cluster, "at-min-size", stopped)

	// Second fault: the other non-primary acting OSD stops. The primary acts
	// alone, below min_size. Wait until its own report says so and one
	// submission is waiting, and take the baseline only then.
	stopOSD(b)
	var base minSizeBaseline
	var baseSample minSizeSample
	var why string
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(time.Second) {
		baseSample = minSizeSampleNow(ctx, cluster, client, pgid, ledger)
		var err error
		if base, err = minSizeBaselineOf(baseSample, 2); err == nil {
			break
		} else {
			why = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("the PG never showed peered below min_size with one submission waiting: %s; last sample %+v", why, baseSample)
		}
	}
	receipt.Baseline, receipt.Blocked = &base, base.Blocked
	t.Logf("OSD_MIN_SIZE_BASELINE %+v state=%s acting=%v", base, baseSample.State, baseSample.Acting)
	samples, err := collectMinSizeWindow(ctx, base.At, minSizeSpan, func() minSizeSample {
		return minSizeSampleNow(ctx, cluster, client, pgid, ledger)
	}, func(c context.Context) error {
		select {
		case <-c.Done():
			return c.Err()
		case <-time.After(time.Second):
			return nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	judgement, err := judgeMinSizeBlock(base, samples, 2, minSizeSpan, minSizeGap, minSizeCount)
	receipt.Window = &judgement
	if err != nil {
		t.Fatalf("the barrier window was not shown: %v; %d samples, last %+v", err, len(samples), samples[len(samples)-1])
	}
	t.Logf("OSD_MIN_SIZE_WINDOW %+v", judgement)

	// B returns: min_size is met again.
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped[b.ID] = false
	secondFaultWaitPG(t, ctx, cluster, "active on two OSDs again", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active") && slices.Contains(pg.Acting, b.ID) && !slices.Contains(pg.Acting, a.ID) && len(pg.Acting) == 2
	})
	secondFaultReceipt(t, ctx, cluster, "min-size-restored", stopped)
	// Still pending: no object was transferred to the spare at any time.
	stillHeld := heldBaseline("after min_size is restored")
	t.Logf("OSD_MIN_SIZE_HELD_AGAIN baseline=%+v", stillHeld)

	// The waiting submission returns on its own. Its outcome is recorded as it
	// came; it is never replayed.
	waitCtx, stopWait := context.WithTimeout(ctx, 3*time.Minute)
	var outcome secondFaultOp
	for {
		_, ops := secondFaultMarkNow(t, waitCtx, cluster, client, ledger)
		i := slices.IndexFunc(ops, func(op secondFaultOp) bool { return op.Name == base.Blocked })
		if i < 0 {
			stopWait()
			t.Fatalf("the waiting submission %s is not in the ledger", base.Blocked)
		}
		if outcome = ops[i]; outcome.Outcome != opOutstanding {
			break
		}
		select {
		case <-waitCtx.Done():
			stopWait()
			t.Fatalf("the waiting submission %s had no outcome after min_size was restored", base.Blocked)
		case <-time.After(time.Second):
		}
	}
	stopWait()
	receipt.BlockedAs = outcome.Outcome
	if outcome.Outcome == opRefused {
		t.Fatalf("the waiting submission was refused: %+v", outcome)
	}
	returned, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	afterReturn := secondFaultAcks(t, ctx, cluster, client, ledger, returned, 2, "after min_size was restored")
	receipt.After = &afterReturn
	readRecoveryObjects(t, ctx, cluster, "seed:20", "delta:100", "above:10")
	t.Logf("OSD_MIN_SIZE_BOUNDARY min_size=2 writer_at_min_size=%+v blocked=%s blocked_outcome=%s writer_after=%+v", above, base.Blocked, outcome.Outcome, afterReturn)

	// Restoration: A returns first, so that the held backfill to the spare is
	// replaced by the normal one; then the hold is lifted.
	if err := a.SetIn(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped[a.ID] = false
	if err := hold.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	secondFaultWaitPG(t, ctx, cluster, "required redundancy restored", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, noOSD) &&
			pg.Stats.ObjectsDegraded == 0 && pg.Stats.ObjectsMisplaced == 0 && pg.Stats.ObjectsUnfound == 0 && pg.Stats.ObjectCopies == 3*pg.Stats.Objects
	})
	secondFaultReceipt(t, ctx, cluster, "restored", stopped)

	// Close the writer and the data oracle.
	execCommand(t, ctx, client, "touch", stopFile)
	select {
	case done := <-actor.done:
		actorDone = true
		if done.err != nil || done.result.ExitCode != 0 || !done.result.Complete() {
			t.Fatalf("live writer shutdown: %v exit %d: %.1024s", done.err, done.result.ExitCode, done.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the live writer did not stop after its stop file appeared")
	}
	ops, err := secondFaultOps(secondFaultLedger(t, ctx, cluster, client, ledger), true)
	if err != nil {
		t.Fatal("writer ledger refused:", err)
	}
	final := secondFaultCount(ops)
	receipt.Final = &final
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	encodedNames, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), secondFaultCommand("independent read", 3*time.Minute, "read", "tc-paths", string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent read of the writer's objects: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		t.Fatalf("independent read %.512s: %v", read.Stdout, err)
	}
	if err := secondFaultVerify(ops, objects); err != nil {
		t.Fatal(err)
	}
	if final.Acked < afterReturn.Acked || final.Acked <= above.Acked || final.Refused != 0 {
		t.Fatalf("writer outcomes %+v do not extend the watermarks %+v and %+v", final, above, afterReturn)
	}
	readRecoveryObjects(t, ctx, cluster, "seed:20", "delta:100", "above:10")
	receipt.Done = true
}
