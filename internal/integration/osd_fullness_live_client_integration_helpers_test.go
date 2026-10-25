//go:build all || (integration && topology)

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
	"github.com/testcontainers/testcontainers-go"
)

const (
	fullnessLiveLedger = "/tmp/tc-fullness-live.jsonl"
	fullnessLiveStop   = "/tmp/tc-fullness-live.stop"
	// fullnessLiveMinAcks is the number of acknowledgments inside the credited
	// barrier window that makes it "served". The writer submits about once per
	// second, so a 25 s window holds many more.
	fullnessLiveMinAcks = 5
	// fullnessLiveGap bounds the time between two credited samples.
	fullnessLiveGap = 10 * time.Second
)

// fullnessLivePhase is the receipt of one barrier case with the live writer.
type fullnessLivePhase struct {
	Case                string                 `json:"case"`
	Written             int                    `json:"written"`
	StoppedByLastUpdate string                 `json:"replica_last_entry_at_most"`
	LogTail             string                 `json:"log_tail"`
	Baseline            backfillBaseline       `json:"baseline"`
	HeldState           string                 `json:"state_before_policy,omitempty"`
	OSDStatesPolicy     map[int][]string       `json:"osd_states_with_policy,omitempty"`
	WriterReplicaDown   *secondFaultMark       `json:"writer_with_replica_down,omitempty"`
	WriterAtPolicy      *secondFaultMark       `json:"writer_when_policy_applied,omitempty"`
	Samples             map[barrierVerdict]int `json:"samples"`
	SampleErrors        int                    `json:"sample_errors"`
	BeforeFirstHold     map[barrierVerdict]int `json:"before_first_toofull"`
	ToofullFor          string                 `json:"toofull_for"`
	StateAtEnd          string                 `json:"state_at_end_of_window"`
	Window              *fullnessLiveJudgement `json:"writer_window,omitempty"`
	WindowError         string                 `json:"writer_window_error,omitempty"`
	Promise             string                 `json:"promise_met"`
	RecoveredHeld       int64                  `json:"recovered_at_first_toofull"`
	RecoveredWindow     int64                  `json:"recovered_at_end_of_window"`
	RecoveredFinal      int64                  `json:"recovered_final"`
	WriterAfterLift     *secondFaultMark       `json:"writer_after_policy_lifted,omitempty"`
	CleanAfterRelease   string                 `json:"clean_after_policy_lifted"`
	Late                []fullnessLiveLate     `json:"outstanding_at_end_of_window"`
	endOps              []secondFaultOp
}

type fullnessLiveReceipt struct {
	PG                  string                `json:"pg"`
	Primary             int                   `json:"primary"`
	Replica             int                   `json:"replica"`
	Retry               string                `json:"backfill_retry_interval"`
	Ratios              ceph.FullRatios       `json:"ratios"`
	Phases              []*fullnessLivePhase  `json:"phases"`
	Final               *secondFaultMark      `json:"writer_final,omitempty"`
	Readback            *fullnessLiveReadback `json:"readback,omitempty"`
	FailedBeforeCleanup bool                  `json:"failed_before_cleanup"`
	FirstFailure        string                `json:"first_failure,omitempty"`
	Done                bool                  `json:"completed"`
}

// fullnessLiveOpsNow reads the writer's ledger and returns its operations.
func fullnessLiveOpsNow(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, final bool) ([]secondFaultOp, error) {
	events, err := minSizeLedger(ctx, cluster, client, fullnessLiveLedger)
	if err != nil {
		return nil, err
	}
	return secondFaultOps(events, final)
}

// testOSDFullnessLiveClient runs the two fullness-policy barrier cases of
// testOSDFullnessBarrier (a reservation revoked after it was granted, and a
// reservation refused at admission) with one retained live writer whose
// operation ledger spans both barriers and their restoration. The barrier
// itself is judged by the same pure functions as there (fullnessBarrierSample,
// judgeBarrier, recordFailedRead); the writer is judged by judgeFullnessLive.
//
// The pinned source says client writes do not depend on backfillfull_ratio, so
// the promise is that they are served. The scenario records the observed class
// (served, blocked, failed or unknown) in the receipt and compares it with the
// promise as a separate step. Each operation's outcome is read where the writer
// recorded it (acknowledgment, error, or the stop); nothing is replayed. After
// the last restoration an independent client reads back every acknowledged
// object byte for byte, and operations with an unknown outcome are inspected,
// never promoted.
//
// The receipt is logged before any restoration, so a failed run keeps it. The
// first failure and failures of the restorations are separate records.
func testOSDFullnessLiveClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t)
	receipt := &fullnessLiveReceipt{Retry: fullnessRetry, Ratios: ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .95}}
	type restoration struct {
		name string
		fn   func(context.Context) error
	}
	var restorations []restoration
	restore := func(name string, fn func(context.Context) error) {
		restorations = append(restorations, restoration{name, fn})
	}
	// One cleanup, registered right after the cluster so that it runs before
	// the cluster is destroyed: it emits the receipt (and the ledger of a failed
	// run) first, then runs the restorations newest first, keeping their
	// failures apart from the first failure.
	t.Cleanup(func() {
		receipt.FailedBeforeCleanup = t.Failed()
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Errorf("encode the receipt: %v", err)
		} else {
			t.Logf("OSD_FULLNESS_LIVE_RECEIPT %s", data)
		}
		if !receipt.Done {
			readCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			events, err := minSizeLedger(readCtx, cluster, client, fullnessLiveLedger)
			stop()
			if err != nil {
				t.Logf("OSD_FULLNESS_LIVE_LEDGER unavailable at failure: %v", err)
			} else {
				ledgerJSON, _ := json.Marshal(events)
				t.Logf("OSD_FULLNESS_LIVE_LEDGER at failure: %.65536s", ledgerJSON)
			}
		}
		var failures []string
		for i := len(restorations) - 1; i >= 0; i-- {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			if err := restorations[i].fn(cleanup); err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", restorations[i].name, err))
				t.Errorf("restore %s: %v", restorations[i].name, err)
			}
			stop()
		}
		t.Logf("OSD_FULLNESS_LIVE_CLEANUP failures=%q", failures)
	})
	fail := func(format string, args ...any) {
		t.Helper()
		message := fmt.Sprintf(format, args...)
		if receipt.FirstFailure == "" {
			receipt.FirstFailure = message
		}
		t.Fatal(message)
	}
	for _, setting := range []ceph.ConfigSetting{
		{Section: "osd", Name: "osd_backfill_retry_interval", Value: fullnessRetry},
		// A short, always-trimmed PG log leaves the returning replica behind the
		// log tail, which needs a backfill.
		{Section: "osd", Name: "osd_min_pg_log_entries", Value: "10"},
		{Section: "osd", Name: "osd_max_pg_log_entries", Value: "10"},
		{Section: "osd", Name: "osd_pg_log_trim_min", Value: "1"},
	} {
		handle, err := cluster.OverrideConfig(ctx, setting)
		if handle != nil {
			restore(setting.Name, handle.Restore)
		}
		if err != nil {
			fail("%v", err)
		}
	}
	fullnessEffective(t, ctx, cluster, "osd_backfill_retry_interval", fullnessRetry)

	pool := ceph.PoolConfig{Name: "tc-paths", PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados"}
	if _, err := cluster.CreatePool(ctx, pool); err != nil {
		fail("%v", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		fail("%v", err)
	}
	recoveryRun(t, ctx, cluster, client, "write", "seed:20")
	clean := waitPG(t, ctx, cluster, "clean on both OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 2 && !slices.Contains(pg.Acting, noOSD)
	})
	pgid, primary := clean.PGID, clean.ActingPrimary
	var victim *ceph.OSDContainer
	for _, osd := range cluster.OSDs() {
		if osd.ID != primary && slices.Contains(clean.Acting, osd.ID) {
			victim = osd
		}
	}
	if victim == nil {
		fail("no replica besides primary osd.%d: %+v", primary, clean)
	}
	receipt.PG, receipt.Primary, receipt.Replica = pgid, primary, victim.ID
	version, err := cluster.Ceph(ctx, "version")
	if err != nil {
		fail("%v", err)
	}
	uname, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"uname", "-sm"}, Timeout: time.Minute, MaxStdout: 256, MaxStderr: 256, Label: "fullness live platform"})
	if err != nil {
		fail("%v", err)
	}
	t.Logf("OSD_FULLNESS_LIVE_RUNTIME ceph=%q platform=%q pg=%s primary=osd.%d replica=osd.%d retry_interval=%s", strings.TrimSpace(string(version)), strings.TrimSpace(string(uname.Stdout)), pgid, primary, victim.ID, fullnessRetry)

	stopped := false
	restore("replica", func(cleanup context.Context) error {
		if stopped {
			stopped = false
			return victim.Start(cleanup)
		}
		return nil
	})
	captured := secondFaultInstanceOf(t, ctx, victim)
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if noout != nil {
		restore("noout", noout.Restore)
	}
	if err != nil {
		fail("%v", err)
	}

	// The live writer keeps one connection and one ledger for the whole run.
	actor := startNative(ctx, cluster, client, secondFaultCommand("actor", 24*time.Minute, "actor", "tc-paths", fullnessLiveLedger, fullnessLiveStop, "1", "1500"))
	actorDone := false
	restore("live writer", func(cleanup context.Context) error {
		if actorDone {
			return nil
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(outcome.err, &unstopped) {
				return unstopped.Stop(cleanup)
			}
			return nil
		case <-time.After(time.Minute):
			return errors.New("the live writer did not stop")
		}
	})
	minSizeAcksAfter(t, ctx, cluster, client, fullnessLiveLedger, 3, "while healthy")

	prefixes := []string{"seed:20"}
	const count = 60
	for _, phase := range []struct {
		name   string
		revoke bool
	}{{"revoked", true}, {"refused", false}} {
		out := &fullnessLivePhase{Case: phase.name, Written: count, Samples: map[barrierVerdict]int{}}
		receipt.Phases = append(receipt.Phases, out)

		// Refuse a replica that is not the running instance captured earlier,
		// so that an unplanned exit is not relabelled as this stop.
		if err := secondFaultAdmitStop(captured, secondFaultInstanceOf(t, ctx, victim)); err != nil {
			fail("osd.%d stop refused: %v", victim.ID, err)
		}
		stopped = true
		timeout := 10 * time.Second
		if err := victim.Stop(ctx, &timeout); err != nil {
			fail("%v", err)
		}
		if after := secondFaultInstanceOf(t, ctx, victim); after.Running || after.OOMKilled {
			fail("osd.%d after the stop request: %+v", victim.ID, after)
		}
		waitPG(t, ctx, cluster, "degraded without the replica", func(pg ceph.PGState) bool {
			return strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Acting, victim.ID)
		})
		// The writer keeps writing, so the primary's last entry at the moment of
		// the stop is not known exactly. Read after the replica is down, it is an
		// upper bound of the replica's own last entry.
		down, _ := queryPG(t, ctx, cluster, pgid)
		stoppedAt, err := parseEversion(down.Info.LastUpdate)
		if err != nil {
			fail("%v", err)
		}
		out.StoppedByLastUpdate = down.Info.LastUpdate
		spec := fmt.Sprintf("%s:%d", phase.name, count)
		recoveryRun(t, ctx, cluster, client, "write", spec)
		prefixes = append(prefixes, spec)
		_, replicaDown := minSizeAcksAfter(t, ctx, cluster, client, fullnessLiveLedger, 2, "with the replica down")
		out.WriterReplicaDown = &replicaDown

		var hold *ceph.OSDFlagOverrideHandle
		applyPolicy := func() *ceph.FullRatiosOverride {
			mark, _ := secondFaultMarkNow(t, ctx, cluster, client, fullnessLiveLedger)
			out.WriterAtPolicy = &mark
			handle, err := cluster.OverrideFullRatios(ctx, receipt.Ratios)
			// A failed application may own a partial override: register its
			// restoration before looking at the error.
			if handle != nil {
				restore("full ratios "+phase.name, handle.Restore)
			}
			if err != nil || handle == nil {
				fail("apply full ratios: %v", err)
			}
			return handle
		}
		var policy *ceph.FullRatiosOverride
		var base backfillBaseline
		var baseRaw []byte
		haveBase := false
		if phase.revoke {
			hold, err = cluster.OverrideOSDFlag(ctx, "nobackfill", true)
			if hold != nil {
				restore("nobackfill "+phase.name, hold.Restore)
			}
			if err != nil {
				fail("%v", err)
			}
			if err := victim.Start(ctx); err != nil {
				fail("%v", err)
			}
			stopped = false
			captured = secondFaultInstanceOf(t, ctx, victim)
			// The hold stops the work, not the reservation: the primary must
			// show the backfill reserved and the destination at MIN.
			deadline := time.Now().Add(3 * time.Minute)
			for {
				raw, err := pgQueryRaw(ctx, cluster, pgid)
				var held pgProgress
				if err == nil {
					if held, err = parsePGProgress(raw); err == nil {
						if base, err = backfillHeld(held, victim.ID); err == nil && !pgStateHas(held.State, "backfilling") {
							err = fmt.Errorf("the backfill is not reserved: state %s", held.State)
						}
						if err == nil {
							baseRaw, haveBase = raw, true
							out.HeldState = held.State
							break
						}
					}
				}
				if time.Now().After(deadline) {
					fail("no reserved, held backfill to osd.%d: %v\n%.16384s", victim.ID, err, raw)
				}
				time.Sleep(500 * time.Millisecond)
			}
			heldQuery, err := parsePGQuery(baseRaw)
			if err != nil {
				fail("%v", err)
			}
			if path, err := pgRecoveryPath(heldQuery, victim.ID, stoppedAt); err != nil || path != "backfill" {
				fail("held path %q: %v\n%.16384s", path, err, baseRaw)
			}
			out.LogTail, out.RecoveredHeld = heldQuery.Info.LogTail, heldQuery.Info.Stats.StatSum.Recovered
			t.Logf("OSD_FULLNESS_LIVE_HELD case=%s baseline=%+v pg query: %.16384s", phase.name, base, baseRaw)
			policy = applyPolicy()
			out.OSDStatesPolicy = fullnessWaitBackfillFull(t, ctx, cluster, true, primary, victim.ID)
			if err := hold.Restore(ctx); err != nil {
				fail("%v", err)
			}
		} else {
			policy = applyPolicy()
			fullnessWaitBackfillFull(t, ctx, cluster, true, primary)
			if err := victim.Start(ctx); err != nil {
				fail("%v", err)
			}
			stopped = false
			captured = secondFaultInstanceOf(t, ctx, victim)
		}

		// Sample the primary's own report and the writer's ledger until the
		// barrier has held long enough.
		var samples []barrierSample
		var live []fullnessLiveSample
		var lastRaw []byte
		start := time.Now()
		var firstHold time.Time
		for {
			now := time.Now()
			if now.Sub(start) > 4*time.Minute {
				fail("no backfill_toofull window within 4 minutes; %d samples %v, %d errors; last pg query: %.16384s", len(samples), out.Samples, out.SampleErrors, lastRaw)
			}
			// The window is long enough when the last observation began a full
			// window after the first one ended; the latency of a read is not held time.
			if !firstHold.IsZero() && len(live) > 0 && live[len(live)-1].Start.Sub(firstHold) >= fullnessWindow {
				break
			}
			at := time.Now()
			raw, err := pgQueryRaw(ctx, cluster, pgid)
			var sample pgProgress
			if err == nil {
				sample, err = parsePGProgress(raw)
			}
			if err != nil {
				out.SampleErrors++
				// A failed read inside the credited window breaks it: it is
				// kept as an inconclusive sample and the run fails.
				if credited := recordFailedRead(samples, time.Now(), !firstHold.IsZero()); len(credited) != len(samples) {
					out.Samples[barrierInconclusive]++
					live = append(live, fullnessLiveSample{Start: at, End: time.Now(), Verdict: barrierInconclusive, Err: err})
					fail("a PG read failed after backfill_toofull was first reported, so the window is not uninterrupted: %v", err)
				}
				if out.SampleErrors > 10 {
					fail("too many failed PG samples before backfill_toofull, last: %v", err)
				}
				time.Sleep(time.Second)
				continue
			}
			lastRaw = raw
			if !haveBase {
				// Refused case: the baseline is the first sample that already
				// reports backfill_toofull for the only backfill target at MIN.
				if pgStateHas(sample.State, "backfilling") {
					fail("a backfill ran before backfill_toofull was reported: state %s\n%.16384s", sample.State, raw)
				}
				if b, err := backfillHeld(sample, victim.ID); err == nil && pgStateHas(sample.State, "backfill_toofull") {
					base, baseRaw, haveBase = b, raw, true
					query, err := parsePGQuery(raw)
					if err != nil {
						fail("%v", err)
					}
					if tail, err := parseEversion(query.Info.LogTail); err != nil || !stoppedAt.less(tail) {
						fail("the replica's last entry %v is not behind the log tail %s (%v): backfill is not what is needed", stoppedAt, query.Info.LogTail, err)
					}
					out.LogTail, out.RecoveredHeld = query.Info.LogTail, query.Info.Stats.StatSum.Recovered
					out.OSDStatesPolicy = fullnessWaitBackfillFull(t, ctx, cluster, true, primary, victim.ID)
					t.Logf("OSD_FULLNESS_LIVE_HELD case=%s baseline=%+v pg query: %.16384s", phase.name, base, raw)
				} else {
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}
			verdict, why := fullnessBarrierSample(base, sample)
			events, eventsErr := minSizeLedger(ctx, cluster, client, fullnessLiveLedger)
			ls := fullnessLiveSampleOf(base, at, time.Now(), raw, nil, events, eventsErr)
			if verdict == barrierHolds && ls.Err != nil {
				// The primary's report is conclusive, the ledger is not: the
				// sample is kept, and inside the credited window it fails the run.
				out.SampleErrors++
				live = append(live, ls)
				if !firstHold.IsZero() {
					out.Samples[barrierInconclusive]++
					fail("the writer ledger was unreadable after backfill_toofull was first reported, so the window is not uninterrupted: %v", ls.Err)
				}
				time.Sleep(time.Second)
				continue
			}
			out.Samples[verdict]++
			// The barrier is timed by the PG query, the live window by both bounds.
			samples = append(samples, barrierSample{Verdict: verdict, At: ls.Start})
			live = append(live, ls)
			if verdict == barrierHolds && firstHold.IsZero() {
				firstHold = ls.End
			}
			if verdict != barrierHolds && verdict != barrierWaiting && !(verdict == barrierRunning && phase.revoke && firstHold.IsZero()) {
				fail("barrier sample %s: %s\n%.16384s", verdict, why, raw)
			}
			out.StateAtEnd = sample.State
			time.Sleep(time.Second)
		}
		judged, err := judgeBarrier(samples, fullnessSpan, phase.revoke)
		out.BeforeFirstHold, out.ToofullFor = judged.BeforeVerdictsByName, judged.HeldFor.Round(time.Second).String()
		if err != nil {
			fail("%v; samples %v\n%.16384s", err, out.Samples, lastRaw)
		}
		endQuery, err := parsePGQuery(lastRaw)
		if err != nil {
			fail("%v", err)
		}
		out.RecoveredWindow = endQuery.Info.Stats.StatSum.Recovered
		if out.RecoveredWindow != out.RecoveredHeld {
			fail("num_objects_recovered went from %d to %d while the barrier held", out.RecoveredHeld, out.RecoveredWindow)
		}
		t.Logf("OSD_FULLNESS_LIVE_WINDOW case=%s samples=%v before_first=%v toofull_for=%s pg query: %.16384s", phase.name, out.Samples, judged.BeforeVerdictsByName, out.ToofullFor, lastRaw)

		// The writer over the credited window, as the ledger recorded it.
		window, err := judgeFullnessLive(live, fullnessLiveMinAcks, fullnessSpan, fullnessLiveGap)
		if err != nil {
			out.WindowError = err.Error()
			fail("the writer window was not shown: %v", err)
		}
		out.Window = &window
		// The operations of the last credited sample, not a new read: one that was
		// outstanding there must stay outstanding in the snapshot.
		out.endOps = window.EndOps
		t.Logf("OSD_FULLNESS_LIVE_WRITER case=%s %+v", phase.name, window)
		// The observation is recorded; the promise is checked as a separate step
		// and does not stop the restoration and readback below.
		if err := fullnessLiveExpect(window); err != nil {
			out.Promise = err.Error()
			if receipt.FirstFailure == "" {
				receipt.FirstFailure = err.Error()
			}
			t.Errorf("%s: %v", phase.name, err)
		} else {
			out.Promise = "yes"
		}

		// Lift the policy: the next retry of the reservation must succeed.
		if err := policy.Restore(ctx); err != nil {
			fail("%v", err)
		}
		fullnessWaitBackfillFull(t, ctx, cluster, false, primary, victim.ID)
		lifted := time.Now()
		// The writer's progress after the restoration is judged from a fresh
		// baseline, taken after the policy was confirmed lifted.
		_, afterLift := minSizeAcksAfter(t, ctx, cluster, client, fullnessLiveLedger, 2, "after the policy was lifted")
		out.WriterAfterLift = &afterLift
		waitPG(t, ctx, cluster, "clean again", func(pg ceph.PGState) bool {
			return pg.State == "active+clean" && slices.Contains(pg.Acting, victim.ID)
		})
		out.CleanAfterRelease = time.Since(lifted).Round(time.Second).String()
		after, _ := queryPG(t, ctx, cluster, pgid)
		out.RecoveredFinal = after.Info.Stats.StatSum.Recovered
		// Every object written while the replica was down is recovered; the
		// writer's own objects past the replica's cursor are recovered as well,
		// so the count is a lower bound.
		if recovered := out.RecoveredFinal - out.RecoveredHeld; recovered < count {
			fail("%s recovered %d objects, want at least the %d written while osd.%d was down", phase.name, recovered, count, victim.ID)
		}
		readRecoveryObjects(t, ctx, cluster, prefixes...)
		out.Baseline = base
	}

	// Close the writer and the data oracle.
	execCommand(t, ctx, client, "touch", fullnessLiveStop)
	select {
	case done := <-actor.done:
		actorDone = true
		if done.err != nil || done.result.ExitCode != 0 || !done.result.Complete() {
			fail("live writer shutdown: %v exit %d: %.1024s", done.err, done.result.ExitCode, done.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		fail("the live writer did not stop after its stop file appeared")
	}
	ops, err := fullnessLiveOpsNow(ctx, cluster, client, true)
	if err != nil {
		fail("writer ledger refused: %v", err)
	}
	for _, phase := range receipt.Phases {
		if phase.Late, err = fullnessLiveSettle(phase.endOps, ops); err != nil {
			fail("%s: %v", phase.Case, err)
		}
		if phase.Window != nil && len(phase.Late) != phase.Window.End.Outstanding {
			fail("%s: %d late outcomes for %d operations outstanding at the end of the window", phase.Case, len(phase.Late), phase.Window.End.Outstanding)
		}
	}
	final := secondFaultCount(ops)
	receipt.Final = &final
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	encodedNames, err := json.Marshal(names)
	if err != nil {
		fail("%v", err)
	}
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), secondFaultCommand("independent read", 3*time.Minute, "read", "tc-paths", string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		fail("independent read of the writer's objects: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		fail("independent read %.512s: %v", read.Stdout, err)
	}
	readback, err := fullnessLiveVerify(ops, objects)
	receipt.Readback = &readback
	if err != nil {
		fail("%v", err)
	}
	if last := receipt.Phases[len(receipt.Phases)-1].WriterAfterLift; final.Acked < last.Acked || final.Refused != 0 {
		fail("writer outcomes %+v do not extend the watermark %+v or include a refusal", final, *last)
	}
	readRecoveryObjects(t, ctx, cluster, prefixes...)
	t.Logf("OSD_FULLNESS_LIVE_BOUNDARY writer_final=%+v readback=%+v", final, readback)
	receipt.Done = true
}
