//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

const (
	// fullnessRetry is osd_backfill_retry_interval in seconds. Ceph's default
	// of 30 s would make every refusal and every recovery after the policy is
	// lifted wait that long.
	fullnessRetry = "5"
	// fullnessSpan is how long backfill_toofull must be observed without a
	// break: four retry intervals, so that refused reservation retries are
	// covered and not just the first refusal.
	fullnessSpan = 20 * time.Second
	// fullnessWindow is how long the barrier is sampled after its first sign.
	fullnessWindow = 25 * time.Second
)

// fullnessPhase is the receipt of one barrier case.
type fullnessPhase struct {
	Case              string                 `json:"case"`
	Written           int                    `json:"written"`
	StoppedAt         string                 `json:"stopped_at"`
	LogTail           string                 `json:"log_tail"`
	Baseline          backfillBaseline       `json:"baseline"`
	HeldState         string                 `json:"state_before_policy,omitempty"`
	OSDStatesPolicy   map[int][]string       `json:"osd_states_with_policy,omitempty"`
	Samples           map[barrierVerdict]int `json:"samples"`
	SampleErrors      int                    `json:"sample_errors"`
	BeforeFirstHold   map[barrierVerdict]int `json:"before_first_toofull"`
	ToofullFor        string                 `json:"toofull_for"`
	StateAtEnd        string                 `json:"state_at_end_of_window"`
	RecoveredHeld     int64                  `json:"recovered_at_first_toofull"`
	RecoveredWindow   int64                  `json:"recovered_at_end_of_window"`
	RecoveredFinal    int64                  `json:"recovered_final"`
	CleanAfterRelease string                 `json:"clean_after_policy_lifted"`
}

type fullnessReceipt struct {
	PG      string          `json:"pg"`
	Primary int             `json:"primary"`
	Replica int             `json:"replica"`
	Retry   string          `json:"backfill_retry_interval"`
	Ratios  ceph.FullRatios `json:"ratios"`
	Phases  []fullnessPhase `json:"phases"`
	Done    bool            `json:"completed"`
}

// fullnessOSDStates reads the state names of every OSD from the monitor.
func fullnessOSDStates(ctx context.Context, cluster *ceph.Container) (map[int][]string, error) {
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	return osdStates(out)
}

// fullnessWaitBackfillFull waits until the monitor's map shows every listed OSD
// up and backfillfull (or neither backfillfull nor full), and returns the map.
func fullnessWaitBackfillFull(t *testing.T, parent context.Context, cluster *ceph.Container, want bool, osds ...int) map[int][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var why string
	for {
		states, err := fullnessOSDStates(ctx, cluster)
		if err == nil {
			ok, reason := backfillFullIs(states, want, osds...)
			if ok {
				return states
			}
			why = reason
		} else if ctx.Err() == nil {
			why = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("OSDs %v never reached backfillfull=%t: %s", osds, want, why)
		case <-time.After(time.Second):
		}
	}
}

// fullnessEffective requires every OSD to report the running value of name.
func fullnessEffective(t *testing.T, ctx context.Context, cluster *ceph.Container, name, want string) {
	t.Helper()
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		out, err := cluster.Ceph(ctx, "tell", "osd."+strconv.Itoa(osd.ID), "config", "get", name)
		var effective map[string]string
		if err != nil || json.Unmarshal(out, &effective) != nil || !abruptSameNumber(effective[name], want) {
			t.Fatalf("osd.%d effective %s: %s: %v, want %s", osd.ID, name, out, err, want)
		}
	}
}

// testOSDFullnessBarrier brings one replica of a one-PG, two-replica pool up
// to date by backfill twice, with Ceph's backfillfull policy refusing it.
// Ratios are cluster-wide, so every OSD is under the policy; the scenario
// does not claim an effect on the destination alone. No disk is filled: the
// ratios are lowered below what the OSDs already use.
//
// Case "revoked": the replica stops, writes continue, nobackfill holds the
// recovery, and the replica returns. The primary must show the backfill
// reserved ("backfilling", destination at MIN). Only then is the policy
// applied, and the monitor must show the OSDs backfillfull. Releasing the hold
// lets the primary scan, the replica refuses at the scan, and the primary
// must report backfill_toofull with the destination still at MIN.
//
// Case "refused": the policy is applied while the replica is down, then the
// replica returns with nothing holding the recovery. Its reservation must be
// refused: backfill_toofull with no sample ever showing a running backfill.
//
// In both cases backfill_toofull must hold for several retry intervals with
// the destination at MIN and no objects recovered. Then the policy is lifted
// and the PG must become clean on both OSDs, recover exactly the objects
// written while the replica was down, and an independent client must read
// every acknowledged object back.
func testOSDFullnessBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 22*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t)
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
	receipt := &fullnessReceipt{Retry: fullnessRetry, Ratios: ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .95}}
	// Registered after the cluster, so that it runs before the cluster is
	// destroyed and reports a failed run too.
	t.Cleanup(func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Errorf("fullness receipt: %v", err)
			return
		}
		t.Logf("OSD_FULLNESS_BARRIER_RECEIPT %s", data)
	})
	for _, setting := range []ceph.ConfigSetting{
		{Section: "osd", Name: "osd_backfill_retry_interval", Value: fullnessRetry},
		// A short, always-trimmed PG log leaves the returning replica behind the
		// log tail, which needs a backfill.
		{Section: "osd", Name: "osd_min_pg_log_entries", Value: "10"},
		{Section: "osd", Name: "osd_max_pg_log_entries", Value: "10"},
		{Section: "osd", Name: "osd_pg_log_trim_min", Value: "1"},
	} {
		handle, err := override.SetConfig(ctx, cluster, setting)
		restore(setting.Name, handle.Restore)
		if err != nil {
			t.Fatal(err)
		}
	}
	fullnessEffective(t, ctx, cluster, "osd_backfill_retry_interval", fullnessRetry)

	pool := ceph.PoolConfig{Name: "tc-paths", PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados"}
	if _, err := cluster.CreatePool(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryRun(t, ctx, cluster, client, "write", "seed:20")
	clean := waitPG(t, ctx, cluster, "clean on both OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 2 && !slices.Contains(pg.Acting, 2147483647)
	})
	pgid, primary := clean.PGID.String(), clean.ActingPrimary
	var victim *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID != primary && slices.Contains(clean.Acting, osd.ID) {
			victim = osd
		}
	}
	if victim == nil {
		t.Fatalf("no replica besides primary osd.%d: %+v", primary, clean)
	}
	receipt.PG, receipt.Primary, receipt.Replica = pgid, primary, victim.ID
	version, err := cluster.Ceph(ctx, "version")
	if err != nil {
		t.Fatal(err)
	}
	uname, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"uname", "-sm"}, Timeout: time.Minute, MaxStdout: 256, MaxStderr: 256, Label: "fullness barrier platform"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_FULLNESS_BARRIER_RUNTIME ceph=%q platform=%q pg=%s primary=osd.%d replica=osd.%d retry_interval=%s", strings.TrimSpace(string(version)), strings.TrimSpace(string(uname.Stdout)), pgid, primary, victim.ID, fullnessRetry)

	stopped := false
	restore("replica", func(cleanup context.Context) error {
		if stopped {
			stopped = false
			return victim.Start(cleanup)
		}
		return nil
	})
	captured := secondFaultInstanceOf(t, ctx, victim)
	noout, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)
	if noout != nil {
		restore("noout", noout.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}

	prefixes := []string{"seed:20"}
	const count = 60
	for _, phase := range []struct {
		name   string
		revoke bool
	}{{"revoked", true}, {"refused", false}} {
		prefix := phase.name
		out := &fullnessPhase{Case: phase.name, Written: count, Samples: map[barrierVerdict]int{}}
		receipt.Phases = append(receipt.Phases, fullnessPhase{})
		slot := len(receipt.Phases) - 1
		// The receipt keeps the phase as far as it got, even on a failure.
		publish := func() { receipt.Phases[slot] = *out }
		defer publish()

		before, _ := queryPG(t, ctx, cluster, pgid)
		stoppedAt, err := parseEversion(before.Info.LastUpdate)
		if err != nil {
			t.Fatal(err)
		}
		out.StoppedAt = before.Info.LastUpdate
		// Refuse a replica that is not the running instance captured earlier,
		// so that an unplanned exit is not relabelled as this stop.
		if err := secondFaultAdmitStop(captured, secondFaultInstanceOf(t, ctx, victim)); err != nil {
			t.Fatalf("osd.%d stop refused: %v", victim.ID, err)
		}
		stopped = true
		timeout := 10 * time.Second
		if err := victim.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		if after := secondFaultInstanceOf(t, ctx, victim); after.Running || after.OOMKilled {
			t.Fatalf("osd.%d after the stop request: %+v", victim.ID, after)
		}
		waitPG(t, ctx, cluster, "degraded without the replica", func(pg ceph.PGState) bool {
			return strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Acting, victim.ID)
		})
		spec := fmt.Sprintf("%s:%d", prefix, count)
		recoveryRun(t, ctx, cluster, client, "write", spec)
		prefixes = append(prefixes, spec)

		var hold *ceph.OSDFlagOverrideHandle
		applyPolicy := func() *override.FullRatiosHandle {
			handle, err := override.SetFullRatios(ctx, cluster, receipt.Ratios)
			// A failed application may own a partial override: register its
			// restoration before looking at the error.
			restore("full ratios "+phase.name, handle.Restore)
			if err != nil {
				t.Fatalf("apply full ratios: %v", err)
			}
			return handle
		}
		var policy *override.FullRatiosHandle
		var base backfillBaseline
		var baseRaw []byte
		haveBase := false
		if phase.revoke {
			hold, err = cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoBackfill, true)
			if hold != nil {
				restore("nobackfill "+phase.name, hold.Restore)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := victim.Start(ctx); err != nil {
				t.Fatal(err)
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
					t.Fatalf("no reserved, held backfill to osd.%d: %v\n%.16384s", victim.ID, err, raw)
				}
				time.Sleep(500 * time.Millisecond)
			}
			heldQuery, err := parsePGQuery(baseRaw)
			if err != nil {
				t.Fatal(err)
			}
			if path, err := pgRecoveryPath(heldQuery, victim.ID, stoppedAt); err != nil || path != "backfill" {
				t.Fatalf("held path %q: %v\n%.16384s", path, err, baseRaw)
			}
			out.LogTail, out.RecoveredHeld = heldQuery.Info.LogTail, heldQuery.Info.Stats.StatSum.Recovered
			t.Logf("OSD_FULLNESS_BARRIER_HELD case=%s baseline=%+v pg query: %.16384s", phase.name, base, baseRaw)
			policy = applyPolicy()
			out.OSDStatesPolicy = fullnessWaitBackfillFull(t, ctx, cluster, true, primary, victim.ID)
			if err := hold.Restore(ctx); err != nil {
				t.Fatal(err)
			}
		} else {
			policy = applyPolicy()
			fullnessWaitBackfillFull(t, ctx, cluster, true, primary)
			if err := victim.Start(ctx); err != nil {
				t.Fatal(err)
			}
			stopped = false
			captured = secondFaultInstanceOf(t, ctx, victim)
		}

		// Sample the primary's own report until the barrier has held long enough.
		var samples []barrierSample
		var lastRaw []byte
		start := time.Now()
		var firstHold time.Time
		for {
			now := time.Now()
			if now.Sub(start) > 4*time.Minute {
				t.Fatalf("no backfill_toofull window within 4 minutes; %d samples %v, %d errors; last pg query: %.16384s", len(samples), out.Samples, out.SampleErrors, lastRaw)
			}
			if !firstHold.IsZero() && now.Sub(firstHold) >= fullnessWindow {
				break
			}
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
					t.Fatalf("a PG read failed after backfill_toofull was first reported, so the window is not uninterrupted: %v", err)
				}
				if out.SampleErrors > 10 {
					t.Fatalf("too many failed PG samples before backfill_toofull, last: %v", err)
				}
				time.Sleep(time.Second)
				continue
			}
			lastRaw = raw
			if !haveBase {
				// Refused case: the baseline is the first sample that already
				// reports backfill_toofull for the only backfill target at MIN.
				if pgStateHas(sample.State, "backfilling") {
					t.Fatalf("a backfill ran before backfill_toofull was reported: state %s\n%.16384s", sample.State, raw)
				}
				if b, err := backfillHeld(sample, victim.ID); err == nil && pgStateHas(sample.State, "backfill_toofull") {
					base, baseRaw, haveBase = b, raw, true
					query, err := parsePGQuery(raw)
					if err != nil {
						t.Fatal(err)
					}
					if tail, err := parseEversion(query.Info.LogTail); err != nil || !stoppedAt.less(tail) {
						t.Fatalf("the replica's last entry %v is not behind the log tail %s (%v): backfill is not what is needed", stoppedAt, query.Info.LogTail, err)
					}
					out.LogTail, out.RecoveredHeld = query.Info.LogTail, query.Info.Stats.StatSum.Recovered
					out.OSDStatesPolicy = fullnessWaitBackfillFull(t, ctx, cluster, true, primary, victim.ID)
					t.Logf("OSD_FULLNESS_BARRIER_HELD case=%s baseline=%+v pg query: %.16384s", phase.name, base, raw)
				} else {
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}
			verdict, why := fullnessBarrierSample(base, sample)
			out.Samples[verdict]++
			samples = append(samples, barrierSample{Verdict: verdict, At: time.Now()})
			if verdict == barrierHolds && firstHold.IsZero() {
				firstHold = time.Now()
			}
			if verdict != barrierHolds && verdict != barrierWaiting && !(verdict == barrierRunning && phase.revoke && firstHold.IsZero()) {
				t.Fatalf("barrier sample %s: %s\n%.16384s", verdict, why, raw)
			}
			out.StateAtEnd = sample.State
			time.Sleep(time.Second)
		}
		judged, err := judgeBarrier(samples, fullnessSpan, phase.revoke)
		out.BeforeFirstHold, out.ToofullFor = judged.BeforeVerdictsByName, judged.HeldFor.Round(time.Second).String()
		if err != nil {
			t.Fatalf("%v; samples %v\n%.16384s", err, out.Samples, lastRaw)
		}
		endQuery, err := parsePGQuery(lastRaw)
		if err != nil {
			t.Fatal(err)
		}
		out.RecoveredWindow = endQuery.Info.Stats.StatSum.Recovered
		if out.RecoveredWindow != out.RecoveredHeld {
			t.Fatalf("num_objects_recovered went from %d to %d while the barrier held", out.RecoveredHeld, out.RecoveredWindow)
		}
		t.Logf("OSD_FULLNESS_BARRIER_WINDOW case=%s samples=%v before_first=%v toofull_for=%s pg query: %.16384s", phase.name, out.Samples, judged.BeforeVerdictsByName, out.ToofullFor, lastRaw)

		// Lift the policy: the next retry of the reservation must succeed.
		if err := policy.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		fullnessWaitBackfillFull(t, ctx, cluster, false, primary, victim.ID)
		lifted := time.Now()
		waitPG(t, ctx, cluster, "clean again", func(pg ceph.PGState) bool {
			return pg.State == "active+clean" && slices.Contains(pg.Acting, victim.ID)
		})
		out.CleanAfterRelease = time.Since(lifted).Round(time.Second).String()
		after, _ := queryPG(t, ctx, cluster, pgid)
		out.RecoveredFinal = after.Info.Stats.StatSum.Recovered
		if recovered := out.RecoveredFinal - out.RecoveredHeld; recovered != count {
			t.Fatalf("%s recovered %d objects, want the %d written while osd.%d was down", phase.name, recovered, count, victim.ID)
		}
		readRecoveryObjects(t, ctx, cluster, prefixes...)
		out.Baseline = base
		publish()
	}
	receipt.Done = true
}
