//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Fixtures for the epoch timeline and the parked-operation evidence of the
// late cluster record. They use synthetic output shaped like the pinned
// source prints it and say nothing about why a native call timed out.

const radosLateEpochZero = "1970-01-01T00:00:00.000000+0000"

func radosLateStamp(base time.Time, offset time.Duration) string {
	return base.Add(offset).UTC().Format("2006-01-02T15:04:05.000000-0700")
}

var radosLateBase = time.Date(2026, 10, 27, 4, 11, 0, 0, time.UTC)

// radosLateDumpAt builds an "osd dump" answer for one epoch.
func radosLateDumpAt(epoch int, modified string, upThru int, flags string) string {
	return fmt.Sprintf(`{"epoch":%d,"modified":%q,"flags":%q,"last_up_change":"up0","last_in_change":"in0",
		"pools":[{"pool":2,"pool_name":"tc-contracts","last_change":"11"}],
		"osds":[{"osd":0,"up":1,"in":1,"up_from":6,"up_thru":%d}]}`, epoch, modified, flags, upThru)
}

func radosLateEpochDumpKey(epoch int) string {
	return fmt.Sprintf("ceph osd dump %d --format json", epoch)
}

// timelineReader answers a baseline at epoch 15 and a failure at epoch last;
// the epochs in between are served by dumpFor.
func timelineReader(last int, dumpFor func(epoch int) string) *fakeRadosLateReader {
	reader := healthyRadosLateReader(last, last)
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(radosLateDumpAt(last, radosLateStamp(radosLateBase, time.Duration(last)*time.Second), 15, "sortbitwise"))
	for epoch := 16; epoch <= last; epoch++ {
		reader.answers[radosLateEpochDumpKey(epoch)] = fixedRadosLateAnswer(dumpFor(epoch))
	}
	return reader
}

func timelineBaseline(t *testing.T) *radosLateClusterState {
	t.Helper()
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(radosLateDumpAt(15, radosLateStamp(radosLateBase, 15*time.Second), 11, "sortbitwise"))
	baseline := collectFixture(reader, "pre-late", nil)
	if baseline.MonEpoch == nil || baseline.MonModifiedUnix == nil || len(baseline.Timeline) != 0 || len(reader.commands) != 5 {
		t.Fatalf("baseline: %+v commands %v", baseline, reader.commands)
	}
	return baseline
}

func TestRadosLateEpochTimelineReadsEachEpochInOrderAndNamesTheChange(t *testing.T) {
	baseline := timelineBaseline(t)
	reader := timelineReader(17, func(epoch int) string {
		if epoch == 16 {
			return radosLateDumpAt(16, radosLateStamp(radosLateBase, 41*time.Second), 15, "sortbitwise") // up_thru moved
		}
		return radosLateDumpAt(17, radosLateStamp(radosLateBase, 43*time.Second), 15, "sortbitwise,pauserd") // flags moved
	})
	state := collectFixture(reader, "failure", baseline)
	want := []string{radosLateKeyInFlight, radosLateKeyHistoric, radosLateKeyOSD, radosLateKeyDump, radosLateKeyStatus, radosLateEpochDumpKey(16), radosLateEpochDumpKey(17)}
	if strings.Join(reader.commands, "|") != strings.Join(want, "|") {
		t.Fatalf("commands %q, want %q", reader.commands, want)
	}
	if len(state.Timeline) != 2 || state.TimelineSkipped != 0 {
		t.Fatalf("timeline %+v skipped %d", state.Timeline, state.TimelineSkipped)
	}
	first, second := state.Timeline[0], state.Timeline[1]
	if first.Epoch != 16 || !first.Compared || len(first.Changes) != 1 || first.Changes[0] != "osd.0 up_thru 11 -> 15" {
		t.Fatalf("epoch 16: %+v", first)
	}
	if second.Epoch != 17 || len(second.Changes) != 1 || second.Changes[0] != `flags "sortbitwise" -> "sortbitwise,pauserd"` {
		t.Fatalf("epoch 17: %+v", second)
	}
	if !(*baseline.MonModifiedUnix < first.ModifiedUnix && first.ModifiedUnix < second.ModifiedUnix) || first.ModifiedUnix <= 0 {
		t.Fatalf("times not strictly ordered and nonzero: %v %v %v", *baseline.MonModifiedUnix, first.ModifiedUnix, second.ModifiedUnix)
	}
	if state.EpochAdvance == nil || *state.EpochAdvance != 2 {
		t.Fatalf("advance %v", state.EpochAdvance)
	}
}

func TestRadosLateEpochTimelineSkipsWithoutAdvanceOrBaseline(t *testing.T) {
	baseline := timelineBaseline(t)
	for name, base := range map[string]*radosLateClusterState{"no baseline": nil, "no advance": baseline} {
		reader := healthyRadosLateReader(15, 15)
		reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(radosLateDumpAt(15, radosLateStamp(radosLateBase, 15*time.Second), 11, "sortbitwise"))
		state := collectFixture(reader, "failure", base)
		if len(reader.commands) != 5 || len(state.Timeline) != 0 || state.TimelineSkipped != 0 {
			t.Fatalf("%s: commands %v timeline %v", name, reader.commands, state.Timeline)
		}
	}
	// A baseline whose epoch was not read cannot anchor a timeline either.
	unreadable := &radosLateClusterState{}
	reader := timelineReader(17, func(e int) string { return radosLateDumpAt(e, radosLateStamp(radosLateBase, 40*time.Second), 15, "x") })
	collectFixture(reader, "failure", unreadable)
	if len(reader.commands) != 5 {
		t.Fatalf("commands %v", reader.commands)
	}
}

func TestRadosLateEpochTimelineRejectsUnusableEpochAnswers(t *testing.T) {
	good := func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, time.Duration(30+e)*time.Second), 15, "sortbitwise")
	}
	cases := []struct {
		name, answer, reason string
	}{
		{"other epoch answered", radosLateDumpAt(15, radosLateStamp(radosLateBase, 31*time.Second), 11, "sortbitwise"), "answer is epoch 15, requested 16"},
		{"zero modified", radosLateDumpAt(16, radosLateEpochZero, 15, "x"), "no usable modified time"},
		{"modified missing", `{"epoch":16,"osds":[{"osd":0,"up":1,"in":1}]}`, "no usable modified time"},
		{"modified not a timestamp", radosLateDumpAt(16, "yesterday", 15, "x"), "no usable modified time"},
		{"modified before the baseline", radosLateDumpAt(16, radosLateStamp(radosLateBase, 5*time.Second), 15, "x"), "before the previous epoch"},
		{"trailing brace", good(16) + "}", "trailing data"},
		{"trailing bracket", good(16) + "]", "trailing data"},
		{"epoch as string", strings.Replace(good(16), `"epoch":16`, `"epoch":"16"`, 1), "wrong type"},
		{"empty", ``, "not a JSON object"},
		{"null", `null`, "JSON null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := timelineBaseline(t)
			reader := timelineReader(17, func(e int) string { return good(e) })
			reader.answers[radosLateEpochDumpKey(16)] = fixedRadosLateAnswer(tc.answer)
			state := collectFixture(reader, "failure", baseline)
			if err := radosLateProbeError(t, state, "osd_dump_e16"); !strings.Contains(err, tc.reason) {
				t.Fatalf("reason %q, want %q", err, tc.reason)
			}
			if len(state.Timeline) != 0 || state.TimelineSkipped != 2 {
				t.Fatalf("an unreadable epoch must end the timeline and be counted: %+v skipped %d", state.Timeline, state.TimelineSkipped)
			}
			for _, command := range reader.commands {
				if command == radosLateEpochDumpKey(17) {
					t.Fatalf("epoch 17 was read after epoch 16 failed")
				}
			}
		})
	}
	t.Run("command failure", func(t *testing.T) {
		baseline := timelineBaseline(t)
		reader := timelineReader(17, func(e int) string { return good(e) })
		reader.answers[radosLateEpochDumpKey(16)] = func(context.Context) ([]byte, error) { return []byte(good(16)), fmt.Errorf("no map for epoch 16") }
		state := collectFixture(reader, "failure", baseline)
		if err := radosLateProbeError(t, state, "osd_dump_e16"); !strings.Contains(err, "no map for epoch 16") || len(state.Timeline) != 0 {
			t.Fatalf("%q %+v", err, state.Timeline)
		}
	})
}

func TestRadosLateEpochTimelineIsBounded(t *testing.T) {
	baseline := timelineBaseline(t)
	last := 15 + radosLateEpochLimit + 4
	reader := timelineReader(last, func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, time.Duration(20+e)*time.Second), 15, "sortbitwise")
	})
	state := collectFixture(reader, "failure", baseline)
	if len(state.Timeline) != radosLateEpochLimit || state.TimelineSkipped != 4 {
		t.Fatalf("kept %d skipped %d", len(state.Timeline), state.TimelineSkipped)
	}
	for _, command := range reader.commands {
		if strings.HasPrefix(command, "ceph osd dump 2") && command != radosLateEpochDumpKey(15+radosLateEpochLimit) {
			if command > radosLateEpochDumpKey(15+radosLateEpochLimit) && len(command) >= len(radosLateEpochDumpKey(15+radosLateEpochLimit)) {
				t.Fatalf("read past the limit: %s", command)
			}
		}
	}
	encoded, _ := json.Marshal(state)
	if len(encoded) > radosLateStateLimit {
		t.Fatalf("record %d bytes", len(encoded))
	}
}

// A comparison is only claimed when a field existed on both sides, and an
// empty change list encodes as [] rather than null.
func TestRadosLateEpochTimelineComparisonIsExplicit(t *testing.T) {
	bare := healthyRadosLateReader(15, 15)
	bare.answers[radosLateKeyDump] = fixedRadosLateAnswer(fmt.Sprintf(`{"epoch":15,"modified":%q,"osds":[{"osd":0,"up":1,"in":1}]}`, radosLateStamp(radosLateBase, 15*time.Second)))
	baseline := collectFixture(bare, "pre-late", nil)
	// The baseline carried no flags, change stamps, pool change or up_thru, so
	// their values at epoch 16 are not differences. Only up/in existed on
	// both sides.
	reader := timelineReader(16, func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, 41*time.Second), 15, "sortbitwise")
	})
	state := collectFixture(reader, "failure", baseline)
	if len(state.Timeline) != 1 || !state.Timeline[0].Compared || len(state.Timeline[0].Changes) != 0 {
		t.Fatalf("a field missing from the baseline was reported as changed: %+v", state.Timeline)
	}
	encoded, _ := json.Marshal(state.Timeline)
	if !strings.Contains(string(encoded), `"changes":[]`) || strings.Contains(string(encoded), `"changes":null`) {
		t.Fatalf("changes must encode as an empty list: %s", encoded)
	}
	// Same fields on both sides and nothing different: compared, still empty.
	full := timelineBaseline(t)
	same := collectFixture(timelineReader(16, func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, 41*time.Second), 11, "sortbitwise")
	}), "failure", full)
	if len(same.Timeline) != 1 || !same.Timeline[0].Compared || len(same.Timeline[0].Changes) != 0 {
		t.Fatalf("%+v", same.Timeline)
	}
}

func radosLateParkedOp(client, object string, epoch int, initiated string, age float64, events ...string) string {
	rendered := make([]string, len(events))
	for index, event := range events {
		rendered[index] = fmt.Sprintf(`{"event":%q,"time":%q,"duration":0.1}`, event, radosLateStamp(radosLateBase, time.Duration(40+index)*time.Second))
	}
	return fmt.Sprintf(`{"description":"osd_op(%s.0:51 2.0 2:abc:suite::%s:head [stat] snapc 0=[] e%d)","initiated_at":%q,"age":%v,"duration":%v,"type_data":{"flag_point":"delayed","events":[%s]}}`,
		client, object, epoch, initiated, age, age, strings.Join(rendered, ","))
}

func TestRadosLateWaitEventsAreExactlyTheSourceReasons(t *testing.T) {
	for event, want := range map[string][2]bool{
		"op must wait for map": {true, true}, "waiting_for_map not empty": {true, true},
		"waiting for rw locks": {true, false}, "waiting for readable": {true, false}, "waiting for active": {true, false},
		"queued_for_pg": {false, false}, "reached_pg": {false, false}, "started": {false, false}, "delayed": {false, false},
		"waitingfor": {false, false}, "": {false, false}, "wait for map": {false, false},
	} {
		wait, forMap := radosLateWaitEvent(event)
		if wait != want[0] || forMap != want[1] {
			t.Errorf("%q: wait %v forMap %v, want %v", event, wait, forMap, want)
		}
	}
}

// The parked-op signature survives the event cap and the shedding of every
// timeline, because it is kept apart from the timeline.
func TestRadosLateParkedOperationSurvivesCapAndShedding(t *testing.T) {
	events := make([]string, 0, radosLateEventLimit+6)
	for i := 0; i < radosLateEventLimit+3; i++ {
		events = append(events, fmt.Sprintf("filler-%02d-%s", i, strings.Repeat("f", 50)))
	}
	events = append(events, "op must wait for map", "waiting for rw locks")
	parked := radosLateParkedOp("client.4218", "late-compound", 16, radosLateStamp(radosLateBase, 42*time.Second), 10.0, events...)
	var short []string
	for i := 0; i < 40; i++ {
		filler := make([]string, 12)
		for j := range filler {
			filler[j] = fmt.Sprintf("short-%02d-%s", j, strings.Repeat("e", 100))
		}
		short = append(short, radosLateParkedOp(fmt.Sprintf("client.%d", 4300+i), "x", 15, radosLateStamp(radosLateBase, 12*time.Second), 0.001, filler...))
	}
	reader := healthyRadosLateReader(16, 15)
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(`{"ops":[` + strings.Join(short, ",") + `,` + parked + `]}`)
	state := collectFixture(reader, "failure", nil)
	if len(state.Historic) == 0 || state.Historic[0].Client != "client.4218" {
		t.Fatalf("the parked op must be ranked first: %+v", state.Historic[:1])
	}
	op := state.Historic[0]
	if !op.WaitsForMap || len(op.Waits) != 2 || op.Waits[0].Event != "op must wait for map" || op.Waits[1].Event != "waiting for rw locks" {
		t.Fatalf("waits lost: %+v", op)
	}
	if op.EventsDropped < 3 {
		t.Fatalf("event cap not recorded: %d", op.EventsDropped)
	}
	if state.HistoricTotal == nil || *state.HistoricTotal != 41 || state.HistoricDropped == 0 {
		t.Fatalf("total %v dropped %d", state.HistoricTotal, state.HistoricDropped)
	}
	if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
		t.Fatalf("%d bytes", size)
	}
	// The relevant operation keeps its own timeline; the uninformative ones lose theirs first.
	if len(op.Events) == 0 {
		t.Fatalf("shedding removed the relevant operation's events before the others'")
	}
	for _, other := range state.Historic[1:] {
		if len(other.Events) != 0 {
			t.Fatalf("an uninformative op kept its events while the budget was exceeded")
		}
	}
}

func TestRadosLateOperationJoinsUseOnlyWhatWasRead(t *testing.T) {
	baseline := timelineBaseline(t) // epoch 15 modified at +15 s
	e16, e17 := 41*time.Second, 50*time.Second
	reader := timelineReader(17, func(epoch int) string {
		at := e16
		if epoch == 17 {
			at = e17
		}
		return radosLateDumpAt(epoch, radosLateStamp(radosLateBase, at), 15, "sortbitwise")
	})
	inFlight := radosLateParkedOp("client.4218", "late-compound", 16, radosLateStamp(radosLateBase, 42*time.Second), 9.5, "initiated", "op must wait for map")
	older := radosLateParkedOp("client.4218", "late-remove", 15, radosLateStamp(radosLateBase, 36*time.Second), 6.0, "initiated", "started")
	beforeAll := radosLateParkedOp("client.4216", "zeroed", 15, radosLateStamp(radosLateBase, 5*time.Second), 6.0, "initiated")
	garbled := radosLateParkedOp("client.4220", "late-truncate", 15, "not a time", 6.0, "initiated")
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` + strings.Join([]string{inFlight, older, beforeAll, garbled}, ",") + `]}`)
	reader.answers[radosLateKeyOSD] = fixedRadosLateAnswer(`{"state":"active","oldest_map":"1","newest_map":"15"}`)
	state := collectFixture(reader, "failure", baseline)
	byObject := map[string]radosLateOp{}
	for _, op := range state.InFlight {
		byObject[op.Description[strings.Index(op.Description, "suite::")+7:strings.Index(op.Description, ":head")]] = op
	}
	if op := byObject["late-compound"]; op.EpochAtInitiated == nil || *op.EpochAtInitiated != 16 || op.EpochLead == nil || *op.EpochLead != 1 {
		t.Fatalf("parked op joins: %+v", op)
	}
	if op := byObject["late-remove"]; op.EpochAtInitiated == nil || *op.EpochAtInitiated != 15 || *op.EpochLead != 0 {
		t.Fatalf("older op joins: %+v", op)
	}
	if op := byObject["zeroed"]; op.EpochAtInitiated != nil {
		t.Fatalf("an op initiated before the baseline epoch was committed must not be attributed: %v", *op.EpochAtInitiated)
	}
	if op := byObject["late-truncate"]; op.InitiatedUnix != nil || op.EpochAtInitiated != nil || op.Initiated != "not a time" {
		t.Fatalf("garbled time became evidence: %+v", op)
	}
	// Without osd.0's stored map range there is no lead.
	reader = timelineReader(17, func(e int) string { return radosLateDumpAt(e, radosLateStamp(radosLateBase, 41*time.Second), 15, "x") })
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` + inFlight + `]}`)
	reader.answers[radosLateKeyOSD] = fixedRadosLateAnswer(`{"state":"active","oldest_map":"1"}`)
	if got := collectFixture(reader, "failure", baseline).InFlight[0]; got.EpochLead != nil {
		t.Fatalf("lead from an unread map range: %v", *got.EpochLead)
	}
}

func TestRadosLateOperationTimestampFieldTypes(t *testing.T) {
	for name, field := range map[string]string{"number": `"initiated_at":12`, "null": `"initiated_at":null`, "object": `"initiated_at":{}`} {
		t.Run(name, func(t *testing.T) {
			reader := healthyRadosLateReader(15, 15)
			reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[{"description":"osd_op(client.1.0:1 2.0 x e1)",` + field + `}]}`)
			state := collectFixture(reader, "failure", nil)
			if err := radosLateProbeError(t, state, "ops_in_flight"); err == "" || state.InFlightTotal != nil {
				t.Fatalf("accepted %s: %q", field, err)
			}
		})
	}
	t.Run("zero and epoch-zero times are not timestamps", func(t *testing.T) {
		for _, text := range []string{radosLateEpochZero, "", "2026-10-27"} {
			if _, ok := radosLateTime(text); ok {
				t.Errorf("%q accepted", text)
			}
		}
		if unix, ok := radosLateTime("2026-10-27T04:11:41.821662+0000"); !ok || unix < 1.7e9 {
			t.Errorf("real stamp rejected: %v %v", unix, ok)
		}
	})
}
