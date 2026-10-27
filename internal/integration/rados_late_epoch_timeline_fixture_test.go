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

// hostileTimeline is the review's counterexample: the largest accepted
// timeline, eight epochs of thirty-two changes whose text is as long as the
// per-string cap allows and expands when JSON-escaped.
func hostileTimeline() []radosLateEpoch {
	var timeline []radosLateEpoch
	for epoch := 16; epoch < 16+radosLateEpochLimit; epoch++ {
		entry := radosLateEpoch{Epoch: epoch, Modified: radosLateStamp(radosLateBase, time.Duration(epoch)*time.Second), ModifiedUnix: float64(1793074000 + epoch), Compared: true}
		for change := 0; change < radosLateListLimit; change++ {
			entry.Changes = append(entry.Changes, strings.Repeat("\x00", radosLateTextLimit))
		}
		timeline = append(timeline, entry)
	}
	return timeline
}

func TestRadosLateEpochTimelineStaysWithinTheRecordBudget(t *testing.T) {
	state := &radosLateClusterState{Label: "failure", Pool: "tc-contracts", Timeline: hostileTimeline()}
	boundRadosLateState(state)
	if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
		t.Fatalf("final record is %d bytes, budget %d (timeline %d epochs, truncated=%v)", size, radosLateStateLimit, len(state.Timeline), state.Truncated)
	}
	if !state.Truncated {
		t.Fatalf("shedding not recorded")
	}
	// Every omission is counted: epochs kept plus omitted equal the epochs
	// read, and changes kept plus changes omitted (including those of omitted
	// epochs) equal the changes read.
	if len(state.Timeline)+state.TimelineOmitted != radosLateEpochLimit {
		t.Fatalf("epochs: kept %d, omitted %d, read %d", len(state.Timeline), state.TimelineOmitted, radosLateEpochLimit)
	}
	kept := 0
	for _, entry := range state.Timeline {
		kept += len(entry.Changes)
	}
	if kept+state.TimelineChangesOmitted != radosLateEpochLimit*radosLateListLimit {
		t.Fatalf("changes: kept %d, omitted %d, read %d", kept, state.TimelineChangesOmitted, radosLateEpochLimit*radosLateListLimit)
	}
	// What stays is the earliest epochs, in order, still with their own time.
	for index, entry := range state.Timeline {
		if entry.Epoch != 16+index || entry.ModifiedUnix == 0 {
			t.Fatalf("retained epoch %d: %+v", index, entry)
		}
	}
}

// Each fallback stage is reached by some input, and the final record is
// within budget after every one of them, whichever of the large fields are
// combined with the largest accepted timeline.
func TestRadosLateRecordBudgetHoldsAfterEveryFallbackStage(t *testing.T) {
	heavy := strings.Repeat("\x00", radosLateTextLimit)
	build := func(timeline bool, probes, errLen int, notes, pgs, osds, ops bool) *radosLateClusterState {
		state := &radosLateClusterState{Label: "failure", Pool: "tc-contracts"}
		if timeline {
			state.Timeline = hostileTimeline()
		}
		for i := 0; i < probes; i++ {
			state.Probes = append(state.Probes, radosLateProbe{Name: heavy, Error: strings.Repeat("\x00", errLen)})
		}
		if notes {
			for i := 0; i < 3*radosLateNoteLimit; i++ {
				state.Notes = append(state.Notes, strings.Repeat("\x00", 3*radosLateErrorLimit))
			}
		}
		if pgs {
			for i := 0; i < 2*radosLateListLimit; i++ {
				state.PGStates = append(state.PGStates, radosLatePGState{State: heavy, Count: 1})
			}
		}
		if osds {
			for i := 0; i < 2*radosLateListLimit; i++ {
				up := i
				state.OSDs = append(state.OSDs, radosLateOSDInfo{ID: i, Up: true, In: true, UpFrom: &up, UpThru: &up})
			}
			state.MonFlags, state.LastUpChange, state.LastInChange, state.PoolLastChange = heavy, heavy, heavy, heavy
		}
		if ops {
			for i := 0; i < radosLateOpLimit; i++ {
				op := radosLateOp{Description: heavy, Client: heavy, FlagPoint: heavy, WaitsForMap: true}
				age := 9.0
				op.Age = &age
				for e := 0; e < radosLateEventLimit; e++ {
					op.Events = append(op.Events, radosLateEvent{Event: heavy, Time: heavy})
				}
				op.Waits = op.Events[:2]
				state.Historic = append(state.Historic, op)
				state.InFlight = append(state.InFlight, op)
			}
		}
		return state
	}
	seen := map[int]bool{}
	type probeLoad struct{ probes, errLen int }
	loads := []probeLoad{{0, 0}, {5, 40}, {5, 200}, {5, 512}, {12, 512}, {20, 512}, {32, 128}, {32, 512}, {32, 2048}}
	for index := 0; index < (1<<5)*len(loads); index++ {
		mask, load := index%(1<<5), loads[index/(1<<5)]
		state := build(mask&1 != 0, load.probes, load.errLen, mask&2 != 0, mask&4 != 0, mask&8 != 0, mask&16 != 0)
		epochs, changes := len(state.Timeline), 0
		for _, entry := range state.Timeline {
			changes += len(entry.Changes)
		}
		boundRadosLateState(state)
		seen[state.fallbackStages] = true
		if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
			t.Fatalf("mask %05b load %+v stages=%d: %d bytes", mask, load, state.fallbackStages, size)
		}
		kept := 0
		for _, entry := range state.Timeline {
			kept += len(entry.Changes)
		}
		if len(state.Timeline)+state.TimelineOmitted != epochs || kept+state.TimelineChangesOmitted != changes {
			t.Fatalf("mask %05b load %+v: timeline omissions do not add up: epochs %d+%d of %d, changes %d+%d of %d", mask, load, len(state.Timeline), state.TimelineOmitted, epochs, kept, state.TimelineChangesOmitted, changes)
		}
	}
	// The per-field caps and the operation shedding leave the later stages
	// idle for most inputs; the stages are exercised one by one below.
	for _, stage := range []int{0, 1, 2, radosLateFallbackStages - 1, radosLateFallbackStages} {
		if !seen[stage] {
			t.Errorf("no input reached fallback stage %d (reached %v)", stage, seen)
		}
	}
}

// Every fallback stage, applied in order to the largest record the caps allow,
// leaves the record no larger than before, counts what it drops, and the
// record fits once the last stage has run.
func TestRadosLateEveryFallbackStageKeepsTheRecordWithinBudget(t *testing.T) {
	heavy := strings.Repeat("\x00", radosLateTextLimit)
	state := &radosLateClusterState{Label: "failure", Pool: "tc-contracts", Timeline: hostileTimeline(), MonFlags: heavy, LastUpChange: heavy, LastInChange: heavy, PoolLastChange: heavy, MonModified: heavy}
	for i := 0; i < radosLateNoteLimit; i++ {
		state.Notes = append(state.Notes, strings.Repeat("\x00", radosLateErrorLimit))
	}
	for i := 0; i < radosLateListLimit; i++ {
		up := i
		state.PGStates = append(state.PGStates, radosLatePGState{State: heavy, Count: 1})
		state.OSDs = append(state.OSDs, radosLateOSDInfo{ID: i, Up: true, In: true, UpFrom: &up, UpThru: &up})
		state.Probes = append(state.Probes, radosLateProbe{Name: strings.Repeat("\x00", radosLateProbeNameLimit), Error: strings.Repeat("\x00", radosLateErrorLimit)})
	}
	capRadosLateFields(state)
	size := func() int { return encodedRadosLateSize(t, state) }
	steps := radosLateFallbackSteps(state, size)
	if len(steps) != radosLateFallbackStages {
		t.Fatalf("%d stages, constant says %d", len(steps), radosLateFallbackStages)
	}
	epochs, changes := len(state.Timeline), 0
	for _, entry := range state.Timeline {
		changes += len(entry.Changes)
	}
	previous := size()
	if previous <= radosLateStateLimit {
		t.Fatalf("fixture does not exceed the budget: %d", previous)
	}
	for index, step := range steps {
		step()
		now := size()
		if now > previous {
			t.Fatalf("stage %d grew the record: %d -> %d", index+1, previous, now)
		}
		kept := 0
		for _, entry := range state.Timeline {
			kept += len(entry.Changes)
		}
		if len(state.Timeline)+state.TimelineOmitted > epochs || kept+state.TimelineChangesOmitted > changes {
			t.Fatalf("stage %d: omission counters overcount", index+1)
		}
		previous = now
	}
	if previous > radosLateStateLimit {
		t.Fatalf("record is %d bytes after the last stage, budget %d", previous, radosLateStateLimit)
	}
	kept := 0
	for _, entry := range state.Timeline {
		kept += len(entry.Changes)
	}
	if len(state.Timeline)+state.TimelineOmitted != epochs || kept+state.TimelineChangesOmitted != changes {
		t.Fatalf("omissions do not add up: epochs %d+%d of %d, changes %d+%d of %d", len(state.Timeline), state.TimelineOmitted, epochs, kept, state.TimelineChangesOmitted, changes)
	}
	// Each timeline stage on its own: it counts what it drops, keeps the
	// epochs' own times and never invents a change.
	for name, apply := range map[string]func(*radosLateClusterState){
		"changes to one":    func(s *radosLateClusterState) { shedRadosLateTimelineChanges(s, 1) },
		"changes to none":   func(s *radosLateClusterState) { shedRadosLateTimelineChanges(s, 0) },
		"epochs to nothing": func(s *radosLateClusterState) { shedRadosLateTimelineEpochs(s, func() bool { return false }) },
		"epochs to fit": func(s *radosLateClusterState) {
			shedRadosLateTimelineEpochs(s, func() bool { return len(s.Timeline) <= 3 })
		},
	} {
		each := &radosLateClusterState{Timeline: hostileTimeline()}
		apply(each)
		keptChanges := 0
		for index, entry := range each.Timeline {
			keptChanges += len(entry.Changes)
			if entry.Epoch != 16+index || entry.ModifiedUnix == 0 {
				t.Fatalf("%s: epoch %d altered: %+v", name, index, entry)
			}
		}
		if len(each.Timeline)+each.TimelineOmitted != radosLateEpochLimit || keptChanges+each.TimelineChangesOmitted != radosLateEpochLimit*radosLateListLimit {
			t.Fatalf("%s: counters %d+%d epochs, %d+%d changes", name, len(each.Timeline), each.TimelineOmitted, keptChanges, each.TimelineChangesOmitted)
		}
	}
}

// The timeline has its own share of the record, so a large timeline cannot
// push the parked-operation evidence out of the budget.
func TestRadosLateTimelineShareProtectsTheOperationEvidence(t *testing.T) {
	state := &radosLateClusterState{Label: "failure", Pool: "tc-contracts", Timeline: hostileTimeline()}
	age := 9.0
	for i := 0; i < 6; i++ {
		op := radosLateOp{Description: fmt.Sprintf("osd_op(client.%d.0:1 2.0 x e16)", 4300+i), Client: fmt.Sprintf("client.%d", 4300+i), Age: &age, WaitsForMap: true,
			Waits: []radosLateEvent{{Event: "op must wait for map", Time: "t"}}}
		for e := 0; e < radosLateEventLimit; e++ {
			op.Events = append(op.Events, radosLateEvent{Event: fmt.Sprintf("event-%02d-%s", e, strings.Repeat("v", 60)), Time: strings.Repeat("t", 30)})
		}
		state.Historic = append(state.Historic, op)
	}
	boundRadosLateState(state)
	if encodedRadosLateTimelineSize(state) > radosLateTimelineShare {
		t.Fatalf("timeline takes %d bytes, share %d", encodedRadosLateTimelineSize(state), radosLateTimelineShare)
	}
	if len(state.Historic) != 6 || state.HistoricDropped != 0 {
		t.Fatalf("operations were shed to make room for the timeline: kept %d, dropped %d", len(state.Historic), state.HistoricDropped)
	}
	for _, op := range state.Historic {
		if len(op.Events) == 0 || !op.WaitsForMap || len(op.Waits) != 1 {
			t.Fatalf("relevant operation lost evidence: %+v", op)
		}
	}
	if len(state.Timeline) == 0 || state.TimelineOmitted+state.TimelineChangesOmitted == 0 {
		t.Fatalf("share applied without a count: %d epochs, omitted %d/%d", len(state.Timeline), state.TimelineOmitted, state.TimelineChangesOmitted)
	}
}

func TestRadosLateTimelineGapIsNotedNextToTheJoin(t *testing.T) {
	baseline := timelineBaseline(t)
	last := 15 + radosLateEpochLimit + 2
	reader := timelineReader(last, func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, time.Duration(20+e)*time.Second), 15, "sortbitwise")
	})
	state := collectFixture(reader, "failure", baseline)
	if state.TimelineSkipped != 2 || len(state.Notes) != 1 || !strings.Contains(state.Notes[0], "newest observation, not the exact epoch") {
		t.Fatalf("skipped %d notes %v", state.TimelineSkipped, state.Notes)
	}
	whole := collectFixture(timelineReader(17, func(e int) string {
		return radosLateDumpAt(e, radosLateStamp(radosLateBase, time.Duration(20+e)*time.Second), 15, "x")
	}), "failure", baseline)
	if len(whole.Notes) != 0 {
		t.Fatalf("a complete timeline needs no note: %v", whole.Notes)
	}
}

// ownerOSDDumps serves a baseline and one following epoch for osds OSDs whose
// up_from and up_thru both change, so the epoch has two changes per OSD.
func changeCapReader(osds int) (*fakeRadosLateReader, *radosLateClusterState) {
	dump := func(epoch, shift int, at time.Duration) string {
		var entries []string
		for id := 0; id < osds; id++ {
			entries = append(entries, fmt.Sprintf(`{"osd":%d,"up":1,"in":1,"up_from":%d,"up_thru":%d}`, id, 6+shift, 11+shift))
		}
		return fmt.Sprintf(`{"epoch":%d,"modified":%q,"flags":"sortbitwise","osds":[%s]}`, epoch, radosLateStamp(radosLateBase, at), strings.Join(entries, ","))
	}
	base := healthyRadosLateReader(15, 15)
	base.answers[radosLateKeyDump] = fixedRadosLateAnswer(dump(15, 0, 15*time.Second))
	baseline := collectFixture(base, "pre-late", nil)
	reader := healthyRadosLateReader(16, 16)
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(dump(16, 1, 41*time.Second))
	reader.answers[radosLateEpochDumpKey(16)] = fixedRadosLateAnswer(dump(16, 1, 41*time.Second))
	return reader, baseline
}

// Changes cut by the per-epoch list cap are counted, so that kept plus
// omitted always equals the changes read, well below the byte budget.
func TestRadosLateEpochChangesCutByTheListCapAreCounted(t *testing.T) {
	for _, tc := range []struct{ osds, read, kept, omitted int }{
		{16, 32, 32, 0}, {17, 34, 32, 2}, {20, 40, 32, 8},
	} {
		reader, baseline := changeCapReader(tc.osds)
		state := collectFixture(reader, "failure", baseline)
		if len(state.Timeline) != 1 {
			t.Fatalf("osds=%d: timeline %+v", tc.osds, state.Timeline)
		}
		kept := len(state.Timeline[0].Changes)
		if kept != tc.kept || state.TimelineChangesOmitted != tc.omitted || kept+state.TimelineChangesOmitted != tc.read {
			t.Fatalf("osds=%d: kept %d omitted %d, want %d and %d of %d read", tc.osds, kept, state.TimelineChangesOmitted, tc.kept, tc.omitted, tc.read)
		}
		if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
			t.Fatalf("osds=%d: %d bytes", tc.osds, size)
		}
		if (tc.omitted > 0) != state.Truncated {
			t.Fatalf("osds=%d: truncated=%v", tc.osds, state.Truncated)
		}
	}
}

// Every list cap counts what it cuts: read equals kept plus dropped for the
// probes, notes, PG states, OSDs, wait events and timeline.
func TestRadosLateEveryListCapCountsWhatItCuts(t *testing.T) {
	var osds, states []string
	for i := 0; i < radosLateListLimit+8; i++ {
		osds = append(osds, fmt.Sprintf(`{"osd":%d,"up":1,"in":1}`, i))
		states = append(states, fmt.Sprintf(`{"state_name":"s%d","count":1}`, i))
	}
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[` + strings.Join(osds, ",") + `]}`)
	reader.answers[radosLateKeyStatus] = fixedRadosLateAnswer(fmt.Sprintf(`{"osdmap":{},"pgmap":{"num_pgs":%d,"pgs_by_state":[%s]}}`, len(states), strings.Join(states, ",")))
	state := collectFixture(reader, "pre-late", nil)
	if len(state.OSDs)+state.OSDsDropped != radosLateListLimit+8 || state.OSDsDropped != 8 {
		t.Fatalf("osds: kept %d dropped %d", len(state.OSDs), state.OSDsDropped)
	}
	if len(state.PGStates)+state.PGStatesDropped != radosLateListLimit+8 || state.PGStatesDropped != 8 {
		t.Fatalf("pg states: kept %d dropped %d", len(state.PGStates), state.PGStatesDropped)
	}

	direct := &radosLateClusterState{}
	for i := 0; i < radosLateListLimit+5; i++ {
		direct.Probes = append(direct.Probes, radosLateProbe{Name: "p"})
	}
	for i := 0; i < radosLateNoteLimit+3; i++ {
		direct.Notes = append(direct.Notes, "n")
	}
	for index := 0; index < radosLateEpochLimit+2; index++ {
		entry := radosLateEpoch{Epoch: 16 + index, ModifiedUnix: 1}
		for c := 0; c < 3+index; c++ {
			entry.Changes = append(entry.Changes, "c")
		}
		direct.Timeline = append(direct.Timeline, entry)
	}
	read := 0
	for _, entry := range direct.Timeline {
		read += len(entry.Changes)
	}
	capRadosLateFields(direct)
	kept := 0
	for _, entry := range direct.Timeline {
		kept += len(entry.Changes)
	}
	if len(direct.Probes)+direct.ProbesDropped != radosLateListLimit+5 || len(direct.Notes)+direct.NotesDropped != radosLateNoteLimit+3 {
		t.Fatalf("probes %d+%d notes %d+%d", len(direct.Probes), direct.ProbesDropped, len(direct.Notes), direct.NotesDropped)
	}
	if len(direct.Timeline)+direct.TimelineOmitted != radosLateEpochLimit+2 || kept+direct.TimelineChangesOmitted != read {
		t.Fatalf("timeline: epochs %d+%d, changes %d+%d of %d", len(direct.Timeline), direct.TimelineOmitted, kept, direct.TimelineChangesOmitted, read)
	}

	// Wait events beyond the cap are counted on the operation.
	var events []string
	for i := 0; i < radosLateEventLimit+4; i++ {
		events = append(events, "op must wait for map")
	}
	parked := radosLateParkedOp("client.4222", "x", 16, radosLateStamp(radosLateBase, 42*time.Second), 10, events...)
	reader = healthyRadosLateReader(16, 16)
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` + parked + `]}`)
	op := collectFixture(reader, "failure", nil).InFlight[0]
	if len(op.Waits)+op.WaitsDropped != radosLateEventLimit+4 || op.WaitsDropped != 4 || !op.WaitsForMap {
		t.Fatalf("waits: kept %d dropped %d", len(op.Waits), op.WaitsDropped)
	}
}

// The whole-record fallback counts the lists it clears and names the scalar
// fields it empties.
func TestRadosLateFallbackCountsWhatItClears(t *testing.T) {
	up := 1
	state := &radosLateClusterState{
		Notes: []string{"a", "b"}, PGStates: []radosLatePGState{{State: "x", Count: 1}}, OSDs: []radosLateOSDInfo{{ID: 0}, {ID: 1, UpFrom: &up}, {ID: 2}},
		MonFlags: "f", LastInChange: "i", MonModified: "m",
	}
	steps := radosLateFallbackSteps(state, func() int { return 0 })
	for _, step := range steps[:4] {
		step()
	}
	if state.NotesDropped != 2 || state.PGStatesDropped != 1 || state.OSDsDropped != 3 || state.Notes != nil || state.PGStates != nil || state.OSDs != nil {
		t.Fatalf("counters: %+v", state)
	}
	if strings.Join(state.DroppedFields, ",") != "last_in_change,mon_flags,mon_modified" || state.MonFlags != "" || state.LastInChange != "" {
		t.Fatalf("dropped fields: %v", state.DroppedFields)
	}
}

// Age is how long ago a finished op was initiated, not how long it took, so
// many old but fast operations must not outrank a slow one or keep their
// events at its expense.
func TestRadosLateRelevanceUsesDurationNotAge(t *testing.T) {
	age, fast, slow := 52.0, 0.0003, 7.8
	if radosLateRelevant(radosLateOp{Age: &age, Duration: &fast}) {
		t.Fatalf("an old, fast operation is relevant")
	}
	if !radosLateRelevant(radosLateOp{Age: &fast, Duration: &slow}) || !radosLateRelevant(radosLateOp{Waits: []radosLateEvent{{Event: "op must wait for map"}}}) {
		t.Fatalf("a slow or parked operation is not relevant")
	}
	reader := healthyRadosLateReader(16, 16)
	var ops []string
	for i := 0; i < 20; i++ {
		filler := make([]string, 14)
		for j := range filler {
			filler[j] = fmt.Sprintf("short-%02d-%s", j, strings.Repeat("e", 110))
		}
		ops = append(ops, strings.Replace(radosLateParkedOp(fmt.Sprintf("client.%d", 4118), "x", 8, radosLateStamp(radosLateBase, 31*time.Second), 52.0, filler...), `"duration":52`, `"duration":0.0003`, 1))
	}
	inFlight := radosLateParkedOp("client.4222", "late-compound", 16, radosLateStamp(radosLateBase, 53*time.Second), 10.1, "initiated", "op must wait for map")
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(`{"ops":[` + strings.Join(ops, ",") + `]}`)
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` + inFlight + `]}`)
	state := collectFixture(reader, "failure", nil)
	if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
		t.Fatalf("%d bytes", size)
	}
	if len(state.InFlight) != 1 || len(state.InFlight[0].Events) != 2 || state.InFlight[0].EventsDropped != 0 {
		t.Fatalf("the parked in-flight op lost its timeline to old fast operations: %+v", state.InFlight)
	}
}
