//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// progressSample builds a "pg query" document by the layout of the pinned
// Tentacle source (PeeringState::dump_peering_state and
// PrimaryLogPG::dump_recovery_info). It is synthetic: no value in it was
// captured from a running cluster, so these controls test the decoder and the
// gate, not what a cluster reports.
type progressSample struct {
	state                         string
	epoch, interval, primarySince uint64
	up, acting                    []int
	cursor                        string
	targets                       []string
	inFlight                      []string
	ahead                         int
	omit                          []string
	logTail                       string
}

func newProgressSample() progressSample {
	return progressSample{
		state: "active+undersized+degraded+remapped+backfilling", epoch: 41, interval: 38, primarySince: 30,
		up: []int{1, 2, 3}, acting: []int{1, 2}, cursor: "MIN", targets: []string{"3"}, ahead: 12, logTail: "41'60",
	}
}

func (s progressSample) json(t *testing.T) []byte {
	t.Helper()
	objects := make([]map[string]string, s.ahead)
	for i := range objects {
		objects[i] = map[string]string{"object": "3:0a000000::::live-x:head", "version": "41'9"}
	}
	progress := map[string]any{
		"backfill_targets":      s.targets,
		"waiting_on_backfill":   []string{},
		"last_backfill_started": "3:0a000000::::live-x:head",
		"backfill_info":         map[string]any{"begin": "3:0a000000::::live-x:head", "end": "MAX", "objects": objects},
		// The native array alternates the peer's name with its interval.
		"peer_backfill_info":  []any{"3", map[string]any{"begin": "MIN", "end": "MIN", "objects": []any{}}},
		"backfills_in_flight": append([]string{}, s.inFlight...),
		"recovering":          []string{},
	}
	doc := map[string]any{
		"state": s.state, "epoch": s.epoch, "up": s.up, "acting": s.acting,
		"info": map[string]any{
			"last_update": "41'90", "last_complete": "41'90", "log_tail": s.logTail,
			"history": map[string]any{"same_interval_since": s.interval, "same_primary_since": s.primarySince, "same_up_since": s.interval},
			"stats":   map[string]any{"stat_sum": map[string]any{"num_objects": 90, "num_objects_recovered": 0, "num_objects_degraded": 90}},
		},
		"peer_info": []any{
			map[string]any{"peer": "2", "last_update": "41'90", "last_complete": "41'90", "last_backfill": "MAX"},
			map[string]any{"peer": "3", "last_update": "41'90", "last_complete": "0'0", "last_backfill": s.cursor},
		},
		"recovery_state": []any{map[string]any{"name": "Started/Primary/Active", "recovery_progress": progress}, map[string]any{"name": "Started"}},
	}
	info := doc["info"].(map[string]any)
	history := info["history"].(map[string]any)
	peers := doc["peer_info"].([]any)
	for _, field := range s.omit {
		switch field {
		case "epoch":
			delete(doc, "epoch")
		case "same_interval_since", "same_primary_since":
			delete(history, field)
		case "peer_cursor":
			delete(peers[1].(map[string]any), "last_backfill")
		case "recovery_progress":
			doc["recovery_state"] = []any{map[string]any{"name": "Started"}}
		case "backfills_in_flight", "backfill_info", "last_backfill_started":
			delete(progress, field)
		default:
			t.Fatalf("unknown field %q", field)
		}
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (s progressSample) parse(t *testing.T) pgProgress {
	t.Helper()
	progress, err := parsePGProgress(s.json(t))
	if err != nil {
		t.Fatal(err)
	}
	return progress
}

func TestBackfillHeldBaseline(t *testing.T) {
	base, err := backfillHeld(newProgressSample().parse(t), 3)
	if err != nil || base.Epoch != 41 || base.SameIntervalSince != 38 || base.SamePrimarySince != 30 || base.Cursor != "MIN" || base.Target != 3 ||
		!slices.Equal(base.Up, []int{1, 2, 3}) || !slices.Equal(base.Acting, []int{1, 2}) {
		t.Fatalf("held baseline %+v: %v", base, err)
	}
	mutate := func(edit func(*progressSample)) progressSample {
		s := newProgressSample()
		edit(&s)
		return s
	}
	for name, sample := range map[string]progressSample{
		"cursor already advanced":  mutate(func(s *progressSample) { s.cursor = "3:0a000000::::live-0001:head" }),
		"cursor already final":     mutate(func(s *progressSample) { s.cursor = "MAX" }),
		"target already acting":    mutate(func(s *progressSample) { s.acting = []int{1, 2, 3} }),
		"target not up":            mutate(func(s *progressSample) { s.up = []int{1, 2} }),
		"two backfill targets":     mutate(func(s *progressSample) { s.targets = []string{"3", "2"} }),
		"another backfill target":  mutate(func(s *progressSample) { s.targets = []string{"2"} }),
		"no backfill target":       mutate(func(s *progressSample) { s.targets = nil }),
		"not a backfill state":     mutate(func(s *progressSample) { s.state = "active+clean" }),
		"no epoch":                 mutate(func(s *progressSample) { s.omit = []string{"epoch"} }),
		"no interval":              mutate(func(s *progressSample) { s.omit = []string{"same_interval_since"} }),
		"no primary interval":      mutate(func(s *progressSample) { s.omit = []string{"same_primary_since"} }),
		"no recovery progress":     mutate(func(s *progressSample) { s.omit = []string{"recovery_progress"} }),
		"no cursor for the target": mutate(func(s *progressSample) { s.omit = []string{"peer_cursor"} }),
	} {
		if base, err := backfillHeld(sample.parse(t), 3); err == nil {
			t.Errorf("%s: accepted as a held baseline %+v", name, base)
		}
	}
	if _, err := parsePGProgress([]byte(`{`)); err == nil {
		t.Error("truncated JSON accepted")
	}
	if _, err := parsePGProgress([]byte(`{"epoch": 4}`)); err == nil {
		t.Error("a document without a state accepted")
	}
}

func TestBackfillProgressGate(t *testing.T) {
	baseline, err := backfillHeld(newProgressSample().parse(t), 3)
	if err != nil {
		t.Fatal(err)
	}
	advanced := func(edit func(*progressSample)) progressSample {
		s := newProgressSample()
		s.cursor, s.inFlight, s.epoch = "3:2c4b7d11::::live-0003:head", []string{"3:2c4b7d11::::live-0004:head"}, 43
		if edit != nil {
			edit(&s)
		}
		return s
	}
	for name, tc := range map[string]struct {
		sample progressSample
		want   progressVerdict
	}{
		// The map epoch moves on when the hold is cleared; the interval does not.
		"advanced cursor with work in flight": {advanced(nil), progressReached},
		"advanced cursor, scanned work only":  {advanced(func(s *progressSample) { s.inFlight = nil }), progressReached},
		"advanced cursor, in-flight only":     {advanced(func(s *progressSample) { s.ahead = 0 }), progressReached},
		"still held at MIN":                   {newProgressSample(), progressWait},
		"backend started, nothing completed":  {advanced(func(s *progressSample) { s.cursor = "MIN" }), progressWait},
		"waiting with an advanced cursor":     {advanced(func(s *progressSample) { s.state = strings.Replace(s.state, "backfilling", "backfill_wait", 1) }), progressWait},
		"no work ahead of the cursor":         {advanced(func(s *progressSample) { s.inFlight, s.ahead = nil, 0 }), progressWait},
		"completed transfer":                  {advanced(func(s *progressSample) { s.cursor, s.inFlight, s.ahead = "MAX", nil, 0 }), progressTerminal},
		"completed, work still listed":        {advanced(func(s *progressSample) { s.cursor = "MAX" }), progressTerminal},
		"another backfill target":             {advanced(func(s *progressSample) { s.targets = []string{"2"} }), progressWait},
		"second backfill target":              {advanced(func(s *progressSample) { s.targets = []string{"3", "2"} }), progressWait},
		"no backfill target":                  {advanced(func(s *progressSample) { s.targets = nil }), progressWait},
		"new interval":                        {advanced(func(s *progressSample) { s.interval = 44 }), progressInvalid},
		"new primary interval":                {advanced(func(s *progressSample) { s.primarySince = 44 }), progressInvalid},
		"acting set changed":                  {advanced(func(s *progressSample) { s.acting = []int{1, 3} }), progressInvalid},
		"up set changed":                      {advanced(func(s *progressSample) { s.up = []int{1, 2} }), progressInvalid},
		"map epoch before the baseline":       {advanced(func(s *progressSample) { s.epoch = 40 }), progressInvalid},
		"no epoch":                            {advanced(func(s *progressSample) { s.omit = []string{"epoch"} }), progressWait},
		"no interval":                         {advanced(func(s *progressSample) { s.omit = []string{"same_interval_since"} }), progressWait},
		"no cursor":                           {advanced(func(s *progressSample) { s.omit = []string{"peer_cursor"} }), progressWait},
		"no recovery progress":                {advanced(func(s *progressSample) { s.omit = []string{"recovery_progress"} }), progressWait},
		"no in-flight field":                  {advanced(func(s *progressSample) { s.omit = []string{"backfills_in_flight"} }), progressWait},
		"no scanned interval":                 {advanced(func(s *progressSample) { s.omit = []string{"backfill_info"} }), progressWait},
		"no started cursor":                   {advanced(func(s *progressSample) { s.omit = []string{"last_backfill_started"} }), progressWait},
	} {
		verdict, why := backfillProgress(baseline, tc.sample.parse(t))
		if verdict != tc.want {
			t.Errorf("%s: %s (%s), want %s", name, verdict, why, tc.want)
		}
	}

	// A cursor is a native identity. A later position that prints before the
	// baseline's is still an advance, and an equal one is not.
	late := baseline
	late.Cursor = "3:ffffffff::::live-0009:head"
	sample := advanced(func(s *progressSample) { s.cursor = "3:00000001::::live-0001:head" })
	if verdict, why := backfillProgress(late, sample.parse(t)); verdict != progressReached {
		t.Errorf("a cursor that prints earlier was %s (%s)", verdict, why)
	}
	sample = advanced(func(s *progressSample) { s.cursor = late.Cursor })
	if verdict, why := backfillProgress(late, sample.parse(t)); verdict != progressWait {
		t.Errorf("an unchanged cursor was %s (%s)", verdict, why)
	}
}

// The held-path classifier keeps refusing an advanced cursor: the progress gate
// is a separate decoder, not a relaxed classifier.
func TestHeldClassifierStaysHeldOnly(t *testing.T) {
	held := newProgressSample()
	held.acting, held.up = []int{1}, []int{1, 3}
	held.targets = []string{"3"}
	query, err := parsePGQuery(held.json(t))
	if err != nil {
		t.Fatal(err)
	}
	// The held sample is a backfill of osd.3, which stopped at 41'20 behind the
	// log tail 41'60.
	if path, err := pgRecoveryPath(query, 3, eversion{41, 20}); err != nil || path != "backfill" {
		t.Fatalf("the held sample classified as %q: %v", path, err)
	}
	held.cursor = "3:2c4b7d11::::live-0003:head"
	query, err = parsePGQuery(held.json(t))
	if err != nil {
		t.Fatal(err)
	}
	if path, err := pgRecoveryPath(query, 3, eversion{41, 20}); err == nil {
		t.Fatalf("an advanced cursor classified as %q", path)
	}
}
