//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package mgr_pg_merge_primary_override_test

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseMergeMapReadsCountsAndOverrides(t *testing.T) {
	dump := `{"epoch":43,"pools":[
		{"pool":2,"pool_name":"target","pg_num":2,"pg_placement_num":1,"pg_num_target":1,"pg_placement_num_target":1,"pg_num_pending":2},
		{"pool":3,"pool_name":"control","pg_num":2,"pg_placement_num":2,"pg_num_target":2,"pg_placement_num_target":2,"pg_num_pending":2}],
		"pg_upmap_primaries":[{"pgid":"2.1","primary_osd":1},{"pgid":"3.1","primary_osd":0}]}`
	got, err := parseMergeMap([]byte(dump))
	if err != nil {
		t.Fatal(err)
	}
	want := mergeMap{Epoch: 43,
		Pools: map[string]poolCounts{
			"target":  {ID: 2, PGNum: 2, Target: 1, PGPNum: 1, Pending: 2},
			"control": {ID: 3, PGNum: 2, Target: 2, PGPNum: 2, Pending: 2}},
		// osd.0 is an OSD, not a missing value.
		Primaries: map[string]int{"2.1": 1, "3.1": 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("map = %+v, want %+v", got, want)
	}
	// A map without pools or overrides is an observation.
	if got, err := parseMergeMap([]byte(`{"epoch":1,"pools":[],"pg_upmap_primaries":[]}`)); err != nil || len(got.Pools) != 0 || len(got.Primaries) != 0 {
		t.Fatalf("empty map = %+v, %v", got, err)
	}
}

func TestParseMergeMapRefusesMissingFields(t *testing.T) {
	pool := `{"pool":2,"pool_name":"target","pg_num":2,"pg_placement_num":1,"pg_num_target":1,"pg_num_pending":2}`
	for name, dump := range map[string]string{
		"not JSON":                       `Error ENOENT`,
		"no epoch":                       `{"pools":[],"pg_upmap_primaries":[]}`,
		"no pool list":                   `{"epoch":1,"pg_upmap_primaries":[]}`,
		"null pool list":                 `{"epoch":1,"pools":null,"pg_upmap_primaries":[]}`,
		"no override list":               `{"epoch":1,"pools":[]}`,
		"null override list":             `{"epoch":1,"pools":[],"pg_upmap_primaries":null}`,
		"pool without a name":            `{"epoch":1,"pools":[` + strings.Replace(pool, `"pool_name":"target",`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"pool without an ID":             `{"epoch":1,"pools":[` + strings.Replace(pool, `"pool":2,`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"pool without pg_num":            `{"epoch":1,"pools":[` + strings.Replace(pool, `"pg_num":2,`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"pool with a null pg_num":        `{"epoch":1,"pools":[` + strings.Replace(pool, `"pg_num":2,`, `"pg_num":null,`, 1) + `],"pg_upmap_primaries":[]}`,
		"pool without pgp_num":           `{"epoch":1,"pools":[` + strings.Replace(pool, `"pg_placement_num":1,`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"pool without a target":          `{"epoch":1,"pools":[` + strings.Replace(pool, `"pg_num_target":1,`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"pool without a pending":         `{"epoch":1,"pools":[` + strings.Replace(pool, `,"pg_num_pending":2`, ``, 1) + `],"pg_upmap_primaries":[]}`,
		"two pools of one name":          `{"epoch":1,"pools":[` + pool + `,` + pool + `],"pg_upmap_primaries":[]}`,
		"override without a PG":          `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"primary_osd":1}]}`,
		"override without an OSD":        `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"pgid":"2.1"}]}`,
		"override with a null OSD":       `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"pgid":"2.1","primary_osd":null}]}`,
		"override of no OSD":             `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"pgid":"2.1","primary_osd":-1}]}`,
		"override of the placeholder":    `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"pgid":"2.1","primary_osd":2147483647}]}`,
		"two overrides of one PG":        `{"epoch":1,"pools":[],"pg_upmap_primaries":[{"pgid":"2.1","primary_osd":1},{"pgid":"2.1","primary_osd":2}]}`,
		"override list of another shape": `{"epoch":1,"pools":[],"pg_upmap_primaries":{"2.1":1}}`,
	} {
		if got, err := parseMergeMap([]byte(dump)); err == nil {
			t.Errorf("%s: read as %+v", name, got)
		}
	}
}

// These cases feed synthetic samples to the judgement. They are not native
// evidence of what a manager does with a primary override.
func TestJudgeMerge(t *testing.T) {
	plan := mergePlan{Source: "2.1", Parent: "2.0", ParentOSD: 1, Control: "3.0", ControlOSD: 0}
	clean := func(acting ...int) pgReport {
		return pgReport{State: "active+clean", Primary: acting[0], Acting: acting}
	}
	both := map[string]int{"2.0": 1, "3.0": 0}
	blocked := mergeSample{Ready: true, Counts: poolCounts{ID: 2, PGNum: 2, Target: 1, PGPNum: 1, Pending: 2}, Primaries: both,
		PGs: map[string]pgReport{"2.0": clean(1, 2, 0), "2.1": clean(0, 2, 1)}}
	merged := mergeSample{Ready: true, Counts: poolCounts{ID: 2, PGNum: 1, Target: 1, PGPNum: 1, Pending: 1}, Primaries: map[string]int{"3.0": 0},
		PGs: map[string]pgReport{"2.0": clean(0, 2, 1)}}
	// change returns a copy of sample after edit.
	change := func(sample mergeSample, edit func(*mergeSample)) mergeSample {
		copied := mergeSample{At: sample.At, Epoch: sample.Epoch, Ready: sample.Ready, Counts: sample.Counts, Primaries: map[string]int{}, PGs: map[string]pgReport{}}
		for pg, osd := range sample.Primaries {
			copied.Primaries[pg] = osd
		}
		for pg, report := range sample.PGs {
			copied.PGs[pg] = report
		}
		edit(&copied)
		return copied
	}
	// A pool that reached the count with the parent's override still in
	// the OSDMap, and one whose merge is under way.
	stale := change(merged, func(s *mergeSample) { s.Primaries["2.0"] = 1 })
	merging := change(blocked, func(s *mergeSample) { s.Counts.Pending = 1; delete(s.Primaries, "2.0"); s.PGs["2.0"] = clean(0, 2, 1) })
	// every returns samples taken one interval apart, the first at 0.
	every := func(interval time.Duration, samples ...mergeSample) []mergeSample {
		spaced := make([]mergeSample, len(samples))
		for i, sample := range samples {
			spaced[i] = sample
			spaced[i].At = time.Duration(i) * interval
		}
		return spaced
	}
	half := mergeHold / 2
	moving := func(edit func(*mergeSample)) []mergeSample { return every(mergeHold, blocked, change(blocked, edit)) }
	for _, tc := range []struct {
		name     string
		got      mergeOutcome
		symptoms int
		want     string
	}{
		{name: "blocked for the hold and merged after the removal", got: mergeOutcome{Held: every(half, blocked, blocked, blocked), AfterRemoval: &merged}, symptoms: 3},
		{name: "blocked for the hold after the PGs were placed anew", got: mergeOutcome{Held: every(half, merging, blocked, blocked, blocked), AfterRemoval: &merged}, symptoms: 3},
		{name: "merged at once", got: mergeOutcome{Held: []mergeSample{merged}}},
		{name: "merged after a merge under way", got: mergeOutcome{Held: every(5*time.Second, merging, merged)}},
		{name: "merged after it looked blocked", got: mergeOutcome{Held: every(half, blocked, blocked, blocked, merged)}},

		{name: "no sample", got: mergeOutcome{}, want: "outcome not observed"},
		{name: "blocked for less than the hold", got: mergeOutcome{Held: every(mergeHold-time.Second, blocked, blocked), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "blocked for less than the hold at the end", got: mergeOutcome{Held: every(half, blocked, blocked, merging, blocked, blocked), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "blocked in one sample only", got: mergeOutcome{Held: []mergeSample{blocked}, AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "blocked, and the override was never removed", got: mergeOutcome{Held: every(mergeHold, blocked, blocked)}, want: "outcome not observed"},
		{name: "blocked, and still not merged without the override", got: mergeOutcome{Held: every(mergeHold, blocked, blocked), AfterRemoval: &merging}, want: "another fault"},
		{name: "a merge under way at the end", got: mergeOutcome{Held: every(mergeHold, blocked, blocked, merging), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "merged only after the test removed the override", got: mergeOutcome{Held: []mergeSample{merged}, AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the count is reached with the override in place", got: mergeOutcome{Held: every(mergeHold, stale, stale), AfterRemoval: &merged}, want: "outcome not observed"},
		// Not blocked by the override at the end: the PGs are not ready for
		// a merge, or the override does not separate their primaries.
		{name: "the source is not clean", got: mergeOutcome{Held: moving(func(s *mergeSample) {
			s.PGs["2.1"] = pgReport{State: "active+recovering", Primary: 0, Acting: []int{0, 2, 1}}
		}), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the source has no primary", got: mergeOutcome{Held: moving(func(s *mergeSample) {
			s.PGs["2.1"] = pgReport{State: "active+clean", Primary: -1, Acting: []int{0, 2, 1}}
		}), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the source has no acting set", got: mergeOutcome{Held: moving(func(s *mergeSample) { s.PGs["2.1"] = pgReport{State: "active+clean", Primary: 0} }), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the source's primary is not in its acting set", got: mergeOutcome{Held: moving(func(s *mergeSample) {
			s.PGs["2.1"] = pgReport{State: "active+clean", Primary: 3, Acting: []int{0, 2, 1}}
		}), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "an acting set with a missing OSD", got: mergeOutcome{Held: moving(func(s *mergeSample) {
			s.PGs["2.1"], s.PGs["2.0"] = pgReport{State: "active+clean", Primary: 0, Acting: []int{0, 2, noOSD}}, pgReport{State: "active+clean", Primary: 1, Acting: []int{1, 2, noOSD}}
		}), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the parent's reported primary is not the override's OSD", got: mergeOutcome{Held: moving(func(s *mergeSample) {
			s.PGs["2.0"] = pgReport{State: "active+clean", Primary: 2, Acting: []int{1, 2, 0}}
		}), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the manager does not have the reports while blocked", got: mergeOutcome{Held: moving(func(s *mergeSample) { s.Ready = false }), AfterRemoval: &merged}, want: "outcome not observed"},
		{name: "the manager does not have the reports of a merged pool", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) { s.Ready = false })}}, want: "outcome not observed"},
		{name: "the merged PG has no primary", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) {
			s.PGs["2.0"] = pgReport{State: "active+clean", Primary: -1, Acting: []int{0, 2, 1}}
		})}}, want: "outcome not observed"},
		{name: "the merged PG has no acting set", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) { s.PGs["2.0"] = pgReport{State: "active+clean", Primary: 0} })}}, want: "outcome not observed"},
		// The control pool's override, and overrides the case did not make.
		{name: "the control override is gone while blocked", got: mergeOutcome{Held: moving(func(s *mergeSample) { delete(s.Primaries, "3.0") }), AfterRemoval: &merged}, want: "control not established"},
		{name: "the control override is gone before the pool is blocked", got: mergeOutcome{Held: every(half, change(merging, func(s *mergeSample) { delete(s.Primaries, "3.0") }), blocked, blocked, blocked), AfterRemoval: &merged}, want: "control not established"},
		{name: "the control override is gone after the merge", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) { delete(s.Primaries, "3.0") })}}, want: "control not established"},
		{name: "the control override names another OSD", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) { s.Primaries["3.0"] = 2 })}}, want: "another fault"},
		{name: "the control override is gone after the removal", got: mergeOutcome{Held: every(mergeHold, blocked, blocked), AfterRemoval: ptr(change(merged, func(s *mergeSample) { delete(s.Primaries, "3.0") }))}, want: "control not established"},
		{name: "an override of the source, which the case did not make", got: mergeOutcome{Held: []mergeSample{change(blocked, func(s *mergeSample) { s.Primaries["2.1"] = 2 })}}, want: "another fault"},
		{name: "an override the case did not make", got: mergeOutcome{Held: []mergeSample{change(merged, func(s *mergeSample) { s.Primaries["4.0"] = 2 })}}, want: "another fault"},
		{name: "the parent's override names another OSD", got: mergeOutcome{Held: moving(func(s *mergeSample) { s.Primaries["2.0"] = 2 }), AfterRemoval: &merged}, want: "another fault"},
	} {
		symptoms, err := judgeMerge(plan, tc.got)
		switch {
		case tc.want == "" && (err != nil || len(symptoms) != tc.symptoms):
			t.Errorf("%s: symptoms %q, error %v, want %d symptoms", tc.name, symptoms, err, tc.symptoms)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || symptoms != nil):
			t.Errorf("%s: symptoms %q, error %v, want an error with %q", tc.name, symptoms, err, tc.want)
		}
	}
}

func ptr[T any](value T) *T { return &value }

func TestHoldTellsMergedFromBlocked(t *testing.T) {
	plan := mergePlan{Source: "2.1", Parent: "2.0", ParentOSD: 1, Control: "3.0", ControlOSD: 0}
	clean := func(acting ...int) pgReport {
		return pgReport{State: "active+clean", Primary: acting[0], Acting: acting}
	}
	blocked := mergeSample{Ready: true, Counts: poolCounts{ID: 2, PGNum: 2, Target: 1, PGPNum: 1, Pending: 2}, Primaries: map[string]int{"2.0": 1, "3.0": 0},
		PGs: map[string]pgReport{"2.0": clean(1, 2, 0), "2.1": clean(0, 2, 1)}}
	merged := mergeSample{Ready: true, Counts: poolCounts{ID: 2, PGNum: 1, Target: 1, PGPNum: 1, Pending: 1}, Primaries: map[string]int{"3.0": 0},
		PGs: map[string]pgReport{"2.0": clean(0, 2, 1)}}
	// A pool whose PG count was never reduced: the trigger was not sent.
	untouched := mergeSample{Ready: true, Counts: poolCounts{ID: 2, PGNum: 2, Target: 2, PGPNum: 2, Pending: 2}, Primaries: map[string]int{"2.0": 1, "3.0": 0},
		PGs: map[string]pgReport{"2.0": clean(1, 2, 0), "2.1": clean(0, 1, 2)}}
	at := func(sample mergeSample, at time.Duration) mergeSample {
		sample.At = at
		return sample
	}
	for _, tc := range []struct {
		name    string
		held    []mergeSample
		want    string
		run     int
		refused bool
	}{
		{name: "merged", held: []mergeSample{blocked, at(merged, time.Second)}, want: mergeMerged},
		{name: "blocked", held: []mergeSample{blocked, at(blocked, mergeHold)}, want: mergeBlocked, run: 2},
		{name: "blocked after the PG count was untouched", held: []mergeSample{untouched, at(blocked, time.Second), at(blocked, time.Minute), at(blocked, mergeHold+time.Second)}, want: mergeBlocked, run: 3},
		{name: "no sample", refused: true},
		{name: "blocked too short", held: []mergeSample{blocked, at(blocked, mergeHold-time.Second)}, refused: true},
		{name: "blocked too short after another state", held: []mergeSample{untouched, at(blocked, time.Second), at(blocked, mergeHold)}, refused: true},
		{name: "blocked once", held: []mergeSample{blocked}, refused: true},
		{name: "merged, then two PGs again", held: []mergeSample{merged, at(blocked, mergeHold)}, refused: true},
		{name: "the PG count was never reduced", held: []mergeSample{untouched, at(untouched, mergeLimit)}, refused: true},
	} {
		got, run, err := plan.hold(tc.held)
		if tc.refused != (err != nil) || got != tc.want || len(run) != tc.run {
			t.Errorf("%s: hold = %q with %d samples, %v, want %q with %d, refused %t", tc.name, got, len(run), err, tc.want, tc.run, tc.refused)
		}
	}
}

func TestPGReportActiveClean(t *testing.T) {
	for state, want := range map[string]bool{
		"active+clean":                true,
		"active+clean+scrubbing+deep": true,
		"active":                      false,
		"clean":                       false,
		"active+recovering":           false,
		"peering":                     false,
		"inactive+unclean":            false,
		"":                            false,
	} {
		if got := (pgReport{State: state}).activeClean(); got != want {
			t.Errorf("%q: active and clean = %t, want %t", state, got, want)
		}
	}
}

func TestMergePrimariesNeedsBothPGsServed(t *testing.T) {
	served := map[string]pgReport{
		"2.0": {State: "active+clean", Primary: 0, Acting: []int{0, 2, 1}},
		"2.1": {State: "active+clean", Primary: 1, Acting: []int{1, 0, 2}},
	}
	if source, parent, err := mergePrimaries(served, "2.1", "2.0"); err != nil || source != 1 || parent != 0 {
		t.Fatalf("primaries = %d, %d, %v, want 1, 0", source, parent, err)
	}
	// The primary is the reported one, not the first member of the set.
	reordered := map[string]pgReport{"2.0": {State: "active+clean", Primary: 2, Acting: []int{0, 2, 1}}, "2.1": served["2.1"]}
	if _, parent, err := mergePrimaries(reordered, "2.1", "2.0"); err != nil || parent != 2 {
		t.Fatalf("parent primary = %d, %v, want 2", parent, err)
	}
	for name, parent := range map[string]*pgReport{
		"a PG that is not reported":     nil,
		"a PG without a state":          {Primary: 0, Acting: []int{0, 2, 1}},
		"a PG that is peering":          {State: "peering", Primary: 0, Acting: []int{0, 2, 1}},
		"a PG without an acting set":    {State: "active+clean", Primary: 0},
		"a PG with an empty acting set": {State: "active+clean", Primary: 0, Acting: []int{}},
		"a PG without a primary":        {State: "active+clean", Primary: -1, Acting: []int{0, 2, 1}},
		"a PG with a missing primary":   {State: "active+clean", Primary: noOSD, Acting: []int{noOSD, 2, 1}},
		"a PG with a missing member":    {State: "active+clean", Primary: 0, Acting: []int{0, noOSD, 1}},
		"a PG with a negative member":   {State: "active+clean", Primary: 0, Acting: []int{0, -1, 1}},
		"a primary outside the set":     {State: "active+clean", Primary: 3, Acting: []int{0, 2, 1}},
		"a zero report (primary osd.0)": {},
	} {
		pgs := map[string]pgReport{"2.1": served["2.1"]}
		if parent != nil {
			pgs["2.0"] = *parent
		}
		for _, order := range [][2]string{{"2.1", "2.0"}, {"2.0", "2.1"}} {
			if a, b, err := mergePrimaries(pgs, order[0], order[1]); err == nil {
				t.Errorf("%s: primaries read as %d and %d", name, a, b)
			}
		}
	}
}
