package expect

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const pgNone = 2147483647

func pgPool() ceph.PoolState {
	return ceph.PoolState{ID: 3, Name: "tc-pgs", Type: "replicated", Size: 3, MinSize: 2, PGNum: 4}
}

func pgClean() ceph.PoolPGSnapshot {
	pool := pgPool()
	return ceph.PoolPGSnapshot{FSID: healthFSID, PoolBefore: pool, PoolAfter: pool, OSDMapEpochBefore: 20, OSDMapEpochAfter: 20, PGReady: true,
		PGs: []ceph.PGState{
			{PGID: "3.0", State: "active+clean", Up: []int{0, 1, 2}, Acting: []int{0, 1, 2}, UpPrimary: 0, ActingPrimary: 0},
			{PGID: "3.1", State: "active+clean", Up: []int{1, 2, 0}, Acting: []int{1, 2, 0}, UpPrimary: 1, ActingPrimary: 1},
		}}
}

func TestPGExpectationMatchesTheObservation(t *testing.T) {
	observed := pgClean()
	got, err := PGs{FSID: healthFSID, Pool: observed.PoolAfter, Count: 2, State: "active+clean", Require: []string{"active"}, Forbid: []string{"degraded"},
		ActingSize: 3, InUp: []int{0}, NotInUp: []int{3}, InActing: []int{2}, NotInActing: []int{pgNone}, ActingPrimary: []int{0, 1}}.Evaluate(observed)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 || len(got.Observed.PGs) != 2 {
		t.Fatalf("observation does not match: %+v %v", got, err)
	}
	want := "pgs of pool tc-pgs/3 want count=2 state=active+clean require=[active] forbid=[degraded] acting-size=3 in-up=[0] not-in-up=[3] in-acting=[2] not-in-acting=[2147483647] primary=[0 1] in 2 observed: match"
	if s := got.String(); s != want {
		t.Fatalf("summary: %q", s)
	}
	selected, err := PGs{Select: []ceph.PGID{"3.1"}, Up: []int{1, 2, 0}, Acting: []int{1, 2, 0}, ActingPrimary: []int{1}}.Evaluate(observed)
	if err != nil || !selected.Matched || selected.String() != "pgs [3.1] want up=[1 2 0] acting=[1 2 0] primary=[1] in 2 observed: match" {
		t.Fatalf("selection: %v %v", selected, err)
	}
}

func TestPGExpectationReportsEachReasonFromTheEvaluatedObservation(t *testing.T) {
	observed := pgClean()
	observed.FSID = healthOther
	observed.PoolBefore.ID, observed.PoolAfter.ID = 4, 4
	observed.PGs = []ceph.PGState{
		{PGID: "4.a", State: "active+undersized+degraded", Up: []int{1, pgNone}, Acting: []int{1, pgNone}, UpPrimary: 1, ActingPrimary: 1},
		{PGID: "4.2", State: "active+clean", Up: []int{0, 1, 2}, Acting: []int{0, 1, 2}, ActingPrimary: 0},
	}
	got, err := PGs{FSID: healthFSID, Pool: pgPool(), Count: 3, State: "active+clean", Require: []string{"clean"}, Forbid: []string{"degraded", "undersized"},
		Up: []int{0, 1, 2}, Acting: []int{0, 1, 2}, ActingSize: 3, InUp: []int{0}, NotInUp: []int{pgNone}, InActing: []int{2}, NotInActing: []int{pgNone},
		ActingPrimary: []int{0}}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []PGMismatch{
		{Reason: PGMismatchFSID, Want: healthFSID, Observed: healthOther},
		{Reason: PGMismatchPool, Want: "tc-pgs/3", Observed: "tc-pgs/4"},
		{Reason: PGMismatchCount, Want: "3", Observed: "2"},
		{Reason: PGMismatchState, PGID: "4.a", Want: "active+clean", Observed: "active+undersized+degraded"},
		{Reason: PGMismatchRequired, PGID: "4.a", Want: "clean", Observed: "active+undersized+degraded"},
		{Reason: PGMismatchForbidden, PGID: "4.a", Want: "degraded", Observed: "active+undersized+degraded"},
		{Reason: PGMismatchForbidden, PGID: "4.a", Want: "undersized", Observed: "active+undersized+degraded"},
		{Reason: PGMismatchUp, PGID: "4.a", Want: "[0 1 2]", Observed: "[1 2147483647]"},
		{Reason: PGMismatchActing, PGID: "4.a", Want: "[0 1 2]", Observed: "[1 2147483647]"},
		{Reason: PGMismatchActingSize, PGID: "4.a", Want: "3", Observed: "[1 2147483647]"},
		{Reason: PGMismatchUpMissing, PGID: "4.a", Want: "0", Observed: "[1 2147483647]"},
		{Reason: PGMismatchUpPresent, PGID: "4.a", Want: "2147483647", Observed: "[1 2147483647]"},
		{Reason: PGMismatchActingMissing, PGID: "4.a", Want: "2", Observed: "[1 2147483647]"},
		{Reason: PGMismatchActingPresent, PGID: "4.a", Want: "2147483647", Observed: "[1 2147483647]"},
		{Reason: PGMismatchPrimary, PGID: "4.a", Want: "[0]", Observed: "1"},
	}
	// 4.2 is clean and sorts before 4.a by PG number, not by text.
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	for i, s := range []string{
		"fsid: want " + healthFSID + ", observed " + healthOther, "pool: want tc-pgs/3, observed tc-pgs/4", "count: want 3, observed 2",
		"pg 4.a state: want active+clean, observed active+undersized+degraded", "pg 4.a required clean: observed active+undersized+degraded",
		"pg 4.a forbidden degraded: observed active+undersized+degraded", "pg 4.a forbidden undersized: observed active+undersized+degraded",
		"pg 4.a up: want [0 1 2], observed [1 2147483647]", "pg 4.a acting: want [0 1 2], observed [1 2147483647]",
		"pg 4.a acting size: want 3, observed [1 2147483647]", "pg 4.a up missing osd.0: observed [1 2147483647]",
		"pg 4.a up present osd.2147483647: observed [1 2147483647]", "pg 4.a acting missing osd.2: observed [1 2147483647]",
		"pg 4.a acting present osd.2147483647: observed [1 2147483647]", "pg 4.a primary: want one of [0], observed 1",
	} {
		if got := want[i].String(); got != s {
			t.Fatalf("mismatch %d: %q, want %q", i, got, s)
		}
	}
	if text := got.String(); !strings.HasSuffix(text, "pg 4.a up: want [0 1 2], observed [1 2147483647]; 7 more") {
		t.Fatalf("summary: %q", text)
	}
}

func TestPGExpectationPinsPoolAndCluster(t *testing.T) {
	observed := pgClean()
	// A replacement pool reuses the name under a new native ID.
	replaced := pgClean()
	replaced.PoolBefore.ID, replaced.PoolAfter.ID = 9, 9
	for i := range replaced.PGs {
		replaced.PGs[i].PGID = ceph.PGID(strings.Replace(string(replaced.PGs[i].PGID), "3.", "9.", 1))
	}
	byName := PGs{Pool: ceph.PoolState{Name: "tc-pgs"}, Count: 2, State: "active+clean"}
	pinned := PGs{Pool: observed.PoolAfter, Count: 2, State: "active+clean"}
	for name, c := range map[string]struct {
		e       PGs
		s       ceph.PoolPGSnapshot
		matched bool
	}{
		"name, original":   {byName, observed, true},
		"name, replaced":   {byName, replaced, true},
		"pinned, original": {pinned, observed, true},
		"pinned, replaced": {pinned, replaced, false},
		"other name":       {PGs{Pool: ceph.PoolState{Name: "other"}, Count: 2}, observed, false},
		"fsid":             {PGs{FSID: healthOther, Count: 2}, observed, false},
	} {
		got, err := c.e.Evaluate(c.s)
		if err != nil || got.Matched != c.matched {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
		if !c.matched && got.Mismatches[0].Reason != PGMismatchPool && got.Mismatches[0].Reason != PGMismatchFSID {
			t.Fatalf("%s: %+v", name, got.Mismatches)
		}
	}
}

func TestPGExpectationMatchesWholeTokensAndExactState(t *testing.T) {
	observed := pgClean()
	observed.PGs[0].State = "active+clean+scrubbing+deep"
	observed.PGs[1].State = "active+remapped+backfill_wait"
	if got, err := (PGs{Count: 2, Forbid: []string{"backfill", "deep-scrub"}, Require: []string{"active"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("a token matched part of another: %+v %v", got, err)
	}
	got, err := PGs{Select: []ceph.PGID{"3.0"}, State: "active+clean"}.Evaluate(observed)
	if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != PGMismatchState {
		t.Fatalf("exact state: %+v %v", got, err)
	}
	if got, err := (PGs{Select: []ceph.PGID{"3.0"}, Require: []string{"active", "clean"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("tokens of a scrubbing PG: %+v %v", got, err)
	}
}

func TestPGExpectationSelectionAndCount(t *testing.T) {
	observed := pgClean()
	got, err := PGs{Select: []ceph.PGID{"3.5", "3.0"}, State: "active+clean"}.Evaluate(observed)
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, []PGMismatch{{Reason: PGMismatchAbsent, PGID: "3.5"}}) || got.Mismatches[0].String() != "pg 3.5 absent" {
		t.Fatalf("absent selection: %+v %v", got, err)
	}
	// An empty report is a count mismatch, never a vacuous match.
	empty := pgClean()
	empty.PGs = []ceph.PGState{}
	got, err = PGs{Count: 2, State: "active+clean"}.Evaluate(empty)
	if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != PGMismatchCount {
		t.Fatalf("empty report: %+v %v", got, err)
	}
	// Up and Acting compare in order; an empty non-nil vector requires empty.
	got, err = PGs{Select: []ceph.PGID{"3.1"}, Acting: []int{0, 1, 2}, Up: []int{}}.Evaluate(observed)
	if err != nil || got.Matched || len(got.Mismatches) != 2 || got.Mismatches[0].Reason != PGMismatchUp || got.Mismatches[1].Reason != PGMismatchActing {
		t.Fatalf("ordered vectors: %+v %v", got, err)
	}
	// -1 states that a PG has no acting primary.
	observed.PGs[0].ActingPrimary, observed.PGs[0].Acting = -1, []int{}
	if got, err := (PGs{Select: []ceph.PGID{"3.0"}, ActingPrimary: []int{-1}, Acting: []int{}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("no primary: %+v %v", got, err)
	}
}

func TestPGExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := pgClean()
	for name, e := range map[string]PGs{
		"selects nothing":        {State: "active+clean"},
		"negative count":         {Count: -1},
		"more selected":          {Count: 1, Select: []ceph.PGID{"3.0", "3.1"}},
		"negative size":          {Count: 1, ActingSize: -1},
		"negative pool":          {Count: 1, Pool: ceph.PoolState{ID: -1}},
		"fsid":                   {Count: 1, FSID: "cluster"},
		"pgid":                   {Select: []ceph.PGID{"3.x"}},
		"duplicate pgid":         {Select: []ceph.PGID{"3.0", "3.0"}},
		"pgid of another pool":   {Select: []ceph.PGID{"4.0"}, Pool: ceph.PoolState{ID: 3}},
		"state":                  {Count: 1, State: "active++clean"},
		"empty token":            {Count: 1, Require: []string{""}},
		"compound token":         {Count: 1, Forbid: []string{"active+clean"}},
		"duplicate token":        {Count: 1, Require: []string{"active", "active"}},
		"require and forbid":     {Count: 1, Require: []string{"clean"}, Forbid: []string{"clean"}},
		"require outside state":  {Count: 1, State: "active+clean", Require: []string{"degraded"}},
		"forbid of state":        {Count: 1, State: "active+clean", Forbid: []string{"clean"}},
		"negative up":            {Count: 1, Up: []int{-1}},
		"acting size":            {Count: 1, Acting: []int{0, 1}, ActingSize: 3},
		"negative in acting":     {Count: 1, InActing: []int{-2}},
		"duplicate not in up":    {Count: 1, NotInUp: []int{1, 1}},
		"in and not in acting":   {Count: 1, InActing: []int{1}, NotInActing: []int{1}},
		"in up outside up":       {Count: 1, Up: []int{0, 1}, InUp: []int{2}},
		"not in acting of exact": {Count: 1, Acting: []int{0, 1}, NotInActing: []int{1}},
		"primary below -1":       {Count: 1, ActingPrimary: []int{-2}},
		"duplicate primary":      {Count: 1, ActingPrimary: []int{0, 0}},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.PGs != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestPGExpectationRefusesMalformedObservations(t *testing.T) {
	e := PGs{FSID: healthFSID, Count: 2}
	for name, mutate := range map[string]func(*ceph.PoolPGSnapshot){
		"no pool":        func(s *ceph.PoolPGSnapshot) { s.PoolBefore.Name, s.PoolAfter.Name = "", "" },
		"pool reads":     func(s *ceph.PoolPGSnapshot) { s.PoolAfter.ID = 5 },
		"pool names":     func(s *ceph.PoolPGSnapshot) { s.PoolAfter.Name = "renamed" },
		"pgid":           func(s *ceph.PoolPGSnapshot) { s.PGs[0].PGID = "3.X" },
		"foreign pg":     func(s *ceph.PoolPGSnapshot) { s.PGs[0].PGID = "33.0" },
		"duplicate pg":   func(s *ceph.PoolPGSnapshot) { s.PGs[1].PGID = s.PGs[0].PGID },
		"no state":       func(s *ceph.PoolPGSnapshot) { s.PGs[0].State = "" },
		"negative osd":   func(s *ceph.PoolPGSnapshot) { s.PGs[0].Acting = []int{-1} },
		"invalid primay": func(s *ceph.PoolPGSnapshot) { s.PGs[0].UpPrimary = -2 },
		"fsid":           func(s *ceph.PoolPGSnapshot) { s.FSID = "" },
	} {
		observed := pgClean()
		mutate(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestPGEvaluationKeepsNoAliasOfCallerSlices(t *testing.T) {
	observed := pgClean()
	e := PGs{Select: []ceph.PGID{"3.0"}, Require: []string{"active"}, Forbid: []string{"degraded"}, Up: []int{0, 1, 2}, Acting: []int{0, 1, 2},
		InUp: []int{0}, NotInUp: []int{3}, InActing: []int{1}, NotInActing: []int{3}, ActingPrimary: []int{0}}
	got, err := e.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	e.Select[0], e.Require[0], e.Forbid[0] = "3.9", "x", "x"
	for _, ids := range [][]int{e.Up, e.Acting, e.InUp, e.NotInUp, e.InActing, e.NotInActing, e.ActingPrimary} {
		ids[0] = 9
	}
	observed.PGs[0].State, observed.PGs[0].Up[0], observed.PGs[0].Acting[0] = "down", 9, 9
	x, o := got.Expectation, got.Observed
	if x.Select[0] != "3.0" || x.Require[0] != "active" || x.Forbid[0] != "degraded" {
		t.Fatalf("expectation follows caller mutation: %+v", x)
	}
	for _, ids := range [][]int{x.Up, x.Acting, x.InUp, x.NotInUp, x.InActing, x.NotInActing, x.ActingPrimary} {
		if ids[0] == 9 {
			t.Fatalf("expectation follows caller mutation: %+v", x)
		}
	}
	if o.PGs[0].State != "active+clean" || o.PGs[0].Up[0] != 0 || o.PGs[0].Acting[0] != 0 {
		t.Fatalf("observation follows caller mutation: %+v", o)
	}
}

func TestPGEvaluationBoundsItsSummaryInPGOrder(t *testing.T) {
	observed := pgClean()
	observed.PGs = nil
	for seed := 19; seed >= 0; seed-- {
		observed.PGs = append(observed.PGs, ceph.PGState{PGID: ceph.PGID(fmt.Sprintf("3.%x", seed)), State: "peering", Up: []int{}, Acting: []int{}, ActingPrimary: -1})
	}
	got, err := PGs{Count: 20, Require: []string{"active"}}.Evaluate(observed)
	if err != nil || len(got.Mismatches) != 20 || got.Mismatches[0].PGID != "3.0" || got.Mismatches[10].PGID != "3.a" || got.Mismatches[19].PGID != "3.13" {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "pg 3.7 required active: observed peering; 12 more") || strings.Contains(text, "pg 3.8") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
