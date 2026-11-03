package ceph

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

const (
	expectUUID0 = "11111111-1111-4111-8111-111111111111"
	expectUUID1 = "22222222-2222-4222-8222-222222222222"
	expectUUID2 = "33333333-3333-4333-8333-333333333333"
)

func expectationBaseline() []OSDState {
	return []OSDState{
		{ID: 0, UUID: expectUUID0, Up: true, In: true, Weight: 1},
		{ID: 1, UUID: expectUUID1, Up: true, In: true, Weight: 1},
	}
}

func TestOSDExpectationMatchesPinnedBaseline(t *testing.T) {
	baseline := expectationBaseline()
	got, err := OSDExpectation{Select: baseline, Up: true, In: true, Exact: true}.Evaluate(baseline)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 || len(got.Observed) != 2 {
		t.Fatalf("baseline does not match itself: %+v %v", got, err)
	}
	if s := got.String(); s != "osds [0 1] want up=true in=true exact in 2 observed: match" {
		t.Fatalf("summary: %q", s)
	}
}

func TestOSDExpectationReportsEachFieldFromTheEvaluatedObservation(t *testing.T) {
	observed := []OSDState{
		{ID: 0, UUID: expectUUID2, Up: true, In: true},   // re-registered under the same ID
		{ID: 1, UUID: expectUUID1, Up: false, In: false}, // down and out
		{ID: 4, UUID: expectUUID0, Up: true, In: true},   // not selected
	}
	selection := append(expectationBaseline(), OSDState{ID: 3})
	got, err := OSDExpectation{Select: selection, Up: true, In: true, Exact: true}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []OSDMismatch{
		{Reason: OSDMismatchReplaced, ID: 0, UUID: expectUUID0, Observed: observed[0]},
		{Reason: OSDMismatchUp, ID: 1, UUID: expectUUID1, Observed: observed[1]},
		{Reason: OSDMismatchIn, ID: 1, UUID: expectUUID1, Observed: observed[1]},
		{Reason: OSDMismatchAbsent, ID: 3},
		{Reason: OSDMismatchUnselected, ID: 4, Observed: observed[2]},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"osds [0 1 3] want up=true in=true exact in 3 observed", "osd.0 replaced: want uuid " + expectUUID0 + ", observed " + expectUUID2,
		"osd.1 up: want true, observed false", "osd.1 in: want true, observed false", "osd.3 absent", "osd.4 unselected: up=true in=true"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
}

func TestOSDExpectationSelectsByIDAndIgnoresUnselectedUnlessExact(t *testing.T) {
	observed := []OSDState{{ID: 0, UUID: expectUUID0, Up: false, In: true}, {ID: 1, UUID: expectUUID1, Up: true, In: true}}
	got, err := OSDExpectation{Select: []OSDState{{ID: 0}}, Up: false, In: true}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatalf("down/in selection by ID: %+v %v", got, err)
	}
	// Only ID and UUID select; the flags of a Select entry are not expectations.
	got, err = OSDExpectation{Select: []OSDState{{ID: 1, Up: false}}, Up: true, In: true}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatalf("select entry flags were read: %+v %v", got, err)
	}
}

func TestOSDExpectationEmptyObservationIsAMismatchNotAMatch(t *testing.T) {
	got, err := OSDExpectation{Select: expectationBaseline(), Up: true, In: true, Exact: true}.Evaluate(nil)
	if err != nil || got.Matched || len(got.Mismatches) != 2 || got.Mismatches[0].Reason != OSDMismatchAbsent {
		t.Fatalf("empty observation: %+v %v", got, err)
	}
}

func TestOSDExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := expectationBaseline()
	for name, e := range map[string]OSDExpectation{
		"empty":     {Up: true, In: true},
		"negative":  {Select: []OSDState{{ID: -1}}},
		"duplicate": {Select: []OSDState{{ID: 0}, {ID: 0, UUID: expectUUID0}}},
		"bad uuid":  {Select: []OSDState{{ID: 0, UUID: "osd-zero"}}},
		"nil uuid":  {Select: []OSDState{{ID: 0, UUID: "00000000-0000-0000-0000-000000000000"}}},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ErrInvalidRequest) || got.Matched || got.Observed != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestOSDExpectationRefusesMalformedObservations(t *testing.T) {
	e := OSDExpectation{Select: []OSDState{{ID: 0}}, Up: true, In: true}
	for name, observed := range map[string][]OSDState{
		"negative":  {{ID: -1, UUID: expectUUID0, Up: true, In: true}},
		"duplicate": {{ID: 0, UUID: expectUUID0, Up: true, In: true}, {ID: 0, UUID: expectUUID0, Up: true, In: true}},
		"no uuid":   {{ID: 0, Up: true, In: true}},
	} {
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestOSDEvaluationKeepsNoAliasOfCallerSlices(t *testing.T) {
	selection, observed := expectationBaseline(), expectationBaseline()
	got, err := OSDExpectation{Select: selection, Up: true, In: true}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	selection[0].ID, observed[0].Up = 9, false
	if got.Expectation.Select[0].ID != 0 || !got.Observed[0].Up {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestOSDEvaluationBoundsItsSummary(t *testing.T) {
	var selection []OSDState
	for id := range 20 {
		selection = append(selection, OSDState{ID: id})
	}
	got, err := OSDExpectation{Select: selection, Up: true, In: true}.Evaluate(nil)
	if err != nil || len(got.Mismatches) != 20 {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "osd.7 absent; 12 more") || strings.Contains(text, "osd.8 absent") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
