package expect

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const (
	healthFSID  = "44444444-4444-4444-8444-444444444444"
	healthOther = "55555555-5555-4555-8555-555555555555"
)

func healthDown() ceph.HealthSnapshot {
	return ceph.HealthSnapshot{FSID: healthFSID, Status: "HEALTH_WARN",
		Checks: map[string]ceph.HealthCheck{
			"OSD_DOWN":     {Severity: "HEALTH_WARN", Summary: "1 osds down", Count: 1, Details: []string{"osd.1 is down"}},
			"OSDMAP_FLAGS": {Severity: "HEALTH_WARN", Summary: "noout flag(s) set", Count: 1, Muted: true},
		},
		Mutes: []ceph.HealthMute{{Code: "OSDMAP_FLAGS", Summary: "noout flag(s) set", Count: 1, Sticky: true}},
	}
}

func TestHealthExpectationMatchesTheObservation(t *testing.T) {
	got, err := Health{FSID: healthFSID, Status: "HEALTH_WARN",
		Require: []HealthCheck{{Code: "OSD_DOWN", Severity: "HEALTH_WARN", Muted: HealthUnmuted, Count: 1}, {Code: "OSDMAP_FLAGS", Muted: HealthMuted}},
		Forbid:  []string{"SLOW_OPS"}, Mutes: []string{"OSDMAP_FLAGS"}, NoMutes: []string{"OSD_DOWN"}, Exact: true,
	}.Evaluate(healthDown())
	if err != nil || !got.Matched || len(got.Mismatches) != 0 || len(got.Observed.Checks) != 2 {
		t.Fatalf("observation does not match: %+v %v", got, err)
	}
	want := "health want status=HEALTH_WARN require=[OSD_DOWN OSDMAP_FLAGS] forbid=[SLOW_OPS] mutes=[OSDMAP_FLAGS] nomutes=[OSD_DOWN] exact in HEALTH_WARN with 2 checks, 1 mutes: match"
	if s := got.String(); s != want {
		t.Fatalf("summary: %q", s)
	}
}

func TestHealthExpectationReportsEachReasonFromTheEvaluatedObservation(t *testing.T) {
	observed := healthDown()
	observed.FSID = healthOther
	observed.Checks["SLOW_OPS"] = ceph.HealthCheck{Severity: "HEALTH_WARN", Count: 3}
	observed.Checks["POOL_NO_REDUNDANCY"] = ceph.HealthCheck{Severity: "HEALTH_WARN", Count: 1}
	observed.Mutes = append(observed.Mutes, ceph.HealthMute{Code: "OSD_DOWN", ExpiresAt: "2026-11-04T00:00:00.000000+0000"}, ceph.HealthMute{Code: "MON_DISK_LOW"})
	got, err := Health{FSID: healthFSID, Status: "HEALTH_OK",
		Require: []HealthCheck{
			{Code: "OSD_DOWN", Severity: "HEALTH_ERR", Muted: HealthMuted, Count: 2},
			{Code: "MGR_DOWN"},
		},
		Forbid: []string{"SLOW_OPS"}, Mutes: []string{"PG_DEGRADED"}, NoMutes: []string{"MON_DISK_LOW"}, Exact: true,
	}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []HealthMismatch{
		{Reason: HealthMismatchFSID, Want: healthFSID, Observed: healthOther},
		{Reason: HealthMismatchStatus, Want: "HEALTH_OK", Observed: "HEALTH_WARN"},
		{Reason: HealthMismatchAbsent, Code: "MGR_DOWN"},
		{Reason: HealthMismatchMutePresent, Code: "MON_DISK_LOW", Observed: "until cleared"},
		{Reason: HealthMismatchUnexpected, Code: "OSDMAP_FLAGS", Observed: "HEALTH_WARN count=1 muted"},
		{Reason: HealthMismatchMuteUnexpected, Code: "OSDMAP_FLAGS", Observed: "sticky"},
		{Reason: HealthMismatchSeverity, Code: "OSD_DOWN", Want: "HEALTH_ERR", Observed: "HEALTH_WARN"},
		{Reason: HealthMismatchMuted, Code: "OSD_DOWN", Want: "muted", Observed: "unmuted"},
		{Reason: HealthMismatchCount, Code: "OSD_DOWN", Want: "2", Observed: "1"},
		{Reason: HealthMismatchMuteUnexpected, Code: "OSD_DOWN", Observed: "expires 2026-11-04T00:00:00.000000+0000"},
		{Reason: HealthMismatchMuteAbsent, Code: "PG_DEGRADED"},
		{Reason: HealthMismatchUnexpected, Code: "POOL_NO_REDUNDANCY", Observed: "HEALTH_WARN count=1 unmuted"},
		{Reason: HealthMismatchPresent, Code: "SLOW_OPS", Observed: "HEALTH_WARN count=3 unmuted"},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"fsid: want " + healthFSID + ", observed " + healthOther, "status: want HEALTH_OK, observed HEALTH_WARN",
		"MGR_DOWN absent", "MON_DISK_LOW mute present: until cleared", "OSD_DOWN severity: want HEALTH_ERR, observed HEALTH_WARN",
		"OSDMAP_FLAGS unexpected: HEALTH_WARN count=1 muted", "OSDMAP_FLAGS mute unexpected: sticky",
		"OSD_DOWN muted: want muted, observed unmuted", "; 5 more"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	for _, m := range want[8:] {
		if s := m.String(); s == "" || strings.Contains(text, s) {
			t.Fatalf("summary %q is not bounded before %q", text, s)
		}
	}
	if s := want[9].String(); s != "OSD_DOWN mute unexpected: expires 2026-11-04T00:00:00.000000+0000" {
		t.Fatal(s)
	}
	if s := want[12].String(); s != "SLOW_OPS present: HEALTH_WARN count=3 unmuted" {
		t.Fatal(s)
	}
}

func TestHealthExpectationComparesOnlyStatedFields(t *testing.T) {
	observed := healthDown()
	// Messages, severity, mute flag and count are not compared unless stated.
	for _, e := range []Health{
		{Require: []HealthCheck{{Code: "OSD_DOWN"}}},
		{Require: []HealthCheck{{Code: "OSDMAP_FLAGS"}}, Forbid: []string{"SLOW_OPS"}},
		{NoMutes: []string{"OSD_DOWN"}},
		{Status: "HEALTH_WARN"},
	} {
		if got, err := e.Evaluate(observed); err != nil || !got.Matched {
			t.Fatalf("%+v: %+v %v", e, got, err)
		}
	}
	// A sticky mute outlives its check: a forbidden check with a wanted mute.
	cleared := ceph.HealthSnapshot{FSID: healthFSID, Status: "HEALTH_OK", Mutes: []ceph.HealthMute{{Code: "OSDMAP_FLAGS", Sticky: true}}}
	got, err := Health{Status: "HEALTH_OK", Forbid: []string{"OSDMAP_FLAGS"}, Mutes: []string{"OSDMAP_FLAGS"}, Exact: true}.Evaluate(cleared)
	if err != nil || !got.Matched {
		t.Fatalf("sticky mute after its check: %+v %v", got, err)
	}
	// Exact alone requires no check and no mute.
	if got, err := (Health{Exact: true}).Evaluate(cleared); err != nil || got.Matched || got.Mismatches[0].Reason != HealthMismatchMuteUnexpected {
		t.Fatalf("exact empty: %+v %v", got, err)
	}
}

func TestHealthExpectationPinsTheCluster(t *testing.T) {
	observed := healthDown()
	observed.FSID = strings.ToUpper(healthFSID)
	if got, err := (Health{FSID: healthFSID, Forbid: []string{"SLOW_OPS"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("same FSID in another case: %+v %v", got, err)
	}
	observed.FSID = healthOther
	got, err := Health{FSID: healthFSID, Forbid: []string{"SLOW_OPS"}}.Evaluate(observed)
	if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != HealthMismatchFSID {
		t.Fatalf("another cluster: %+v %v", got, err)
	}
}

func TestHealthExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := healthDown()
	for name, e := range map[string]Health{
		"nothing":            {},
		"fsid":               {FSID: "cluster"},
		"nil fsid":           {FSID: "00000000-0000-0000-0000-000000000000"},
		"status":             {Status: "HEALTH_BAD"},
		"empty require":      {Require: []HealthCheck{{Severity: "HEALTH_WARN"}}},
		"duplicate require":  {Require: []HealthCheck{{Code: "OSD_DOWN"}, {Code: "OSD_DOWN", Muted: HealthMuted}}},
		"severity":           {Require: []HealthCheck{{Code: "OSD_DOWN", Severity: "WARN"}}},
		"mute state":         {Require: []HealthCheck{{Code: "OSD_DOWN", Muted: "yes"}}},
		"empty forbid":       {Forbid: []string{""}},
		"duplicate forbid":   {Forbid: []string{"SLOW_OPS", "SLOW_OPS"}},
		"require and forbid": {Require: []HealthCheck{{Code: "SLOW_OPS"}}, Forbid: []string{"SLOW_OPS"}},
		"duplicate mute":     {Mutes: []string{"OSD_DOWN", "OSD_DOWN"}},
		"mute and no mute":   {Mutes: []string{"OSD_DOWN"}, NoMutes: []string{"OSD_DOWN"}},
		"empty no mute":      {NoMutes: []string{""}},
		"muted without mute": {Require: []HealthCheck{{Code: "OSD_DOWN", Muted: HealthMuted}}, NoMutes: []string{"OSD_DOWN"}},
		"unmuted with mute":  {Require: []HealthCheck{{Code: "OSD_DOWN", Muted: HealthUnmuted}}, Mutes: []string{"OSD_DOWN"}},
		"unmuted over status": {Status: "HEALTH_OK",
			Require: []HealthCheck{{Code: "OSD_DOWN", Severity: "HEALTH_WARN", Muted: HealthUnmuted}}},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Checks != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestHealthExpectationRefusesMalformedObservations(t *testing.T) {
	e := Health{FSID: healthFSID, Forbid: []string{"SLOW_OPS"}}
	for name, mutate := range map[string]func(*ceph.HealthSnapshot){
		"status":         func(s *ceph.HealthSnapshot) { s.Status = "" },
		"empty code":     func(s *ceph.HealthSnapshot) { s.Checks[""] = ceph.HealthCheck{Severity: "HEALTH_WARN"} },
		"severity":       func(s *ceph.HealthSnapshot) { s.Checks["SLOW_OPS"] = ceph.HealthCheck{Severity: "warn"} },
		"empty mute":     func(s *ceph.HealthSnapshot) { s.Mutes = append(s.Mutes, ceph.HealthMute{}) },
		"duplicate mute": func(s *ceph.HealthSnapshot) { s.Mutes = append(s.Mutes, s.Mutes[0]) },
		"fsid":           func(s *ceph.HealthSnapshot) { s.FSID = "" },
	} {
		observed := healthDown()
		mutate(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
	// An unpinned FSID is not read.
	observed := healthDown()
	observed.FSID = ""
	if got, err := (Health{Forbid: []string{"SLOW_OPS"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("unpinned FSID: %+v %v", got, err)
	}
}

func TestHealthEvaluationKeepsNoAliasOfCallerData(t *testing.T) {
	observed := healthDown()
	e := Health{Require: []HealthCheck{{Code: "OSD_DOWN"}}, Forbid: []string{"SLOW_OPS"}, Mutes: []string{"OSDMAP_FLAGS"}, NoMutes: []string{"OSD_DOWN"}}
	got, err := e.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	e.Require[0].Code, e.Forbid[0], e.Mutes[0], e.NoMutes[0] = "X", "X", "X", "X"
	check := observed.Checks["OSD_DOWN"]
	check.Details[0] = "changed"
	observed.Checks["OSD_DOWN"] = ceph.HealthCheck{}
	observed.Checks["NEW"] = ceph.HealthCheck{}
	observed.Mutes[0].Code = "X"
	x, o := got.Expectation, got.Observed
	if x.Require[0].Code != "OSD_DOWN" || x.Forbid[0] != "SLOW_OPS" || x.Mutes[0] != "OSDMAP_FLAGS" || x.NoMutes[0] != "OSD_DOWN" {
		t.Fatalf("expectation follows caller mutation: %+v", x)
	}
	if len(o.Checks) != 2 || o.Checks["OSD_DOWN"].Count != 1 || o.Checks["OSD_DOWN"].Details[0] != "osd.1 is down" || o.Mutes[0].Code != "OSDMAP_FLAGS" {
		t.Fatalf("observation follows caller mutation: %+v", o)
	}
}

func TestHealthEvaluationBoundsItsSummaryInCodeOrder(t *testing.T) {
	var forbid []string
	observed := ceph.HealthSnapshot{Status: "HEALTH_WARN", Checks: map[string]ceph.HealthCheck{}}
	for i := range 20 {
		code := fmt.Sprintf("CHECK_%02d", i)
		forbid = append(forbid, code)
		observed.Checks[code] = ceph.HealthCheck{Severity: "HEALTH_WARN", Count: 1}
	}
	slices.Reverse(forbid)
	got, err := Health{Forbid: forbid}.Evaluate(observed)
	if err != nil || len(got.Mismatches) != 20 || got.Mismatches[0].Code != "CHECK_00" || got.Mismatches[19].Code != "CHECK_19" {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "CHECK_07 present: HEALTH_WARN count=1 unmuted; 12 more") || strings.Contains(text, "CHECK_08 present") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
