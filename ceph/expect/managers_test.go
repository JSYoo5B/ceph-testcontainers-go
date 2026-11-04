package expect

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// managerStatus builds an observation the way the native JSON decodes;
// standbys alternate name and GID.
func managerStatus(t *testing.T, available bool, active string, gid uint64, standbys ...any) ceph.ManagerStatus {
	t.Helper()
	type standby struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	}
	native := struct {
		Available  bool      `json:"available"`
		ActiveName string    `json:"active_name"`
		ActiveGID  uint64    `json:"active_gid"`
		Standbys   []standby `json:"standbys"`
	}{Available: available, ActiveName: active, ActiveGID: gid}
	for i := 0; i < len(standbys); i += 2 {
		native.Standbys = append(native.Standbys, standby{Name: standbys[i].(string), GID: uint64(standbys[i+1].(int))})
	}
	data, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	var status ceph.ManagerStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestManagerExpectationMatchesPinnedRegistrations(t *testing.T) {
	observed := managerStatus(t, true, "a", 4100, "b", 4200)
	got, err := Managers{Available: true, Active: "a", ActiveGID: 4100, Standbys: []string{"b"}, StandbyCount: 1}.Evaluate(observed)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 {
		t.Fatalf("observation does not match itself: %+v %v", got, err)
	}
	if s := got.String(); s != "mgrs want available=true active mgr.a gid 4100 standbys [b] standbys=1 in available=true active mgr.a gid 4100 standbys [b]: match" {
		t.Fatalf("summary: %q", s)
	}
}

func TestManagerExpectationReportsEachReasonFromTheEvaluatedObservation(t *testing.T) {
	observed := managerStatus(t, false, "b", 4100, "c", 4300, "d", 4400)
	got, err := Managers{Available: true, Active: "a", ActiveGID: 4200, FormerGID: 4100, Standbys: []string{"b", "c"}, StandbyCount: 2}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []ManagerMismatch{
		{Reason: ManagerMismatchAvailable, ObservedAvailable: false},
		{Reason: ManagerMismatchActive, Name: "a", ObservedName: "b"},
		{Reason: ManagerMismatchActiveGID, GID: 4200, ObservedGID: 4100},
		{Reason: ManagerMismatchFormerGID, GID: 4100, ObservedGID: 4100},
		{Reason: ManagerMismatchStandbyAbsent, Name: "b"},
		{Reason: ManagerMismatchStandbyUnexpected, Name: "d", ObservedGID: 4400},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"in available=false active mgr.b gid 4100 standbys [c d]: available: want true, observed false",
		"active: want mgr.a, observed mgr.b", "active gid: want 4200, observed 4100", "active gid: want other than 4100, observed 4100",
		"mgr.b not a standby", "mgr.d unexpected standby: gid 4400"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	got, err = Managers{Available: true, StandbyCount: 2}.Evaluate(managerStatus(t, true, "a", 4100, "b", 4200))
	if err != nil || !slices.Equal(got.Mismatches, []ManagerMismatch{{Reason: ManagerMismatchStandbyCount, Count: 2, ObservedCount: 1}}) ||
		!strings.HasSuffix(got.String(), "standbys: want 2, observed 1") {
		t.Fatalf("standby count: %+v %v", got, err)
	}
}

func TestManagerExpectationPinsTheActiveRegistration(t *testing.T) {
	// The same name re-registered after a restart carries a new GID.
	restarted := managerStatus(t, true, "a", 4500, "b", 4200)
	got, err := Managers{Available: true, Active: "a", ActiveGID: 4100}.Evaluate(restarted)
	if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != ManagerMismatchActiveGID {
		t.Fatalf("restarted active: %+v %v", got, err)
	}
	// A failover is a new active GID, also under the same name.
	if got, err := (Managers{Available: true, FormerGID: 4100}).Evaluate(restarted); err != nil || !got.Matched {
		t.Fatalf("failover: %+v %v", got, err)
	}
	got, err = Managers{Available: true, Active: "b", FormerGID: 4100}.Evaluate(managerStatus(t, true, "b", 4200))
	if err != nil || !got.Matched {
		t.Fatalf("standby promoted: %+v %v", got, err)
	}
	// No active MGR is not a failover.
	got, err = Managers{FormerGID: 4100}.Evaluate(managerStatus(t, false, "", 0, "b", 4200))
	if err != nil || !slices.Equal(got.Mismatches, []ManagerMismatch{{Reason: ManagerMismatchFormerGID, GID: 4100}}) ||
		!strings.HasSuffix(got.String(), "active gid: want other than 4100, observed 0") {
		t.Fatalf("no active: %+v %v", got, err)
	}
}

func TestManagerExpectationChecksOnlyTheStatedFields(t *testing.T) {
	// The zero value expects no available MGR and checks nothing else.
	if got, err := (Managers{}).Evaluate(managerStatus(t, false, "a", 4100)); err != nil || !got.Matched {
		t.Fatalf("unavailable: %+v %v", got, err)
	}
	if got, err := (Managers{Available: true}).Evaluate(managerStatus(t, true, "a", 4100, "b", 4200)); err != nil || !got.Matched {
		t.Fatalf("available alone: %+v %v", got, err)
	}
	// An empty non-nil Standbys requires no standby; nil checks none.
	got, err := Managers{Standbys: []string{}}.Evaluate(managerStatus(t, false, "", 0, "b", 4200))
	if err != nil || !slices.Equal(got.Mismatches, []ManagerMismatch{{Reason: ManagerMismatchStandbyUnexpected, Name: "b", ObservedGID: 4200}}) {
		t.Fatalf("no standby: %+v %v", got, err)
	}
	if !strings.Contains(got.String(), "mgrs want available=false standbys [] in") {
		t.Fatalf("summary: %q", got.String())
	}
	if got, err := (Managers{Standbys: []string{}}).Evaluate(managerStatus(t, false, "", 0)); err != nil || !got.Matched {
		t.Fatalf("empty MGRMap: %+v %v", got, err)
	}
}

func TestManagerExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := managerStatus(t, true, "a", 4100, "b", 4200)
	for name, e := range map[string]Managers{
		"negative count":   {Available: true, StandbyCount: -1},
		"empty standby":    {Available: true, Standbys: []string{""}},
		"repeated standby": {Available: true, Standbys: []string{"b", "b"}},
		"count mismatch":   {Available: true, Standbys: []string{"b"}, StandbyCount: 2},
		"former is pinned": {Available: true, ActiveGID: 4100, FormerGID: 4100},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Standbys != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestManagerExpectationRefusesMalformedObservations(t *testing.T) {
	e := Managers{Available: true}
	for name, observed := range map[string]ceph.ManagerStatus{
		"name without gid":     managerStatus(t, true, "a", 0),
		"gid without name":     managerStatus(t, false, "", 4100),
		"available, no active": managerStatus(t, true, "", 0),
		"standby without name": managerStatus(t, true, "a", 4100, "", 4200),
		"standby without gid":  managerStatus(t, true, "a", 4100, "b", 0),
		"standby active gid":   managerStatus(t, true, "a", 4100, "b", 4100),
		"repeated standby gid": managerStatus(t, true, "a", 4100, "b", 4200, "c", 4200),
	} {
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
	// A restarted daemon may briefly register under a name the map already
	// lists; only its GID is new.
	got, err := e.Evaluate(managerStatus(t, true, "a", 4100, "a", 4300))
	if err != nil || !got.Matched {
		t.Fatalf("re-registered name: %+v %v", got, err)
	}
}

func TestManagerEvaluationKeepsNoAliasOfCallerSlices(t *testing.T) {
	standbys := []string{"b"}
	observed := managerStatus(t, true, "a", 4100, "b", 4200)
	got, err := Managers{Available: true, Standbys: standbys}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	standbys[0], observed.Standbys[0].Name, observed.Standbys[0].GID = "x", "x", 9
	if got.Expectation.Standbys[0] != "b" || got.Observed.Standbys[0].Name != "b" || got.Observed.Standbys[0].GID != 4200 {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestManagerEvaluationBoundsItsSummary(t *testing.T) {
	var names []string
	for _, c := range "abcdefghijklmnopqrst" {
		names = append(names, string(c))
	}
	got, err := Managers{Standbys: names}.Evaluate(managerStatus(t, false, "", 0))
	if err != nil || len(got.Mismatches) != 20 {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "mgr.h not a standby; 12 more") || strings.Contains(text, "mgr.i not") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
