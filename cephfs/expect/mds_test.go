package expect

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// mdsBaseline is one owned active rank, one owned standby and one owned
// journal follower of rank 0.
func mdsBaseline() cephfs.FilesystemStatus {
	return cephfs.FilesystemStatus{
		FilesystemName: "tc-cephfs", FilesystemID: 1, MaxMDS: 1,
		Active:        []cephfs.MDSStatus{{Name: "a", Rank: 0, GID: 4100, State: "up:active", Owned: true}},
		Standby:       []cephfs.MDSStatus{{Name: "b", Rank: -1, GID: 4200, State: "up:standby", Owned: true}},
		StandbyReplay: []cephfs.MDSStatus{{Name: "c", Rank: 0, GID: 4300, State: "up:standby-replay", Owned: true}},
	}
}

func mdsPinned() MDSs {
	return MDSs{
		FilesystemID: 1, MaxMDS: 1, Owned: true, Exact: true,
		Ranks:         []MDSRank{{Rank: 0, Name: "a", GID: 4100}},
		Standby:       &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{Name: "b", GID: 4200}}},
		StandbyReplay: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{GID: 4300}}},
	}
}

func TestMDSExpectationMatchesPinnedBaseline(t *testing.T) {
	got, err := mdsPinned().Evaluate(mdsBaseline())
	if err != nil || !got.Matched || len(got.Mismatches) != 0 || got.Observed.FilesystemName != "tc-cephfs" {
		t.Fatalf("baseline does not match itself: %+v %v", got, err)
	}
	want := `mds want fs=1 max_mds=1 ranks [0] owned exact standby=1 standby-replay=1 in "tc-cephfs" fs=1 max_mds=1 active=1 standby=1 standby-replay=1: match`
	if s := got.String(); s != want {
		t.Fatalf("summary:\n got %q\nwant %q", s, want)
	}
}

func TestMDSExpectationReportsEachFieldFromTheEvaluatedObservation(t *testing.T) {
	observed := cephfs.FilesystemStatus{
		FilesystemName: "tc-cephfs", FilesystemID: 2, MaxMDS: 3,
		Active: []cephfs.MDSStatus{
			{Name: "x", Rank: 0, GID: 4900, State: "up:active", Owned: false}, // an external daemon took rank 0
			{Name: "d", Rank: 2, GID: 4400, State: "up:active", Owned: true},  // not selected
		},
		Standby:       []cephfs.MDSStatus{{Name: "b", Rank: -1, GID: 4201, State: "up:standby", Owned: true}}, // restarted
		StandbyReplay: []cephfs.MDSStatus{{Name: "e", Rank: 0, GID: 4500, State: "up:standby-replay", Owned: true}},
	}
	e := mdsPinned()
	e.MaxMDS = 2
	e.Ranks = append(e.Ranks, MDSRank{Rank: 1, NotGID: 4100})
	e.Standby.Daemons = append(e.Standby.Daemons, MDSDaemon{Name: "f"})
	e.Standby.Count = 2
	got, err := e.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []MDSMismatch{
		{Reason: MDSMismatchFilesystem, WantValue: 1, ObservedValue: 2},
		{Reason: MDSMismatchMaxMDS, WantValue: 2, ObservedValue: 3},
		{Reason: MDSMismatchName, List: MDSListActive, Rank: 0, Want: MDSDaemon{Name: "a", GID: 4100}, Observed: observed.Active[0]},
		{Reason: MDSMismatchGID, List: MDSListActive, Rank: 0, Want: MDSDaemon{Name: "a", GID: 4100}, Observed: observed.Active[0]},
		{Reason: MDSMismatchUnowned, List: MDSListActive, Rank: 0, Want: MDSDaemon{Name: "a", GID: 4100}, Observed: observed.Active[0]},
		{Reason: MDSMismatchInactive, List: MDSListActive, Rank: 1, Want: MDSDaemon{NotGID: 4100}},
		{Reason: MDSMismatchUnselected, List: MDSListActive, Rank: 2, Observed: observed.Active[1]},
		{Reason: MDSMismatchCount, List: MDSListStandby, WantValue: 2, ObservedValue: 1},
		{Reason: MDSMismatchGID, List: MDSListStandby, Want: MDSDaemon{Name: "b", GID: 4200}, Observed: observed.Standby[0]},
		{Reason: MDSMismatchAbsent, List: MDSListStandby, Want: MDSDaemon{Name: "f"}},
		{Reason: MDSMismatchAbsent, List: MDSListStandbyReplay, Want: MDSDaemon{GID: 4300}},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{
		`mds want fs=1 max_mds=2 ranks [0 1] owned exact standby=2 standby-replay=1 in "tc-cephfs" fs=2 max_mds=3 active=2 standby=1 standby-replay=1: `,
		"filesystem id: want 1, observed 2", "max_mds: want 2, observed 3",
		"rank 0 name: want mds.a, observed mds.x", "rank 0 gid: want 4100, observed mds.x gid 4900", "rank 0 unowned: mds.x gid 4900 owned=false",
		"rank 1 inactive", "rank 2 unselected: mds.d gid 4400 owned=true", "standby count: want 2, observed 1",
		"; 3 more",
	} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	// The three after the bound.
	for i, text := range []string{"standby mds.b gid: want 4200, observed mds.b gid 4201", "standby mds.f absent", "standby-replay gid 4300 absent"} {
		if s := got.Mismatches[8+i].String(); s != text {
			t.Fatalf("mismatch %d text %q, want %q", 8+i, s, text)
		}
	}
}

func TestMDSExpectationTakeoverAndRestartAreNotGIDs(t *testing.T) {
	old := mdsBaseline().Active[0]
	takeover := MDSs{
		Ranks: []MDSRank{{Rank: 0, NotGID: old.GID}}, Owned: true, Exact: true,
		Standby: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{Name: old.Name, NotGID: old.GID}}},
	}
	// Before the kill: rank 0 still has the baseline GID and the killed
	// daemon is not a standby.
	got, err := takeover.Evaluate(mdsBaseline())
	if err != nil || got.Matched {
		t.Fatalf("baseline satisfies the takeover: %+v %v", got, err)
	}
	if got.Mismatches[0].Reason != MDSMismatchUnchanged || got.Mismatches[0].String() != "rank 0 unchanged: mds.a still has gid 4100" ||
		got.Mismatches[1].Reason != MDSMismatchAbsent || got.Mismatches[1].String() != "standby mds.a absent" {
		t.Fatalf("baseline mismatches: %+v", got.Mismatches)
	}
	// The daemon a came back under its old GID: replaced in place it is not.
	sameGID := cephfs.FilesystemStatus{FilesystemID: 1, MaxMDS: 1,
		Active:  []cephfs.MDSStatus{{Name: "b", Rank: 0, GID: 4200, State: "up:active", Owned: true}},
		Standby: []cephfs.MDSStatus{{Name: "a", Rank: -1, GID: 4100, State: "up:standby", Owned: true}}}
	got, err = takeover.Evaluate(sameGID)
	if err != nil || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != MDSMismatchUnchanged || got.Mismatches[0].List != MDSListStandby {
		t.Fatalf("restart under the old GID: %+v %v", got, err)
	}
	// After the takeover and the restart.
	sameGID.Standby[0].GID = 4101
	if got, err = takeover.Evaluate(sameGID); err != nil || !got.Matched {
		t.Fatalf("completed takeover: %+v %v", got, err)
	}
	// Pinning the baseline GIDs rejects the replacement of either daemon.
	pinned := MDSs{Ranks: []MDSRank{{Rank: 0, GID: 4200}}, Standby: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{Name: "a", GID: 4100}}}}
	if got, err = pinned.Evaluate(sameGID); err != nil || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != MDSMismatchGID {
		t.Fatalf("restarted standby matches its old GID: %+v %v", got, err)
	}
}

func TestMDSExpectationFilesystemIDPinsTheRecreatedFilesystem(t *testing.T) {
	recreated := mdsBaseline()
	recreated.FilesystemID = 7
	got, err := MDSs{FilesystemID: 1}.Evaluate(recreated)
	if err != nil || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != MDSMismatchFilesystem {
		t.Fatalf("recreated filesystem: %+v %v", got, err)
	}
}

func TestMDSExpectationExactAndAllRanksActive(t *testing.T) {
	twoRanks := cephfs.FilesystemStatus{FilesystemID: 1, MaxMDS: 2, Active: []cephfs.MDSStatus{
		{Name: "a", Rank: 0, GID: 4100, State: "up:active", Owned: true},
		{Name: "b", Rank: 1, GID: 4200, State: "up:active", Owned: true},
	}}
	if got, err := (MDSs{Ranks: []MDSRank{{Rank: 1, GID: 4200}}}).Evaluate(twoRanks); err != nil || !got.Matched {
		t.Fatalf("non-exact selection of rank 1: %+v %v", got, err)
	}
	if got, err := (MDSs{Ranks: []MDSRank{{Rank: 1, GID: 4200}}, Exact: true}).Evaluate(twoRanks); err != nil || len(got.Mismatches) != 1 ||
		got.Mismatches[0].Reason != MDSMismatchUnselected || got.Mismatches[0].Rank != 0 {
		t.Fatalf("exact selection of rank 1: %+v %v", got, err)
	}
	// Rank 1 in replay during a takeover is in no list.
	replaying := twoRanks
	replaying.Active = replaying.Active[:1]
	all := MDSs{Ranks: []MDSRank{{Rank: 0, GID: 4100}}, AllRanksActive: true}
	if got, err := all.Evaluate(twoRanks); err != nil || !got.Matched {
		t.Fatalf("all ranks active: %+v %v", got, err)
	}
	if got, err := all.Evaluate(replaying); err != nil || len(got.Mismatches) != 1 || got.Mismatches[0] != (MDSMismatch{Reason: MDSMismatchInactive, List: MDSListActive, Rank: 1}) {
		t.Fatalf("rank 1 in replay: %+v %v", got, err)
	}
	// Exact with no Ranks states that no rank is active.
	if got, err := (MDSs{Exact: true}).Evaluate(replaying); err != nil || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != MDSMismatchUnselected {
		t.Fatalf("exact without ranks: %+v %v", got, err)
	}
}

func TestMDSExpectationEmptyStandbysAreACondition(t *testing.T) {
	got, err := MDSs{Standby: &MDSStandbys{}, StandbyReplay: &MDSStandbys{}}.Evaluate(mdsBaseline())
	if err != nil || len(got.Mismatches) != 2 || got.Mismatches[0].String() != "standby count: want 0, observed 1" ||
		got.Mismatches[1].String() != "standby-replay count: want 0, observed 1" {
		t.Fatalf("empty standby lists: %+v %v", got, err)
	}
}

func TestMDSExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := mdsBaseline()
	for name, e := range map[string]MDSs{
		"empty":               {},
		"owned without ranks": {Owned: true, MaxMDS: 1},
		"negative fs":         {FilesystemID: -1},
		"negative max_mds":    {MaxMDS: -1},
		"negative rank":       {Ranks: []MDSRank{{Rank: -1}}},
		"duplicate rank":      {Ranks: []MDSRank{{Rank: 0}, {Rank: 0, GID: 4100}}},
		"rank above max_mds":  {MaxMDS: 1, Ranks: []MDSRank{{Rank: 1}}},
		"gid is not gid":      {Ranks: []MDSRank{{Rank: 0, GID: 4100, NotGID: 4100}}},
		"name twice":          {Ranks: []MDSRank{{Rank: 0, Name: "a"}}, Standby: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{Name: "a"}}}},
		"gid twice":           {Ranks: []MDSRank{{Rank: 0, GID: 4100}}, StandbyReplay: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{GID: 4100}}}},
		"negative count":      {Standby: &MDSStandbys{Count: -1}},
		"count below daemons": {Standby: &MDSStandbys{Count: 0, Daemons: []MDSDaemon{{Name: "b"}}}},
		"unselected standby":  {Standby: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{NotGID: 4200}}}},
		"standby gid not gid": {Standby: &MDSStandbys{Count: 1, Daemons: []MDSDaemon{{Name: "b", GID: 1, NotGID: 1}}}},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Active != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestMDSExpectationRefusesMalformedObservations(t *testing.T) {
	e := MDSs{Ranks: []MDSRank{{Rank: 0}}}
	for name, change := range map[string]func(*cephfs.FilesystemStatus){
		"zero gap status":   func(s *cephfs.FilesystemStatus) { *s = cephfs.FilesystemStatus{} },
		"negative fs":       func(s *cephfs.FilesystemStatus) { s.FilesystemID = -1 },
		"no name":           func(s *cephfs.FilesystemStatus) { s.Active[0].Name = "" },
		"no gid":            func(s *cephfs.FilesystemStatus) { s.Standby[0].GID = 0 },
		"replay as active":  func(s *cephfs.FilesystemStatus) { s.Active[0].State = "up:replay" },
		"active as standby": func(s *cephfs.FilesystemStatus) { s.Standby[0].State = "up:active" },
		"unowned standby":   func(s *cephfs.FilesystemStatus) { s.StandbyReplay[0].Owned = false },
		"negative rank":     func(s *cephfs.FilesystemStatus) { s.Active[0].Rank = -1 },
		"repeated rank": func(s *cephfs.FilesystemStatus) {
			s.Active = append(s.Active, cephfs.MDSStatus{Name: "z", GID: 9, State: "up:active"})
		},
		"name in two lists":  func(s *cephfs.FilesystemStatus) { s.Standby[0].Name = "a" },
		"gid in two entries": func(s *cephfs.FilesystemStatus) { s.StandbyReplay[0].GID = 4200 },
	} {
		observed := mdsBaseline()
		change(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestMDSEvaluationKeepsNoAliasOfCallerSlices(t *testing.T) {
	e, observed := mdsPinned(), mdsBaseline()
	got, err := e.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	e.Ranks[0].GID, e.Standby.Count, e.Standby.Daemons[0].Name, e.StandbyReplay.Daemons[0].GID = 1, 5, "z", 2
	observed.Active[0].GID, observed.Standby[0].Name, observed.StandbyReplay[0].GID = 1, "z", 2
	if got.Expectation.Ranks[0].GID != 4100 || got.Expectation.Standby.Count != 1 || got.Expectation.Standby.Daemons[0].Name != "b" ||
		got.Expectation.StandbyReplay.Daemons[0].GID != 4300 || got.Observed.Active[0].GID != 4100 || got.Observed.Standby[0].Name != "b" ||
		got.Observed.StandbyReplay[0].GID != 4300 {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestMDSEvaluationBoundsItsSummaryInRankOrder(t *testing.T) {
	var ranks []MDSRank
	for rank := 19; rank >= 0; rank-- {
		ranks = append(ranks, MDSRank{Rank: rank})
	}
	got, err := MDSs{Ranks: ranks}.Evaluate(cephfs.FilesystemStatus{FilesystemID: 1, MaxMDS: 20})
	if err != nil || len(got.Mismatches) != 20 {
		t.Fatal(got, err)
	}
	for i, m := range got.Mismatches {
		if m.Rank != i || m.Reason != MDSMismatchInactive {
			t.Fatalf("mismatch %d out of rank order: %+v", i, got.Mismatches)
		}
	}
	text := got.String()
	if !strings.HasSuffix(text, "rank 7 inactive; 12 more") || strings.Contains(text, "rank 8 inactive") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
