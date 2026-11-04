package expect

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const (
	expectFSID      = "44444444-4444-4444-8444-444444444444"
	expectOtherFSID = "55555555-5555-4555-8555-555555555555"
)

// quorumStatus builds an observation the way the native JSON decodes.
func quorumStatus(t *testing.T, fsid string, members []string, quorum ...string) ceph.QuorumStatus {
	t.Helper()
	type mon struct {
		Name        string `json:"name"`
		PublicAddrs struct {
			Addrvec []map[string]string `json:"addrvec"`
		} `json:"public_addrs"`
	}
	native := struct {
		QuorumNames []string `json:"quorum_names"`
		MonMap      struct {
			FSID string `json:"fsid"`
			Mons []mon  `json:"mons"`
		} `json:"monmap"`
	}{QuorumNames: quorum}
	native.MonMap.FSID = fsid
	for _, name := range members {
		m := mon{Name: name}
		m.PublicAddrs.Addrvec = []map[string]string{{"type": "v2", "addr": "192.0.2.1:3300"}}
		native.MonMap.Mons = append(native.MonMap.Mons, m)
	}
	data, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	var status ceph.QuorumStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestMonitorExpectationMatchesAFullQuorum(t *testing.T) {
	observed := quorumStatus(t, expectFSID, []string{"a", "b", "c"}, "a", "b", "c")
	got, err := Monitors{FSID: expectFSID, Members: []string{"c", "b", "a"}, Quorum: []string{"b", "a", "c"}}.Evaluate(observed)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 {
		t.Fatalf("full quorum does not match: %+v %v", got, err)
	}
	if s := got.String(); s != "mons want fsid "+expectFSID+" members [c b a] quorum [b a c] in monmap [a b c] quorum [a b c]: match" {
		t.Fatalf("summary: %q", s)
	}
	got, err = Monitors{MemberCount: 3, QuorumSize: 3}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatalf("counts do not match: %+v %v", got, err)
	}
}

func TestMonitorExpectationReportsEachReasonFromTheEvaluatedObservation(t *testing.T) {
	observed := quorumStatus(t, expectOtherFSID, []string{"a", "b", "d"}, "a", "d")
	got, err := Monitors{FSID: expectFSID, Members: []string{"a", "b", "c"}, Quorum: []string{"a", "b"}}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []MonitorMismatch{
		{Reason: MonitorMismatchFSID, FSID: expectFSID, ObservedFSID: expectOtherFSID},
		{Reason: MonitorMismatchOutOfQuorum, Name: "b"},
		{Reason: MonitorMismatchAbsent, Name: "c"},
		{Reason: MonitorMismatchUnexpected, Name: "d"},
		{Reason: MonitorMismatchInQuorum, Name: "d"},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"in monmap [a b d] quorum [a d]: fsid: want " + expectFSID + ", observed " + expectOtherFSID,
		"mon.b out of quorum", "mon.c absent", "mon.d unexpected", "mon.d in quorum"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	got, err = Monitors{MemberCount: 3, QuorumSize: 3}.Evaluate(quorumStatus(t, expectFSID, []string{"a", "b", "c", "d"}, "a", "b", "c"))
	want = []MonitorMismatch{{Reason: MonitorMismatchMemberCount, Count: 3, ObservedCount: 4}}
	if err != nil || !slices.Equal(got.Mismatches, want) || !strings.HasSuffix(got.String(), "members: want 3, observed 4") {
		t.Fatalf("member count: %+v %v", got, err)
	}
	got, err = Monitors{QuorumSize: 3}.Evaluate(quorumStatus(t, expectFSID, []string{"a", "b", "c"}, "a", "b"))
	want = []MonitorMismatch{{Reason: MonitorMismatchQuorumSize, Count: 3, ObservedCount: 2}}
	if err != nil || !slices.Equal(got.Mismatches, want) || !strings.HasSuffix(got.String(), "quorum: want 3, observed 2") {
		t.Fatalf("quorum size: %+v %v", got, err)
	}
}

func TestMonitorExpectationPinsTheClusterFSID(t *testing.T) {
	members := []string{"a", "b", "c"}
	got, err := Monitors{FSID: expectFSID, Quorum: members}.Evaluate(quorumStatus(t, strings.ToUpper(expectFSID), members, members...))
	if err != nil || !got.Matched {
		t.Fatalf("the same FSID in another case: %+v %v", got, err)
	}
	// A rebuilt cluster that reuses the MON names is another cluster.
	got, err = Monitors{FSID: expectFSID, Quorum: members}.Evaluate(quorumStatus(t, expectOtherFSID, members, members...))
	if err != nil || got.Matched || len(got.Mismatches) != 1 || got.Mismatches[0].Reason != MonitorMismatchFSID {
		t.Fatalf("rebuilt cluster: %+v %v", got, err)
	}
}

func TestMonitorExpectationChecksOnlyTheStatedFields(t *testing.T) {
	observed := quorumStatus(t, expectFSID, []string{"a", "b", "c"}, "a", "b")
	// Members alone do not require the quorum to hold them.
	if got, err := (Monitors{Members: []string{"a", "b", "c"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("members alone: %+v %v", got, err)
	}
	// Quorum alone does not limit the monmap.
	if got, err := (Monitors{Quorum: []string{"a", "b"}}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("quorum alone: %+v %v", got, err)
	}
	if got, err := (Monitors{FSID: expectFSID}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("fsid alone: %+v %v", got, err)
	}
}

func TestMonitorExpectationRefusesInvalidExpectations(t *testing.T) {
	observed := quorumStatus(t, expectFSID, []string{"a", "b", "c"}, "a", "b", "c")
	for name, e := range map[string]Monitors{
		"empty":             {},
		"empty slices":      {Members: []string{}, Quorum: []string{}},
		"bad fsid":          {FSID: "cluster"},
		"nil fsid":          {FSID: "00000000-0000-0000-0000-000000000000"},
		"negative members":  {MemberCount: -1},
		"negative quorum":   {QuorumSize: -1},
		"empty name":        {Members: []string{"a", ""}},
		"duplicate member":  {Members: []string{"a", "a"}},
		"duplicate quorum":  {Quorum: []string{"a", "a"}},
		"members count":     {Members: []string{"a", "b"}, MemberCount: 3},
		"quorum count":      {Quorum: []string{"a", "b"}, QuorumSize: 3},
		"quorum outside":    {Members: []string{"a", "b", "c"}, Quorum: []string{"a", "d"}},
		"quorum too large":  {MemberCount: 2, QuorumSize: 3},
		"quorum minority":   {MemberCount: 3, QuorumSize: 1},
		"quorum half":       {Members: []string{"a", "b", "c", "d"}, Quorum: []string{"a", "b"}},
		"quorum half count": {MemberCount: 4, Quorum: []string{"a", "b"}},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.QuorumNames != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestMonitorExpectationRefusesMalformedObservations(t *testing.T) {
	e := Monitors{QuorumSize: 1}
	for name, observed := range map[string]ceph.QuorumStatus{
		"no fsid":          quorumStatus(t, "", []string{"a"}, "a"),
		"bad fsid":         quorumStatus(t, "cluster", []string{"a"}, "a"),
		"no member":        quorumStatus(t, expectFSID, nil, "a"),
		"empty member":     quorumStatus(t, expectFSID, []string{""}, ""),
		"repeated member":  quorumStatus(t, expectFSID, []string{"a", "a"}, "a"),
		"empty quorum":     quorumStatus(t, expectFSID, []string{"a"}),
		"quorum outside":   quorumStatus(t, expectFSID, []string{"a"}, "a", "b"),
		"repeated quorum ": quorumStatus(t, expectFSID, []string{"a", "b"}, "a", "a"),
	} {
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestMonitorEvaluationKeepsNoAliasOfCallerSlices(t *testing.T) {
	members, quorum := []string{"a", "b", "c"}, []string{"a", "b", "c"}
	observed := quorumStatus(t, expectFSID, members, quorum...)
	got, err := Monitors{Members: members, Quorum: quorum}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	members[0], quorum[0] = "x", "x"
	observed.QuorumNames[0], observed.MonMap.Mons[0].Name = "x", "x"
	observed.MonMap.Mons[0].PublicAddrs.Addrvec[0].Addr = "198.51.100.1:3300"
	if got.Expectation.Members[0] != "a" || got.Expectation.Quorum[0] != "a" || got.Observed.QuorumNames[0] != "a" ||
		got.Observed.MonMap.Mons[0].Name != "a" || got.Observed.MonMap.Mons[0].PublicAddrs.Addrvec[0].Addr != "192.0.2.1:3300" {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestMonitorEvaluationBoundsItsSummary(t *testing.T) {
	var members []string
	for _, c := range "abcdefghijklmnopqrst" {
		members = append(members, string(c))
	}
	got, err := Monitors{Members: members}.Evaluate(quorumStatus(t, expectFSID, []string{"z"}, "z"))
	if err != nil || len(got.Mismatches) != 21 {
		t.Fatal(got, err)
	}
	text := got.String()
	if !strings.HasSuffix(text, "mon.h absent; 13 more") || strings.Contains(text, "mon.i absent") {
		t.Fatalf("unbounded summary: %q", text)
	}
}
