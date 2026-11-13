//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_group_snap_late_complete_test

import (
	"slices"
	"strings"
	"testing"
)

// These cases feed synthetic outcomes to judgeLateComplete. They are not
// native evidence of what a server does with a late update.
func TestJudgeLateCompleteReportsOnlyTheResurrectedSnapshot(t *testing.T) {
	const victim = "1a2b"
	removed := []string{"image_x", "snap_max_order", "snap_order_0f0f", "snapshot_0f0f"}
	orphan := append([]string{"snapshot_" + victim}, removed...)
	restored := append([]string{"snap_order_" + victim}, orphan...)
	for _, tc := range []struct {
		name     string
		got      groupSnapOutcome
		symptoms int
		want     string
	}{
		{name: "accepted and listed again", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy", "victim"}}, symptoms: 2},
		{name: "accepted and the listing fails on the orphan record", got: groupSnapOutcome{Keys: orphan, ListExit: 22, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, symptoms: 2},
		// The listing command reports the refusal of a record without an
		// order by its status alone.
		{name: "accepted and the listing exits as invalid without output", got: groupSnapOutcome{Keys: orphan, ListExit: 22}, symptoms: 2},
		{name: "accepted and the listing exits as invalid with a text without an errno", got: groupSnapOutcome{Keys: orphan, ListExit: 22, ListOutput: "rbd: listing snapshots failed"}, symptoms: 2},
		{name: "accepted and the listing exits as invalid with a text of another errno", got: groupSnapOutcome{Keys: orphan, ListExit: 22, ListOutput: "rbd: (110) Connection timed out"}, symptoms: 2},
		// A listing that failed with another status saw nothing of the
		// group, whatever the header object holds and whatever it printed.
		{name: "orphan record and the listing times out", got: groupSnapOutcome{Keys: orphan, ListExit: 110, ListOutput: "rbd: couldn't connect to the cluster: (110) Connection timed out"}, want: "outcome not observed:"},
		{name: "orphan record and the listing is not permitted", got: groupSnapOutcome{Keys: orphan, ListExit: 1, ListOutput: "rbd: failed to list group snapshots: (1) Operation not permitted"}, want: "outcome not observed:"},
		{name: "orphan record and the listing does not find the group", got: groupSnapOutcome{Keys: orphan, ListExit: 2, ListOutput: "rbd: failed to list group snapshots: (2) No such file or directory"}, want: "outcome not observed:"},
		{name: "orphan record and the listing reports the errno with another exit code", got: groupSnapOutcome{Keys: orphan, ListExit: 1, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, want: "outcome not observed:"},
		{name: "orphan record and the listing is killed", got: groupSnapOutcome{Keys: orphan, ListExit: 137}, want: "outcome not observed:"},
		{name: "orphan record and the listing fails with status 1 without output", got: groupSnapOutcome{Keys: orphan, ListExit: 1}, want: "outcome not observed:"},
		{name: "orphan record and the listing exits 255", got: groupSnapOutcome{Keys: orphan, ListExit: 255}, want: "outcome not observed:"},
		{name: "refused as invalid, absent", got: groupSnapOutcome{Return: -22, Keys: removed, Listed: []string{"healthy"}}},
		{name: "accepted without a record, absent", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy"}}},
		{name: "accepted with an orphan record that the listing hides", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy"}}},
		{name: "timed out", got: groupSnapOutcome{Return: -110, Keys: removed, Listed: []string{"healthy"}}, want: "trigger not delivered:"},
		{name: "not permitted", got: groupSnapOutcome{Return: -1, Keys: removed, Listed: []string{"healthy"}}, want: "trigger not delivered:"},
		{name: "an unknown positive result", got: groupSnapOutcome{Return: 1, Keys: orphan, Listed: []string{"healthy", "victim"}}, want: "trigger not delivered:"},
		{name: "refused but listed again", got: groupSnapOutcome{Return: -22, Keys: orphan, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again without a record", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again with its order entry", got: groupSnapOutcome{Keys: restored, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again without the sibling", got: groupSnapOutcome{Keys: orphan, Listed: []string{"victim"}}, want: "another fault:"},
		{name: "listed again with a third snapshot", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy", "victim", "other"}}, want: "another fault:"},
		{name: "the listing fails as invalid without an orphan record", got: groupSnapOutcome{Keys: removed, ListExit: 22, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, want: "another fault:"},
		{name: "the listing exits as invalid without output and without an orphan record", got: groupSnapOutcome{Keys: removed, ListExit: 22}, want: "another fault:"},
		{name: "the listing exits as invalid without output after a refusal", got: groupSnapOutcome{Return: -22, Keys: orphan, ListExit: 22}, want: "another fault:"},
		{name: "the listing fails as invalid with the order entry", got: groupSnapOutcome{Keys: restored, ListExit: 22, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, want: "another fault:"},
		{name: "the listing fails as invalid after a refusal", got: groupSnapOutcome{Return: -22, Keys: orphan, ListExit: 22, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, want: "another fault:"},
		{name: "the listing fails otherwise without an orphan record", got: groupSnapOutcome{Keys: removed, ListExit: 2, ListOutput: "rbd: failed to list group snapshots"}, want: "outcome not observed:"},
		{name: "the listing is empty", got: groupSnapOutcome{Keys: removed}, want: "another fault:"},
		{name: "the listing has another snapshot", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy", "other"}}, want: "another fault:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got.ListExit == 0 && tc.got.Listed == nil {
				tc.got.Listed = []string{}
			}
			symptoms, err := judgeLateComplete(victim, tc.got)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			if len(symptoms) != tc.symptoms {
				t.Fatalf("symptoms %q, want %d", symptoms, tc.symptoms)
			}
		})
	}
}

func TestJudgeLateCompleteRefusesAVictimWithoutAnID(t *testing.T) {
	got := groupSnapOutcome{Keys: []string{"snapshot_", "snapshot_0f0f", "snap_order_0f0f"}, Listed: []string{"healthy", "victim"}}
	if symptoms, err := judgeLateComplete("", got); err == nil || !strings.HasPrefix(err.Error(), "outcome not observed:") {
		t.Fatalf("symptoms %q, error %v", symptoms, err)
	}
}

// These reports are synthetic outputs of the header reading script.
func TestParseGroupSnapHeaderTellsAnObservedZeroFromAMissingField(t *testing.T) {
	const record = `{"id":"0f0f","name":"healthy","state":1,"value":"0101"}`
	report := func(fields string) string { return `{"object":"rbd_group_header.1",` + fields + `}` }
	for _, tc := range []struct {
		name, report string
		update       bool
		wantReturn   int
		wantState    int
		wantErr      bool
	}{
		{name: "a read", report: report(`"keys":["snapshot_0f0f"],"snapshots":[` + record + `]`), wantState: 1},
		{name: "a read of a group without snapshots", report: report(`"keys":[],"snapshots":[]`)},
		{name: "an accepted update", update: true, report: report(`"return":0,"keys":["snapshot_0f0f"],"snapshots":[` + record + `]`), wantState: 1},
		{name: "a refused update", update: true, wantReturn: -22, report: report(`"return":-22,"error":"InvalidArgumentError","keys":["snapshot_0f0f"],"snapshots":[` + record + `]`), wantState: 1},
		{name: "an incomplete record", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","state":0,"value":"0101"}]`)},
		{name: "an update without a result", update: true, report: report(`"keys":["snapshot_0f0f"],"snapshots":[` + record + `]`), wantErr: true},
		{name: "an update with a null result", update: true, report: report(`"return":null,"keys":["snapshot_0f0f"],"snapshots":[` + record + `]`), wantErr: true},
		{name: "an update with a text result", update: true, report: report(`"return":"0","keys":[],"snapshots":[]`), wantErr: true},
		{name: "a read with a result", report: report(`"return":0,"keys":[],"snapshots":[]`), wantErr: true},
		{name: "no keys", report: report(`"snapshots":[]`), wantErr: true},
		{name: "null keys", report: report(`"keys":null,"snapshots":[]`), wantErr: true},
		{name: "no records", report: report(`"keys":[]`), wantErr: true},
		{name: "null records", report: report(`"keys":[],"snapshots":null`), wantErr: true},
		{name: "a record without a state", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","value":"0101"}]`), wantErr: true},
		{name: "a record with a null state", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","state":null,"value":"0101"}]`), wantErr: true},
		{name: "a record without an ID", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"name":"healthy","state":1,"value":"0101"}]`), wantErr: true},
		{name: "a record with an empty ID", report: report(`"keys":["snapshot_"],"snapshots":[{"id":"","name":"healthy","state":1,"value":"0101"}]`), wantErr: true},
		{name: "a record without a name", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","state":1,"value":"0101"}]`), wantErr: true},
		{name: "a record without a value", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","state":1}]`), wantErr: true},
		{name: "a record with an empty value", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","state":1,"value":""}]`), wantErr: true},
		{name: "a record with a value that is not hex", report: report(`"keys":["snapshot_0f0f"],"snapshots":[{"id":"0f0f","name":"healthy","state":1,"value":"xy"}]`), wantErr: true},
		{name: "a record without its key", report: report(`"keys":["snap_order_0f0f"],"snapshots":[` + record + `]`), wantErr: true},
		{name: "no object", report: `{"keys":[],"snapshots":[]}`, wantErr: true},
		{name: "an empty document", report: `{}`, wantErr: true},
		{name: "a traceback", report: "Traceback (most recent call last):", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGroupSnapHeader([]byte(tc.report), tc.update)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, want one: %t", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if got.Return != tc.wantReturn || got.Keys == nil {
				t.Fatalf("header %+v, want the result %d and a key list", got, tc.wantReturn)
			}
			if len(got.Snapshots) > 0 && got.Snapshots[0].State != tc.wantState {
				t.Fatalf("state %d, want %d", got.Snapshots[0].State, tc.wantState)
			}
		})
	}
}

// These listings are synthetic outputs of the listing command.
func TestParseGroupSnapListingTellsAnEmptyListingFromNone(t *testing.T) {
	for _, tc := range []struct {
		name, listing string
		want          []string
		wantErr       bool
	}{
		{name: "two snapshots", listing: `[{"id":"0f0f","snapshot":"healthy","state":"complete"},{"id":"1a2b","snapshot":"victim","state":"complete"}]`, want: []string{"healthy", "victim"}},
		{name: "names only", listing: `[{"snapshot":"healthy"}]`, want: []string{"healthy"}},
		{name: "an empty listing", listing: `[]`, want: []string{}},
		{name: "null", listing: `null`, wantErr: true},
		{name: "nothing", listing: ``, wantErr: true},
		{name: "an object", listing: `{"snapshot":"healthy"}`, wantErr: true},
		{name: "an entry without a name", listing: `[{"id":"0f0f","state":"complete"}]`, wantErr: true},
		{name: "an entry with a null name", listing: `[{"snapshot":null}]`, wantErr: true},
		{name: "an entry with an empty name", listing: `[{"snapshot":""}]`, wantErr: true},
		{name: "a warning before the listing", listing: "warning: something\n[]", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGroupSnapListing([]byte(tc.listing))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, want one: %t", err, tc.wantErr)
			}
			if err == nil && (got == nil || !slices.Equal(got, tc.want)) {
				t.Fatalf("names %#v, want %#v", got, tc.want)
			}
		})
	}
}

// These records are synthetic stored values: a header of six bytes, the ID
// and the name with their lengths, the state byte and the member list.
func TestGroupSnapRecordWithStateReplacesOnlyTheStateByte(t *testing.T) {
	const complete = "010117000000" + "04000000" + "30663066" + "07000000" + "6865616c746879" + "01" + "00000000"
	const incomplete = "010117000000" + "04000000" + "30663066" + "07000000" + "6865616c746879" + "00" + "00000000"
	record := groupSnapRecord{ID: "0f0f", Name: "healthy", State: groupSnapComplete, Value: complete}
	if got, err := record.withState(groupSnapIncomplete); err != nil || got != incomplete {
		t.Fatalf("value %s, error %v, want %s", got, err, incomplete)
	}
	if got, err := record.withState(groupSnapComplete); err != nil || got != complete {
		t.Fatalf("value %s, error %v, want the value unchanged", got, err)
	}
	for name, bad := range map[string]groupSnapRecord{
		"another state than the stored byte": {ID: "0f0f", Name: "healthy", State: groupSnapIncomplete, Value: complete},
		"a longer name than the stored one":  {ID: "0f0f", Name: "healthy-and-more", State: groupSnapComplete, Value: complete},
		"a value that ends before the state": {ID: "0f0f", Name: "healthy", State: groupSnapComplete, Value: complete[:len(complete)-10]},
		"a value that is not hex":            {ID: "0f0f", Name: "healthy", State: groupSnapComplete, Value: "xy"},
		"no ID":                              {Name: "healthy", State: groupSnapComplete, Value: complete},
		"no name":                            {ID: "0f0f", State: groupSnapComplete, Value: complete},
		"a state that is no byte":            {ID: "0f0f", Name: "healthy", State: 257, Value: complete},
	} {
		if got, err := bad.withState(groupSnapIncomplete); err == nil {
			t.Errorf("%s: value %s, want an error", name, got)
		}
	}
}
