//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

var readyRevisionBound = readyRevisionBinding{Daemon: "osd.0", Generation: "container-generation-1", Capture: "log-offset-0"}

// mutateReadyLine edits the rev1 field of a saved native READY line. The
// results are synthetic, not observed Ceph output.
func mutateReadyLine(t *testing.T, line, from, to string) string {
	t.Helper()
	if strings.Count(line, from) != 1 {
		t.Fatalf("line does not contain %q exactly once", from)
	}
	return strings.Replace(line, from, to, 1)
}

func TestParseReadyRevisionSavedLines(t *testing.T) {
	observations := parseReadyRevisionLines(readyRevisionBound, readyLogSample)
	if len(observations) != 4 {
		t.Fatalf("%d observations", len(observations))
	}
	for i, o := range observations {
		if o.Revision != readyRevision1 || o.Reason != "" || o.Line != readyLogSample[i] {
			t.Errorf("observation %d: %+v", i, o)
		}
		if problems := readyRevision1Problems(observations[i : i+1]); len(problems) != 0 {
			t.Errorf("saved native line rejected: %v", problems)
		}
	}
	// The revision parser selects exactly the sessions the compression parser does.
	sessions := parseReadyLogLines(readyLogSample)
	for i, o := range observations {
		if o.Mode != sessions[i].Mode || o.PeerType != sessions[i].PeerType || o.PeerID != sessions[i].PeerID {
			t.Errorf("session %d: revision %+v, compression %+v", i, o, sessions[i])
		}
	}
}

func TestParseReadyRevisionLabelledMutations(t *testing.T) {
	line := readyLogSample[0]
	cases := []struct {
		name     string
		line     string
		revision readyRevision
		problem  bool
	}{
		{"explicit revision 0 (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=0"), readyRevision0, true},
		{"missing field (synthetic)", mutateReadyLine(t, line, " rev1=1", ""), readyRevisionUnavailable, true},
		{"empty value (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1="), readyRevisionUnavailable, true},
		{"value 2 (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=2"), readyRevisionUnavailable, true},
		{"repeated field (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=1 rev1=0"), readyRevisionUnavailable, true},
		{"original saved line", line, readyRevision1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseReadyRevisionLines(readyRevisionBound, []string{c.line})
			if len(got) != 1 || got[0].Revision != c.revision || got[0].Line != c.line {
				t.Fatalf("%+v", got)
			}
			if (len(readyRevision1Problems(got)) != 0) != c.problem {
				t.Fatalf("problems %v", readyRevision1Problems(got))
			}
			if c.revision == readyRevisionUnavailable && got[0].Reason == "" {
				t.Fatal("unavailable revision has no reason")
			}
			// Compression parsing is unchanged by the revision field.
			if before, after := parseReadyLogLines([]string{line}), parseReadyLogLines([]string{c.line}); !slices.Equal(before, after) {
				t.Fatalf("compression fields moved: %+v vs %+v", before, after)
			}
		})
	}
}

func TestReadyRevisionRequiresBinding(t *testing.T) {
	for name, binding := range map[string]readyRevisionBinding{
		"no daemon":     {Generation: "g", Capture: "c"},
		"no generation": {Daemon: "osd.0", Capture: "c"},
		"no capture":    {Daemon: "osd.0", Generation: "g"},
		"empty":         {},
	} {
		got := parseReadyRevisionLines(binding, readyLogSample[:1])
		if len(got) != 1 || got[0].Revision != readyRevision1 {
			t.Fatalf("%s: revision lost: %+v", name, got)
		}
		if len(readyRevision1Problems(got)) == 0 {
			t.Errorf("%s: unbound observation accepted", name)
		}
	}
	if len(readyRevision1Problems(nil)) == 0 {
		t.Error("no observations accepted")
	}
	if len(parseReadyRevisionLines(readyRevisionBound, []string{"", "osd.0 some other line"})) != 0 {
		t.Error("non-READY lines selected")
	}
}
