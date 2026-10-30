//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

var readyRevisionBound = harness.ReadyRevisionBinding{Daemon: "osd.0", Generation: "container-generation-1", Capture: "log-offset-0"}

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
	observations := harness.ParseReadyRevisionLines(readyRevisionBound, readyLogSample)
	if len(observations) != 4 {
		t.Fatalf("%d observations", len(observations))
	}
	for i, o := range observations {
		if o.Revision != harness.ReadyRevision1 || o.Reason != "" || o.Line != readyLogSample[i] {
			t.Errorf("observation %d: %+v", i, o)
		}
		if problems := harness.ReadyRevision1Problems(observations[i : i+1]); len(problems) != 0 {
			t.Errorf("saved native line rejected: %v", problems)
		}
	}
	// The revision parser selects exactly the sessions the compression parser does.
	sessions := harness.ParseReadyLogLines(readyLogSample)
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
		revision harness.ReadyRevision
		problem  bool
	}{
		{"explicit revision 0 (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=0"), harness.ReadyRevision0, true},
		{"missing field (synthetic)", mutateReadyLine(t, line, " rev1=1", ""), harness.ReadyRevisionUnavailable, true},
		{"empty value (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1="), harness.ReadyRevisionUnavailable, true},
		{"value 2 (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=2"), harness.ReadyRevisionUnavailable, true},
		{"repeated field (synthetic)", mutateReadyLine(t, line, " rev1=1", " rev1=1 rev1=0"), harness.ReadyRevisionUnavailable, true},
		{"original saved line", line, harness.ReadyRevision1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := harness.ParseReadyRevisionLines(readyRevisionBound, []string{c.line})
			if len(got) != 1 || got[0].Revision != c.revision || got[0].Line != c.line {
				t.Fatalf("%+v", got)
			}
			if (len(harness.ReadyRevision1Problems(got)) != 0) != c.problem {
				t.Fatalf("problems %v", harness.ReadyRevision1Problems(got))
			}
			if c.revision == harness.ReadyRevisionUnavailable && got[0].Reason == "" {
				t.Fatal("unavailable revision has no reason")
			}
			// Compression parsing is unchanged by the revision field.
			if before, after := harness.ParseReadyLogLines([]string{line}), harness.ParseReadyLogLines([]string{c.line}); !slices.Equal(before, after) {
				t.Fatalf("compression fields moved: %+v vs %+v", before, after)
			}
		})
	}
}

func TestReadyRevisionRequiresBinding(t *testing.T) {
	for name, binding := range map[string]harness.ReadyRevisionBinding{
		"no daemon":     {Generation: "g", Capture: "c"},
		"no generation": {Daemon: "osd.0", Capture: "c"},
		"no capture":    {Daemon: "osd.0", Generation: "g"},
		"empty":         {},
	} {
		got := harness.ParseReadyRevisionLines(binding, readyLogSample[:1])
		if len(got) != 1 || got[0].Revision != harness.ReadyRevision1 {
			t.Fatalf("%s: revision lost: %+v", name, got)
		}
		if len(harness.ReadyRevision1Problems(got)) == 0 {
			t.Errorf("%s: unbound observation accepted", name)
		}
	}
	if len(harness.ReadyRevision1Problems(nil)) == 0 {
		t.Error("no observations accepted")
	}
	if len(harness.ParseReadyRevisionLines(readyRevisionBound, []string{"", "osd.0 some other line"})) != 0 {
		t.Error("non-READY lines selected")
	}
}
