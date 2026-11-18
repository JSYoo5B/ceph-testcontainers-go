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
			if before, after := parseReadyLogLines([]string{line}), parseReadyLogLines([]string{c.line}); !slices.Equal(before, after) {
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

func TestReadyRevisionRefusesMalformedFramingBesideValidLines(t *testing.T) {
	line := readyLogSample[0]
	for name, malformed := range map[string]string{
		"connection":  strings.Replace(line, "conn(", "conn?", 1),
		"crypto":      strings.Replace(line, " crypto rx=", " crypto missing=", 1),
		"compression": strings.Replace(line, " comp rx=", " comp missing=", 1),
		"entity":      strings.Split(line, ").ready")[0],
	} {
		t.Run(name, func(t *testing.T) {
			lines := append(append([]string{}, readyLogSample...), malformed)
			observations := harness.ParseReadyRevisionLines(readyRevisionBound, lines)
			if len(observations) != len(harness.ParseReadyRevisionLines(readyRevisionBound, readyLogSample))+1 || observations[len(observations)-1].Revision != harness.ReadyRevisionUnavailable {
				t.Fatalf("malformed READY vanished: %+v", observations)
			}
			if len(harness.ReadyRevision1Problems(observations)) == 0 {
				t.Fatal("valid rows hid unavailable READY evidence")
			}
		})
	}
	if got := harness.ParseReadyRevisionLines(readyRevisionBound, []string{strings.Replace(line, "s=READY", "s=STANDBY", 1), "unrelated log"}); len(got) != 0 {
		t.Fatalf("unrelated lines selected: %+v", got)
	}
}

// Saved native Ceph 20.2.4 diagnostics carry s=READY but are not ready events.
func TestReadyRevisionIgnoresNativeReadyStateDiagnostics(t *testing.T) {
	lines := []string{
		`2026-11-18T11:39:00.426+0000 7419205a5640  1 --2- v2:172.18.0.5:6800/632839410 >> 172.18.0.2:0/3436711500 conn(0x5d7208166000 0x5d7207dd3600 secure :-1 s=READY pgs=3 gs=22 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0x5d72087bf170 tx=0x5d7207bbb4a0 comp rx=0 tx=0).handle_read_frame_preamble_main read frame preamble failed r=-1`,
		`2026-11-18T11:39:00.426+0000 7419205a5640  1 --2- v2:172.18.0.5:6800/632839410 >> 172.18.0.2:0/3436711500 conn(0x5d7208166000 0x5d7207dd3600 secure :-1 s=READY pgs=3 gs=22 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0x5d72087bf170 tx=0x5d7207bbb4a0 comp rx=0 tx=0).stop`,
	}
	for _, line := range lines {
		if harness.IsReadySessionLine(line) {
			t.Errorf("diagnostic classified as ready event: %s", line)
		}
	}
	observations := harness.ParseReadyRevisionLines(readyRevisionBound, append(lines, readyLogSample...))
	if len(observations) != 4 || len(harness.ReadyRevision1Problems(observations)) != 0 {
		t.Fatalf("normal diagnostics poisoned ready events: %+v", observations)
	}
}
