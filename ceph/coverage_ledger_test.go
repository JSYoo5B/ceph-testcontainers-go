package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockerclient "github.com/moby/moby/client"
)

// This file is a Docker-free characterization of what the current snapshot
// collectors (CollectCrashMetadata's reader and the diagnostic log tail)
// can and cannot say about coverage. It keeps three dimensions apart that a
// single Complete flag or a raw record count would merge:
//
//   - read_complete: the requested snapshot was read without a failure or a
//     rejected entry;
//   - event_complete: the event itself is finalized (a crash's done marker);
//   - window_coverage: whether the snapshot is known to span the interval it
//     is later used for, with explicit gaps when it is not.
//
// It also keeps an event's identity apart from its sightings: the same crash
// ID seen under the unposted and the posted path, or in the crash directory of
// two container generations, is one event with several retained sightings.
//
// The collectors are unchanged. The fakes honor the requested tail and carry
// synthetic crash metadata, so nothing here shows a native crash, real log
// rotation, the selected log driver or that two clocks agree.

type coverageSighting struct {
	Generation string `json:"generation"`
	Path       string `json:"path"`
	Posted     bool   `json:"posted"`
	Done       bool   `json:"done"`
}

type coverageEvent struct {
	ID        string             `json:"id"`
	Sightings []coverageSighting `json:"sightings"`
	// EventComplete holds when at least one sighting carries the done marker.
	EventComplete bool `json:"event_complete"`
	Generations   int  `json:"generations"`
}

type coverageLedger struct {
	// ReadComplete mirrors the collector's own flag and nothing more.
	ReadComplete bool            `json:"read_complete"`
	Events       []coverageEvent `json:"events"`
	Sightings    int             `json:"sightings"`
	// Gaps names every container whose crash directory could not be read.
	Gaps []string `json:"gaps"`
	// WindowCoverage is "snapshot_of_read_generations" when no gap exists and
	// "gap" otherwise. It never claims that the interval before the snapshot
	// or a removed generation is covered.
	WindowCoverage string `json:"window_coverage"`
}

func buildCrashLedger(report *CrashMetadataReport) coverageLedger {
	ledger := coverageLedger{ReadComplete: report.Complete, Events: []coverageEvent{}, Gaps: []string{}}
	index := map[string]int{}
	for _, container := range report.Containers {
		switch container.Outcome {
		case CrashRecordsFound, CrashDirectoryEmpty, CrashDirectoryAbsent:
		default:
			ledger.Gaps = append(ledger.Gaps, fmt.Sprintf("%s/%s:%s", container.Role, container.Name, container.Outcome))
		}
		for _, record := range container.Records {
			at, ok := index[record.identity]
			if !ok {
				at = len(ledger.Events)
				index[record.identity] = at
				ledger.Events = append(ledger.Events, coverageEvent{ID: record.identity, Sightings: []coverageSighting{}})
			}
			event := &ledger.Events[at]
			event.Sightings = append(event.Sightings, coverageSighting{Generation: container.ContainerID, Path: record.Path, Posted: record.Posted, Done: record.Done})
			event.EventComplete = event.EventComplete || record.Done
			ledger.Sightings++
		}
	}
	for i := range ledger.Events {
		generations := map[string]bool{}
		for _, sighting := range ledger.Events[i].Sightings {
			generations[sighting.Generation] = true
		}
		ledger.Events[i].Generations = len(generations)
	}
	ledger.WindowCoverage = "snapshot_of_read_generations"
	if len(ledger.Gaps) > 0 {
		ledger.WindowCoverage = "gap"
	}
	return ledger
}

func coverageMeta(id string) string {
	return fmt.Sprintf(`{"crash_id":%q,"entity_name":"mgr.a","process_name":"ceph-mgr","timestamp":"2026-10-12T10:06:08.307241Z"}`, id)
}

func coverageCrashLedger(t *testing.T, containers map[string]*fakeCrashContainer, targets ...DiagnosticsContainer) coverageLedger {
	t.Helper()
	report, _ := collectCrashes(t, t.Context(), CrashMetadataConfig{}, &fakeCrashDocker{containers: containers}, crashSnapshot(targets...))
	if report == nil {
		t.Fatal("no report")
	}
	return buildCrashLedger(report)
}

func TestCoverageLedgerSeparatesCrashCompletionFromReadCompletion(t *testing.T) {
	const id = "2026-10-12_a"
	exited := dockercontainer.State{Status: "exited", ExitCode: 139}
	cases := []struct {
		name          string
		entries       []crashEntry
		eventComplete bool
	}{
		{"meta-without-done", []crashEntry{{name: "crash/" + id + "/meta", body: coverageMeta(id)}}, false},
		{"meta-with-done", []crashEntry{{name: "crash/" + id + "/meta", body: coverageMeta(id)}, {name: "crash/" + id + "/done"}}, true},
		{"done-without-meta", []crashEntry{{name: "crash/" + id + "/done"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: crashArchive(t, tc.entries...), state: exited}},
				crashContainer("mgr", "a", "mgr-a"))
			// The read is complete in every case; only the event differs.
			if !ledger.ReadComplete || ledger.WindowCoverage != "snapshot_of_read_generations" || len(ledger.Gaps) != 0 {
				t.Fatalf("ledger %+v", ledger)
			}
			if tc.name == "done-without-meta" {
				if len(ledger.Events) != 0 {
					t.Fatalf("a done marker alone must not be an event: %+v", ledger.Events)
				}
				return
			}
			if len(ledger.Events) != 1 || ledger.Events[0].EventComplete != tc.eventComplete {
				t.Fatalf("events %+v", ledger.Events)
			}
		})
	}
}

func TestCoverageLedgerKeepsIdentityApartFromSightings(t *testing.T) {
	const id = "2026-10-12_a"
	state := dockercontainer.State{Status: "exited", ExitCode: 139}
	t.Run("unposted-and-posted-path", func(t *testing.T) {
		archive := crashArchive(t,
			crashEntry{name: "crash/" + id + "/meta", body: coverageMeta(id)},
			crashEntry{name: "crash/posted/" + id + "/meta", body: coverageMeta(id)},
			crashEntry{name: "crash/posted/" + id + "/done"})
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: archive, state: state}}, crashContainer("mgr", "a", "mgr-a"))
		if ledger.Sightings != 2 || len(ledger.Events) != 1 || ledger.Events[0].ID != id || ledger.Events[0].Generations != 1 {
			t.Fatalf("ledger %+v", ledger)
		}
		// Each sighting keeps its own path and done marker.
		got := ledger.Events[0].Sightings
		if got[0].Posted || got[0].Done || !got[1].Posted || !got[1].Done || !ledger.Events[0].EventComplete {
			t.Fatalf("sightings %+v", got)
		}
	})
	t.Run("done-sighting-first", func(t *testing.T) {
		archive := crashArchive(t,
			crashEntry{name: "crash/" + id + "/meta", body: coverageMeta(id)},
			crashEntry{name: "crash/" + id + "/done"},
			crashEntry{name: "crash/posted/" + id + "/meta", body: coverageMeta(id)})
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: archive, state: state}}, crashContainer("mgr", "a", "mgr-a"))
		if len(ledger.Events) != 1 || !ledger.Events[0].EventComplete || ledger.Events[0].Sightings[1].Done {
			t.Fatalf("ledger %+v", ledger)
		}
	})
	t.Run("two-generations", func(t *testing.T) {
		archive := crashArchive(t, crashEntry{name: "crash/" + id + "/meta", body: coverageMeta(id)}, crashEntry{name: "crash/" + id + "/done"})
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{
			"mgr-a-old": {archive: archive, state: state},
			"mgr-a-new": {archive: archive, state: state},
		}, crashContainer("mgr", "a", "mgr-a-old"), crashContainer("mgr", "a", "mgr-a-new"))
		if ledger.Sightings != 2 || len(ledger.Events) != 1 || ledger.Events[0].Generations != 2 {
			t.Fatalf("ledger %+v", ledger)
		}
	})
	t.Run("different-ids-stay-different", func(t *testing.T) {
		archive := crashArchive(t,
			crashEntry{name: "crash/" + id + "/meta", body: coverageMeta(id)},
			crashEntry{name: "crash/2026-10-12_b/meta", body: coverageMeta("2026-10-12_b")})
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: archive, state: state}}, crashContainer("mgr", "a", "mgr-a"))
		if ledger.Sightings != 2 || len(ledger.Events) != 2 {
			t.Fatalf("ledger %+v", ledger)
		}
	})
}

func TestCoverageLedgerDeclaresWindowGaps(t *testing.T) {
	archive := crashArchive(t, crashEntry{name: "crash/2026-10-12_a/meta", body: coverageMeta("2026-10-12_a")})
	state := dockercontainer.State{Status: "exited"}
	cases := []struct {
		name       string
		container  *fakeCrashContainer
		wantRead   bool
		wantWindow string
		wantGap    string
	}{
		{"absent-directory", &fakeCrashContainer{noCrashDir: true, state: state}, true, "snapshot_of_read_generations", ""},
		{"removed-container", &fakeCrashContainer{removed: true}, false, "gap", "mgr/a:unavailable"},
		{"removed-after-inspect", &fakeCrashContainer{removedAfterInspect: true, archive: archive, state: state}, false, "gap", "mgr/a:unavailable"},
		{"cut-archive", &fakeCrashContainer{archive: archive[:len(archive)/2], state: state}, false, "gap", "mgr/a:incomplete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": tc.container}, crashContainer("mgr", "a", "mgr-a"))
			if ledger.ReadComplete != tc.wantRead || ledger.WindowCoverage != tc.wantWindow {
				t.Fatalf("ledger %+v", ledger)
			}
			if (tc.wantGap == "") != (len(ledger.Gaps) == 0) || (tc.wantGap != "" && !slices.Equal(ledger.Gaps, []string{tc.wantGap})) {
				t.Fatalf("gaps %v, want %q", ledger.Gaps, tc.wantGap)
			}
		})
	}
	t.Run("a-gap-does-not-hide-other-sightings", func(t *testing.T) {
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: archive, state: state}, "mon-a": {removed: true}},
			crashContainer("mgr", "a", "mgr-a"), crashContainer("mon", "a", "mon-a"))
		if ledger.ReadComplete || ledger.WindowCoverage != "gap" || len(ledger.Events) != 1 || !slices.Equal(ledger.Gaps, []string{"mon/a:unavailable"}) {
			t.Fatalf("ledger %+v", ledger)
		}
	})
	t.Run("json-shape", func(t *testing.T) {
		ledger := coverageCrashLedger(t, map[string]*fakeCrashContainer{"mgr-a": {archive: archive, state: state}}, crashContainer("mgr", "a", "mgr-a"))
		data, err := json.Marshal(ledger)
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]any
		if err := json.Unmarshal(data, &object); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"read_complete", "events", "sightings", "gaps", "window_coverage"} {
			if _, ok := object[key]; !ok {
				t.Fatalf("ledger lacks %q: %s", key, data)
			}
		}
	})
}

// tailWindow is what the diagnostic tail can show about its own window. A
// tail request returns the last N lines; fewer lines than N means that the
// whole retained log was returned, but exactly N lines (or a byte clip, or an
// error) leaves the earlier part unknown. Retention itself, such as rotation,
// is outside this rule.
type tailWindow struct {
	Requested     int  `json:"requested_tail"`
	Returned      int  `json:"returned_lines"`
	ReadComplete  bool `json:"read_complete"`
	ByteTruncated bool `json:"byte_truncated"`
	// WindowCoverage is "whole_retained_log" or "unproven".
	WindowCoverage string `json:"window_coverage"`
	Bytes          int    `json:"bytes"`
}

func lastLines(text string, n int) string {
	lines := strings.SplitAfter(text, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "")
}

// countLines counts lines the way lastLines cuts them: every newline ends a
// line, and a nonempty final fragment without a newline is a line too. Blank
// lines count; a carriage return stays part of its line.
func countLines(text string) int {
	n := strings.Count(text, "\n")
	if text != "" && !strings.HasSuffix(text, "\n") {
		n++
	}
	return n
}

func observeTail(t *testing.T, retained string, tail, maxBytes int) (string, tailWindow) {
	t.Helper()
	config := diagnosticsUnitConfig(t)
	config.LogTail, config.MaxOutputBytes = tail, maxBytes
	docker := diagnosticsTestDocker{
		logs: func(_ context.Context, _ string, opts dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
			if opts.Tail != fmt.Sprint(tail) {
				t.Errorf("tail request %q", opts.Tail)
			}
			return io.NopCloser(strings.NewReader(lastLines(retained, tail))), nil
		},
	}
	output, truncated, err := diagnosticLogs(t.Context(), docker, "owned", config)
	window := tailWindow{Requested: tail, Returned: countLines(output), ReadComplete: err == nil, ByteTruncated: truncated, Bytes: len(output)}
	window.WindowCoverage = "unproven"
	if err == nil && !truncated && window.Returned < tail {
		window.WindowCoverage = "whole_retained_log"
	}
	return output, window
}

func TestCoverageLedgerTailOverflowIsNotVisibleToReadCompleteness(t *testing.T) {
	const early = "cluster [WRN] early sentinel\n"
	noise := strings.Repeat("noise\n", 5)
	retained := early + noise
	t.Run("overflow", func(t *testing.T) {
		output, window := observeTail(t, retained, 3, 1<<16)
		if strings.Contains(output, "early sentinel") {
			t.Fatalf("the early event survived the tail: %q", output)
		}
		// The read is complete and the bytes are not clipped, yet the window
		// is unproven: the early event is absent and nothing says so.
		if !window.ReadComplete || window.ByteTruncated || window.WindowCoverage != "unproven" || window.Returned != 3 {
			t.Fatalf("window %+v", window)
		}
	})
	t.Run("exactly-tail-lines-is-unproven", func(t *testing.T) {
		_, window := observeTail(t, "a\nb\nc\n", 3, 1<<16)
		if window.WindowCoverage != "unproven" || window.Returned != 3 {
			t.Fatalf("window %+v", window)
		}
	})
	t.Run("short-log-is-whole-retained-log", func(t *testing.T) {
		output, window := observeTail(t, retained, 100, 1<<16)
		if !strings.Contains(output, "early sentinel") || window.WindowCoverage != "whole_retained_log" || window.Returned != 6 || window.Bytes != len(retained) {
			t.Fatalf("window %+v, output %q", window, output)
		}
	})
	t.Run("byte-clip-is-unproven", func(t *testing.T) {
		output, window := observeTail(t, retained, 100, 8)
		if strings.Contains(output, "early sentinel") || !window.ByteTruncated || window.WindowCoverage != "unproven" {
			t.Fatalf("window %+v, output %q", window, output)
		}
	})
	t.Run("final-newline-does-not-change-the-window", func(t *testing.T) {
		cases := []struct {
			name, retained string
			tail           int
			returned       int
			want           string
		}{
			{"overflow", "early sentinel\nnoise1\nnoise2\nnoise3", 3, 3, "unproven"},
			{"exact", "a\nb\nc", 3, 3, "unproven"},
			{"short", "a\nb", 3, 2, "whole_retained_log"},
			{"crlf-overflow", "early\r\nn1\r\nn2\r\nn3", 3, 3, "unproven"},
			{"crlf-short", "a\r\nb", 3, 2, "whole_retained_log"},
			{"trailing-blank-lines-overflow", "early\nn1\nn2\n\n", 3, 3, "unproven"},
			{"empty", "", 3, 0, "whole_retained_log"},
			{"only-newline", "\n", 3, 1, "whole_retained_log"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, bare := observeTail(t, tc.retained, tc.tail, 1<<16)
				terminated := tc.retained
				if terminated != "" && !strings.HasSuffix(terminated, "\n") {
					terminated += "\n"
				}
				_, withNewline := observeTail(t, terminated, tc.tail, 1<<16)
				for _, window := range []tailWindow{bare, withNewline} {
					if window.Returned != tc.returned || window.WindowCoverage != tc.want {
						t.Fatalf("window %+v, want %d lines and %s", window, tc.returned, tc.want)
					}
				}
			})
		}
	})
	t.Run("json-shape", func(t *testing.T) {
		_, window := observeTail(t, retained, 3, 1<<16)
		data, err := json.Marshal(window)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"requested_tail", "returned_lines", "read_complete", "byte_truncated", "window_coverage", "bytes"} {
			if !bytes.Contains(data, []byte(`"`+key+`"`)) {
				t.Fatalf("window lacks %q: %s", key, data)
			}
		}
	})
}
