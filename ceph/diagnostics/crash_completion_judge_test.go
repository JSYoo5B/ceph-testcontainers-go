package diagnostics

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	dockercontainer "github.com/moby/moby/api/types/container"
)

// This file judges, without Docker, whether two crash-directory snapshots of
// one expected crash show that the crash report was completed. It builds on
// the coverage ledger's separation of read completion, event completion and
// window coverage and adds the missing piece for the crash check: what two
// reads of the same container generations, one before and one after the
// completion marker, may and may not be taken to prove.
//
// Mechanism, from the pinned Ceph sources (Tentacle 20.2.4): the fatal-signal
// handler creates the crash directory, creates and then writes the meta file,
// and only afterwards creates the done file, even when the meta file could not
// be opened. ceph-crash waits for the done file and renames the directory to
// posted/ only after it has seen it. So a meta file without done is either a
// dump in progress or an interrupted one, a meta file that is read while it is
// still empty or partial is rejected by the collector, a done file alone is
// not an event, and a posted directory without done contradicts the writer
// order. The fixtures here replay those shapes through the real collector over
// the fake engine; they do not reproduce a native crash, a real signal handler
// timing or ceph-crash behavior, and the synthetic metadata timestamp is not
// compared with any clock.

// crashRead is one snapshot read: the caller's own bounds around the read and
// the collector's report.
type crashRead struct {
	Start, End time.Time
	Report     *evidence.CrashReport
}

type crashCompletionKind string

const (
	// crashCompleted: a sighting of the expected crash carries the done
	// marker, and nothing contradicts it.
	crashCompleted crashCompletionKind = "completed"
	// crashInterrupted: the expected crash has a meta file but no done marker
	// in the later read, and every container that holds it is no longer
	// running. It is not completed evidence and is never promoted by waiting.
	crashInterrupted crashCompletionKind = "interrupted"
	// crashPending: as above, but a container that holds it is still running
	// (or its state is unknown), so the writer may not be finished.
	crashPending crashCompletionKind = "pending"
	// crashUnobserved: both reads were complete and neither shows the crash.
	// It is not proof that no crash happened.
	crashUnobserved crashCompletionKind = "unobserved"
	// crashInconclusive: the evidence is missing, out of order, incomplete
	// where absence matters, or contradictory.
	crashInconclusive crashCompletionKind = "inconclusive"
)

type crashCompletionVerdict struct {
	Kind crashCompletionKind
	// Transition names how the done marker was first seen:
	// "done_at_first_read", "pending_then_done", "absent_then_done" or "".
	Transition string
	Reasons    []string
	// Unexpected lists other crash identities in the later read. They never
	// change the verdict of the expected crash and are never dropped.
	Unexpected []string
}

type crashSighting struct {
	Generation string
	Path       string
	Posted     bool
	Done       bool
	State      string
	// Read is true when the container's whole crash directory was read.
	Read bool
}

// crashSightings collects every sighting of id and every rejected meta file
// that belongs to id, plus the other identities, and the containers whose
// read did not end in a state that supports an absence claim.
func crashSightings(r crashRead, id string) (found []crashSighting, rejected []string, others []string, unread []string) {
	seen := map[string]bool{}
	for _, container := range r.Report.Containers {
		fullRead := false
		switch container.Outcome {
		case evidence.CrashRecordsFound, evidence.CrashDirectoryEmpty, evidence.CrashDirectoryAbsent:
			fullRead = true
		default:
			unread = append(unread, fmt.Sprintf("%s/%s:%s", container.Role, container.Name, container.Outcome))
		}
		for _, record := range container.Records {
			if record.Identity != id {
				if !seen[record.Identity] {
					seen[record.Identity] = true
					others = append(others, record.Identity)
				}
				continue
			}
			found = append(found, crashSighting{Generation: container.ContainerID, Path: record.Path, Posted: record.Posted, Done: record.Done, State: container.State, Read: fullRead})
		}
		for _, omission := range container.Omitted {
			if path.Base(path.Dir(omission.Path)) == id && path.Base(omission.Path) == "meta" {
				rejected = append(rejected, container.ContainerID+":"+omission.Path)
			}
		}
	}
	slices.Sort(others)
	return found, rejected, others, unread
}

// crashReadProblem returns why a read cannot be used at all.
func crashReadProblem(name string, r crashRead) string {
	switch {
	case r.Report == nil:
		return name + " read has no report"
	case r.Start.IsZero() || r.End.IsZero() || r.End.Before(r.Start):
		return name + " read has missing or inverted bounds"
	case r.Report.SchemaVersion != 1:
		return fmt.Sprintf("%s read has an unknown schema version %d", name, r.Report.SchemaVersion)
	case r.Report.StartedAt.IsZero() || r.Report.FinishedAt.IsZero():
		// The collector's own sample times must exist to be compared with the
		// caller's interval.
		return name + " report has no start or finish time"
	case r.Report.StartedAt.Before(r.Start) || r.Report.FinishedAt.After(r.End) || r.Report.FinishedAt.Before(r.Report.StartedAt):
		return name + " report times are outside the caller's read interval"
	}
	for _, c := range r.Report.Containers {
		if c.ContainerID == "" {
			return fmt.Sprintf("%s read has a container (%s/%s) without a generation id", name, c.Role, c.Name)
		}
	}
	return ""
}

func judgeCrashCompletion(expected string, early, late crashRead) crashCompletionVerdict {
	inconclusive := func(reasons ...string) crashCompletionVerdict {
		return crashCompletionVerdict{Kind: crashInconclusive, Reasons: reasons}
	}
	if expected == "" || strings.ContainsAny(expected, "/ ") {
		return inconclusive("the expected crash identity is empty or not a directory name")
	}
	for _, p := range []string{crashReadProblem("early", early), crashReadProblem("late", late)} {
		if p != "" {
			return inconclusive(p)
		}
	}
	if late.Start.Before(early.End) {
		return inconclusive("the late read began before the early read ended")
	}
	eFound, eRejected, _, eUnread := crashSightings(early, expected)
	lFound, lRejected, lOthers, lUnread := crashSightings(late, expected)
	verdict := crashCompletionVerdict{Unexpected: lOthers}
	done := func(s []crashSighting) bool {
		for _, x := range s {
			if x.Done {
				return true
			}
		}
		return false
	}
	// A rejected meta of the expected identity makes every verdict about it
	// inconclusive, whether or not another sighting of it was accepted.
	if len(eRejected) > 0 || len(lRejected) > 0 {
		return inconclusive(fmt.Sprintf("a meta file of the expected crash was rejected (early %v, late %v)", eRejected, lRejected))
	}
	// Writer-order contradictions in either read.
	for name, sightings := range map[string][]crashSighting{"early": eFound, "late": lFound} {
		for _, s := range sightings {
			if s.Posted && !s.Done {
				verdict.Kind = crashInconclusive
				verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("%s read: posted report %s has no done marker, which contradicts the writer order", name, s.Path))
			}
		}
	}
	// A completion seen early must still be there for the same generation.
	for _, e := range eFound {
		if !e.Done {
			continue
		}
		kept := false
		for _, l := range lFound {
			if l.Generation == e.Generation && l.Done {
				kept = true
			}
		}
		if !kept {
			verdict.Kind = crashInconclusive
			verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("generation %s showed done at the early read but not at the late read", e.Generation))
		}
	}
	if verdict.Kind == crashInconclusive {
		return verdict
	}
	switch {
	case done(lFound):
		verdict.Kind = crashCompleted
		switch {
		case done(eFound):
			verdict.Transition = "done_at_first_read"
		case len(eFound) > 0:
			verdict.Transition = "pending_then_done"
		default:
			verdict.Transition = "absent_then_done"
		}
		return verdict
	case len(lFound) > 0:
		verdict.Kind = crashInterrupted
		for _, s := range lFound {
			if !s.Read {
				// The unread suffix of that generation may hold the marker.
				return inconclusive(fmt.Sprintf("generation %s was not fully read, so the missing done marker is not evidence", s.Generation))
			}
			if s.State == "" || s.State == "running" || s.State == "restarting" || s.State == "created" || s.State == "paused" {
				verdict.Kind = crashPending
			}
		}
		verdict.Reasons = append(verdict.Reasons, "no done marker in the late read")
		return verdict
	case len(eUnread) > 0 || len(lUnread) > 0:
		return inconclusive(fmt.Sprintf("absence is not supported: unread containers early %v, late %v", eUnread, lUnread))
	case len(eFound) > 0:
		return inconclusive("the expected crash was seen early and is gone at the late read")
	}
	verdict.Kind = crashUnobserved
	verdict.Reasons = append(verdict.Reasons, "both reads were complete and show no such crash")
	return verdict
}

// ---- fixtures over the real collector and the fake engine ----

var crashJudgeBase = time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)

func crashJudgeAt(seconds int) time.Time {
	return crashJudgeBase.Add(time.Duration(seconds) * time.Second)
}

// crashJudgeRead runs the real collector once and stamps the report and the
// caller bounds as a live read would: the report times lie inside the bounds.
func crashJudgeRead(t *testing.T, start int, containers map[string]*fakeCrashContainer, targets ...Target) crashRead {
	t.Helper()
	report, _ := collectRawCrashes(t, t.Context(), CrashMetadataConfig{}, &fakeCrashDocker{containers: containers}, crashSnapshot(targets...))
	if report == nil {
		t.Fatal("no report")
	}
	report.StartedAt, report.FinishedAt = crashJudgeAt(start+1), crashJudgeAt(start+2)
	return crashRead{Start: crashJudgeAt(start), End: crashJudgeAt(start + 3), Report: report}
}

const crashJudgeID = "2026-10-12_a"

func crashJudgeEntries(t *testing.T, parts ...string) []byte {
	t.Helper()
	var entries []crashEntry
	for _, p := range parts {
		switch p {
		case "meta":
			entries = append(entries, crashEntry{name: "crash/" + crashJudgeID + "/meta", body: coverageMeta(crashJudgeID)})
		case "partial-meta":
			entries = append(entries, crashEntry{name: "crash/" + crashJudgeID + "/meta", body: `{"crash_id": "2026-10`})
		case "empty-meta":
			entries = append(entries, crashEntry{name: "crash/" + crashJudgeID + "/meta"})
		case "done":
			entries = append(entries, crashEntry{name: "crash/" + crashJudgeID + "/done"})
		case "posted-meta":
			entries = append(entries, crashEntry{name: "crash/posted/" + crashJudgeID + "/meta", body: coverageMeta(crashJudgeID)})
		case "posted-done":
			entries = append(entries, crashEntry{name: "crash/posted/" + crashJudgeID + "/done"})
		case "bad-posted-meta":
			entries = append(entries, crashEntry{name: "crash/posted/" + crashJudgeID + "/meta", body: `{"crash_id": "2026-10`})
		case "other":
			entries = append(entries, crashEntry{name: "crash/2026-10-12_b/meta", body: coverageMeta("2026-10-12_b")})
		default:
			t.Fatalf("unknown entry %q", p)
		}
	}
	return crashArchive(t, entries...)
}

var (
	crashJudgeExited  = dockercontainer.State{Status: "exited", ExitCode: 139}
	crashJudgeRunning = dockercontainer.State{Status: "running"}
)

func crashJudgeOne(t *testing.T, start int, state dockercontainer.State, parts ...string) crashRead {
	return crashJudgeRead(t, start, map[string]*fakeCrashContainer{"mgr-a": {archive: crashJudgeEntries(t, parts...), state: state}}, crashContainer("mgr", "a", "mgr-a"))
}

func TestCrashCompletionJudgeVerdicts(t *testing.T) {
	cases := []struct {
		name       string
		early      []string
		late       []string
		lateState  dockercontainer.State
		kind       crashCompletionKind
		transition string
	}{
		{"completed-after-pending", []string{"meta"}, []string{"meta", "done"}, crashJudgeExited, crashCompleted, "pending_then_done"},
		{"completed-after-absent", nil, []string{"meta", "done"}, crashJudgeExited, crashCompleted, "absent_then_done"},
		{"completed-at-first-read", []string{"meta", "done"}, []string{"meta", "done"}, crashJudgeExited, crashCompleted, "done_at_first_read"},
		{"completed-and-posted", []string{"meta"}, []string{"posted-meta", "posted-done"}, crashJudgeExited, crashCompleted, "pending_then_done"},
		{"interrupted-dump", []string{"meta"}, []string{"meta"}, crashJudgeExited, crashInterrupted, ""},
		{"writer-may-still-run", []string{"meta"}, []string{"meta"}, crashJudgeRunning, crashPending, ""},
		{"unobserved", nil, nil, crashJudgeExited, crashUnobserved, ""},
		{"done-alone-is-not-an-event", nil, []string{"done"}, crashJudgeExited, crashUnobserved, ""},
		{"partial-meta-is-not-absence", []string{"partial-meta"}, nil, crashJudgeExited, crashInconclusive, ""},
		{"empty-meta-is-not-absence", nil, []string{"empty-meta"}, crashJudgeExited, crashInconclusive, ""},
		{"partial-meta-beside-another-record", nil, []string{"partial-meta", "other"}, crashJudgeExited, crashInconclusive, ""},
		{"empty-meta-seen-early-beside-another-record", []string{"empty-meta", "other"}, nil, crashJudgeExited, crashInconclusive, ""},
		{"accepted-active-and-rejected-posted-same-id", []string{"meta"}, []string{"meta", "done", "bad-posted-meta"}, crashJudgeExited, crashInconclusive, ""},
		{"accepted-active-pending-and-rejected-posted-same-id", []string{"meta"}, []string{"meta", "bad-posted-meta"}, crashJudgeExited, crashInconclusive, ""},
		{"rejected-posted-same-id-seen-early", []string{"meta", "bad-posted-meta"}, []string{"meta", "done"}, crashJudgeExited, crashInconclusive, ""},
		{"posted-same-id-accepted-control", []string{"meta"}, []string{"meta", "done", "posted-meta", "posted-done"}, crashJudgeExited, crashCompleted, "pending_then_done"},
		{"posted-without-done-contradicts", []string{"meta"}, []string{"posted-meta"}, crashJudgeExited, crashInconclusive, ""},
		{"done-then-gone", []string{"meta", "done"}, nil, crashJudgeExited, crashInconclusive, ""},
		{"done-then-lost-marker", []string{"meta", "done"}, []string{"meta"}, crashJudgeExited, crashInconclusive, ""},
		{"seen-then-gone", []string{"meta"}, nil, crashJudgeExited, crashInconclusive, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			early := crashJudgeOne(t, 0, crashJudgeExited, tc.early...)
			late := crashJudgeOne(t, 10, tc.lateState, tc.late...)
			got := judgeCrashCompletion(crashJudgeID, early, late)
			if got.Kind != tc.kind || got.Transition != tc.transition {
				t.Fatalf("verdict %+v, want %s/%q", got, tc.kind, tc.transition)
			}
			if tc.kind != crashCompleted && got.Kind == crashCompleted {
				t.Fatalf("not completed evidence: %+v", got)
			}
		})
	}
}

// Completion is one result per expected identity: another crash is reported
// and never completes or hides the expected one.
func TestCrashCompletionJudgeOtherCrashesStayApart(t *testing.T) {
	early := crashJudgeOne(t, 0, crashJudgeExited, "meta")
	late := crashJudgeOne(t, 10, crashJudgeExited, "meta", "other")
	got := judgeCrashCompletion(crashJudgeID, early, late)
	if got.Kind != crashInterrupted || !slices.Equal(got.Unexpected, []string{"2026-10-12_b"}) {
		t.Fatalf("verdict %+v", got)
	}
	// An unrelated crash does not make an absent expected crash observed.
	got = judgeCrashCompletion(crashJudgeID, crashJudgeOne(t, 0, crashJudgeExited), crashJudgeOne(t, 10, crashJudgeExited, "other"))
	if got.Kind != crashUnobserved || len(got.Unexpected) != 1 {
		t.Fatalf("verdict %+v", got)
	}
}

// A sighting in another container generation neither completes nor
// contradicts a generation that never showed done.
func TestCrashCompletionJudgeKeepsGenerationsApart(t *testing.T) {
	two := func(oldParts, newParts []string, start int) crashRead {
		return crashJudgeRead(t, start, map[string]*fakeCrashContainer{
			"mgr-a-old": {archive: crashJudgeEntries(t, oldParts...), state: crashJudgeExited},
			"mgr-a-new": {archive: crashJudgeEntries(t, newParts...), state: crashJudgeExited},
		}, crashContainer("mgr", "a", "mgr-a-old"), crashContainer("mgr", "a", "mgr-a-new"))
	}
	early := two([]string{"meta", "done"}, nil, 0)
	t.Run("old-generation-keeps-done", func(t *testing.T) {
		got := judgeCrashCompletion(crashJudgeID, early, two([]string{"meta", "done"}, nil, 10))
		if got.Kind != crashCompleted || got.Transition != "done_at_first_read" {
			t.Fatalf("verdict %+v", got)
		}
	})
	t.Run("done-on-another-generation-does-not-replace-the-old-one", func(t *testing.T) {
		got := judgeCrashCompletion(crashJudgeID, early, two(nil, []string{"meta", "done"}, 10))
		if got.Kind != crashInconclusive {
			t.Fatalf("verdict %+v", got)
		}
	})
}

// Missing, inverted, unstamped or foreign evidence is never decoded into an
// absence or a completion.
func TestCrashCompletionJudgeRefusesBadEvidence(t *testing.T) {
	good := func(start int, parts ...string) crashRead { return crashJudgeOne(t, start, crashJudgeExited, parts...) }
	mutate := func(start int, f func(r *crashRead)) crashRead {
		r := good(start, "meta", "done")
		f(&r)
		return r
	}
	cases := []struct {
		name        string
		early, late crashRead
		id          string
	}{
		{"nil-late-report", good(0), mutate(10, func(r *crashRead) { r.Report = nil }), crashJudgeID},
		{"nil-early-report", mutate(0, func(r *crashRead) { r.Report = nil }), good(10, "meta", "done"), crashJudgeID},
		{"empty-id", good(0), good(10, "meta", "done"), ""},
		{"path-like-id", good(0), good(10, "meta", "done"), crashJudgeID + "/meta"},
		{"zero-late-start", good(0), mutate(10, func(r *crashRead) { r.Start = time.Time{} }), crashJudgeID},
		{"inverted-bounds", good(0), mutate(10, func(r *crashRead) { r.Start, r.End = r.End, r.Start }), crashJudgeID},
		{"reads-overlap", good(0, "meta"), good(2, "meta", "done"), crashJudgeID},
		{"reads-in-reverse-order", good(10, "meta"), good(0, "meta", "done"), crashJudgeID},
		{"report-time-after-end", good(0), mutate(10, func(r *crashRead) { r.Report.FinishedAt = r.End.Add(time.Nanosecond) }), crashJudgeID},
		{"report-time-before-start", good(0), mutate(10, func(r *crashRead) { r.Report.StartedAt = r.Start.Add(-time.Nanosecond) }), crashJudgeID},
		{"report-times-missing", good(0), mutate(10, func(r *crashRead) { r.Report.StartedAt, r.Report.FinishedAt = time.Time{}, time.Time{} }), crashJudgeID},
		{"unknown-schema", good(0), mutate(10, func(r *crashRead) { r.Report.SchemaVersion = 0 }), crashJudgeID},
		{"container-without-generation", good(0), mutate(10, func(r *crashRead) { r.Report.Containers[0].ContainerID = "" }), crashJudgeID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := judgeCrashCompletion(tc.id, tc.early, tc.late); got.Kind != crashInconclusive || len(got.Reasons) == 0 {
				t.Fatalf("verdict %+v", got)
			}
		})
	}
}

// A read gap blocks only the claims that depend on the missing read: absence
// and a completion that is no longer shown. A done sighting that was read
// stays positive evidence.
func TestCrashCompletionJudgeReadGaps(t *testing.T) {
	removed := &fakeCrashContainer{removed: true}
	cut := &fakeCrashContainer{archive: crashJudgeEntries(t, "meta", "done")[:40], state: crashJudgeExited}
	twoLate := func(gap *fakeCrashContainer, parts ...string) crashRead {
		return crashJudgeRead(t, 10, map[string]*fakeCrashContainer{
			"mgr-a": {archive: crashJudgeEntries(t, parts...), state: crashJudgeExited},
			"mon-a": gap,
		}, crashContainer("mgr", "a", "mgr-a"), crashContainer("mon", "a", "mon-a"))
	}
	early := crashJudgeOne(t, 0, crashJudgeExited, "meta")
	for name, gap := range map[string]*fakeCrashContainer{"removed": removed, "cut-archive": cut} {
		t.Run(name+"-blocks-absence", func(t *testing.T) {
			got := judgeCrashCompletion(crashJudgeID, crashJudgeOne(t, 0, crashJudgeExited), twoLate(gap))
			if got.Kind != crashInconclusive {
				t.Fatalf("verdict %+v", got)
			}
		})
		t.Run(name+"-keeps-a-read-done-marker", func(t *testing.T) {
			got := judgeCrashCompletion(crashJudgeID, early, twoLate(gap, "meta", "done"))
			if got.Kind != crashCompleted {
				t.Fatalf("verdict %+v", got)
			}
		})
	}
}

// An unfinished dump, a completed report and an absent crash are three
// different verdicts over the same reads.
func TestCrashCompletionJudgeKindsAreDistinct(t *testing.T) {
	early := crashJudgeOne(t, 0, crashJudgeExited, "meta")
	pending := judgeCrashCompletion(crashJudgeID, early, crashJudgeOne(t, 10, crashJudgeExited, "meta"))
	complete := judgeCrashCompletion(crashJudgeID, early, crashJudgeOne(t, 10, crashJudgeExited, "meta", "done"))
	absent := judgeCrashCompletion(crashJudgeID, crashJudgeOne(t, 0, crashJudgeExited), crashJudgeOne(t, 10, crashJudgeExited))
	kinds := map[crashCompletionKind]bool{pending.Kind: true, complete.Kind: true, absent.Kind: true}
	if len(kinds) != 3 {
		t.Fatalf("pending %+v, complete %+v, absent %+v are not three distinct verdicts", pending, complete, absent)
	}
}

// An incomplete archive read keeps the records read before the cut. A record
// without done from such a read does not show that done is absent, while a
// done marker that was read stays positive evidence.
func TestCrashCompletionJudgeIncompleteReadOfExpectedCrash(t *testing.T) {
	cutRead := func(start int, cut int, parts ...string) crashRead {
		archive := crashJudgeEntries(t, parts...)
		if cut < 0 || cut > len(archive) {
			t.Fatalf("bad cut %d of %d", cut, len(archive))
		}
		return crashJudgeRead(t, start, map[string]*fakeCrashContainer{"mgr-a": {archive: archive[:cut], state: crashJudgeExited}}, crashContainer("mgr", "a", "mgr-a"))
	}
	early := crashJudgeOne(t, 0, crashJudgeExited, "meta")
	// One meta entry is a 512-byte header and one padded 512-byte block.
	beforeDone := cutRead(10, 1024, "meta", "done")
	if c := beforeDone.Report.Containers[0]; c.Outcome != evidence.CrashArchiveIncomplete || len(c.Records) != 1 || c.Records[0].Done {
		t.Fatalf("fixture is not a cut after the meta: %+v", c)
	}
	if got := judgeCrashCompletion(crashJudgeID, early, beforeDone); got.Kind != crashInconclusive {
		t.Fatalf("a cut before done: %+v", got)
	}
	// The same record from a complete read is the interrupted dump.
	if got := judgeCrashCompletion(crashJudgeID, early, crashJudgeOne(t, 10, crashJudgeExited, "meta")); got.Kind != crashInterrupted {
		t.Fatalf("complete control: %+v", got)
	}
	// Cutting after done keeps the positive marker.
	afterDone := cutRead(10, 1536, "meta", "done")
	if c := afterDone.Report.Containers[0]; c.Outcome != evidence.CrashArchiveIncomplete || !c.Records[0].Done {
		t.Fatalf("fixture is not a cut after done: %+v", c)
	}
	if got := judgeCrashCompletion(crashJudgeID, early, afterDone); got.Kind != crashCompleted {
		t.Fatalf("a cut after done: %+v", got)
	}
}
