package expect

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

// MultisiteDataSyncState is the condition a MultisiteDataSync expectation
// states for one data sync stream.
type MultisiteDataSyncState string

// The conditions of a data sync stream.
const (
	// MultisiteDataSyncCaughtUp: the stream reports caught up with its
	// source (multisite.DataSyncStatus.CaughtUp): syncing, every shard
	// incremental, none behind and none recovering.
	MultisiteDataSyncCaughtUp MultisiteDataSyncState = "caught-up"
	// MultisiteDataSyncBehind: the stream is not caught up and the native
	// comparison with the source's log finds a positive number of shards
	// behind. A stream that is only not caught up, such as one in full sync
	// or one without a shard count, is not behind.
	MultisiteDataSyncBehind MultisiteDataSyncState = "behind"
)

// MultisiteDataSync states what a test expects of one directional data sync
// stream in one multisite.SyncStatus observation: the data that Zone, the
// receiving zone the observation was read in, syncs from Source. Zone and
// Source are zone names and are required and distinct, and State is
// required. Each zone's status is its own observation, so a test that
// expects a lag in one direction and none in the other evaluates two
// expectations against the two zones' observations.
//
// A match proves only what that one status read reported. Two observations
// read at different times are not one atomic state of both zones, and
// neither a caught-up stream nor a behind one proves that a particular
// object arrived or is missing: compare the S3 payloads separately. Metadata
// sync is not compared; assert multisite.SyncStatus.Metadata on its own.
type MultisiteDataSync struct {
	Zone, Source string
	State        MultisiteDataSyncState
}

// MultisiteDataSyncMismatchReason names why an observation does not satisfy
// a MultisiteDataSync expectation.
type MultisiteDataSyncMismatchReason string

// The reasons a MultisiteDataSyncEvaluation reports.
const (
	// MultisiteDataSyncMismatchZone: the observation was read in another
	// zone, as when the direction is swapped. No other field is compared.
	MultisiteDataSyncMismatchZone MultisiteDataSyncMismatchReason = "zone"
	// MultisiteDataSyncMismatchSource: the observation lists no data sync
	// stream from Source. No other field is compared.
	MultisiteDataSyncMismatchSource MultisiteDataSyncMismatchReason = "source"
	// MultisiteDataSyncMismatchCaughtUp: the stream's caught-up verdict
	// differs from the expected state's.
	MultisiteDataSyncMismatchCaughtUp MultisiteDataSyncMismatchReason = "caught-up"
	// MultisiteDataSyncMismatchSyncing: a caught-up expectation found the
	// stream not syncing.
	MultisiteDataSyncMismatchSyncing MultisiteDataSyncMismatchReason = "syncing"
	// MultisiteDataSyncMismatchFull: a caught-up expectation found shards
	// still in full sync.
	MultisiteDataSyncMismatchFull MultisiteDataSyncMismatchReason = "full-shards"
	// MultisiteDataSyncMismatchBehind: a caught-up expectation found shards
	// behind, or a behind expectation found none.
	MultisiteDataSyncMismatchBehind MultisiteDataSyncMismatchReason = "behind-shards"
	// MultisiteDataSyncMismatchRecovering: a caught-up expectation found
	// shards recovering.
	MultisiteDataSyncMismatchRecovering MultisiteDataSyncMismatchReason = "recovering-shards"
)

// MultisiteDataSyncMismatch is one reason an observation does not satisfy a
// MultisiteDataSync expectation. Want and Observed are the compared values as
// text; a Want of ">0" requires a positive shard count.
type MultisiteDataSyncMismatch struct {
	Reason         MultisiteDataSyncMismatchReason
	Want, Observed string
}

// String names the reason and the evaluated values.
func (m MultisiteDataSyncMismatch) String() string {
	return fmt.Sprintf("%s: want %s, observed %s", m.Reason, quoteEmpty(m.Want), quoteEmpty(m.Observed))
}

// MultisiteDataSyncEvaluation is the result of evaluating one observation.
// Expectation and Observed are copies of what was evaluated, and Stream is
// the observed stream from the expected source, zero when the observation
// was read in another zone or lists none. Mismatches are in the order of the
// reason constants; Matched means there are none. A zone or source mismatch
// is the only one reported.
type MultisiteDataSyncEvaluation struct {
	Expectation MultisiteDataSync
	Observed    multisite.SyncStatus
	Stream      multisite.DataSyncStatus
	Mismatches  []MultisiteDataSyncMismatch
	Matched     bool
}

// String summarizes the expectation, the evaluated stream's shard counts and
// at most eight mismatches.
func (e MultisiteDataSyncEvaluation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "data sync of zone %s from %s want %s", e.Expectation.Zone, e.Expectation.Source, e.Expectation.State)
	if e.Stream.SourceZone != "" {
		s := e.Stream
		fmt.Fprintf(&b, " (observed %d shards: %d incremental, %d full, %d behind, %d recovering)", s.Shards, s.IncrementalShards, s.FullShards, s.BehindShards, s.RecoveringShards)
	}
	b.WriteString(": ")
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == evaluationShown {
			fmt.Fprintf(&b, "; %d more", len(e.Mismatches)-i)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// Evaluate checks one multisite.SyncStatus observation (from
// Fixture.SyncStatus, Fixture.WaitSyncReady or a sync watch event) against
// the expectation. It reads only its arguments: no query, wait or retry, so
// a test composes it with its own polling or assertion helpers. A mismatch is
// a result; an error means no verdict. An expectation without a Zone or a
// Source, with Source equal to Zone, or with an unknown State is
// ceph.ErrInvalidRequest. An observation without its zone's name and ID, or
// with a data sync stream whose source is unidentified, is the observed zone
// itself, is listed twice, has negative shard counts or claims to be caught
// up against its own counts, is a malformed observation, never a mismatch.
//
//purpose:observation
func (e MultisiteDataSync) Evaluate(observed multisite.SyncStatus) (MultisiteDataSyncEvaluation, error) {
	if err := e.validate(); err != nil {
		return MultisiteDataSyncEvaluation{}, err
	}
	if err := validateSyncObservation(observed); err != nil {
		return MultisiteDataSyncEvaluation{}, err
	}
	observed.DataSources = slices.Clone(observed.DataSources)
	result := MultisiteDataSyncEvaluation{Expectation: e, Observed: observed}
	if observed.Zone != e.Zone {
		result.Mismatches = []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchZone, Want: e.Zone, Observed: observed.Zone}}
		return result, nil
	}
	i := slices.IndexFunc(observed.DataSources, func(s multisite.DataSyncStatus) bool { return s.SourceZone == e.Source })
	if i < 0 {
		var sources []string
		for _, s := range observed.DataSources {
			sources = append(sources, s.SourceZone)
		}
		result.Mismatches = []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchSource, Want: e.Source, Observed: "[" + strings.Join(sources, " ") + "]"}}
		return result, nil
	}
	result.Stream = observed.DataSources[i]
	result.Mismatches = e.mismatches(result.Stream)
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e MultisiteDataSync) validate() error {
	if e.Zone == "" || e.Source == "" || e.Zone == e.Source {
		return fmt.Errorf("%w: data sync expectation needs a receiving zone and another source zone", ceph.ErrInvalidRequest)
	}
	switch e.State {
	case MultisiteDataSyncCaughtUp, MultisiteDataSyncBehind:
	default:
		return fmt.Errorf("%w: data sync expectation selects unknown state %q", ceph.ErrInvalidRequest, e.State)
	}
	return nil
}

func validateSyncObservation(observed multisite.SyncStatus) error {
	if observed.Zone == "" || observed.ZoneID == "" {
		return fmt.Errorf("malformed sync observation: no zone name or ID")
	}
	names, ids := map[string]bool{}, map[string]bool{}
	for _, s := range observed.DataSources {
		if s.SourceZone == "" || s.SourceZoneID == "" {
			return fmt.Errorf("malformed sync observation: data sync stream without a source zone name or ID")
		}
		if s.SourceZone == observed.Zone || s.SourceZoneID == observed.ZoneID {
			return fmt.Errorf("malformed sync observation: zone %s syncs data from itself", observed.Zone)
		}
		if names[s.SourceZone] || ids[s.SourceZoneID] {
			return fmt.Errorf("malformed sync observation: data sync from %s listed twice", s.SourceZone)
		}
		names[s.SourceZone], ids[s.SourceZoneID] = true, true
		if s.Shards < 0 || s.IncrementalShards < 0 || s.FullShards < 0 || s.BehindShards < 0 || s.RecoveringShards < 0 {
			return fmt.Errorf("malformed sync observation: negative shard count from %s", s.SourceZone)
		}
		if s.CaughtUp && (!s.Syncing || s.Shards == 0 || s.IncrementalShards != s.Shards || s.FullShards != 0 || s.BehindShards != 0 || s.RecoveringShards != 0) {
			return fmt.Errorf("malformed sync observation: data sync from %s caught up against its own shard counts", s.SourceZone)
		}
	}
	return nil
}

func (e MultisiteDataSync) mismatches(s multisite.DataSyncStatus) []MultisiteDataSyncMismatch {
	var result []MultisiteDataSyncMismatch
	count := func(reason MultisiteDataSyncMismatchReason, n int) {
		if n != 0 {
			result = append(result, MultisiteDataSyncMismatch{Reason: reason, Want: "0", Observed: strconv.Itoa(n)})
		}
	}
	caughtUp := e.State == MultisiteDataSyncCaughtUp
	if s.CaughtUp != caughtUp {
		result = append(result, MultisiteDataSyncMismatch{Reason: MultisiteDataSyncMismatchCaughtUp, Want: strconv.FormatBool(caughtUp), Observed: strconv.FormatBool(s.CaughtUp)})
	}
	if !caughtUp {
		if s.BehindShards == 0 {
			result = append(result, MultisiteDataSyncMismatch{Reason: MultisiteDataSyncMismatchBehind, Want: ">0", Observed: "0"})
		}
		return result
	}
	if !s.Syncing {
		result = append(result, MultisiteDataSyncMismatch{Reason: MultisiteDataSyncMismatchSyncing, Want: "true", Observed: "false"})
	}
	count(MultisiteDataSyncMismatchFull, s.FullShards)
	count(MultisiteDataSyncMismatchBehind, s.BehindShards)
	count(MultisiteDataSyncMismatchRecovering, s.RecoveringShards)
	return result
}
