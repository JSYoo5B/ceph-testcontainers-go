//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// radosLateEpochLimit bounds how many epochs after the baseline are read.
const radosLateEpochLimit = 8

// radosLateEpoch is one OSDMap epoch committed between the pre-late record and
// the failure record, read from "osd dump <epoch>" so that its own commit time
// ("modified") and its differences from the previous epoch are known without
// guessing from the MON log. Changes names only fields this record parses; an
// empty list with Compared true means that none of those differed, not that
// nothing else did.
type radosLateEpoch struct {
	Epoch        int      `json:"epoch"`
	Modified     string   `json:"modified"`
	ModifiedUnix float64  `json:"modified_unix"`
	Compared     bool     `json:"compared"`
	Changes      []string `json:"changes"`
}

// radosLateSnapshot is the comparable part of one parsed OSDMap.
type radosLateSnapshot struct {
	flags, lastUp, lastIn, poolChange string
	osds                              map[int]radosLateOSDInfo
}

func radosLateSnapshotOf(state *radosLateClusterState) radosLateSnapshot {
	snapshot := radosLateSnapshot{flags: state.MonFlags, lastUp: state.LastUpChange, lastIn: state.LastInChange, poolChange: state.PoolLastChange, osds: map[int]radosLateOSDInfo{}}
	for _, osd := range state.OSDs {
		snapshot.osds[osd.ID] = osd
	}
	return snapshot
}

// radosLateDiff lists what differs between two snapshots. A field that either
// side did not carry is not compared. compared reports whether any field was.
func radosLateDiff(before, after radosLateSnapshot) (changes []string, compared bool) {
	text := func(name, a, b string) {
		if a == "" || b == "" {
			return
		}
		compared = true
		if a != b {
			changes = append(changes, fmt.Sprintf("%s %q -> %q", name, a, b))
		}
	}
	text("flags", before.flags, after.flags)
	text("last_up_change", before.lastUp, after.lastUp)
	text("last_in_change", before.lastIn, after.lastIn)
	text("pool_last_change", before.poolChange, after.poolChange)
	ids := make([]int, 0, len(after.osds))
	for id := range after.osds {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		a, aok := before.osds[id]
		b := after.osds[id]
		if !aok {
			continue
		}
		compared = true
		if a.Up != b.Up || a.In != b.In {
			changes = append(changes, fmt.Sprintf("osd.%d up/in %t/%t -> %t/%t", id, a.Up, a.In, b.Up, b.In))
		}
		for _, field := range []struct {
			name string
			a, b *int
		}{{"up_from", a.UpFrom, b.UpFrom}, {"up_thru", a.UpThru, b.UpThru}} {
			if field.a == nil || field.b == nil {
				continue
			}
			if *field.a != *field.b {
				changes = append(changes, fmt.Sprintf("osd.%d %s %d -> %d", id, field.name, *field.a, *field.b))
			}
		}
	}
	return changes, compared
}

// collectRadosLateEpochTimeline reads "osd dump <e>" for the epochs after the
// baseline up to the record's own epoch, in order. Each answer must be the
// requested epoch (an answer for another epoch is rejected) and carry a
// parseable commit time that does not precede the previous one. Epochs beyond
// radosLateEpochLimit, or not reached before the budget ends, are counted in
// TimelineSkipped and never read as if they had no change.
func collectRadosLateEpochTimeline(overall context.Context, reader radosLateReader, pool string, state, baseline *radosLateClusterState) {
	if baseline == nil || baseline.MonEpoch == nil || state.MonEpoch == nil || *state.MonEpoch <= *baseline.MonEpoch {
		return
	}
	previous := radosLateSnapshotOf(baseline)
	var previousUnix float64
	if baseline.MonModifiedUnix != nil {
		previousUnix = *baseline.MonModifiedUnix
	}
	first, last := *baseline.MonEpoch+1, *state.MonEpoch
	for epoch := first; epoch <= last; epoch++ {
		if epoch-first >= radosLateEpochLimit {
			state.TimelineSkipped = last - epoch + 1
			return
		}
		var scratch radosLateClusterState
		scratch.Probes = nil
		want := epoch
		radosLateRun(overall, state, "osd_dump_e"+strconv.Itoa(epoch), func(ctx context.Context) ([]byte, error) {
			return reader.Ceph(ctx, "osd", "dump", strconv.Itoa(want), "--format", "json")
		}, func(raw []byte) error {
			if err := parseRadosLateOSDDump(raw, pool, &scratch); err != nil {
				return err
			}
			if *scratch.MonEpoch != want {
				return fmt.Errorf("answer is epoch %d, requested %d", *scratch.MonEpoch, want)
			}
			if scratch.MonModifiedUnix == nil {
				return fmt.Errorf("epoch %d has no usable modified time (%q)", want, scratch.MonModified)
			}
			if *scratch.MonModifiedUnix < previousUnix {
				return fmt.Errorf("epoch %d was modified at %.6f, before the previous epoch at %.6f", want, *scratch.MonModifiedUnix, previousUnix)
			}
			return nil
		})
		if scratch.MonEpoch == nil || scratch.MonModifiedUnix == nil || state.Probes[len(state.Probes)-1].Error != "" {
			// An unreadable epoch ends the timeline: a later epoch cannot be
			// compared with a predecessor that was not read.
			state.TimelineSkipped = last - epoch + 1
			return
		}
		snapshot := radosLateSnapshotOf(&scratch)
		changes, compared := radosLateDiff(previous, snapshot)
		if changes == nil {
			changes = []string{}
		}
		state.Timeline = append(state.Timeline, radosLateEpoch{Epoch: epoch, Modified: scratch.MonModified, ModifiedUnix: *scratch.MonModifiedUnix, Compared: compared, Changes: changes})
		previous, previousUnix = snapshot, *scratch.MonModifiedUnix
	}
}

// A skipped or omitted epoch is never read as "no change": when the timeline
// has gaps, the per-operation epoch join says so in the record's notes.
func noteRadosLateTimelineGap(state *radosLateClusterState) {
	if state.TimelineSkipped > 0 {
		state.Notes = append(state.Notes, fmt.Sprintf("%d epochs after the last one read were not read; epoch_committed_at_initiated is the newest observation, not the exact epoch", state.TimelineSkipped))
	}
}

// joinRadosLateOps fills the two derived per-operation fields from facts that
// were all read: the op's stamped epoch against the newest map osd.0 stored,
// and the newest epoch already committed when the op was initiated.
func joinRadosLateOps(state, baseline *radosLateClusterState) {
	type committed struct {
		epoch int
		unix  float64
	}
	var epochs []committed
	if baseline != nil && baseline.MonEpoch != nil && baseline.MonModifiedUnix != nil {
		epochs = append(epochs, committed{*baseline.MonEpoch, *baseline.MonModifiedUnix})
	}
	for _, entry := range state.Timeline {
		epochs = append(epochs, committed{entry.Epoch, entry.ModifiedUnix})
	}
	for _, list := range [][]radosLateOp{state.InFlight, state.Historic} {
		for index := range list {
			op := &list[index]
			if op.Epoch != nil && state.OSDNewestMap != nil {
				lead := *op.Epoch - *state.OSDNewestMap
				op.EpochLead = &lead
			}
			if op.InitiatedUnix == nil || len(epochs) == 0 || *op.InitiatedUnix < epochs[0].unix {
				continue
			}
			best := epochs[0].epoch
			for _, entry := range epochs {
				if entry.unix <= *op.InitiatedUnix {
					best = entry.epoch
				}
			}
			op.EpochAtInitiated = &best
		}
	}
}

// radosLateTimelineShare is the encoded size the timeline may take of the
// record before any other shedding starts, so that a large timeline cannot
// push the operation evidence out of the budget.
const radosLateTimelineShare = 6 << 10

// radosLateTimelineChangesKept is how many changes per epoch stay after the
// first timeline shedding stage.
const radosLateTimelineChangesKept = 4

func encodedRadosLateTimelineSize(state *radosLateClusterState) int {
	encoded, err := json.Marshal(state.Timeline)
	if err != nil {
		return math.MaxInt
	}
	return len(encoded)
}

// shedRadosLateTimelineChanges keeps at most keep changes per epoch and counts
// every dropped change.
func shedRadosLateTimelineChanges(state *radosLateClusterState, keep int) {
	for index := range state.Timeline {
		changes := state.Timeline[index].Changes
		if len(changes) > keep {
			state.TimelineChangesOmitted += len(changes) - keep
			state.Timeline[index].Changes = changes[:keep]
		}
	}
}

// shedRadosLateTimelineEpochs drops epochs from the tail (the earliest, which
// are the nearest to the first change, stay longest) until fits reports true
// or none is left. Dropped epochs and the changes they still held are counted.
func shedRadosLateTimelineEpochs(state *radosLateClusterState, fits func() bool) {
	for len(state.Timeline) > 0 && !fits() {
		last := len(state.Timeline) - 1
		state.TimelineChangesOmitted += len(state.Timeline[last].Changes)
		state.TimelineOmitted++
		state.Timeline = state.Timeline[:last]
	}
}

// boundRadosLateTimelineShare applies the timeline's own share of the record.
func boundRadosLateTimelineShare(state *radosLateClusterState) {
	fits := func() bool { return encodedRadosLateTimelineSize(state) <= radosLateTimelineShare }
	if fits() {
		return
	}
	state.Truncated = true
	shedRadosLateTimelineChanges(state, radosLateTimelineChangesKept)
	if fits() {
		return
	}
	shedRadosLateTimelineChanges(state, 0)
	shedRadosLateTimelineEpochs(state, fits)
}
