//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// pgProgress is the part of a replicated PG's "pg query" that tells how far a
// backfill has come, and in which interval of the PG it is measured. It is
// decoded apart from pgQuery on purpose: pgRecoveryPath classifies a recovery
// that is still held, where the target's last_backfill must be MIN, and has to
// keep refusing an advanced cursor. Fields the native report may omit are
// pointers, so that an absent field is told from a zero one.
//
// The fields come from the pinned Ceph source: PeeringState::dump_peering_state
// (epoch, up, acting, info with its history, peer_info) and
// PrimaryLogPG::dump_recovery_info under the Active state's recovery_progress
// (waiting_on_backfill, last_backfill_started, backfill_info,
// peer_backfill_info, backfills_in_flight).
type pgProgress struct {
	State  string  `json:"state"`
	Epoch  *uint64 `json:"epoch"`
	Up     []int   `json:"up"`
	Acting []int   `json:"acting"`
	Info   struct {
		History struct {
			SameIntervalSince *uint64 `json:"same_interval_since"`
			SamePrimarySince  *uint64 `json:"same_primary_since"`
		} `json:"history"`
	} `json:"info"`
	PeerInfo []struct {
		Peer         string `json:"peer"`
		LastBackfill string `json:"last_backfill"`
	} `json:"peer_info"`
	RecoveryState []struct {
		Name     string            `json:"name"`
		Progress *recoveryProgress `json:"recovery_progress"`
	} `json:"recovery_state"`
}

// recoveryProgress is PrimaryLogPG::dump_recovery_info as the Active state
// reports it.
type recoveryProgress struct {
	BackfillTargets     []string          `json:"backfill_targets"`
	WaitingOnBackfill   *[]string         `json:"waiting_on_backfill"`
	LastBackfillStarted *string           `json:"last_backfill_started"`
	BackfillInfo        *backfillInterval `json:"backfill_info"`
	PeerBackfillInfo    []json.RawMessage `json:"peer_backfill_info"`
	InFlight            *[]string         `json:"backfills_in_flight"`
}

// backfillInterval is the primary's own scanned range, ahead of its cursor.
type backfillInterval struct {
	Begin   string            `json:"begin"`
	End     string            `json:"end"`
	Objects []json.RawMessage `json:"objects"`
}

func parsePGProgress(data []byte) (pgProgress, error) {
	var progress pgProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return progress, fmt.Errorf("pg query: %w", err)
	}
	if progress.State == "" {
		return progress, fmt.Errorf("pg query without a state: %.256s", data)
	}
	return progress, nil
}

// cursorOf returns the primary's last_backfill for peer osd, a native
// identity: "MIN", "MAX" or a printed object position. The positions are not
// ordered by their text, so callers compare them only for equality.
func (p pgProgress) cursorOf(osd int) (string, bool) {
	for _, peer := range p.PeerInfo {
		if id, err := pgShardOSD(peer.Peer); err == nil && id == osd {
			return peer.LastBackfill, peer.LastBackfill != ""
		}
	}
	return "", false
}

// progress returns the primary's recovery_progress section, if it has one.
func (p pgProgress) progress() (*recoveryProgress, bool) {
	for _, state := range p.RecoveryState {
		if state.Progress != nil {
			return state.Progress, true
		}
	}
	return nil, false
}

// backfillBaseline is what a held backfill to one target looks like before it
// is released: the interval it is measured in and the cursor it starts from.
type backfillBaseline struct {
	Epoch, SameIntervalSince, SamePrimarySince uint64
	Up, Acting                                 []int
	Target                                     int
	Cursor                                     string
}

// backfillHeld accepts a sample as the baseline of a backfill to target, taken
// while the backfill is held: target is an up member outside the acting set,
// the primary's only backfill target, still at MIN, and the PG reports the
// interval it is in.
func backfillHeld(p pgProgress, target int) (backfillBaseline, error) {
	var base backfillBaseline
	if p.Epoch == nil || p.Info.History.SameIntervalSince == nil || p.Info.History.SamePrimarySince == nil {
		return base, fmt.Errorf("pg query without its epoch or interval: %+v", p)
	}
	progress, ok := p.progress()
	if !ok {
		return base, fmt.Errorf("the PG reports no recovery progress: state %s", p.State)
	}
	cursor, found := p.cursorOf(target)
	switch {
	case !slices.Contains(p.Up, target) || slices.Contains(p.Acting, target):
		return base, fmt.Errorf("osd.%d is not a backfill destination: up %v, acting %v", target, p.Up, p.Acting)
	case len(progress.BackfillTargets) != 1:
		return base, fmt.Errorf("backfill targets %v, want only osd.%d", progress.BackfillTargets, target)
	case osdOf(progress.BackfillTargets[0]) != target:
		return base, fmt.Errorf("backfill targets %v, want osd.%d", progress.BackfillTargets, target)
	case !found || cursor != "MIN":
		return base, fmt.Errorf("osd.%d last_backfill %q (found %t), want MIN while held", target, cursor, found)
	case !strings.Contains(p.State, "backfill"):
		return base, fmt.Errorf("state %s is not a backfill", p.State)
	}
	return backfillBaseline{
		Epoch: *p.Epoch, SameIntervalSince: *p.Info.History.SameIntervalSince, SamePrimarySince: *p.Info.History.SamePrimarySince,
		Up: slices.Clone(p.Up), Acting: slices.Clone(p.Acting), Target: target, Cursor: cursor,
	}, nil
}

func osdOf(shard string) int {
	osd, err := pgShardOSD(shard)
	if err != nil {
		return -1
	}
	return osd
}

// progressVerdict is the outcome of one progress sample against its baseline.
type progressVerdict string

const (
	// progressWait: the backfill has not yet advanced, or the sample cannot
	// show that it has. Keep sampling.
	progressWait progressVerdict = "wait"
	// progressReached: the target's cursor moved past its baseline, is not
	// final, and the primary still lists work ahead of it.
	progressReached progressVerdict = "reached"
	// progressTerminal: the transfer is complete, so the interruption window
	// is gone. The phase was not reached.
	progressTerminal progressVerdict = "terminal"
	// progressInvalid: the sample is from another interval or placement, so
	// the baseline no longer applies.
	progressInvalid progressVerdict = "invalid"
)

// backfillProgress compares one sample of the primary with the baseline of a
// held backfill that has since been released. It admits a second fault only
// when all of these hold in the baseline's own interval:
//
//   - the PG, its up and acting sets and its primary interval are unchanged
//     (the map epoch may advance, for example when a flag is cleared);
//   - the target is still the only backfill target and the PG still reports a
//     running backfill (not just a waiting one);
//   - the target's cursor differs from the baseline, so at least one object was
//     pushed and acknowledged (the primary moves the peer's last_backfill only
//     after pushes complete), and is not MIN or MAX;
//   - the primary still lists work ahead: objects in its scanned interval or
//     objects in flight.
//
// An in-flight entry alone shows only that work started (the primary inserts it
// before calling the backend, which can still fail), so it never replaces the
// cursor. Absent fields never pass.
func backfillProgress(base backfillBaseline, p pgProgress) (progressVerdict, string) {
	if p.Epoch == nil || p.Info.History.SameIntervalSince == nil || p.Info.History.SamePrimarySince == nil {
		return progressWait, "the sample has no epoch or interval"
	}
	if *p.Info.History.SameIntervalSince != base.SameIntervalSince || *p.Info.History.SamePrimarySince != base.SamePrimarySince ||
		!slices.Equal(p.Up, base.Up) || !slices.Equal(p.Acting, base.Acting) {
		return progressInvalid, fmt.Sprintf("interval since %d (primary since %d), up %v, acting %v, baseline %d (%d), %v, %v",
			*p.Info.History.SameIntervalSince, *p.Info.History.SamePrimarySince, p.Up, p.Acting,
			base.SameIntervalSince, base.SamePrimarySince, base.Up, base.Acting)
	}
	if *p.Epoch < base.Epoch {
		return progressInvalid, fmt.Sprintf("map epoch %d is before the baseline's %d", *p.Epoch, base.Epoch)
	}
	cursor, found := p.cursorOf(base.Target)
	if !found {
		return progressWait, fmt.Sprintf("osd.%d has no cursor", base.Target)
	}
	if cursor == "MAX" {
		return progressTerminal, fmt.Sprintf("osd.%d last_backfill is MAX in state %s", base.Target, p.State)
	}
	progress, ok := p.progress()
	if !ok {
		return progressWait, "the PG reports no recovery progress: state " + p.State
	}
	if len(progress.BackfillTargets) != 1 || osdOf(progress.BackfillTargets[0]) != base.Target {
		return progressWait, fmt.Sprintf("backfill targets %v, want only osd.%d", progress.BackfillTargets, base.Target)
	}
	if !strings.Contains(p.State, "backfilling") {
		return progressWait, "state " + p.State + " is not a running backfill"
	}
	if cursor == "MIN" || cursor == base.Cursor {
		return progressWait, fmt.Sprintf("osd.%d last_backfill %s has not left the baseline %s", base.Target, cursor, base.Cursor)
	}
	if progress.LastBackfillStarted == nil || progress.InFlight == nil || progress.BackfillInfo == nil {
		return progressWait, "recovery progress without its detail fields"
	}
	if len(*progress.InFlight) == 0 && len(progress.BackfillInfo.Objects) == 0 {
		return progressWait, "the primary lists no work ahead of the cursor"
	}
	return progressReached, fmt.Sprintf("osd.%d last_backfill %s (baseline %s), %d in flight, %d scanned ahead", base.Target, cursor, base.Cursor, len(*progress.InFlight), len(progress.BackfillInfo.Objects))
}
