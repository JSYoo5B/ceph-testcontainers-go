//go:build all || integration

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// RetainedMapSampleLimit bounds the distinct status samples that one map
// wait keeps. When it is reached, the newest sample replaces the last one
// and Dropped counts the replaced samples, so the last observed status is
// always kept.
const RetainedMapSampleLimit = 32

// retainedMapSample is one distinct raw status of the retained client, with
// the probe that first saw it. OSDEpoch is the Objecter's map epoch;
// Barrier is the client's cap epoch barrier, which is a separate value and
// is not part of the readiness criterion.
type retainedMapSample struct {
	Probe       int    `json:"probe"`
	ElapsedMs   int64  `json:"elapsed_ms"`
	ID          uint64 `json:"id"`
	OSDEpoch    uint32 `json:"osd_epoch"`
	Barrier     uint32 `json:"osd_epoch_barrier"`
	Blocklisted bool   `json:"blocklisted"`
}

// retainedMapWait is the record of one wait for the retained client to hold
// a map epoch.
type retainedMapWait struct {
	Phase         string              `json:"phase"`
	TargetEpoch   uint32              `json:"target_epoch"`
	PokeCompleted bool                `json:"poke_completed"`
	Probes        int                 `json:"probes"`
	ElapsedMs     int64               `json:"elapsed_ms"`
	Samples       []retainedMapSample `json:"samples,omitempty"`
	Dropped       int                 `json:"dropped_samples,omitempty"`
	LastError     string              `json:"last_error,omitempty"`
	// Outcome is empty while the wait runs, then "reached", "timeout",
	// "timeout-late-result" (the last probe satisfied the criterion after the
	// bound ended, so it is not evidence), "identity-mismatch", "blocklisted"
	// or "probe-error".
	Outcome string `json:"outcome,omitempty"`
}

// retainedProbeFailure marks an error that a status read returned while the
// phase still ran. observe returns such an error wrapped, and returns the
// phase expiry without it, so a probe that the phase deadline canceled is not
// reported as an independent probe failure.
type retainedProbeFailure struct{ err error }

func (f retainedProbeFailure) Error() string { return f.err.Error() }
func (f retainedProbeFailure) Unwrap() error { return f.err }

// RetainedClientReceipt is the phase record of the retained-client full-map
// scenario. Fields of a phase that was not reached stay absent: zero values
// are omitted and pointers are nil.
type RetainedClientReceipt struct {
	Client         uint64             `json:"client"`
	Inode          uint64             `json:"inode"`
	DataPool       int64              `json:"data_pool"`
	Phases         []string           `json:"phases"`
	NotFullEpoch   *uint32            `json:"not_full_epoch,omitempty"`
	FullEpoch      *uint32            `json:"full_epoch,omitempty"`
	DrainedEpoch   *uint32            `json:"drained_epoch,omitempty"`
	Unprompted     *retainedMapSample `json:"unprompted,omitempty"`
	Waits          []retainedMapWait  `json:"waits,omitempty"`
	RestoreStarted bool               `json:"restore_started,omitempty"`
	RestoreError   string             `json:"restore_error,omitempty"`
	// MapEvidence has one entry for each map wait that failed, in the order
	// of the waits, so the full-map and the drained-map incidents stay apart.
	MapEvidence []retainedMapEvidence `json:"map_evidence,omitempty"`

	mapBaselines map[string]mapEvidenceBaseline
}

func NewRetainedClientReceipt(client, inode uint64, pool int64) *RetainedClientReceipt {
	return &RetainedClientReceipt{Client: client, Inode: inode, DataPool: pool}
}

// phase records that a phase completed.
func (r *RetainedClientReceipt) Phase(name string) { r.Phases = append(r.Phases, name) }

// unpromptedStatus records the raw status read before the poke.
func (r *RetainedClientReceipt) unpromptedStatus(status CephFSClientStatus) {
	sample := retainedMapSample{ID: status.ID, OSDEpoch: status.OSDEpoch, Barrier: status.Barrier, Blocklisted: status.Blocklisted}
	r.Unprompted = &sample
}

// WaitMap observes, with the bounds of observe, until the expected client
// holds target or a newer map. The criterion on the epoch is unchanged. A
// status of another client ID, and a status that reports blocklisted, end
// the wait at once and are not undone by a later status; the barrier is
// recorded and never required.
func (r *RetainedClientReceipt) WaitMap(ctx context.Context, phase string, target uint32, pokeCompleted bool, bound, probeBound, interval time.Duration, read func(context.Context) (CephFSClientStatus, error)) error {
	r.Waits = append(r.Waits, retainedMapWait{Phase: phase, TargetEpoch: target, PokeCompleted: pokeCompleted})
	wait := &r.Waits[len(r.Waits)-1]
	started := time.Now()
	var violation, lastProbeReady bool
	var outcome string
	var last CephFSClientStatus
	seen, err := Observe(ctx, bound, probeBound, interval, func(probeCtx context.Context) (CephFSClientStatus, bool, error) {
		status, err := read(probeCtx)
		wait.Probes++
		lastProbeReady = false
		if err != nil {
			wait.LastError = err.Error()
			return status, false, retainedProbeFailure{err}
		}
		wait.LastError = ""
		last = status
		sample := retainedMapSample{Probe: wait.Probes, ElapsedMs: time.Since(started).Milliseconds(), ID: status.ID, OSDEpoch: status.OSDEpoch, Barrier: status.Barrier, Blocklisted: status.Blocklisted}
		if last := len(wait.Samples) - 1; last < 0 || !sameRetainedStatus(wait.Samples[last], sample) {
			if len(wait.Samples) == RetainedMapSampleLimit {
				wait.Samples = wait.Samples[:last]
				wait.Dropped++
			}
			wait.Samples = append(wait.Samples, sample)
		}
		switch {
		case status.ID != r.Client:
			violation, outcome = true, "identity-mismatch"
			return status, false, fmt.Errorf("status of client %d, want the retained client %d", status.ID, r.Client)
		case status.Blocklisted:
			violation, outcome = true, "blocklisted"
			return status, false, fmt.Errorf("client %d reports blocklisted at e%d", status.ID, status.OSDEpoch)
		}
		lastProbeReady = status.OSDEpoch >= target
		return status, lastProbeReady, nil
	})
	wait.ElapsedMs = seen.Elapsed.Milliseconds()
	switch {
	case err == nil:
		wait.Outcome = "reached"
	case violation:
		wait.Outcome = outcome
	case errors.As(err, new(retainedProbeFailure)):
		wait.Outcome = "probe-error"
	case lastProbeReady:
		wait.Outcome = "timeout-late-result"
	default:
		wait.Outcome = "timeout"
	}
	if err != nil {
		return fmt.Errorf("client status %s: target e%d, want client %d: last client %d e%d barrier %d blocklisted=%t, %d probes, poke completed=%t: %w",
			phase, target, r.Client, last.ID, last.OSDEpoch, last.Barrier, last.Blocklisted, wait.Probes, pokeCompleted, err)
	}
	return nil
}

func sameRetainedStatus(a, b retainedMapSample) bool {
	return a.ID == b.ID && a.OSDEpoch == b.OSDEpoch && a.Barrier == b.Barrier && a.Blocklisted == b.Blocklisted
}

// line is the receipt as one log line. It is emitted before the full ratios
// are restored, so a restore that follows cannot be mistaken for the state
// that failed.
func (r *RetainedClientReceipt) Line() string {
	stage := "pre-restore"
	if r.RestoreStarted {
		stage = "restore-started"
	}
	out, err := json.Marshal(r)
	if err != nil {
		return fmt.Sprintf("CEPHFS_OSD_FULL_PHASE_RECEIPT %s unmarshalable: %v", stage, err)
	}
	return fmt.Sprintf("CEPHFS_OSD_FULL_PHASE_RECEIPT %s %s", stage, out)
}
