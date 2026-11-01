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
	// ServingMaps has one entry for each wait, before a poke, for the PG
	// that serves the poke to apply the target map.
	ServingMaps []servingMapWait `json:"serving_maps,omitempty"`

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

// ServingMapView is one reading of the PG that serves a poke: the epoch of
// the map the PG has applied ("pg query" prints it as "epoch") and the
// newest map its primary OSD has received ("status" newest_map). The OSD
// stores a received map before it applies it, so NewestMap can be ahead of
// PGEpoch. Either stays nil when its read failed.
type ServingMapView struct {
	PGEpoch   *uint32 `json:"pg_epoch,omitempty"`
	NewestMap *uint64 `json:"primary_newest_map,omitempty"`
}

// servingMapWait is the record of one wait for the serving PG.
type servingMapWait struct {
	Phase       string         `json:"phase"`
	Object      string         `json:"object"`
	PGID        string         `json:"pgid"`
	Primary     int            `json:"primary"`
	TargetEpoch uint32         `json:"target_epoch"`
	Probes      int            `json:"probes"`
	ElapsedMs   int64          `json:"elapsed_ms"`
	Last        ServingMapView `json:"last"`
	LastError   string         `json:"last_error,omitempty"`
	// Outcome is empty while the wait runs, then "applied" or "timeout".
	Outcome string `json:"outcome,omitempty"`
}

// WaitServingMap observes, with the bounds of observe, until the PG that
// serves object has applied target or a newer map. The OSD answers a client
// operation with the maps between the operation's epoch and the map of the
// PG that runs it, so a poke sent before the PG applied target can leave the
// client below it; the primary's received newest map is recorded and never
// accepted in place of the PG's. A failed read is recorded and retried.
func (r *RetainedClientReceipt) WaitServingMap(ctx context.Context, phase, object, pgid string, primary int, target uint32, bound, probeBound, interval time.Duration, read func(context.Context) (ServingMapView, error)) error {
	r.ServingMaps = append(r.ServingMaps, servingMapWait{Phase: phase, Object: object, PGID: pgid, Primary: primary, TargetEpoch: target})
	wait := &r.ServingMaps[len(r.ServingMaps)-1]
	seen, err := Observe(ctx, bound, probeBound, interval, func(probeCtx context.Context) (ServingMapView, bool, error) {
		view, err := read(probeCtx)
		wait.Probes++
		wait.Last = view
		wait.LastError = ""
		if err != nil {
			wait.LastError = err.Error()
		}
		return view, view.PGEpoch != nil && *view.PGEpoch >= target, nil
	})
	wait.ElapsedMs = seen.Elapsed.Milliseconds()
	if err != nil {
		wait.Outcome = "timeout"
		pg, newest := "unread", "unread"
		if wait.Last.PGEpoch != nil {
			pg = fmt.Sprintf("e%d", *wait.Last.PGEpoch)
		}
		if wait.Last.NewestMap != nil {
			newest = fmt.Sprintf("e%d", *wait.Last.NewestMap)
		}
		return fmt.Errorf("serving map %s: pg %s of %s on osd.%d at %s (primary newest %s), want e%d, %d probes, last error %q: %w",
			phase, pgid, object, primary, pg, newest, target, wait.Probes, wait.LastError, err)
	}
	wait.Outcome = "applied"
	return nil
}

// ParsePGMapEpoch reads the epoch of the map a PG has applied from the
// output of "pg <pgid> query".
func ParsePGMapEpoch(out []byte) (uint32, error) {
	var query struct {
		Epoch *uint32 `json:"epoch"`
	}
	if err := json.Unmarshal(out, &query); err != nil {
		return 0, fmt.Errorf("pg query: %w", err)
	}
	if query.Epoch == nil || *query.Epoch == 0 {
		return 0, errors.New("pg query has no epoch")
	}
	return *query.Epoch, nil
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
