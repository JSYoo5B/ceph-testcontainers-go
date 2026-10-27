//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// The types below model, without Ceph or Docker, the driver of the first
// bounded step of a private mixed-version upgrade: the transition of ONE
// retained MON store from the source image to the destination image, followed
// by an acknowledged workload with exact data checks across the mixed
// interval. It does not decide anything itself. Every verdict comes from a
// merged judge:
//
//   - upgradeSequence/upgradeLedger: admission, step order, store ownership
//     and the settled-evidence check (inventory equals the checkpoint, quorum
//     restored, PGs settled);
//   - upgradeMonHandoff: the retained address vector, checked before any
//     writer starts and never followed by mkfs or a membership change;
//   - upgradeCollector: the eight read-only observations, bound to the phase
//     and the executor generation;
//   - upgradeWorkloadLedger (new, below): an operation is acknowledged only by
//     a successful return, and acknowledged data must read back byte for byte.
//
// The container effects are an injected seam (upgradeMonEffects) so that the
// driver is Docker-free. A native adapter for that seam does not exist yet,
// so this is orchestration and failure-path evidence only, not native upgrade
// evidence.
//
// Rules the driver keeps:
//
//   - Admission first: nothing is fenced or started unless the baseline
//     workload was acknowledged and read back exactly, and the sequence
//     admitted the transition (approved pair, preflight, order, store owner).
//   - No replay: the destination is created at most once per transition and
//     a workload write is submitted at most once. After a start attempt the
//     only allowed action is to observe that attempt. An unknown outcome is
//     classified and kept, and a later readback never promotes it.
//   - Every outcome is a record: the receipt is handed to OnReceipt before
//     cleanup runs, and the first failure and the cleanup failure are kept
//     apart.
//   - The sequence is not finalized. One settled MON transition is a
//     checkpoint, not a completed upgrade.

var (
	// errUpgradeWriteRejected marks a write failure that proves the operation
	// was not applied (for example a local validation error before submission).
	// Any other write error leaves the outcome unknown.
	errUpgradeWriteRejected = errors.New("upgrade workload write rejected before submission")
	errUpgradeStepConfig    = errors.New("upgrade MON step is misconfigured")
	// errUpgradeStepConsumed is returned by every call after the first, whether
	// the first call is finished, failed, cancelled or still running.
	errUpgradeStepConsumed = errors.New("upgrade MON step was already run; its receipt is kept")
)

// upgradeMonEffects is the only seam to containers. Fence stops the source
// writer of the retained store and reports the identity it stopped.
// SourcePresent reports whether any source container still exists. Retained
// reports whether store.db exists and the endpoint reserved for the
// destination. Launch creates and starts the destination instance with the
// handoff plan. Observe reports what the recorded instance natively is.
// Dispose removes the destination instances the driver names.
type upgradeMonEffects interface {
	Fence(ctx context.Context, daemon upgradeDaemonID) (upgradeDaemonID, error)
	SourcePresent(ctx context.Context, daemon upgradeDaemonID) (bool, error)
	Retained(ctx context.Context, daemon upgradeDaemonID) (hasStore bool, reserved upgradeMonVector, err error)
	Launch(ctx context.Context, instance string, plan upgradeMonPlan) error
	Observe(ctx context.Context, instance string) (upgradeNativeEvidence, error)
	Dispose(ctx context.Context, instances []string) error
}

// upgradeWorkload is the client side. Write returns nil only when the
// cluster acknowledged the whole object. ReadFresh uses a connection that
// never wrote; present is false only for a definite not-found.
type upgradeWorkload interface {
	Write(ctx context.Context, object string, data []byte) error
	ReadFresh(ctx context.Context, object string) (data []byte, present bool, err error)
}

func isNilUpgradeValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// ---- workload ledger --------------------------------------------------------

type upgradeWriteOutcome int

const (
	// upgradeWriteIssued is the zero value: intent recorded, no outcome yet.
	upgradeWriteIssued upgradeWriteOutcome = iota
	upgradeWriteAcked
	upgradeWriteRejected
	upgradeWriteUnknown
)

func (o upgradeWriteOutcome) String() string {
	return [...]string{"issued", "acknowledged", "rejected", "unknown"}[o]
}

// upgradeWorkloadPayload is the exact content of write number seq.
func upgradeWorkloadPayload(seq int) []byte {
	data := []byte(fmt.Sprintf("upgrade-seq=%d;", seq))
	for i := 0; i < 65539+seq*4096; i++ {
		data = append(data, byte((i*13+seq)%251))
	}
	return data
}

func upgradeDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type upgradeWriteRecord struct {
	Object  string
	Phase   string
	Seq     int
	SHA256  string
	Outcome upgradeWriteOutcome
	Err     error
}

// upgradeWorkloadLedger records each write intent before the call and its
// outcome after it. A write is never retried.
type upgradeWorkloadLedger struct {
	records []*upgradeWriteRecord
}

func (l *upgradeWorkloadLedger) write(ctx context.Context, w upgradeWorkload, phase string) *upgradeWriteRecord {
	seq := len(l.records) + 1
	data := upgradeWorkloadPayload(seq)
	rec := &upgradeWriteRecord{Object: fmt.Sprintf("upgrade-%s-%03d", phase, seq), Phase: phase, Seq: seq, SHA256: upgradeDigest(data)}
	l.records = append(l.records, rec) // intent first
	err := w.Write(ctx, rec.Object, data)
	switch {
	case err == nil:
		rec.Outcome = upgradeWriteAcked
	case errors.Is(err, errUpgradeWriteRejected):
		rec.Outcome, rec.Err = upgradeWriteRejected, err
	default:
		rec.Outcome, rec.Err = upgradeWriteUnknown, err
	}
	return rec
}

// upgradeWorkloadVerdict classifies every record after a fresh readback.
type upgradeWorkloadVerdict struct {
	Classes      map[string]string // object -> class
	Violations   []string          // data the ledger says must hold and does not
	Inconclusive []string          // readback unavailable or malformed
	Unknown      []string          // unknown writes kept as unknown
}

// OK reports that every acknowledged object read back exactly and nothing
// contradicts the ledger. Unknown writes do not fail it and are not counted
// as acknowledged.
func (v upgradeWorkloadVerdict) OK() bool {
	return len(v.Violations) == 0 && len(v.Inconclusive) == 0
}

func (l *upgradeWorkloadLedger) acknowledged() int {
	n := 0
	for _, r := range l.records {
		if r.Outcome == upgradeWriteAcked {
			n++
		}
	}
	return n
}

// verify reads every recorded object once through a fresh connection. A read
// is never repeated until it agrees, and a readback never changes a record.
func (l *upgradeWorkloadLedger) verify(ctx context.Context, w upgradeWorkload) upgradeWorkloadVerdict {
	v := upgradeWorkloadVerdict{Classes: map[string]string{}}
	for _, r := range l.records {
		data, present, err := w.ReadFresh(ctx, r.Object)
		class := ""
		switch {
		case err != nil:
			class = "readback-unavailable"
			v.Inconclusive = append(v.Inconclusive, r.Object+": "+err.Error())
		case !present && data != nil, present && data == nil:
			class = "readback-malformed"
			v.Inconclusive = append(v.Inconclusive, r.Object+": presence and data disagree")
		default:
			exact := present && bytes.Equal(data, upgradeWorkloadPayload(r.Seq)) && upgradeDigest(data) == r.SHA256
			switch r.Outcome {
			case upgradeWriteAcked:
				switch {
				case exact:
					class = "acknowledged-exact"
				case !present:
					class = "acknowledged-missing"
					v.Violations = append(v.Violations, r.Object+": acknowledged object is missing")
				default:
					class = "acknowledged-corrupt"
					v.Violations = append(v.Violations, r.Object+": acknowledged object differs from what was written")
				}
			case upgradeWriteUnknown:
				v.Unknown = append(v.Unknown, r.Object)
				switch {
				case exact:
					class = "unknown-applied"
				case !present:
					class = "unknown-absent"
				default:
					class = "unknown-corrupt"
					v.Violations = append(v.Violations, r.Object+": unknown write left different content")
				}
			case upgradeWriteRejected:
				if present {
					class = "rejected-present"
					v.Violations = append(v.Violations, r.Object+": rejected write is present")
				} else {
					class = "rejected-absent"
				}
			default:
				class = "issued-without-outcome"
				v.Inconclusive = append(v.Inconclusive, r.Object+": no recorded outcome")
			}
		}
		v.Classes[r.Object] = class
	}
	sort.Strings(v.Violations)
	sort.Strings(v.Inconclusive)
	sort.Strings(v.Unknown)
	return v
}

// ---- the step driver --------------------------------------------------------

type upgradeMonStepConfig struct {
	Sequence  *upgradeSequence
	Targets   *upgradeTargets
	Handoff   *upgradeMonHandoff
	Collector *upgradeCollector
	Effects   upgradeMonEffects
	Workload  upgradeWorkload
	// Baseline and Mixed are the number of writes before the fault and after
	// the transition settled.
	Baseline, Mixed int
	// OnReceipt receives the receipt before cleanup destroys anything.
	OnReceipt func(upgradeMonReceipt)
}

// upgradeMonReceipt is what the step reports on every outcome.
type upgradeMonReceipt struct {
	Events     []string
	Transition string
	Step       upgradeStep
	Instances  []string // destination instances attempted, all disposed by cleanup
	Settled    bool     // the sequence accepted the settled evidence
	Workload   upgradeWorkloadVerdict
	Writes     []upgradeWriteRecord
	Err        error // first failure
	CleanupErr error // cleanup failures, never merged into Err
}

type upgradeMonStep struct {
	cfg upgradeMonStepConfig

	mu       sync.Mutex
	consumed bool
	first    *upgradeMonReceipt // set once the first call finished
}

// consume claims the single run atomically. It is the first thing run does,
// before any workload, effect or cleanup, so a second call (also a concurrent
// one, or one after a failed or cancelled first call) performs nothing.
func (s *upgradeMonStep) consume() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumed {
		return false
	}
	s.consumed = true
	return true
}

// firstReceipt returns a copy of the receipt of the one run, once it finished.
// A refused call never replaces it.
func (s *upgradeMonStep) firstReceipt() (upgradeMonReceipt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == nil {
		return upgradeMonReceipt{}, false
	}
	rc := *s.first
	rc.Events = slices.Clone(rc.Events)
	rc.Instances = slices.Clone(rc.Instances)
	rc.Writes = slices.Clone(rc.Writes)
	return rc, true
}

func newUpgradeMonStep(cfg upgradeMonStepConfig) (*upgradeMonStep, error) {
	switch {
	case cfg.Sequence == nil, cfg.Targets == nil, cfg.Handoff == nil, cfg.Collector == nil:
		return nil, fmt.Errorf("%w: sequence, targets, handoff and collector are required", errUpgradeStepConfig)
	case isNilUpgradeValue(cfg.Effects):
		return nil, fmt.Errorf("%w: no container effects", errUpgradeStepConfig)
	case isNilUpgradeValue(cfg.Workload):
		return nil, fmt.Errorf("%w: no workload", errUpgradeStepConfig)
	case cfg.Baseline < 1 || cfg.Mixed < 1:
		return nil, fmt.Errorf("%w: baseline and mixed workloads need at least one write", errUpgradeStepConfig)
	case len(cfg.Sequence.plan) == 0 || cfg.Sequence.plan[0].Role != "mon":
		return nil, fmt.Errorf("%w: the first planned daemon is not a MON", errUpgradeStepConfig)
	}
	return &upgradeMonStep{cfg: cfg}, nil
}

// run performs the step once. It always returns a receipt. Cleanup runs on
// every outcome, with a context that survives cancellation of ctx.
func (s *upgradeMonStep) run(ctx context.Context) upgradeMonReceipt {
	if !s.consume() {
		// No ledger, write, effect, receipt callback or cleanup: the original
		// receipt (see firstReceipt) is the only record of the run.
		return upgradeMonReceipt{Err: errUpgradeStepConsumed}
	}
	rc := s.execute(ctx)
	s.mu.Lock()
	kept := rc
	kept.Events = slices.Clone(rc.Events)
	kept.Instances = slices.Clone(rc.Instances)
	kept.Writes = slices.Clone(rc.Writes)
	s.first = &kept
	s.mu.Unlock()
	return rc
}

func (s *upgradeMonStep) execute(ctx context.Context) upgradeMonReceipt {
	var (
		rc     upgradeMonReceipt
		ledger upgradeWorkloadLedger
	)
	event := func(format string, args ...any) { rc.Events = append(rc.Events, fmt.Sprintf(format, args...)) }
	fail := func(err error) upgradeMonReceipt {
		if rc.Err == nil {
			rc.Err = err
		}
		s.cfg.Sequence.halt(err)
		event("halted: %v", err)
		return s.finish(ctx, rc, &ledger)
	}
	cfg := s.cfg
	seq := cfg.Sequence

	// 1. Baseline workload, before any fault.
	for i := 0; i < cfg.Baseline; i++ {
		if rec := ledger.write(ctx, cfg.Workload, "baseline"); rec.Outcome != upgradeWriteAcked {
			event("baseline write %s: %s", rec.Object, rec.Outcome)
			return s.finishWithoutFault(ctx, rc, &ledger, fmt.Errorf("baseline write %s was %s: %v", rec.Object, rec.Outcome, rec.Err))
		}
	}
	if v := ledger.verify(ctx, cfg.Workload); !v.OK() || len(v.Unknown) > 0 {
		event("baseline readback is not exact")
		rc.Workload = v
		return s.finishWithoutFault(ctx, rc, &ledger, fmt.Errorf("baseline readback is not exact: violations %v, inconclusive %v", v.Violations, v.Inconclusive))
	}
	event("baseline workload acknowledged and exact (%d objects)", ledger.acknowledged())

	// 2. Admission by the sequence and the retained record.
	id, daemon, err := seq.admit()
	if err != nil {
		return s.finishWithoutFault(ctx, rc, &ledger, fmt.Errorf("admission: %w", err))
	}
	rc.Transition = id
	retained := cfg.Handoff.record()
	if daemon.Role != "mon" || daemon.ID != retained.Name || daemon.FSID != retained.FSID || daemon.StoreID != retained.StoreID {
		return fail(fmt.Errorf("admitted %+v is not the retained MON %s", daemon, retained.Name))
	}
	event("admitted %s", id)

	// 3. Fence, then prove the source container is gone.
	observed, err := cfg.Effects.Fence(ctx, daemon)
	if err != nil {
		return fail(fmt.Errorf("fence %s: %w", daemon.key(), err))
	}
	if err := seq.ledger.fence(id, observed); err != nil {
		return fail(err)
	}
	event("fenced %s", daemon.key())
	present, err := cfg.Effects.SourcePresent(ctx, daemon)
	if err := seq.ledger.oldAbsent(id, present, err); err != nil {
		return fail(err)
	}
	event("source container absent")

	// 4. Handoff admission before any writer exists.
	instance := "dst-" + id
	hasStore, reserved, err := cfg.Effects.Retained(ctx, daemon)
	if err != nil {
		return fail(fmt.Errorf("inspect retained store: %w", err))
	}
	plan, err := cfg.Handoff.admit(upgradeMonLaunch{Transition: id, Name: retained.Name, FSID: retained.FSID, StoreID: retained.StoreID, HasStore: hasStore, Reserved: reserved})
	if err != nil {
		return fail(fmt.Errorf("handoff: %w", err))
	}
	event("handoff admitted for %s", retained.Name)

	// 5. Record the attempt, then launch exactly once. The destination image
	// changed the target from here on, whatever Launch returns.
	if err := seq.ledger.startAttempted(id, instance); err != nil {
		return fail(err)
	}
	rc.Instances = append(rc.Instances, instance)
	if err := cfg.Targets.advance(daemon.key()); err != nil {
		return fail(err)
	}
	launchErr := cfg.Effects.Launch(ctx, instance, plan)
	if launchErr != nil {
		rc.Err = fmt.Errorf("launch %s: %w", instance, launchErr)
		event("launch returned an error; the attempt is observed, never repeated")
	}

	// 6. Observe the recorded attempt and confirm only on native evidence.
	evidence, err := cfg.Effects.Observe(ctx, instance)
	if err != nil {
		return fail(fmt.Errorf("observe %s: %w", instance, err))
	}
	if err := seq.ledger.confirm(id, evidence); err != nil {
		return fail(err)
	}
	event("native-confirmed %s", instance)
	if launchErr != nil {
		return fail(rc.Err)
	}

	// 7. Fresh observations of the new phase settle the sequence.
	phase := cfg.Targets.begin("after " + daemon.key())
	want, err := upgradeCheckpointAfter(seq.plan, seq.pair, 1)
	if err != nil {
		return fail(err)
	}
	if err := cfg.Collector.collectAndSettle(ctx, phase, daemon.key(), want, seq); err != nil {
		return fail(fmt.Errorf("settle %s: %w", daemon.key(), err))
	}
	rc.Settled = true
	event("settled: mixed-version checkpoint accepted")

	// 8. Workload across the mixed interval, then the exact check of
	// everything acknowledged so far, including the baseline.
	for i := 0; i < cfg.Mixed; i++ {
		rec := ledger.write(ctx, cfg.Workload, "mixed")
		event("mixed write %s: %s", rec.Object, rec.Outcome)
	}
	rc.Workload = ledger.verify(ctx, cfg.Workload)
	if !rc.Workload.OK() {
		return fail(fmt.Errorf("workload check: violations %v, inconclusive %v", rc.Workload.Violations, rc.Workload.Inconclusive))
	}
	event("workload exact: %d acknowledged, %d unknown", ledger.acknowledged(), len(rc.Workload.Unknown))
	return s.finish(ctx, rc, &ledger)
}

// finishWithoutFault ends a run that stopped before the source was fenced.
func (s *upgradeMonStep) finishWithoutFault(ctx context.Context, rc upgradeMonReceipt, ledger *upgradeWorkloadLedger, err error) upgradeMonReceipt {
	if rc.Err == nil {
		rc.Err = err
	}
	rc.Events = append(rc.Events, "stopped before the fault: "+err.Error())
	return s.finish(ctx, rc, ledger)
}

// finish emits the receipt, then cleans up.
func (s *upgradeMonStep) finish(ctx context.Context, rc upgradeMonReceipt, ledger *upgradeWorkloadLedger) upgradeMonReceipt {
	if rc.Transition != "" {
		if step, err := s.cfg.Sequence.ledger.stepOf(rc.Transition); err == nil {
			rc.Step = step
		}
	}
	rc.Writes = rc.Writes[:0]
	for _, r := range ledger.records {
		rc.Writes = append(rc.Writes, *r)
	}
	if s.cfg.OnReceipt != nil {
		snapshot := rc
		snapshot.Events = slices.Clone(rc.Events)
		snapshot.Instances = slices.Clone(rc.Instances)
		snapshot.Writes = slices.Clone(rc.Writes)
		s.cfg.OnReceipt(snapshot)
	}
	if len(rc.Instances) > 0 {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		if err := s.cfg.Effects.Dispose(cleanup, slices.Clone(rc.Instances)); err != nil {
			rc.CleanupErr = fmt.Errorf("dispose %s: %w", strings.Join(rc.Instances, ","), err)
		}
	}
	return rc
}
