package fault

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
	"github.com/moby/moby/api/pkg/stdcopy"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// ClockHelperImage is the clock helper that ceph-testcontainers-images
// publishes for contract 1 of its clock extension: the libfaketime default
// build and its build manifest, for both linux/amd64 and linux/arm64. Pass it,
// or another image that meets the contract, to ceph.ClockConfig.WithHelperImage.
const ClockHelperImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-clock-v1"

// ClockOffset is the wall-clock offset that ApplyClockOffset gives one MON.
type ClockOffset struct {
	// Offset is added to the daemon's wall clock: a nonzero whole number of
	// seconds, at most 365 days either way. A positive offset puts the
	// daemon's clock ahead.
	Offset time.Duration
}

// ClockOffsetState is the verdict of a measurement of an offset lease.
type ClockOffsetState string

const (
	// ClockOffsetApplied: the daemon's own clock shows the lease's offset
	// after the write was acknowledged, in the process the lease admitted.
	ClockOffsetApplied ClockOffsetState = "applied"
	// ClockOffsetPending: the daemon does not show the offset yet, but the
	// measurement is still inside the interposer's cache window.
	ClockOffsetPending ClockOffsetState = "pending"
	// ClockOffsetNotApplied: the evidence shows that the daemon does not run
	// with the offset: the file was replaced or changed, the process was
	// replaced, or the daemon still shows another offset after the window.
	ClockOffsetNotApplied ClockOffsetState = "not_applied"
	// ClockOffsetInconclusive: the measurement cannot tell, for example
	// because its interval contains both zero and the offset.
	ClockOffsetInconclusive ClockOffsetState = "inconclusive"
)

// ClockOffsetStatus is one measurement of the daemon of an offset lease.
type ClockOffsetStatus struct {
	// MeasuredMin and MeasuredMax bound the daemon's own clock minus an
	// uninstrumented reference clock in the same container, at the time of
	// the measurement.
	MeasuredMin, MeasuredMax time.Duration
	State                    ClockOffsetState
	// Reason explains State.
	Reason string
	// Replaced reports that the measured process is not the one the lease
	// admitted: the daemon was restarted, and a restarted daemon reads the
	// owned file again when it starts.
	Replaced bool
}

// ClockOffsetLease owns one wall-clock offset of one armed MON. A non-nil
// handle returned with an error must be restored: the offset may have been
// written. Restore is retryable and idempotent.
type ClockOffsetLease struct {
	// ContainerID is the MON container and Daemon its name, such as mon.b.
	ContainerID, Daemon string
	// Offset is the offset that ApplyClockOffset wrote.
	Offset time.Duration

	mu       sync.Mutex
	engine   clockEngine
	target   testcontainers.Container
	ledger   clockOffsetLedger
	step     clockOffsetStep
	complete bool
}

// clockOffsetLimit bounds an offset to what the fixture purpose needs.
const clockOffsetLimit = 365 * 24 * time.Hour

// clockMeasureMargin is added to the interposer's cache window before a
// measurement that must see a write.
const clockMeasureMargin = 200 * time.Millisecond

// clockLogStampLag bounds how far a daemon's log stamp may trail its clock:
// Ceph stamps log entries with a coarse realtime clock by default
// (log_coarse_timestamps), which lags the precise clock by up to one kernel
// tick.
const clockLogStampLag = 10 * time.Millisecond

// ApplyClockOffset moves the wall clock of one MON process by spec.Offset
// while the process keeps running: the
// fixture rewrites, in place, the timestamp file that the MON's preloaded
// libfaketime interposer reads, so that a test can watch the cluster and its
// SUT react to a MON whose clock is wrong (MON_CLOCK_SKEW, timecheck, lease and
// ticket timing) and then to its restoration. ctr must be the container of an
// owned MON that was armed when it was created, with
// ceph.ClockConfig.WithMonitors or DaemonOptions.WithClockInstrumentation;
// an interposer cannot be attached to a running process, so any other target
// is refused. Ceph measures clock skew relative to the leader only: an offset
// of the leader reads as the opposite skew of every follower.
//
// Before writing, the lease admits the daemon: its own environment preloads
// the cluster's interposer with the owned file and FAKETIME_DONT_FAKE_MONOTONIC,
// has no FAKETIME, the interposer is mapped, the file holds +0, and a
// measurement of the daemon contains zero. Each measurement runs an
// uninstrumented python3 in the MON container that reads the real clock
// before and after a mon_status request on the daemon's admin socket, and
// takes the daemon's own time from the audit line the daemon logs for that
// request; the coarse clock of Ceph's log stamps makes the interval about
// 10 ms wide. CLOCK_MONOTONIC and the clock of every other process, including
// every command executed in the container, stay unchanged. ApplyClockOffset
// returns without error once a measurement after the interposer's one-second
// cache window shows the offset in the same process.
//
// Only one offset per daemon is allowed. The cluster retains the handle and
// restores it during Terminate, after paused containers are resumed and
// before daemons stop. The offset survives a restart of the MON: the owned
// file keeps it, and Restore rewrites it.
//
//purpose:condition
func ApplyClockOffset(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, spec ClockOffset) (*ClockOffsetLease, error) {
	return applyClockOffsetInCluster(ctx, c, ctr, spec.Offset, defaultClockEngine())
}

func applyClockOffsetInCluster(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, offset time.Duration, engine clockEngine) (lease *ClockOffsetLease, err error) {
	if err := requireCluster(c, "a clock offset"); err != nil {
		return nil, err
	}
	text, whole := clockOffsetText(int64(offset))
	switch {
	case offset == 0 || !whole:
		return nil, nshelper.InvalidRequest("a clock offset must be a nonzero whole number of seconds")
	case offset > clockOffsetLimit || offset < -clockOffsetLimit:
		return nil, nshelper.InvalidRequest("a clock offset must be at most 365 days either way")
	}
	err = faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) {
			_, terminated := faulthook.Terminated()
			return terminated
		}
		if ctr == nil || ctr.GetContainerID() == "" {
			return nshelper.InvalidRequest("a clock offset requires a live cluster and a started MON container")
		}
		id := ctr.GetContainerID()
		daemon, armed, owned := faulthook.ClockTarget(c, id)
		switch {
		case !owned:
			return nshelper.InvalidRequest("a clock offset needs the container of a MON that this cluster owns")
		case !armed:
			return nshelper.InvalidRequest("%s was not armed for clock offsets when it was created: arm it with ceph.ClockConfig.WithMonitors or DaemonOptions.WithClockInstrumentation", daemon)
		}
		// An incomplete previous offset is returned with the refusal; a
		// complete one stays registered until this one replaces it.
		token, previous, err := faulthook.Reserve(ctx, c, faulthook.ClockOffset, id)
		if err != nil {
			lease, _ = previous.(*ClockOffsetLease)
			return err
		}
		lease, err = applyClockOffset(ctx, engine, ctr, daemon, offset, text)
		held := faulthook.Held{}
		if lease != nil {
			held = faulthook.Held{Handle: lease, Undo: lease.Restore, Complete: lease.isComplete}
		}
		_, settleErr := faulthook.Settle(c, token, held)
		return errors.Join(err, settleErr)
	})
	return lease, err
}

func applyClockOffset(ctx context.Context, engine clockEngine, ctr testcontainers.Container, daemon string, offset time.Duration, text string) (*ClockOffsetLease, error) {
	interposer, path := faulthook.ClockFiles()
	id := ctr.GetContainerID()
	seen, err := engine.inspect(ctx, ctr, interposer, path)
	if err != nil {
		return nil, err
	}
	if err := seen.admitInstrumentation(interposer); err != nil {
		return nil, err
	}
	target := clockOffsetTarget{ID: id, PID: 1, StartTicks: seen.StartTicks}
	ledger := clockOffsetLedger{
		Target:   target,
		Owned:    clockOwnedFile{Namespace: id, Path: path, Identity: seen.File.Identity, ExpectedText: "+0"},
		CacheSec: seen.cacheSeconds(),
	}
	if r := admitClockLiveFileMode(seen.environment(), ledger.Owned, seen.File); r.Verdict != clockProvenanceValid {
		return nil, nshelper.InvalidRequest("%s does not read its owned timestamp file as a live +0 channel: %s", daemon, r.Reason)
	}
	if _, known := settleWindowNs(ledger.CacheSec); !known {
		return nil, nshelper.InvalidRequest("%s has no readable FAKETIME_CACHE_DURATION", daemon)
	}
	baseline, err := engine.measure(ctx, ctr, daemon)
	if err != nil {
		return nil, fmt.Errorf("measure %s before the offset: %w", daemon, err)
	}
	ledger.Baseline = baseline.Offset
	if r := admitClockBaseline(target, baseline.Target(id), baseline.Offset); r.Verdict != clockProvenanceValid {
		return nil, nshelper.InvalidRequest("%s is not at the reference clock before the offset: measured %s", daemon, baseline.Offset)
	}
	lease := &ClockOffsetLease{ContainerID: id, Daemon: daemon, Offset: offset, engine: engine, target: ctr, ledger: ledger}
	lease.step = clockOffsetStep{IntendedNs: int64(offset), Write: clockWriteFailed}
	file, err := engine.write(ctx, ctr, path, text)
	if err != nil {
		// The write may have landed: the handle owns its restore.
		return lease, fmt.Errorf("write the clock offset of %s: %w", daemon, err)
	}
	lease.step.Write, lease.step.AckKnown, lease.step.AckedMono, lease.step.File = clockWriteAcked, true, engine.now(), file
	lease.ledger.Owned.ExpectedText = text
	state, reason, err := lease.measureStep(ctx, true)
	if err != nil {
		return lease, err
	}
	if state != clockStepAdmitted {
		return lease, nshelper.Category(fmt.Sprintf("%s did not take the clock offset %s: %s", daemon, text, reason), ceph.ErrNativeFailure)
	}
	return lease, nil
}

// measureStep measures the daemon after the cache window that follows the
// acknowledged write and judges the step. A pending verdict is measured
// once more after the window. When record is set, the measurement becomes
// the step's evidence.
func (lease *ClockOffsetLease) measureStep(ctx context.Context, record bool) (clockStepState, string, error) {
	window, _ := settleWindowNs(lease.ledger.CacheSec)
	step := lease.step
	for attempt := 0; ; attempt++ {
		if wait := time.Duration(step.AckedMono+window-lease.engine.now()) + clockMeasureMargin; wait > 0 {
			if err := lease.engine.sleep(ctx, wait); err != nil {
				return "", "", err
			}
		}
		sample, err := lease.engine.measure(ctx, lease.target, lease.Daemon)
		if err != nil {
			return "", "", fmt.Errorf("measure %s: %w", lease.Daemon, err)
		}
		step.Target, step.Measured, step.SampleKnown, step.SampleLoMono, step.TargetSampleOK = sample.Target(lease.ContainerID), sample.Offset, true, sample.LoMono, true
		state, reason := judgeClockStep(lease.ledger, step)
		if record {
			lease.step = step
		}
		if state != clockStepPending || attempt > 0 {
			return state, reason, nil
		}
	}
}

// isComplete reports whether the lease no longer needs Restore.
func (lease *ClockOffsetLease) isComplete(ctx context.Context) (bool, error) {
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return false, err
	}
	defer lease.mu.Unlock()
	return lease.complete, nil
}

// Status measures the daemon's clock now and judges it against the lease's
// offset, for a test that needs to know that the daemon still runs with it.
// It refuses a stopped, paused or removed container, and a restored lease
// with an error matching ceph.ErrInvalidRequest: after Restore there is no
// offset left to measure.
//
//purpose:observation
func (lease *ClockOffsetLease) Status(ctx context.Context) (ClockOffsetStatus, error) {
	if lease == nil {
		return ClockOffsetStatus{}, unavailable("clock offset")
	}
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return ClockOffsetStatus{}, err
	}
	defer lease.mu.Unlock()
	if lease.complete {
		return ClockOffsetStatus{}, nshelper.InvalidRequest("the clock offset of %s was restored", lease.Daemon)
	}
	running, err := lease.engine.state(ctx, lease.ContainerID)
	if err != nil {
		return ClockOffsetStatus{}, err
	}
	if running != clockContainerRunning {
		return ClockOffsetStatus{}, nshelper.InvalidRequest("the container of %s is %s; only a running daemon can be measured", lease.Daemon, running)
	}
	if !lease.step.AckKnown {
		return ClockOffsetStatus{}, errors.New("the offset write was not acknowledged; restore the lease")
	}
	step := lease.step
	sample, err := lease.engine.measure(ctx, lease.target, lease.Daemon)
	if err != nil {
		return ClockOffsetStatus{}, fmt.Errorf("measure %s: %w", lease.Daemon, err)
	}
	step.Target, step.Measured, step.SampleKnown, step.SampleLoMono, step.TargetSampleOK = sample.Target(lease.ContainerID), sample.Offset, true, sample.LoMono, true
	file, err := lease.engine.inspect(ctx, lease.target, "", lease.ledger.Owned.Path)
	if err != nil {
		return ClockOffsetStatus{}, err
	}
	step.File = file.File
	state, reason := judgeClockStep(lease.ledger, step)
	var status ClockOffsetStatus
	status.MeasuredMin, status.MeasuredMax = time.Duration(sample.Offset.Lo), time.Duration(sample.Offset.Hi)
	status.State, status.Reason = clockStatusState(state), reason
	status.Replaced = !sameInstance(lease.ledger.Target, step.Target)
	return status, nil
}

func clockStatusState(state clockStepState) ClockOffsetState {
	switch state {
	case clockStepAdmitted:
		return ClockOffsetApplied
	case clockStepPending:
		return ClockOffsetPending
	case clockStepRejected:
		return ClockOffsetNotApplied
	default:
		return ClockOffsetInconclusive
	}
}

// Restore rewrites the owned timestamp file to +0 in place and, for a running
// daemon, returns only after a measurement after the cache window shows the
// same process back at the reference clock. A removed container needs
// nothing. A stopped container gets the +0 file back, which its daemon reads
// when it starts. A restarted daemon is a new process: Restore then requires
// the +0 file and a measurement of the new process at the reference. A paused
// container is refused and stays retryable. A nil lease has nothing to
// restore.
//
//purpose:condition,lifetime
func (lease *ClockOffsetLease) Restore(ctx context.Context) error {
	if lease == nil {
		return nil
	}
	if err := lockMutex(ctx, &lease.mu); err != nil {
		return err
	}
	defer lease.mu.Unlock()
	if lease.complete {
		return nil
	}
	state, err := lease.engine.state(ctx, lease.ContainerID)
	if err != nil {
		return err
	}
	switch state {
	case clockContainerMissing:
		// The process and its owned file went with the container.
		lease.complete = true
		return nil
	case clockContainerPaused:
		return nshelper.InvalidRequest("resume the paused container before restoring its clock offset")
	case clockContainerStopped:
		if err := lease.engine.replace(ctx, lease.target, lease.ledger.Owned.Path, "+0"); err != nil {
			return fmt.Errorf("rewrite the clock offset of stopped %s: %w", lease.Daemon, err)
		}
		lease.complete = true
		return nil
	}
	file, err := lease.engine.write(ctx, lease.target, lease.ledger.Owned.Path, "+0")
	if err != nil {
		return fmt.Errorf("restore the clock offset of %s: %w", lease.Daemon, err)
	}
	restore := clockOffsetStep{Write: clockWriteAcked, AckKnown: true, AckedMono: lease.engine.now(), File: file}
	window, _ := settleWindowNs(lease.ledger.CacheSec)
	if wait := time.Duration(restore.AckedMono+window-lease.engine.now()) + clockMeasureMargin; wait > 0 {
		if err := lease.engine.sleep(ctx, wait); err != nil {
			return err
		}
	}
	sample, err := lease.engine.measure(ctx, lease.target, lease.Daemon)
	if err != nil {
		return fmt.Errorf("measure restored %s: %w", lease.Daemon, err)
	}
	restore.Target, restore.Measured, restore.SampleKnown, restore.SampleLoMono, restore.TargetSampleOK = sample.Target(lease.ContainerID), sample.Offset, true, sample.LoMono, true
	verdict, reason := lease.judgeRestore(restore)
	if verdict != clockRestoreVerified {
		return nshelper.Category(fmt.Sprintf("%s was not restored to the reference clock: %s", lease.Daemon, reason), ceph.ErrNativeFailure)
	}
	lease.complete = true
	return nil
}

// judgeRestore judges restore against the lease's one step. The daemon may
// still show the measured offset of an admitted step, or else the intended
// one, so a restore is verified only when it discriminates the reference from
// that offset. A replaced process is judged as a fresh instance: its owned
// file must hold +0 and it must be measured at the reference.
func (lease *ClockOffsetLease) judgeRestore(restore clockOffsetStep) (clockRestoreState, string) {
	l := lease.ledger
	l.Owned.ExpectedText = "+0"
	intended := clockOffsetInterval{Known: true, Lo: lease.step.IntendedNs, Hi: lease.step.IntendedNs}
	if !sameInstance(l.Target, restore.Target) && restore.Target.complete() {
		if r := verifyClockLiveFileMode(l.Owned, restore.File); r.Verdict != clockProvenanceValid {
			return clockRestoreNotRestored, "owned file: " + r.Reason
		}
		if restore.Measured.overlaps(clockOffsetInterval{Known: true}) && !restore.Measured.overlaps(intended) {
			return clockRestoreVerified, "the restarted daemon reads the +0 file and was measured at the reference"
		}
		return clockRestoreNotRestored, "the restarted daemon was not measured at the reference"
	}
	shift := intended
	if state, _ := judgeClockStep(l, lease.step); state == clockStepAdmitted {
		shift = lease.step.Measured
	}
	return judgeClockRestore(l, restore, shift)
}

// clockContainerState is the engine's state of the target container.
type clockContainerState string

const (
	clockContainerRunning clockContainerState = "running"
	clockContainerPaused  clockContainerState = "paused"
	clockContainerStopped clockContainerState = "stopped"
	clockContainerMissing clockContainerState = "removed"
)

// clockInspection is what the in-container helper read: the daemon's start
// time, its environment and mappings, the helper's own environment and the
// owned file.
type clockInspection struct {
	StartTicks  uint64
	Environ     map[string]string
	EnvKnown    bool
	Mapped      bool
	SelfPreload bool
	File        clockObservedFile
}

// admitInstrumentation requires the daemon to preload exactly the cluster's
// interposer with an unfaked monotonic clock, and the commands executed in
// the container not to.
func (s clockInspection) admitInstrumentation(interposer string) error {
	refuse := func(format string, args ...any) error {
		return nshelper.InvalidRequest("the MON is not instrumented as its arming requires: "+format, args...)
	}
	switch {
	case !s.EnvKnown:
		return refuse("the daemon's environment could not be read")
	case s.Environ["LD_PRELOAD"] != interposer:
		return refuse("its LD_PRELOAD is %q, not the cluster's interposer", s.Environ["LD_PRELOAD"])
	case !s.Mapped:
		return refuse("the interposer is not mapped into the daemon, so its loader refused it")
	case s.Environ["FAKETIME_DONT_FAKE_MONOTONIC"] != "1":
		return refuse("FAKETIME_DONT_FAKE_MONOTONIC is not 1, so CLOCK_MONOTONIC would be faked")
	case s.SelfPreload:
		return refuse("commands executed in the container preload the interposer too")
	}
	return nil
}

func (s clockInspection) environment() clockTargetEnvironment {
	env := clockTargetEnvironment{Known: s.EnvKnown}
	if v, ok := s.Environ["FAKETIME"]; ok {
		env.Faketime = &v
	}
	if v, ok := s.Environ["FAKETIME_TIMESTAMP_FILE"]; ok {
		env.TimestampFile = &v
	}
	return env
}

func (s clockInspection) cacheSeconds() *int {
	n, err := strconv.Atoi(s.Environ["FAKETIME_CACHE_DURATION"])
	if err != nil || n < 0 {
		return nil
	}
	return &n
}

// clockSample is one bracketed measurement of the daemon's clock.
type clockSample struct {
	// Offset bounds the daemon's clock minus the reference clock.
	Offset clockOffsetInterval
	// StartTicks is the daemon's start time, the same before and after.
	StartTicks uint64
	// LoMono is the controller's monotonic time before the measurement.
	LoMono int64
}

// Target is the process instance the sample came from.
func (s clockSample) Target(id string) clockOffsetTarget {
	return clockOffsetTarget{ID: id, PID: 1, StartTicks: s.StartTicks}
}

func (a clockOffsetInterval) String() string {
	if !a.Known {
		return "unknown"
	}
	return fmt.Sprintf("[%s, %s]", time.Duration(a.Lo), time.Duration(a.Hi))
}

// clockEngine reaches the target container and the engine. Only these are
// replaceable in unit tests; admission, the ledger verdicts, registry
// ownership and Terminate use the actual implementation.
//
//testdouble:dependency-port
type clockEngine struct {
	// run executes the clock helper script inside ctr with args and returns
	// its standard output.
	run func(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error)
	// logs returns the target's log output that the engine received at or
	// after since, in nanoseconds of the Unix epoch.
	logs func(ctx context.Context, id string, since int64) ([]byte, error)
	// status reports the target container's state.
	status func(ctx context.Context, id string) (clockContainerState, error)
	// copy writes data at path into a stopped ctr.
	copy func(ctx context.Context, ctr testcontainers.Container, path string, data []byte) error
	// now is the controller's monotonic time and sleep waits on it.
	now   func() int64
	sleep func(ctx context.Context, d time.Duration) error
	// auditWait bounds the wait for the daemon's audit line.
	auditWait time.Duration
}

func defaultClockEngine() clockEngine {
	origin := time.Now()
	return clockEngine{run: runClockScript, logs: clockLogs, status: clockStatus, copy: copyClockFile,
		now: func() int64 { return int64(time.Since(origin)) }, sleep: sleepContext, auditWait: 5 * time.Second}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e clockEngine) state(ctx context.Context, id string) (clockContainerState, error) {
	return e.status(ctx, id)
}

// inspect reads the daemon and the owned file. An empty interposer reads the
// file only.
func (e clockEngine) inspect(ctx context.Context, ctr testcontainers.Container, interposer, path string) (clockInspection, error) {
	out, err := e.run(ctx, ctr, "inspect", interposer, path)
	if err != nil {
		return clockInspection{}, fmt.Errorf("inspect the clock instrumentation: %w", err)
	}
	var raw struct {
		StartTicks  uint64            `json:"start_ticks"`
		Environ     map[string]string `json:"environ"`
		Mapped      bool              `json:"mapped"`
		SelfPreload bool              `json:"self_preload"`
		File        clockRawFile      `json:"file"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return clockInspection{}, fmt.Errorf("decode the clock instrumentation: %w", err)
	}
	file, err := raw.File.observed()
	if err != nil {
		return clockInspection{}, err
	}
	return clockInspection{StartTicks: raw.StartTicks, Environ: raw.Environ, EnvKnown: raw.Environ != nil && raw.StartTicks > 0,
		Mapped: raw.Mapped, SelfPreload: raw.SelfPreload, File: file}, nil
}

// write rewrites the owned file in place and reads it back.
func (e clockEngine) write(ctx context.Context, ctr testcontainers.Container, path, text string) (clockObservedFile, error) {
	out, err := e.run(ctx, ctr, "write", path, text+"\n")
	if err != nil {
		return clockObservedFile{}, err
	}
	var raw struct {
		File clockRawFile `json:"file"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return clockObservedFile{}, fmt.Errorf("decode the rewritten file: %w", err)
	}
	return raw.File.observed()
}

// replace writes text into the file of a stopped container.
func (e clockEngine) replace(ctx context.Context, ctr testcontainers.Container, path, text string) error {
	return e.copy(ctx, ctr, path, []byte(text+"\n"))
}

// clockRawFile is the helper's report of the owned file.
type clockRawFile struct {
	Present bool   `json:"present"`
	Regular bool   `json:"regular"`
	Device  uint64 `json:"dev"`
	Inode   uint64 `json:"ino"`
	Text    string `json:"text"`
}

func (f clockRawFile) observed() (clockObservedFile, error) {
	if !f.Present {
		return clockObservedFile{KnownPresence: true}, nil
	}
	if !f.Regular {
		return clockObservedFile{}, errors.New("the owned timestamp file is not a regular file")
	}
	return clockObservedFile{KnownPresence: true, Present: true, Identity: clockFileIdentity{HasIdentity: true, Device: f.Device, Inode: f.Inode},
		KnownContent: true, Text: f.Text}, nil
}

// measure brackets the daemon's clock with the reference clock of an
// uninstrumented process in the same container.
func (e clockEngine) measure(ctx context.Context, ctr testcontainers.Container, daemon string) (clockSample, error) {
	var last error
	for range 3 {
		sample, ambiguous, err := e.measureOnce(ctx, ctr, daemon)
		if err == nil {
			return sample, nil
		}
		last = err
		if !ambiguous {
			break
		}
	}
	return clockSample{}, last
}

// measureOnce reports ambiguous when another mon_status request logged an
// audit line in the same window, so that a repetition may succeed.
func (e clockEngine) measureOnce(ctx context.Context, ctr testcontainers.Container, daemon string) (clockSample, bool, error) {
	lo := e.now()
	out, err := e.run(ctx, ctr, "measure", "/var/run/ceph/ceph-"+daemon+".asok")
	if err != nil {
		return clockSample{}, false, err
	}
	var raw struct {
		Before      int64  `json:"before"`
		After       int64  `json:"after"`
		TicksBefore uint64 `json:"ticks_before"`
		TicksAfter  uint64 `json:"ticks_after"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return clockSample{}, false, fmt.Errorf("decode the clock measurement: %w", err)
	}
	if raw.After < raw.Before || raw.TicksBefore == 0 || raw.TicksBefore != raw.TicksAfter {
		return clockSample{}, false, errors.New("the daemon was replaced or the reference clock went backwards during the measurement")
	}
	deadline := e.now() + int64(e.auditWait)
	for {
		logs, err := e.logs(ctx, ctr.GetContainerID(), raw.Before)
		if err != nil {
			return clockSample{}, false, fmt.Errorf("read the daemon's audit line: %w", err)
		}
		stamps := clockAuditStamps(logs)
		switch {
		case len(stamps) == 1:
			stamp := stamps[0]
			hi := stamp.at + stamp.resolution + int64(clockLogStampLag) - raw.Before
			return clockSample{Offset: clockOffsetInterval{Known: true, Lo: stamp.at - raw.After, Hi: hi},
				StartTicks: raw.TicksBefore, LoMono: lo}, false, nil
		case len(stamps) > 1:
			return clockSample{}, true, errors.New("more than one mon_status audit line in the measurement window")
		}
		if e.now() >= deadline {
			return clockSample{}, false, errors.New("the daemon logged no audit line for the measurement's mon_status request")
		}
		if err := e.sleep(ctx, 100*time.Millisecond); err != nil {
			return clockSample{}, false, err
		}
	}
}

// clockStamp is a daemon log time in nanoseconds of the Unix epoch and the
// resolution of the printed stamp.
type clockStamp struct{ at, resolution int64 }

// clockAuditStamps returns the stamps of the audit lines that a mon_status
// request on the admin socket makes the daemon log in its own debug log, as
// in
//
//	2026-11-27T08:07:27.123+0000 7f6b2a 0 log_channel(audit) log [DBG] : from='admin socket' entity='admin socket' cmd='mon_status' args=[]: dispatch
//
// The cluster log copy of an audit entry, which the leader prints in another
// format, has no log_channel prefix and is not counted.
func clockAuditStamps(logs []byte) []clockStamp {
	var stamps []clockStamp
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "log_channel(audit)") || !strings.Contains(line, "from='admin socket'") ||
			!strings.Contains(line, "mon_status") || !strings.Contains(line, ": dispatch") {
			continue
		}
		field, _, _ := strings.Cut(strings.TrimSpace(line), " ")
		if stamp, ok := parseClockStamp(field); ok {
			stamps = append(stamps, stamp)
		}
	}
	return stamps
}

// parseClockStamp reads a Ceph log stamp such as 2026-11-27T08:07:27.123+0000.
func parseClockStamp(field string) (clockStamp, bool) {
	at, err := time.Parse("2006-01-02T15:04:05.999999999-0700", field)
	if err != nil {
		return clockStamp{}, false
	}
	dot := strings.IndexByte(field, '.')
	digits := 0
	for i := dot + 1; dot >= 0 && i < len(field) && field[i] >= '0' && field[i] <= '9'; i++ {
		digits++
	}
	resolution := int64(time.Second)
	for range digits {
		resolution /= 10
	}
	return clockStamp{at: at.UnixNano(), resolution: resolution}, true
}

// clockScript runs inside the MON container with the container's own
// environment, which carries no interposer variable, so its clock is the
// uninstrumented reference. inspect reads the daemon (PID 1) and the owned
// file; write rewrites the file in place (truncate, no create, no symlink);
// measure reads the real clock before and after a mon_status request on the
// daemon's admin socket.
const clockScript = `
import json, os, socket, stat, struct, sys, time

def ticks():
    with open('/proc/1/stat') as f:
        data = f.read()
    return int(data[data.rindex(')') + 2:].split()[19])

def owned(path):
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return {'present': False}
    if not stat.S_ISREG(st.st_mode):
        return {'present': True, 'regular': False}
    with open(path, 'rb') as f:
        text = f.read(4096).decode('utf-8', 'replace')
    return {'present': True, 'regular': True, 'dev': st.st_dev, 'ino': st.st_ino, 'text': text}

def environ():
    with open('/proc/1/environ', 'rb') as f:
        items = f.read().split(b'\0')
    env = {}
    for item in items:
        key, sep, value = item.partition(b'=')
        if sep:
            env[key.decode('utf-8', 'replace')] = value.decode('utf-8', 'replace')
    return env

def mapped(pid, path):
    with open('/proc/%s/maps' % pid) as f:
        return any(line.rstrip('\n').endswith(' ' + path) for line in f)

def recv(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise SystemExit('admin socket closed early')
        data += chunk
    return data

op = sys.argv[1]
if op == 'inspect':
    interposer, path = sys.argv[2], sys.argv[3]
    out = {'file': owned(path)}
    if interposer:
        out.update(start_ticks=ticks(), environ=environ(), mapped=mapped(1, interposer),
                   self_preload='LD_PRELOAD' in os.environ or mapped('self', interposer))
elif op == 'write':
    path, text = sys.argv[2], sys.argv[3]
    fd = os.open(path, os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW)
    try:
        os.write(fd, text.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    out = {'file': owned(path)}
elif op == 'measure':
    path = sys.argv[2]
    if not stat.S_ISSOCK(os.stat(path).st_mode):
        raise SystemExit(path + ' is not a socket')
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect(path)
    ticks_before = ticks()
    before = time.time_ns()
    sock.sendall(b'{"prefix": "mon_status"}\0')
    size = struct.unpack('>I', recv(sock, 4))[0]
    recv(sock, size)
    after = time.time_ns()
    sock.close()
    out = {'before': before, 'after': after, 'ticks_before': ticks_before, 'ticks_after': ticks()}
else:
    raise SystemExit('unknown operation ' + op)
json.dump(out, sys.stdout)
`

// clockScriptLimit bounds the helper's output.
const clockScriptLimit = 1 << 20

func runClockScript(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	code, reader, err := ctr.Exec(ctx, append([]string{"python3", "-I", "-c", clockScript}, args...))
	if err != nil {
		return nil, fmt.Errorf("run the clock helper script: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, io.LimitReader(reader, clockScriptLimit)); err != nil {
		return nil, fmt.Errorf("read the clock helper script: %w", err)
	}
	if code != 0 {
		return nil, faulthook.NativeFailure("python3", code, stdout.Bytes(), stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

func clockLogs(ctx context.Context, id string, since int64) ([]byte, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	stream, err := docker.ContainerLogs(ctx, id, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true,
		Since: fmt.Sprintf("%d.%09d", since/int64(time.Second), since%int64(time.Second))})
	if err != nil {
		return nil, faulthook.ClassifyDocker(ctx, err)
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, io.LimitReader(stream, clockScriptLimit)); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func clockStatus(ctx context.Context, id string) (clockContainerState, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = docker.Close() }()
	result, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		if faulthook.OnlyMissingHostResource(err) {
			return clockContainerMissing, nil
		}
		return "", faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the clock offset container: %w", err))
	}
	state := result.Container.State
	switch {
	case state == nil:
		return "", errors.New("the clock offset container's inspection has no state")
	case state.Paused:
		return clockContainerPaused, nil
	case state.Running || state.Restarting:
		return clockContainerRunning, nil
	}
	return clockContainerStopped, nil
}

func copyClockFile(ctx context.Context, ctr testcontainers.Container, path string, data []byte) error {
	return ctr.CopyToContainer(ctx, data, path, 0o644)
}
