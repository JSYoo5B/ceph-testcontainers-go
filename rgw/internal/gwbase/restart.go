package gwbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// restartGrace is the Stop grace of a gateway restart.
	restartGrace = 3 * time.Second
	// restartStopBound bounds a Stop, which runs on after its caller's ctx
	// ends so that it is never left half-done.
	restartStopBound = time.Minute
	// restartStopSettle is how long a restart still waits for its Stop once
	// the caller's ctx ended.
	restartStopSettle = 10 * time.Second
)

// RestartTarget is a gateway container and the cluster that owns it. Every
// restart of the container, whoever requests it (a TLS replacement, a
// placement activation, a multisite period reload), is one generation-checked
// protocol with one record per container, kept with the cluster: a restart
// that one caller left unfinished is settled by the next caller of any of
// them. The caller holds the cluster's topology mutex.
type RestartTarget struct {
	Cluster   *ceph.Container
	Container testcontainers.Container
}

// Restart is one restart request: Owner identifies who requested it, so that
// the requester can tell its own unfinished restart from another's.
type Restart struct {
	Owner any
	// BeforeStart, when set, runs before a restart whose Stop was
	// interrupted starts the stopped generation again, whichever caller
	// resumes it.
	BeforeStart func(context.Context) error
	// AwaitCompletion keeps the record after the start is confirmed, until
	// the owner calls Complete once it verified the started generation.
	AwaitCompletion bool
}

// Resumed is what Resume found.
type Resumed struct {
	// Owner is the owner of the settled record; nil when there was none.
	Owner any
	// Started reports that this call started the stopped generation.
	Started bool
	// Running reports that the gateway runs a generation started after the
	// record's Stop: the restart is done. Replaced reports that it is not
	// the generation that the restart started and confirmed.
	Running, Replaced bool
}

type restartRecord struct {
	Restart
	originalStart  string
	startConfirmed bool
	// observedStart is the generation whose start was confirmed.
	observedStart string
	// stopping is closed when a Stop that its caller stopped waiting for
	// returns.
	stopping chan struct{}
}

type restartRecords map[string]*restartRecord

func (t RestartTarget) records() restartRecords {
	return servicehook.Extension(t.Cluster, "rgw/gateway-restarts", func() any { return make(restartRecords) }).(restartRecords)
}

// Pending reports whether the container still has a restart record.
func (t RestartTarget) Pending() bool {
	return t.records()[t.Container.GetContainerID()] != nil
}

// Resume settles the restart that the container still owes, whoever
// requested it. It waits for a Stop that an earlier caller stopped waiting
// for, then decides by the container's run state and generation:
//
//   - the original generation still runs after a Stop that did not take
//     effect: an error, and the record stays for a retry once it stopped;
//   - the original generation is stopped: the record's BeforeStart, then
//     Start and a confirmed new generation (Started);
//   - a generation started after the Stop runs (Running);
//   - another generation is stopped, or the started one stopped again: the
//     gateway was stopped outside the restart and stays stopped; the record
//     is dropped and an error returned.
func (t RestartTarget) Resume(ctx context.Context) (Resumed, error) {
	id := t.Container.GetContainerID()
	record := t.records()[id]
	if record == nil {
		return Resumed{}, nil
	}
	result := Resumed{Owner: record.Owner}
	if record.stopping != nil {
		select {
		case <-record.stopping:
			record.stopping = nil
		case <-ctx.Done():
			return result, fmt.Errorf("the earlier Stop of the RGW gateway restart has not returned: %w", ctx.Err())
		}
	}
	state, err := t.Container.State(ctx)
	if err != nil {
		return result, fmt.Errorf("inspect RGW for its pending restart: %w", err)
	}
	if state == nil || state.Paused || state.Restarting {
		return result, errors.New("RGW run state cannot confirm the pending restart")
	}
	if record.observedStart != "" {
		if !state.Running {
			delete(t.records(), id)
			return result, errors.New("RGW was stopped outside its restart; leaving it stopped")
		}
		result.Running = true
		if state.StartedAt != record.observedStart {
			delete(t.records(), id)
			result.Replaced = true
		}
		return result, nil
	}
	original := state.StartedAt == record.originalStart
	switch {
	case state.Running && original:
		return result, errors.New("the earlier RGW stop is unconfirmed and its original generation still runs; retry after it stops or its generation changes")
	case state.Running:
		// Start was dispatched but not confirmed, or another start won.
		t.confirmed(record, id, state.StartedAt)
		result.Running = true
		return result, nil
	case !original || record.startConfirmed:
		delete(t.records(), id)
		return result, errors.New("RGW was stopped outside its restart; leaving it stopped")
	}
	if record.BeforeStart != nil {
		if err := record.BeforeStart(ctx); err != nil {
			return result, err
		}
	}
	if err := t.start(ctx, record, id); err != nil {
		return result, err
	}
	result.Started, result.Running = true, true
	return result, nil
}

// Restart stops and starts the running container once for r and confirms
// that a new generation runs. A record whose restart is done is replaced; an
// unsettled one is refused (Resume settles it). Stop runs with a three-second
// grace under its own one-minute bound. When ctx ends while Stop runs, the
// restart still waits ten seconds for it; a Stop that has not returned by
// then stays with the record, ctx's error is returned and the next Resume
// waits for the Stop. Once Stop has returned, the restart runs on a
// settlement budget (SettlementContext) that the caller's cancellation does
// not end: when Stop failed, Resume decides by the run state whether the stop
// took effect; then Start runs.
func (t RestartTarget) Restart(ctx context.Context, r Restart) error {
	id := t.Container.GetContainerID()
	state, err := t.Container.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect RGW before its restart: %w", err)
	}
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return servicehook.Invalid(errors.New("RGW restart requires a running gateway"))
	}
	if earlier := t.records()[id]; earlier != nil && earlier.observedStart == "" {
		return errors.New("an earlier RGW restart is unsettled")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	record := &restartRecord{Restart: r, originalStart: state.StartedAt, stopping: make(chan struct{})}
	t.records()[id] = record
	var stopErr error
	stopped := record.stopping
	go func() {
		defer close(stopped)
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), restartStopBound)
		defer cancel()
		grace := restartGrace
		stopErr = t.Container.Stop(stopCtx, &grace)
	}()
	// A Stop that returns soon after the caller ends its ctx still settles
	// the restart in this call; one that does not is left to the next Resume.
	select {
	case <-stopped:
	case <-ctx.Done():
		wait := time.NewTimer(restartStopSettle)
		defer wait.Stop()
		select {
		case <-stopped:
		case <-wait.C:
			return fmt.Errorf("stop RGW for its restart: the Stop has not returned and the next restart settles it: %w", ctx.Err())
		}
	}
	record.stopping = nil
	// From here the restart runs on a settlement budget that the caller's
	// cancellation does not end.
	settle, cancelSettle := SettlementContext(ctx)
	defer cancelSettle()
	if stopErr != nil {
		if _, err := t.Resume(settle); err != nil {
			return errors.Join(fmt.Errorf("stop RGW for its restart: %w", stopErr), err)
		}
		return nil
	}
	return t.start(settle, record, id)
}

// Complete drops the record that owner's restart kept for its verification
// once that verification passed. It refuses a generation that changed since
// the start was confirmed, and a record that is gone or another owner's.
func (t RestartTarget) Complete(ctx context.Context, owner any) error {
	id := t.Container.GetContainerID()
	record := t.records()[id]
	if record == nil || record.Owner != owner || record.observedStart == "" {
		return errors.New("the RGW restart awaiting verification is no longer recorded")
	}
	state, err := t.Container.State(ctx)
	if err != nil {
		return fmt.Errorf("confirm the restarted RGW generation: %w", err)
	}
	delete(t.records(), id)
	if state == nil || !state.Running || state.Paused || state.Restarting || state.StartedAt != record.observedStart {
		return errors.New("RGW generation changed during restart verification")
	}
	return nil
}

// start starts the stopped original generation of record and confirms that
// a new one runs. Docker Start acts on a container ID, so the generation is
// checked again right before it.
func (t RestartTarget) start(ctx context.Context, record *restartRecord, id string) error {
	ctx, cancel := SettlementContext(ctx)
	defer cancel()
	state, err := t.Container.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect stopped RGW before Start: %w", err)
	}
	if state == nil || state.Running || state.Paused || state.Restarting {
		return errors.New("cannot confirm the RGW gateway is stopped before Start")
	}
	if state.StartedAt != record.originalStart {
		delete(t.records(), id)
		return errors.New("another RGW generation was stopped before Start; leaving it stopped")
	}
	if err := t.Container.Start(ctx); err != nil {
		return fmt.Errorf("start RGW after its stop: %w", err)
	}
	record.startConfirmed = true
	state, err = t.Container.State(ctx)
	if err != nil || state == nil || !state.Running || state.Paused || state.Restarting || state.StartedAt == record.originalStart {
		return fmt.Errorf("confirm the restarted RGW generation: %v", err)
	}
	t.confirmed(record, id, state.StartedAt)
	return nil
}

// confirmed records the generation that runs after the restart and drops the
// record unless its owner verifies first.
func (t RestartTarget) confirmed(record *restartRecord, id, startedAt string) {
	record.startConfirmed, record.observedStart = true, startedAt
	if !record.AwaitCompletion {
		delete(t.records(), id)
	}
}
