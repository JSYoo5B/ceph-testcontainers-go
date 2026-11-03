package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// OSDWatchConfig configures Observer.WatchOSDs. A zero field takes its
// default; a negative field, an Interval below 100 milliseconds and a Buffer
// above 1024 are ErrInvalidRequest.
type OSDWatchConfig struct {
	// Interval is the time between the starts of consecutive reads, 2 seconds
	// by default. Reads never overlap: a read that takes longer delays the
	// next one. A transition that is shorter than the interval can be missed.
	Interval time.Duration
	// QueryTimeout bounds one read, including its wait for the topology lock,
	// 30 seconds by default. A read that exceeds it is a gap, not the end of
	// the watch.
	QueryTimeout time.Duration
	// Buffer is the capacity of the event channel, 16 by default.
	Buffer int
}

const (
	osdWatchDefaultInterval = 2 * time.Second
	osdWatchMinInterval     = 100 * time.Millisecond
	osdWatchDefaultTimeout  = 30 * time.Second
	osdWatchDefaultBuffer   = 16
	osdWatchMaxBuffer       = 1024
)

// osdWatchSettings is a validated OSDWatchConfig.
type osdWatchSettings struct {
	interval, timeout time.Duration
	buffer            int
}

func (cfg OSDWatchConfig) settings() (osdWatchSettings, error) {
	s := osdWatchSettings{interval: cfg.Interval, timeout: cfg.QueryTimeout, buffer: cfg.Buffer}
	switch {
	case s.interval < 0, s.timeout < 0, s.buffer < 0:
		return osdWatchSettings{}, invalidRequest("watch OSDs: negative interval, query timeout or buffer")
	case s.interval != 0 && s.interval < osdWatchMinInterval:
		return osdWatchSettings{}, invalidRequest("watch OSDs: interval %v is below %v", s.interval, osdWatchMinInterval)
	case s.buffer > osdWatchMaxBuffer:
		return osdWatchSettings{}, invalidRequest("watch OSDs: buffer %d exceeds %d", s.buffer, osdWatchMaxBuffer)
	}
	if s.interval == 0 {
		s.interval = osdWatchDefaultInterval
	}
	if s.timeout == 0 {
		s.timeout = osdWatchDefaultTimeout
	}
	if s.buffer == 0 {
		s.buffer = osdWatchDefaultBuffer
	}
	return s, nil
}

// OSDWatchEventKind names what an OSDWatchEvent reports.
type OSDWatchEventKind string

// The kinds of OSDWatchEvent.
const (
	// OSDWatchBaseline is the first event of every watch: the read that
	// WatchOSDs made before it returned, which fixes the watched OSDs.
	OSDWatchBaseline OSDWatchEventKind = "baseline"
	// OSDWatchChanged reports a successful read whose States differ from the
	// previous successful read.
	OSDWatchChanged OSDWatchEventKind = "changed"
	// OSDWatchGap reports one failed read. States is nil and Err is the
	// failure; the watch keeps polling.
	OSDWatchGap OSDWatchEventKind = "gap"
	// OSDWatchResumed reports the first successful read after one or more
	// gaps, whether or not States changed across them.
	OSDWatchResumed OSDWatchEventKind = "resumed"
)

// OSDWatchEvent is one event of an OSDWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state. Epoch is the native OSDMap epoch
// of that read; an epoch change that leaves States equal produces no event.
//
// States are the watched OSDs, sorted by ID, compared field by field (ID,
// UUID, Up, In, Weight). Each event has its own slice, which the consumer may
// keep or change. Two States separated by a gap, or simply by the polling
// interval, do not establish the transitions in between.
type OSDWatchEvent struct {
	Sequence          uint64
	Kind              OSDWatchEventKind
	Started, Finished time.Time
	Epoch             uint64
	States            []OSDState
	Err               error
}

// ErrWatchOverflow ends a watch whose consumer left the event buffer full
// for one polling interval while an event waited. The watch stops instead of
// replacing or dropping events; the error names the undelivered event.
var ErrWatchOverflow = errors.New("watch event buffer overflow")

// errWatchStopped is the cancellation cause of OSDWatch.Stop.
var errWatchStopped = errors.New("watch stopped")

// OSDWatch is a running watch of owned OSD state. One goroutine reads the
// OSDMap and sends events; Events closes after that goroutine has stopped
// using the cluster. Stop ends the watch and waits for it.
type OSDWatch struct {
	events chan OSDWatchEvent
	cancel context.CancelCauseFunc
	done   chan struct{}
	err    error
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchOSDs returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *OSDWatch) Events() <-chan OSDWatchEvent {
	if w == nil {
		return nil
	}
	return w.events
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchOSDs
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster, or an identity change of a watched OSD). Buffered
// events stay readable after Stop. Stop is safe to call more than once and
// from any goroutine; every call returns the same cause. Call it before the
// fixture's cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *OSDWatch) Stop() error {
	if w == nil || w.done == nil {
		return invalidRequest("watch OSDs: the watch is not started by WatchOSDs")
	}
	w.cancel(errWatchStopped)
	<-w.done
	return w.err
}

// osdSampleRead is one serialized read of the OSDMap and the owned
// registrations; Container.ownedOSDSample in production.
type osdSampleRead func(context.Context) (osdPolicySnapshot, map[int]*osdRegistration, error)

// WatchOSDs polls the native OSDMap and reports changes of the owned OSDs.
//
// Before it returns, WatchOSDs makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing; a cluster that owns
// no OSD is ErrInvalidRequest. That read is the baseline: the first event,
// already buffered, and the fixed set of watched OSDs, pinned by ID and native
// UUID. OSDs added later are not watched. Receive the baseline before
// triggering the change to observe.
//
// Each later read takes the topology lock for one osd dump, as OSDStates
// does, and releases it before the event is sent and during the wait for the
// next read, so the consumer may call any fixture operation while the watch
// runs. A successful read whose watched states equal the previous successful
// read produces no event. A failed read produces an OSDWatchGap and the watch
// continues; the next successful read is OSDWatchResumed. These end the watch
// instead: ctx ending, Stop, a terminated cluster, a watched OSD that is no
// longer owned (removed or detached) or whose native ID now has another UUID,
// and a full buffer (ErrWatchOverflow).
//
// Up and down are the reported OSDMap state, not proof that the daemon process
// runs or that clients can reach it. The stream reports differences between
// samples, not a complete history of native transitions.
//
//purpose:observation
func (o *Observer) WatchOSDs(ctx context.Context, cfg OSDWatchConfig) (*OSDWatch, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	settings, err := cfg.settings()
	if err != nil {
		return nil, err
	}
	return startOSDWatch(ctx, settings, c.ownedOSDSample)
}

// osdWatchRun is the producer state of one watch. Only its goroutine uses it.
type osdWatchRun struct {
	watch    *OSDWatch
	settings osdWatchSettings
	read     osdSampleRead
	cohort   map[int]uuid.UUID
	last     []OSDState
	sequence uint64
	gap      bool
}

func startOSDWatch(ctx context.Context, settings osdWatchSettings, read osdSampleRead) (*OSDWatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run := &osdWatchRun{settings: settings, read: read}
	started := time.Now()
	dump, owned, err := run.sample(ctx)
	finished := time.Now()
	if err != nil {
		return nil, err
	}
	if len(owned) == 0 {
		return nil, invalidRequest("watch OSDs: the cluster owns no OSD")
	}
	states, err := resolveOwnedOSDStates(dump, owned)
	if err != nil {
		return nil, err
	}
	run.cohort = make(map[int]uuid.UUID, len(states))
	for _, s := range states {
		run.cohort[s.ID] = uuid.MustParse(s.UUID) // resolveOwnedOSDState parsed it
	}
	run.last = states
	watchCtx, cancel := context.WithCancelCause(ctx)
	run.watch = &OSDWatch{events: make(chan OSDWatchEvent, settings.buffer), cancel: cancel, done: make(chan struct{})}
	run.watch.events <- run.event(OSDWatchBaseline, started, finished, dump.epoch, states, nil)
	go run.loop(watchCtx, started)
	return run.watch, nil
}

// sample makes one read bounded by the query timeout.
func (r *osdWatchRun) sample(ctx context.Context) (osdPolicySnapshot, map[int]*osdRegistration, error) {
	ctx, cancel := context.WithTimeout(ctx, r.settings.timeout)
	defer cancel()
	return r.read(ctx)
}

func (r *osdWatchRun) event(kind OSDWatchEventKind, started, finished time.Time, epoch uint64, states []OSDState, err error) OSDWatchEvent {
	r.sequence++
	return OSDWatchEvent{Sequence: r.sequence, Kind: kind, Started: started, Finished: finished, Epoch: epoch, States: slices.Clone(states), Err: err}
}

func (r *osdWatchRun) loop(ctx context.Context, previous time.Time) {
	w := r.watch
	defer func() {
		w.cancel(nil)
		close(w.events)
		close(w.done)
	}()
	w.err = r.poll(ctx, previous)
	if errors.Is(w.err, errWatchStopped) {
		w.err = nil
	}
}

// poll runs the reads until one ends the watch, and returns that cause.
func (r *osdWatchRun) poll(ctx context.Context, previous time.Time) error {
	for {
		if err := r.wait(ctx, time.Until(previous.Add(r.settings.interval))); err != nil {
			return err
		}
		started := time.Now()
		previous = started
		dump, owned, err := r.sample(ctx)
		finished := time.Now()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		var states []OSDState
		if err == nil {
			states, err = r.resolve(dump, owned)
			if err != nil {
				return err
			}
		} else if errors.Is(err, errClusterTerminated) {
			return err
		}
		var event OSDWatchEvent
		switch {
		case err != nil:
			r.gap = true
			event = r.event(OSDWatchGap, started, finished, 0, nil, err)
		case r.gap:
			r.gap = false
			r.last = states
			event = r.event(OSDWatchResumed, started, finished, dump.epoch, states, nil)
		case !slices.Equal(states, r.last):
			r.last = states
			event = r.event(OSDWatchChanged, started, finished, dump.epoch, states, nil)
		default:
			continue
		}
		if err := r.deliver(ctx, event); err != nil {
			return err
		}
	}
}

// resolve checks the watched OSDs of one successful read. Every refusal is an
// identity change that a later read cannot undo, so it ends the watch.
func (r *osdWatchRun) resolve(dump osdPolicySnapshot, owned map[int]*osdRegistration) ([]OSDState, error) {
	states := make([]OSDState, 0, len(r.cohort))
	for id, pinned := range r.cohort {
		state, err := resolveOwnedOSDState(dump, id, owned[id])
		if err != nil {
			return nil, fmt.Errorf("watch OSDs: watched osd.%d: %w", id, err)
		}
		if uuid.MustParse(state.UUID) != pinned {
			return nil, fmt.Errorf("watch OSDs: watched osd.%d now has native UUID %s instead of %s", id, state.UUID, pinned)
		}
		states = append(states, state)
	}
	slices.SortFunc(states, func(a, b OSDState) int { return a.ID - b.ID })
	return states, nil
}

// wait sleeps for d unless ctx ends first. Every return of an ended ctx is
// its cause, so poll can tell Stop from the WatchOSDs context ending, also
// when the next read is already overdue.
func (r *osdWatchRun) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

// deliver sends event, waiting at most one interval for buffer space.
func (r *osdWatchRun) deliver(ctx context.Context, event OSDWatchEvent) error {
	select {
	case r.watch.events <- event:
		return nil
	default:
	}
	timer := time.NewTimer(r.settings.interval)
	defer timer.Stop()
	select {
	case r.watch.events <- event:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return fmt.Errorf("%w: event %d (%s) undelivered: the consumer left all %d buffered events unread for %v", ErrWatchOverflow, event.Sequence, event.Kind, r.settings.buffer, r.settings.interval)
	}
}
