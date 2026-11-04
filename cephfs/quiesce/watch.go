package quiesce

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// setWatchDefaultInterval is the default WatchConfig.Interval of Set.Watch:
// each read is one FSMap read and one quiesce database query, and the
// shortest native expiration is one second.
const setWatchDefaultInterval = time.Second

// SetWatchEvent is one event of a SetWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time the set changed; the native query carries no
// transition time.
//
// State is the native set of a successful read, nil on a gap. Its State,
// Version, Timeout and Expiration are compared exactly as Ceph reports them;
// Members is fixed by the watch and equal in every event. An expired, timed
// out, canceled, failed or released set is an outcome in State, not an Err:
// Err is only the failure of a read. Each event has its own State and Members
// slice, which the consumer may keep or change. Two states separated by a gap,
// or simply by the polling interval, do not establish the transitions in
// between.
type SetWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	State             *State
	Err               error
}

// SetWatch is a running watch of one owned quiesce set. One goroutine reads
// the native set and sends events; Events closes after that goroutine has
// stopped using the filesystem. Stop ends the watch and waits for it.
type SetWatch struct {
	stream *watch.Stream[SetWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when Set.Watch returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *SetWatch) Events() <-chan SetWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it or a terminal state was delivered, the
// context's error when the Set.Watch context ended, a ceph.ErrWatchOverflow
// error, or the ceph.ErrInvalidRequest read error that ended it (a terminated
// cluster, a filesystem that is no longer the active one, or a set that is
// absent, has other members or went back to an earlier version), which names
// the set. An expired or timed-out set is not an error of Stop; its event
// carries the outcome. Buffered events stay readable after Stop. Stop is safe
// to call more than once and from any goroutine; every call returns the same
// cause. It never releases the set. Call it before the fixture's cleanup:
// cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *SetWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return servicehook.Invalid(errors.New("watch quiesce set: the watch is not started by Set.Watch"))
	}
	return w.stream.Stop()
}

// Watch polls the native state of this owned quiesce set and reports its
// changes until a terminal state, so that a SUT taking a checkpoint can stop
// claiming consistency once the set is no longer quiesced. The default
// WatchConfig.Interval is 1 second.
//
// Before it returns, Watch makes one read under ctx and QueryTimeout, with
// the checks of Status: the filesystem's FSMap ID must be the one captured by
// Start, and the set must list exactly this handle's members, none excluded.
// A failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered. Receive it before running the
// work that the set must cover.
//
// A read queries the set without renewing or extending its native expiration
// and never adopts a version: Release still releases only the version Start
// confirmed. Each read holds the filesystem's subvolume operation gate, taken
// through its QueryTimeout context, for that read only, and releases it
// before the event is sent and during the wait for the next read, so the
// consumer may call Release or any other fixture operation while the watch
// runs. Unlike Status, a read never enables the MGR volumes module. A
// successful read whose state, version, timeout and expiration equal the
// previous successful read produces no event; an outside edit of the timeout
// or expiration is a change. A failed read produces a WatchGap and the watch
// continues; the next successful read is WatchResumed. A RELEASED, EXPIRED,
// TIMEDOUT, CANCELED or FAILED state ends the watch after its event is
// delivered, and Stop then returns nil; when the baseline is already
// terminal, it is the only event. These end the watch with an error instead:
// ctx ending, Stop, a terminated cluster, a filesystem that is no longer the
// active one created by this cluster or whose FSMap ID or original pools
// changed, a set that is absent from the native quiesce database, changed or
// excluded members, a version lower than one already seen or than the one
// Start confirmed (the set was recreated), and a full buffer
// (ceph.ErrWatchOverflow).
//
// A QUIESCED event is a sample, not a lease: the set can expire right after
// the read, so the absence of a later event never proves that a checkpoint
// stayed consistent. Release remains the consistency result, and the stream
// reports differences between samples, not a complete history of native
// transitions.
//
//purpose:observation
func (q *Set) Watch(ctx context.Context, cfg ceph.WatchConfig) (*SetWatch, error) {
	if q == nil || q.filesystem == nil || q.state == nil {
		return nil, servicehook.Invalid(errors.New("watch quiesce set: the set is not started by quiesce.Start"))
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, setWatchDefaultInterval)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("watch quiesce set: %v", err))
	}
	read := func(ctx context.Context) (*State, error) {
		return q.status(ctx, volbase.BeginObservation)
	}
	return startSetWatch(ctx, settings, q.state.id, q.state.version, read)
}

// startSetWatch runs the watch of the set id over read, which is one
// admitted, serialized read of the set. confirmed is the version Start
// confirmed, zero when it confirmed none.
func startSetWatch(ctx context.Context, settings watch.Settings, id string, confirmed uint64, read func(context.Context) (*State, error)) (*SetWatch, error) {
	floor := confirmed
	stream, err := watch.Start(ctx, watch.Spec[*State, SetWatchEvent]{
		Name:     fmt.Sprintf("watch quiesce set %q", id),
		Settings: settings,
		Read: func(ctx context.Context) (*State, error) {
			state, err := read(ctx)
			if err == nil && state.Version < floor {
				err = ownershipError(fmt.Sprintf("native quiesce set version went back from %d to %d: the set was recreated", floor, state.Version))
			}
			if err != nil {
				var owned ownershipError
				if errors.As(err, &owned) || fsbase.IdentityChanged(err) {
					return nil, watch.Fatal(servicehook.Invalid(fmt.Errorf("watch quiesce set %q: %w", id, err)))
				}
				return nil, err
			}
			floor = state.Version
			return state, nil
		},
		Equal: func(a, b *State) bool {
			return a.State == b.State && a.Version == b.Version && a.Timeout == b.Timeout && a.Expiration == b.Expiration
		},
		Final: func(s *State) bool { return terminalState(s.State) },
		Event: func(m watch.Meta, s *State, err error) SetWatchEvent {
			e := SetWatchEvent{Sequence: m.Sequence, Kind: ceph.WatchEventKind(m.Kind), Started: m.Started, Finished: m.Finished, Err: err}
			if err == nil {
				copied := *s
				copied.Members = slices.Clone(s.Members)
				e.State = &copied
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &SetWatch{stream: stream}, nil
}

// terminalState reports a native state that ends the set; only an outside
// reset, which the fixture never sends, would make it active again.
func terminalState(state string) bool {
	switch state {
	case "RELEASED", "EXPIRED", "TIMEDOUT", "CANCELED", "FAILED":
		return true
	}
	return false
}
