package snapschedule

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// leaseWatchDefaultInterval is the default WatchConfig.Interval of
// Lease.Watch: each read is one FSMap read and one schedule status query, and
// the module takes snapshots at minute boundaries at the finest.
const leaseWatchDefaultInterval = 5 * time.Second

// LeaseWatchEvent is one event of a LeaseWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time the module took or pruned a snapshot; First and Last
// in Status are the module's own times.
//
// Status is the schedule's state of a successful read, nil on a gap. Active,
// Created, Pruned, First, Last and Retention are compared; Raw travels with
// the event but causes none by itself. A deactivated schedule is an outcome
// in Status, not an Err: Err is only the failure of a read. Each event has
// its own Status, Retention map and Raw, which the consumer may keep or
// change.
//
// The counters and times are the module's aggregate progress, not a history
// of its runs: one event can cover several snapshots taken or pruned between
// two reads, and a read equal to the previous one produces no event even
// though time passed.
type LeaseWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            *Status
	Err               error
}

// LeaseWatch is a running watch of one leased snapshot schedule. One
// goroutine reads the module's status and sends events; Events closes after
// that goroutine has stopped using the filesystem. Stop ends the watch and
// waits for it.
type LeaseWatch struct {
	stream *watch.Stream[LeaseWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when Lease.Watch returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *LeaseWatch) Events() <-chan LeaseWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the Lease.Watch
// context ended, a ceph.ErrWatchOverflow error, or the ceph.ErrInvalidRequest
// read error that ended it (a terminated cluster, a removed or replaced
// filesystem, a restored lease, or a schedule that the module no longer lists
// or that another schedule with its repeat makes ambiguous), which names the
// schedule. A deactivated schedule is not an error of Stop; its event carries
// the outcome. Buffered events stay readable after Stop. Stop is safe to call
// more than once and from any goroutine; every call returns the same cause.
// It never changes the schedule. Call it before Restore and the fixture's
// cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *LeaseWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return servicehook.Invalid(errors.New("watch snapshot schedule: the watch is not started by Lease.Watch"))
	}
	return w.stream.Stop()
}

// Watch polls the module's status of this lease's schedule, as Status reads
// it, and reports changes of its active state, its created and pruned
// counters, its first and last snapshot times and the directory's retention.
// The default WatchConfig.Interval is 5 seconds.
//
// Before it returns, Watch makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing; a lease whose add
// was never confirmed, or that was restored, is refused with
// ceph.ErrInvalidRequest: restore it instead. That read is the baseline: the
// first event, already buffered. The watch follows the schedule with the
// lease's native start and creation time; the module's identity of the
// schedule is fixed by Add, never adopted from a later read.
//
// Each read takes the filesystem's admission, as Status does, through its
// QueryTimeout context, for that read only, and releases it before the event
// is sent and during the wait for the next read, so the consumer may call
// Snapshots or any other fixture operation while the watch runs; a read that
// waits for a long fixture operation is a gap. A successful read whose
// compared fields equal the previous successful read produces no event. A
// failed read produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. A deactivated schedule, which the module
// can activate again, is a change, not the end of the watch. These end the
// watch with an error instead: ctx ending, Stop, a terminated cluster, a
// filesystem that is no longer the registered one or whose FSMap ID or
// original pools changed, a restored lease, a schedule that the module no
// longer lists (removed, even if added again with the same path and repeat),
// another schedule of the directory with the lease's repeat, and a full buffer
// (ceph.ErrWatchOverflow). The watch has no terminal sample: it runs until one
// of these.
//
// The stream reports the module's aggregate progress, emitted only when it
// changes; it is not a per-snapshot event history. A created counter that
// rose by two between reads is one event, a snapshot taken and pruned
// between reads shows only as counters, and nothing classifies why the
// module deactivated a schedule. List the snapshots themselves with
// Snapshots. The watch never adds, activates or removes a schedule or a
// retention rule.
//
//purpose:observation
func (l *Lease) Watch(ctx context.Context, cfg ceph.WatchConfig) (*LeaseWatch, error) {
	if l == nil || l.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("watch snapshot schedule: the lease is not from snapschedule.Add"))
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, leaseWatchDefaultInterval)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("watch snapshot schedule: %v", err))
	}
	return startLeaseWatch(ctx, settings, fmt.Sprintf("watch snapshot schedule %s of %s", l.Repeat, l.Path), l.watchRead)
}

// watchRead is one admitted read of the lease's bound schedule. A refusal
// that no later read can undo is a leaseWatchEnded error.
func (l *Lease) watchRead(ctx context.Context) (Status, error) {
	ctx, done, err := l.begin(ctx)
	if err != nil {
		if fsbase.IdentityChanged(err) {
			return Status{}, leaseWatchEnded{err}
		}
		return Status{}, err
	}
	defer done()
	// Restore changes these under the admission that this read holds.
	switch {
	case l.restored:
		return Status{}, leaseWatchEnded{errors.New("the lease was restored")}
	case l.added == "":
		return Status{}, leaseWatchEnded{errors.New("the lease's add was never confirmed: restore the lease")}
	}
	listed, err := statuses(ctx, l.filesystem.Volumes(), l.Path)
	if err != nil {
		return Status{}, err
	}
	status, err := l.match(listed)
	if err != nil {
		return Status{}, leaseWatchEnded{err}
	}
	if status == nil {
		return Status{}, leaseWatchEnded{fmt.Errorf("the module no longer lists the lease's schedule, added %s", l.added)}
	}
	return *status, nil
}

// leaseWatchEnded is a read refusal that a later read cannot undo.
type leaseWatchEnded struct{ err error }

func (e leaseWatchEnded) Error() string { return e.err.Error() }
func (e leaseWatchEnded) Unwrap() error { return e.err }

func startLeaseWatch(ctx context.Context, settings watch.Settings, name string, read func(context.Context) (Status, error)) (*LeaseWatch, error) {
	stream, err := watch.Start(ctx, watch.Spec[Status, LeaseWatchEvent]{
		Name:     name,
		Settings: settings,
		Read: func(ctx context.Context) (Status, error) {
			status, err := read(ctx)
			var ended leaseWatchEnded
			if errors.As(err, &ended) {
				return Status{}, watch.Fatal(servicehook.Invalid(fmt.Errorf("%s: %w", name, ended.err)))
			}
			return status, err
		},
		Equal: func(a, b Status) bool {
			return a.Active == b.Active && a.Created == b.Created && a.Pruned == b.Pruned &&
				a.First.Equal(b.First) && a.Last.Equal(b.Last) && maps.Equal(a.Retention, b.Retention)
		},
		Event: func(m watch.Meta, s Status, err error) LeaseWatchEvent {
			e := LeaseWatchEvent{Sequence: m.Sequence, Kind: ceph.WatchEventKind(m.Kind), Started: m.Started, Finished: m.Finished, Err: err}
			if err == nil {
				s.Retention = maps.Clone(s.Retention)
				s.Raw = bytes.Clone(s.Raw)
				e.Status = &s
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &LeaseWatch{stream: stream}, nil
}
