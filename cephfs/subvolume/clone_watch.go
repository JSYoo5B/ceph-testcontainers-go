package subvolume

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// cloneWatchDefaultInterval is the default WatchConfig.Interval of
// Clone.Watch: each read runs four native commands (five once Clone.Wait has
// adopted the target), one of which mounts the filesystem through libcephfs
// in the client container to verify the target incarnation.
const cloneWatchDefaultInterval = 3 * time.Second

// CloneWatchEvent is one event of a CloneWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state; the native status carries no
// timestamp.
//
// Status is the native clone status of a successful read, nil on a gap. Every
// field of it is compared, Progress entry by entry, exactly as Ceph reports
// it; nothing is parsed into a percentage. Source fields are normally absent
// once the clone is complete. A failed or canceled clone is an outcome in
// Status (State, FailureErrno and FailureMessage), not an Err: Err is only the
// failure of a read. Each event has its own Status and Progress map, which the
// consumer may keep or change. Two statuses separated by a gap, or simply by
// the polling interval, do not establish the transitions in between.
type CloneWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            *CloneStatus
	Err               error
}

// CloneWatch is a running watch of one owned clone request. One goroutine
// reads the native clone status and sends events; Events closes after that
// goroutine has stopped using the filesystem. Stop ends the watch and waits
// for it.
type CloneWatch struct {
	stream *watch.Stream[CloneWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when Clone.Watch returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *CloneWatch) Events() <-chan CloneWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it or a terminal status was delivered, the
// context's error when the Clone.Watch context ended, a ceph.ErrWatchOverflow
// error, or the ceph.ErrInvalidRequest read error that ended it (a terminated
// cluster, a filesystem that is no longer active, or a replaced or removed
// target), which names the clone and keeps the refusal. A failed or
// canceled clone is not an error of Stop; its event carries the outcome.
// Buffered events stay readable after Stop. Stop is safe to call more than
// once and from any goroutine; every call returns the same cause. It never
// cancels the clone. Call it before the fixture's cleanup: cancelling the
// context alone does not wait.
//
//purpose:observation,lifetime
func (w *CloneWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return servicehook.Invalid(errors.New("watch clone: the watch is not started by Clone.Watch"))
	}
	return w.stream.Stop()
}

// cloneStatusRead is one admitted, serialized read of the clone status;
// Clone.statusSample with volbase.BeginObservation in production.
type cloneStatusRead func(context.Context) (*CloneStatus, error)

// Watch polls the native status of this owned clone request and reports its
// changes until a terminal state. The default WatchConfig.Interval is 3
// seconds.
//
// Before it returns, Watch makes one read under ctx and QueryTimeout, with
// the checks of Clone.Status: the request must be one this filesystem
// submitted, and the target's native UUID, inode and birth time, its source
// and the filesystem's FSMap ID must still be the ones captured at
// submission. A failed first read returns its error and starts nothing. That
// read is the baseline: the first event, already buffered. Receive it before
// triggering the change to observe.
//
// Each read holds the filesystem's subvolume operation gate, taken through
// its QueryTimeout context, for that read only, and releases it before the
// event is sent and during the wait for the next read, so the consumer may
// call any fixture operation, Clone.Cancel included, while the watch runs.
// Unlike Status, a read never enables the MGR volumes module. A successful
// read whose status equals the previous successful read produces no event. A
// failed read produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. A complete, failed or canceled status ends
// the watch after its event is delivered, and Stop then returns nil; when the
// baseline is already terminal, it is the only event. These end the watch
// with an error instead: ctx ending, Stop, a terminated cluster, a filesystem
// that is no longer the active one created by this cluster or whose FSMap ID
// or original pools changed, a target removed by Clone.RemovePartial, a target
// or source replaced outside this fixture, and a full buffer
// (ceph.ErrWatchOverflow).
//
// The watch is read-only: stopping it neither cancels the clone nor removes
// partial data, and a complete status is not a usable subvolume; Clone.Wait
// remains the readiness result. The stream reports differences between
// samples, not a complete history of native transitions: a short pending or
// in-progress phase can fall between two reads.
//
//purpose:observation
func (clone *Clone) Watch(ctx context.Context, cfg ceph.WatchConfig) (*CloneWatch, error) {
	fs, err := clone.filesystem()
	if err != nil {
		return nil, err
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, cloneWatchDefaultInterval)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("watch clone: %v", err))
	}
	if err := validateCloneHandle(fs, clone); err != nil {
		return nil, err
	}
	read := func(ctx context.Context) (*CloneStatus, error) {
		return clone.statusSample(ctx, fs, volbase.BeginObservation)
	}
	return startCloneWatch(ctx, settings, clone.identity.name, read)
}

func startCloneWatch(ctx context.Context, settings watch.Settings, name string, read cloneStatusRead) (*CloneWatch, error) {
	stream, err := watch.Start(ctx, watch.Spec[*CloneStatus, CloneWatchEvent]{
		Name:     fmt.Sprintf("watch clone %q", name),
		Settings: settings,
		Read: func(ctx context.Context) (*CloneStatus, error) {
			status, err := read(ctx)
			if err != nil {
				if cloneIdentityChanged(err) {
					return nil, watch.Fatal(servicehook.Invalid(fmt.Errorf("watch clone %q: %w", name, err)))
				}
				return nil, err
			}
			return status, nil
		},
		Equal: equalCloneStatus,
		Final: func(s *CloneStatus) bool {
			return s.State == "complete" || s.State == "failed" || s.State == "canceled"
		},
		Event: func(m watch.Meta, s *CloneStatus, err error) CloneWatchEvent {
			e := CloneWatchEvent{Sequence: m.Sequence, Kind: ceph.WatchEventKind(m.Kind), Started: m.Started, Finished: m.Finished, Err: err}
			if err == nil {
				e.Status = copyCloneStatus(s)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &CloneWatch{stream: stream}, nil
}

func equalCloneStatus(a, b *CloneStatus) bool {
	return a.State == b.State && a.SourceFilesystem == b.SourceFilesystem && a.SourceSubvolume == b.SourceSubvolume &&
		a.SourceGroup == b.SourceGroup && a.SourceSnapshot == b.SourceSnapshot && a.FailureErrno == b.FailureErrno &&
		a.FailureMessage == b.FailureMessage && maps.Equal(a.Progress, b.Progress)
}

func copyCloneStatus(s *CloneStatus) *CloneStatus {
	copied := *s
	copied.Progress = maps.Clone(s.Progress)
	return &copied
}
