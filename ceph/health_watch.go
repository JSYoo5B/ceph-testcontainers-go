package ceph

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// healthWatchDefaultInterval is the default WatchConfig.Interval of
// WatchHealth: each read is the three CLI calls of HealthDetails, and Ceph
// raises and clears most checks on monitor and manager ticks of a few
// seconds.
const healthWatchDefaultInterval = 5 * time.Second

// HealthWatchEvent is one event of a HealthWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph raised or cleared a check.
//
// Health is the read as HealthDetails returns it, with Mutes sorted by code.
// Its FSID is the original cluster's, the same in every event. The watch
// compares Status, the set of check codes and, for each check, Severity,
// Count and Muted, and for each stored mute its Code, Count, Sticky and
// ExpiresAt. Summary and detail messages are not compared: a read that
// differs only in their text or order, such as a countdown or a reordered
// detail list, produces no event, so they are as fresh as the last event, not
// the last read. Each event has its own snapshot, which the consumer may keep
// or change. Messages can contain pool names, paths or other application
// data; do not print or publish events unfiltered. Two snapshots separated by
// a gap, or simply by the polling interval, do not establish the transitions
// in between: a check can appear and clear unseen. On a WatchGap, Health is
// zero and Err is the failure.
type HealthWatchEvent struct {
	Sequence          uint64
	Kind              WatchEventKind
	Started, Finished time.Time
	Health            HealthSnapshot
	Err               error
}

// HealthWatch is a running watch of the cluster's health checks. One
// goroutine reads them and sends events; Events closes after that goroutine
// has stopped using the cluster. Stop ends the watch and waits for it.
type HealthWatch struct {
	stream *watch.Stream[HealthWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchHealth returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *HealthWatch) Events() <-chan HealthWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchHealth
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster or a foreign native FSID). Buffered events stay
// readable after Stop. Stop is safe to call more than once and from any
// goroutine; every call returns the same cause. Call it before the fixture's
// cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *HealthWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return notStarted("watch health")
	}
	return w.stream.Stop()
}

// WatchHealth polls the original cluster's health checks and reports their
// appearance, clearing, severity, count and mute changes. The default
// WatchConfig.Interval is 5 seconds.
//
// Before it returns, WatchHealth makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered, which fixes the watched
// identity, the original cluster FSID. Checks are not pinned: any native
// code, unknown and future codes included, can appear or clear. Receive the
// baseline before triggering the change to observe.
//
// Each later read is one HealthDetails observation, which takes the topology
// lock for its three CLI calls and releases it before the event is sent and
// during the wait for the next read, so the consumer may call any fixture
// operation while the watch runs. A successful read whose compared fields
// equal the previous successful read produces no event. A failed read, a
// malformed report included, produces a WatchGap and the watch continues; it
// is never a healthy or empty observation. The next successful read is
// WatchResumed. These end the watch instead: ctx ending, Stop, a terminated
// cluster, a native FSID of another cluster, and a full buffer
// (ErrWatchOverflow).
//
// The watch only reads: it never mutes, clears or repairs a check. HEALTH_OK
// can include muted checks, and a sticky mute stays after its check clears;
// assert on a check's code and Muted flag. The three reads of one sample are
// not atomic. A check that appears or clears is what the monitor reported,
// not proof of the SUT's behavior, and sampling can miss a short warning, so
// the stream is no audit history and the absence of a check proves no
// absence of a fault.
//
//purpose:observation
func (o *Observer) WatchHealth(ctx context.Context, cfg WatchConfig) (*HealthWatch, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	settings, err := watchSettings("watch health", cfg, healthWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startHealthWatch(ctx, settings, c.healthDetails)
}

// healthRead is one HealthDetails read with its observationEnd marks;
// Container.healthDetails in production.
type healthRead func(context.Context) (HealthSnapshot, error)

func startHealthWatch(ctx context.Context, settings watch.Settings, read healthRead) (*HealthWatch, error) {
	const operation = "watch health"
	var fsid string
	stream, err := watch.Start(ctx, watch.Spec[HealthSnapshot, HealthWatchEvent]{
		Name:     operation,
		Settings: settings,
		Read: func(ctx context.Context) (HealthSnapshot, error) {
			snapshot, err := read(ctx)
			if cause := observationEndCause(err); cause != nil {
				return HealthSnapshot{}, watch.Fatal(fmt.Errorf("%s: %w", operation, cause))
			}
			if err != nil {
				return HealthSnapshot{}, err
			}
			switch {
			case fsid == "":
				fsid = snapshot.FSID
			case snapshot.FSID != fsid:
				return HealthSnapshot{}, watch.Fatal(fmt.Errorf("%s: %w", operation, invalid(errForeignFSID)))
			}
			slices.SortFunc(snapshot.Mutes, func(a, b HealthMute) int { return strings.Compare(a.Code, b.Code) })
			return snapshot, nil
		},
		Equal: equalHealthSamples,
		Event: func(m watch.Meta, s HealthSnapshot, err error) HealthWatchEvent {
			e := HealthWatchEvent{Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = watchMeta(m)
			if err == nil {
				e.Health = cloneHealthSnapshot(s)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &HealthWatch{stream: stream}, nil
}

// equalHealthSamples compares the semantic fields of two samples whose mutes
// are sorted by code: status, check codes, severities, counts and mute
// state, never the free-form messages.
func equalHealthSamples(a, b HealthSnapshot) bool {
	if a.Status != b.Status || len(a.Checks) != len(b.Checks) || len(a.Mutes) != len(b.Mutes) {
		return false
	}
	for code, x := range a.Checks {
		y, ok := b.Checks[code]
		if !ok || x.Severity != y.Severity || x.Count != y.Count || x.Muted != y.Muted {
			return false
		}
	}
	for i, x := range a.Mutes {
		y := b.Mutes[i]
		if x.Code != y.Code || x.Count != y.Count || x.Sticky != y.Sticky || x.ExpiresAt != y.ExpiresAt {
			return false
		}
	}
	return true
}

func cloneHealthSnapshot(s HealthSnapshot) HealthSnapshot {
	checks := maps.Clone(s.Checks)
	for code, check := range checks {
		check.Details = slices.Clone(check.Details)
		checks[code] = check
	}
	s.Checks, s.Mutes = checks, slices.Clone(s.Mutes)
	return s
}
