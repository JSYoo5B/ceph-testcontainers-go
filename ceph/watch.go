package ceph

import (
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// WatchConfig configures every Watch of this module: the Observer watches in
// this package and the watches of the service packages. A zero field takes
// its default; a negative field, an Interval below 100 milliseconds and a
// Buffer above 1024 are ErrInvalidRequest.
type WatchConfig struct {
	// Interval is the time between the starts of consecutive reads. Each
	// Watch documents its own default, chosen from the cost of its read.
	// Reads never overlap, and after a read the watch rests at least as long
	// as that read took, so it leaves the fixture's locks free at least half
	// the time: a read that takes longer than half the interval delays the
	// next one. A transition that is shorter than the interval can be missed.
	Interval time.Duration
	// QueryTimeout bounds one read, including its wait for fixture locks, 30
	// seconds by default. A read that exceeds it is a gap, not the end of the
	// watch.
	QueryTimeout time.Duration
	// Buffer is the capacity of the event channel, 16 by default.
	Buffer int
}

// WatchEventKind names what a watch event reports. Every Watch of this module
// uses the same kinds.
type WatchEventKind string

// The kinds of watch event.
const (
	// WatchBaseline is the first event of every watch: the read that the
	// Watch call made before it returned, which fixes the watched identities.
	WatchBaseline WatchEventKind = WatchEventKind(watch.Baseline)
	// WatchChanged reports a successful read whose compared state differs
	// from the previous successful read.
	WatchChanged WatchEventKind = WatchEventKind(watch.Changed)
	// WatchGap reports one failed read. Err is the failure; the watch keeps
	// polling. A gap is never an empty or healthy observation.
	WatchGap WatchEventKind = WatchEventKind(watch.Gap)
	// WatchResumed reports the first successful read after one or more gaps,
	// whether or not the compared state changed across them.
	WatchResumed WatchEventKind = WatchEventKind(watch.Resumed)
)

// ErrWatchOverflow ends a watch whose consumer left the event buffer full
// for one polling interval while an event waited. The watch stops instead of
// replacing or dropping events; the error names the undelivered event.
var ErrWatchOverflow = watch.ErrOverflow

// watchSettings validates cfg for one Watch with the target's default
// interval. The service packages validate with watch.Validate and
// servicehook.Invalid in the same way.
func watchSettings(operation string, cfg WatchConfig, defaultInterval time.Duration) (watch.Settings, error) {
	s, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, defaultInterval)
	if err != nil {
		return watch.Settings{}, invalidRequest("%s: %v", operation, err)
	}
	return s, nil
}

// watchMeta is the target-independent part of an event in public form.
func watchMeta(m watch.Meta) (uint64, WatchEventKind, time.Time, time.Time) {
	return m.Sequence, WatchEventKind(m.Kind), m.Started, m.Finished
}

// notStarted is the Stop error of a watch that no Watch call started.
func notStarted(operation string) error {
	return invalidRequest("%s: the watch is not started by its Watch call", operation)
}
