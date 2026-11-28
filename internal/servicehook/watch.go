package servicehook

import (
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// WatchSettings validates cfg for one Watch of a service package with the
// target's default interval. An invalid cfg is ceph.ErrInvalidRequest named
// by operation, as package ceph refuses it for its own watches.
func WatchSettings(operation string, cfg ceph.WatchConfig, defaultInterval time.Duration) (watch.Settings, error) {
	s, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, defaultInterval)
	if err != nil {
		return watch.Settings{}, Invalid(fmt.Errorf("%s: %v", operation, err))
	}
	return s, nil
}

// WatchMeta is the target-independent part of an event in public form: its
// sequence number, kind, and the start and end of its read.
func WatchMeta(m watch.Meta) (uint64, ceph.WatchEventKind, time.Time, time.Time) {
	return m.Sequence, ceph.WatchEventKind(m.Kind), m.Started, m.Finished
}

// WatchNotStarted is the Stop error of a watch value that no Watch call
// started, named by operation.
func WatchNotStarted(operation string) error {
	return Invalid(fmt.Errorf("%s: the watch is not started by its Watch call", operation))
}
