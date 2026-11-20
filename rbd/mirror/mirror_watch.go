package mirror

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/rbdobservation"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// rbdGateBound bounds the native reads that one status observation or one
// namespace binding makes while it holds the fixture gate. The wait for the
// gate comes before it and is bounded by the caller's context alone: a caller
// queued behind other observations, each of which may hold the gate for many
// seconds, does not spend this bound on them.
const rbdGateBound = 30 * time.Second

// rbdGateContext starts rbdGateBound. Call it once the gate is held.
func rbdGateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, rbdGateBound)
}

// rbdWatchSettings validates cfg for one Watch of this package with the
// target's default interval.
func rbdWatchSettings(operation string, cfg ceph.WatchConfig, defaultInterval time.Duration) (watch.Settings, error) {
	s, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, defaultInterval)
	if err != nil {
		return watch.Settings{}, servicehook.Invalid(fmt.Errorf("%s: %v", operation, err))
	}
	return s, nil
}

// rbdWatchMeta is the target-independent part of an event in public form.
func rbdWatchMeta(m watch.Meta) (uint64, ceph.WatchEventKind, time.Time, time.Time) {
	return m.Sequence, ceph.WatchEventKind(m.Kind), m.Started, m.Finished
}

// rbdWatchNotStarted is the Stop error of a watch that no Watch call started.
func rbdWatchNotStarted(operation string) error {
	return servicehook.Invalid(fmt.Errorf("%s: the watch is not started by its Watch call", operation))
}

// errRBDWatchTerminated marks the read of a watch whose mirror or cluster is
// terminated; the watch ends with it.
var errRBDWatchTerminated = errors.New("terminated")

// checkWatchOpen refuses a watch read once the mirror or either of its
// clusters is terminated. It takes the owner gate and then each cluster's
// topology lock, one at a time and bounded by ctx, only to read that fact,
// and holds none of them when it returns. A refusal wraps
// errRBDWatchTerminated and is ceph.ErrInvalidRequest; a lock that ctx
// outlasts is a query error.
func (m *Container) checkWatchOpen(ctx context.Context, operation string) error {
	observation := rbdobservation.Begin(ctx, "watch-open")
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		observation.Finish(ctx, err)
		return rbdReceiverQuery(ctx, "admit "+operation, err)
	}
	observation.Acquired()
	closed := m.closed
	sites := [2]*ceph.Container{m.pair.config.Source, m.pair.config.Destination}
	observation.Releasing()
	m.mu.Unlock()
	observation.Finish(ctx, nil)
	if closed {
		return servicehook.Invalid(fmt.Errorf("%s: the RBD mirror is %w", operation, errRBDWatchTerminated))
	}
	for i, cluster := range sites {
		if cluster == nil {
			continue // the read itself refuses an incomplete fixture
		}
		if err := servicehook.LockTopology(ctx, cluster); err != nil {
			return rbdReceiverQuery(ctx, "admit "+operation, err)
		}
		closed := servicehook.Closed(cluster)
		servicehook.Unlock(cluster)
		if closed {
			return servicehook.Invalid(fmt.Errorf("%s: the %s cluster is %w", operation, [2]string{"source", "destination"}[i], errRBDWatchTerminated))
		}
	}
	return nil
}
