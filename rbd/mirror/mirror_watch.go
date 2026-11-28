package mirror

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/rbdobservation"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
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
	if err := clusterutil.LockMutex(ctx, &m.mu); err != nil {
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
