package ceph

import (
	"context"
	"errors"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// monitorPortStart is one role's start of a MON container, which
// startMonitorRetryingPorts retries after the MON lost its host ports to
// another process. Each role keeps its own refusal classification,
// verification and cleanup in its callbacks.
type monitorPortStart struct {
	// host is set on the host network; on a bridge the start runs once.
	host bool
	// attempt runs one start and returns the container it created, if any.
	// retry marks a failure that a lost port race can explain; the loop
	// retries it only when the container reports a port conflict.
	attempt func(ctx context.Context) (ctr testcontainers.Container, retry bool, err error)
	// discard removes the container of a conflicted attempt before the next
	// one. When that fails it returns the container that stays owned.
	discard func(ctx context.Context, ctr testcontainers.Container) (testcontainers.Container, error)
}

// startMonitorRetryingPorts runs s.attempt up to hostPortAttempts times on the
// host network and once on a bridge. A success, a failure that is not a lost
// port race, the last attempt's failure and a failure after ctx ended return
// with the attempt's container. The caller holds c.mu.
func (c *Container) startMonitorRetryingPorts(ctx context.Context, s monitorPortStart) (testcontainers.Container, error) {
	attempts := 1
	if s.host {
		attempts = hostPortAttempts
	}
	for attempt := 1; ; attempt++ {
		ctr, retry, err := s.attempt(ctx)
		if err == nil || !retry || attempt >= attempts || ctx.Err() != nil || !c.lostMonitorPorts(ctx, ctr) {
			return ctr, err
		}
		if kept, discardErr := s.discard(ctx, ctr); discardErr != nil {
			return kept, errors.Join(err, discardErr)
		}
	}
}

// startOnLeasedPorts reserves the MON's host ports, gives them to create,
// which makes the container without starting it, releases them and starts
// the container, so that the MON binds them as the allocator gives them up.
// The lease is tracked as soon as its allocator exists. started reports that
// the reservation, the creation and the release succeeded, so that a failure
// is the start's, which a lost port race can explain. The caller holds c.mu.
func (c *Container) startOnLeasedPorts(ctx context.Context, image, address string, create func(ctx context.Context, ports []int) (testcontainers.Container, error)) (ctr testcontainers.Container, ports []int, started bool, err error) {
	lease, err := reserveHostPorts(ctx, image, address, c.settings.monitorPortCount(), c.settings.startupTimeout, c.startup.ownedLabel())
	c.portLeases.track(lease)
	if err != nil {
		return nil, nil, false, err
	}
	ctr, err = create(ctx, lease.Ports)
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	releaseErr := lease.Release(cleanupCtx)
	cancel()
	if err != nil || releaseErr != nil {
		return ctr, lease.Ports, false, errors.Join(err, releaseErr)
	}
	if ctr == nil {
		return nil, lease.Ports, false, errors.New("MON container creation returned no container")
	}
	return ctr, lease.Ports, true, ctr.Start(ctx)
}

// terminateLostPortsMonitor removes the container of a MON without a store
// volume that lost its host ports. A container that is already gone is not
// an error.
func terminateLostPortsMonitor(ctx context.Context, ctr testcontainers.Container) (testcontainers.Container, error) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := ctr.Terminate(cleanupCtx); !onlyMissingHostResource(err) {
		return ctr, err
	}
	return nil, nil
}
