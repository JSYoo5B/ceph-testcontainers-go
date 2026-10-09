package cluster

import (
	"context"
	"errors"
	"time"
)

// Called before native allocation, without the topology owner held. A non-nil
// partial allocator remains owned even when reservation or its context fails.
func (c *Container) reserveTrackedHostPorts(ctx context.Context, count int) (*hostPortLease, error) {
	return c.reserveTrackedHostPortsWith(ctx, count, reserveHostPorts)
}

// The callback seam is private; production always uses reserveHostPorts. The
// owner is obtained before the callback so termination cannot miss publication.
func (c *Container) reserveTrackedHostPortsWith(ctx context.Context, count int, reserve func(context.Context, string, string, int, time.Duration) (*hostPortLease, error)) (*hostPortLease, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	lease, err := reserve(ctx, c.settings.controlImage, c.PublicAddress(), count, c.settings.startupTimeout)
	if lease != nil {
		// Do not gate this publication on ctx: the native allocator now exists.
		c.portLeases = append(c.portLeases, lease)
	}
	if lease == nil && err == nil {
		err = errors.New("host port reservation returned no lease")
	}
	return lease, errors.Join(err, ctx.Err())
}
