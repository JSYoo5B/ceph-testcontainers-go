package cluster

import (
	"context"
	"errors"
)

// Caller holds the topology mutex. Waits need context-bounded control
// admission without changing poolPolicyReady's other policy callers.
func (c *Container) waitPolicyReady(ctx context.Context) error {
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return err
	}
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	return nil
}
