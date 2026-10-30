package ceph

import (
	"context"
	"errors"
)

// Caller holds the topology mutex. Waits need context-bounded control
// admission without changing poolPolicyReady's other policy callers.
func (c *Container) waitPolicyReady(ctx context.Context) error {
	if c.closed {
		return errClusterTerminated
	}
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return err
	}
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	return nil
}
