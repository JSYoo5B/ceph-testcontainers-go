package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// ConnectionConfig returns the independent bootstrap ceph.conf and admin
// keyring copies, waiting for topology and the configuration cache only while
// ctx remains active. On error it returns the zero ConnectionConfig.
func (c *Container) ConnectionConfig(ctx context.Context) (ConnectionConfig, error) {
	if gateErr := c.sharedGate("ConnectionConfig"); gateErr != nil {
		return ConnectionConfig{}, gateErr
	}
	if c == nil {
		return ConnectionConfig{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return ConnectionConfig{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return ConnectionConfig{}, errClusterTerminated
	}
	config, keyring, complete, err := c.bootstrap.connection(ctx)
	if err != nil {
		return ConnectionConfig{}, err
	}
	if !complete {
		return ConnectionConfig{}, errors.New("ceph cluster bootstrap is incomplete")
	}
	return ConnectionConfig{Conf: config, Keyring: keyring}, nil
}

// Managers returns sorted owned candidate descriptors. The slice is
// independent; descriptors and their containers retain fixture ownership.
func (c *Container) Managers(ctx context.Context) ([]*ManagerContainer, error) {
	if gateErr := c.sharedGate("Managers"); gateErr != nil {
		return nil, gateErr
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	result := make([]*ManagerContainer, 0, len(c.managers))
	for _, manager := range c.managers {
		result = append(result, manager)
	}
	slices.SortFunc(result, func(a, b *ManagerContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result, nil
}

// Monitors returns the owned MON membership sorted by daemon name,
// like Monitors, waiting for the topology only while ctx remains active.
func (c *Container) Monitors(ctx context.Context) ([]*MonitorContainer, error) {
	if gateErr := c.sharedGate("Monitors"); gateErr != nil {
		return nil, gateErr
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	return c.monitorsLocked(), nil
}

// OSDs returns the owned OSD descriptors sorted by ID, like OSDs,
// waiting for the topology only while ctx remains active.
func (c *Container) OSDs(ctx context.Context) ([]*OSDContainer, error) {
	if gateErr := c.sharedGate("OSDs"); gateErr != nil {
		return nil, gateErr
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	return c.osdsLocked(), nil
}

// ControlContainer returns the stable CLI handle while bounding a
// concurrent control-plane creation or termination publication wait by ctx.
// The cluster owns the handle; this snapshot does not promise it remains live.
func (c *Container) ControlContainer(ctx context.Context) (testcontainers.Container, error) {
	if err := c.sharedGate("ControlContainer"); err != nil {
		return nil, err
	}
	return c.controlContainerContext(ctx)
}

func (c *Container) controlContainerContext(ctx context.Context) (testcontainers.Container, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	control, err := c.controlPlane.currentContext(ctx)
	if err != nil {
		return nil, err
	}
	if control != nil {
		return control, nil
	}
	return c.Container, nil
}
