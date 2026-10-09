package cluster

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// ConnectionConfigContext returns independent bootstrap copies, waiting for
// topology and the configuration cache only while ctx remains active.
func (c *Container) ConnectionConfigContext(ctx context.Context) ([]byte, []byte, error) {
	if c == nil {
		return nil, nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, errors.New("ceph cluster is terminated")
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return nil, nil, err
	}
	defer c.configMu.RUnlock()
	if len(c.config) == 0 || len(c.keyring) == 0 {
		return nil, nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	return bytes.Clone(c.config), bytes.Clone(c.keyring), nil
}

// ManagersContext returns sorted owned candidate descriptors. The slice is
// independent; descriptors and their containers retain fixture ownership.
func (c *Container) ManagersContext(ctx context.Context) ([]*ManagerContainer, error) {
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

// gatewaysContext returns sorted owned gateway descriptors, including partial
// startup attempts, while bounding the wait for fixture topology by ctx.
func (c *Container) gatewaysContext(ctx context.Context) ([]*RGWContainer, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	result := make([]*RGWContainer, 0, len(c.gateways))
	for _, gateway := range c.gateways {
		result = append(result, gateway)
	}
	slices.SortFunc(result, func(a, b *RGWContainer) int { return strings.Compare(a.GatewayName, b.GatewayName) })
	return result, nil
}

// ControlContainerContext returns the stable CLI handle while bounding a
// concurrent control-plane creation or termination publication wait by ctx.
// The cluster owns the handle; this snapshot does not promise it remains live.
func (c *Container) ControlContainerContext(ctx context.Context) (testcontainers.Container, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := lockTopologyReadMutex(ctx, &c.controlMu); err != nil {
		return nil, err
	}
	defer c.controlMu.RUnlock()
	if c.controlPlane != nil {
		return c.controlPlane, nil
	}
	return c.Container, nil
}
