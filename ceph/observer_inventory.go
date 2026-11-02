package ceph

import (
	"context"
	"errors"

	"github.com/testcontainers/testcontainers-go"
)

// ServiceContainers returns a fresh slice of tracked optional-service handles
// (such as RGW and MDS), sorted by service name. It reads only the ownership
// registry: it does not query Docker or claim that a service is running.
//
// The caller's context bounds acquisition of the topology lock. Partially
// started and terminated clusters remain inspectable; stopped handles and
// handles retained after failed cleanup are included. Successful service
// removal or termination can forget a handle. An empty inventory means no
// handles are tracked, not that Docker has no cluster-owned resources (a lost
// create reply can leave a resource without a handle). A missing tracked handle
// is an error, not an omitted entry. The cluster retains cleanup ownership.
//
//purpose:observation
func (o *Observer) ServiceContainers(ctx context.Context) ([]testcontainers.Container, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	return c.serviceContainers(ctx)
}

func (c *Container) serviceContainers(ctx context.Context) ([]testcontainers.Container, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	services := c.members.roster.sortedServices()
	for _, service := range services {
		if service == nil {
			return nil, errors.New("ceph tracked service container handle is unavailable")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return services, nil
}
