package ceph

import (
	"context"
	"errors"
	"fmt"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// clusterOwnerLabel marks every container that a cluster creates with the
// cluster's owner ID. Docker can create a container although the reply to the
// create request was lost, for example when the request's context ended. Such
// a container has no handle, and Terminate finds it by this label.
const clusterOwnerLabel = "org.testcontainers.ceph.cluster"

type ownedContainerClient interface {
	ContainerList(context.Context, mobycl.ContainerListOptions) (mobycl.ContainerListResult, error)
	ContainerRemove(context.Context, string, mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error)
	Close() error
}

// ownedLabel labels a container that the cluster creates.
func (c *Container) ownedLabel() testcontainers.CustomizeRequestOption {
	return testcontainers.WithLabels(map[string]string{clusterOwnerLabel: c.owner})
}

// removeLabeledContainers force-removes every container left with the
// cluster's label, including those whose create reply was lost.
func (c *Container) removeLabeledContainers(ctx context.Context) error {
	if c.owner == "" {
		return nil
	}
	newClient := c.ownedContainerDocker
	if newClient == nil {
		newClient = func(ctx context.Context) (ownedContainerClient, error) {
			return testcontainers.NewDockerClientWithOpts(ctx)
		}
	}
	docker, err := newClient(ctx)
	if err != nil {
		return fmt.Errorf("list the cluster's containers: %w", err)
	}
	defer func() { _ = docker.Close() }()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", clusterOwnerLabel+"="+c.owner)})
	if err != nil {
		return fmt.Errorf("list the cluster's containers: %w", err)
	}
	var errs []error
	for _, ctr := range list.Items {
		if _, err := docker.ContainerRemove(ctx, ctr.ID, mobycl.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("remove container %.12s of the cluster: %w", ctr.ID, err))
		}
	}
	return errors.Join(errs...)
}
