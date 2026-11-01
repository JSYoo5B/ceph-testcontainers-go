package ceph

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	mobycl "github.com/moby/moby/client"
)

// clusterOwnerLabel marks every container that a cluster creates with the
// cluster's owner ID. Docker can create a container although the reply to the
// create request was lost, for example when the request's context ended. Such
// a container has no handle, and Terminate finds it by this label.
const clusterOwnerLabel = clusterutil.OwnerLabel

type ownedContainerClient interface {
	ContainerList(context.Context, mobycl.ContainerListOptions) (mobycl.ContainerListResult, error)
	ContainerRemove(context.Context, string, mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error)
	Close() error
}
