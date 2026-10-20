package fault

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type networkLinkClient interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	NetworkDisconnect(context.Context, string, mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error)
	NetworkConnect(context.Context, string, mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error)
	Close() error
}

// NetworkInterruption owns one disconnected Docker endpoint. Restore reconnects
// its original addresses and aliases without restarting the container or Ceph
// daemon. It is an endpoint-to-network interruption, not a pairwise packet filter.
// A non-nil handle returned with an error must be restored: transport failure
// cannot prove that Docker did not disconnect it. Restore is retryable.
type NetworkInterruption struct {
	ContainerID, NetworkName string
	OriginalAddress          netip.Addr
	mu                       sync.Mutex
	docker                   networkLinkClient
	endpoint                 *dockernetwork.EndpointSettings
	complete                 bool
}

// InterruptNetwork disconnects a container from one of this cluster's bridges.
// The container may be an owned daemon or a caller-owned WithClient container.
// With a single bridge, both planes select that same endpoint. Host mode is
// rejected because Docker cannot disconnect a host-network endpoint.
// The fixture retains the handle and attempts restoration during Terminate.
// Restore explicitly before destroying caller-owned containers or the cluster.
func InterruptNetwork(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, plane ceph.NetworkPlane) (link *NetworkInterruption, err error) {
	if gateErr := faulthook.Admit(c, "fault.InterruptNetwork"); gateErr != nil {
		return nil, gateErr
	}
	err = faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) || c.UsesHostNetwork() || ctr == nil || ctr.GetContainerID() == "" {
			return errors.New("network interruption requires a live bridge cluster and an attached container")
		}
		var name string
		switch plane {
		case ceph.PublicNetworkPlane:
			name = c.NetworkName()
		case ceph.ClusterNetworkPlane:
			name = c.ClusterNetworkName()
		default:
			return errors.New("invalid Ceph network plane")
		}
		if name == "" {
			return errors.New("Ceph bridge is not initialized") //nolint:staticcheck // Preserve the existing refusal text; Ceph is a proper name.
		}
		token, previous, reserveErr := faulthook.Reserve(ctx, c, faulthook.NetworkInterruption, name+"/"+ctr.GetContainerID())
		if reserveErr != nil {
			link, _ = previous.(*NetworkInterruption)
			return reserveErr
		}
		var applyErr error
		link, applyErr = InterruptLink(ctx, ctr, name)
		held := faulthook.Held{}
		if link != nil {
			held = faulthook.Held{Network: name, Handle: link, Undo: link.Restore, Complete: link.isComplete}
		}
		_, settleErr := faulthook.Settle(c, token, held)
		return errors.Join(applyErr, settleErr)
	})
	return link, err
}

// InterruptLink disconnects one existing bridge endpoint. The caller owns
// the container and the returned restoration handle; this function does not
// stop a daemon, change Ceph policies or remove the network. Fixture-specific
// helpers also register restoration with their cleanup. Host networking cannot
// be interrupted through Docker endpoint operations.
func InterruptLink(ctx context.Context, ctr testcontainers.Container, name string) (*NetworkInterruption, error) {
	if ctr == nil || ctr.GetContainerID() == "" || name == "" || name == "host" || name == "none" || name == "bridge" {
		return nil, errors.New("network interruption requires a container and a named bridge")
	}
	id := ctr.GetContainerID()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, errors.Join(err, docker.Close())
	}
	var endpoint *dockernetwork.EndpointSettings
	if inspection.Container.NetworkSettings != nil {
		endpoint = inspection.Container.NetworkSettings.Networks[name]
	}
	if endpoint == nil || !endpoint.IPAddress.IsValid() || !endpoint.IPAddress.Is4() {
		return nil, errors.Join(fmt.Errorf("container is not attached to Ceph network %s with IPv4", name), docker.Close())
	}
	link := &NetworkInterruption{ContainerID: id, NetworkName: name, OriginalAddress: endpoint.IPAddress, docker: docker, endpoint: dockerbridge.RestoreEndpoint(endpoint)}
	// Return ownership even after uncertain transport outcomes.
	_, err = docker.NetworkDisconnect(ctx, name, mobycl.NetworkDisconnectOptions{Container: id})
	if err != nil {
		return link, fmt.Errorf("disconnect Ceph network endpoint: %w", err)
	}
	return link, nil
}

func (link *NetworkInterruption) isComplete(ctx context.Context) (bool, error) {
	if err := lockMutex(ctx, &link.mu); err != nil {
		return false, err
	}
	defer link.mu.Unlock()
	return link.complete, nil
}

// Restore reattaches the original endpoint, preserving the IPv4 and, on a
// dual-stack bridge, the IPv6 address advertised by Ceph. Removed containers
// need no restoration. An externally reattached endpoint with a different
// address is refused rather than overwritten, and the handle stays
// retryable.
func (link *NetworkInterruption) Restore(ctx context.Context) error {
	if err := lockMutex(ctx, &link.mu); err != nil {
		return err
	}
	defer link.mu.Unlock()
	if link.complete {
		return nil
	}
	inspection, err := link.docker.ContainerInspect(ctx, link.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil && !faulthook.OnlyMissingHostResource(err) {
		return fmt.Errorf("inspect interrupted endpoint: %w", err)
	}
	if err == nil {
		var current *dockernetwork.EndpointSettings
		if inspection.Container.NetworkSettings != nil {
			current = inspection.Container.NetworkSettings.Networks[link.NetworkName]
		}
		if current != nil {
			if current.IPAddress != link.endpoint.IPAMConfig.IPv4Address || current.GwPriority != link.endpoint.GwPriority {
				return errors.New("interrupted endpoint was externally reattached with a different address or gateway priority")
			}
			if original := link.endpoint.IPAMConfig.IPv6Address; original.IsValid() && current.GlobalIPv6Address != original {
				return fmt.Errorf("interrupted endpoint was externally reattached without its original IPv6 address %s", original)
			}
			for _, alias := range link.endpoint.Aliases {
				if !slices.Contains(current.Aliases, alias) {
					return errors.New("interrupted endpoint was externally reattached without its original aliases")
				}
			}
		} else {
			if _, err := link.docker.NetworkConnect(ctx, link.NetworkName, mobycl.NetworkConnectOptions{Container: link.ContainerID, EndpointConfig: link.endpoint}); err != nil {
				// A missing network is not a restored live caller container. Only
				// a container removed during reconnect needs no restoration.
				if !faulthook.OnlyMissingHostResource(err) {
					return fmt.Errorf("restore Ceph network endpoint: %w", err)
				}
				if _, inspectErr := link.docker.ContainerInspect(ctx, link.ContainerID, mobycl.ContainerInspectOptions{}); inspectErr == nil || !faulthook.OnlyMissingHostResource(inspectErr) {
					return fmt.Errorf("restore Ceph network endpoint: %w", errors.Join(err, inspectErr))
				}
			}
		}
	}
	if err := link.docker.Close(); err != nil {
		return fmt.Errorf("close interruption Docker client: %w", err)
	}
	link.complete = true
	return nil
}
