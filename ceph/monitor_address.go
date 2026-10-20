package ceph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
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

// newMonitorAddressDocker opens the Docker client of the MON address hooks.
var newMonitorAddressDocker = func(ctx context.Context) (networkLinkClient, error) {
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// keepMonitorAddress keeps a MON's bridge addresses across Stop and Start.
// The monmap records the address that the MON had when it joined, and the MON
// binds exactly that address. Docker allocates an endpoint's address when the
// container starts, so after several containers stopped, another one that
// starts first can take the MON's address, and the MON then fails to bind.
// After the first start the hooks record the endpoint. Before a later start
// of a stopped container whose endpoint has a dynamic address, they reconnect
// it with the recorded address requested explicitly, as
// NetworkInterruption.Restore does; the bridge's explicit subnet permits this.
//
// The hooks never attach a detached endpoint: a network interruption stays in
// force until its owner restores it, and the MON does not start meanwhile.
// An endpoint attached with another explicit address is not replaced either.
func keepMonitorAddress(network string) testcontainers.CustomizeRequestOption {
	var mu sync.Mutex
	var recorded *dockernetwork.EndpointSettings
	return testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
		PostReadies: []testcontainers.ContainerHook{func(ctx context.Context, ctr testcontainers.Container) error {
			if err := lockTopologyMutex(ctx, &mu); err != nil {
				return err
			}
			defer mu.Unlock()
			if recorded != nil {
				return nil
			}
			endpoint, err := monitorEndpoint(ctx, ctr.GetContainerID(), network, func(_ networkLinkClient, _ bool, endpoint *dockernetwork.EndpointSettings) error {
				if endpoint == nil || !endpoint.IPAddress.IsValid() {
					return fmt.Errorf("MON container is not attached to %s", network)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("record MON address: %w", err)
			}
			recorded = dockerbridge.RestoreEndpoint(endpoint)
			return nil
		}},
		PreStarts: []testcontainers.ContainerHook{func(ctx context.Context, ctr testcontainers.Container) error {
			if err := lockTopologyMutex(ctx, &mu); err != nil {
				return err
			}
			defer mu.Unlock()
			if recorded == nil {
				return nil
			}
			_, err := monitorEndpoint(ctx, ctr.GetContainerID(), network, func(docker networkLinkClient, running bool, current *dockernetwork.EndpointSettings) error {
				switch {
				case running:
					// Start does nothing to a running container.
					return nil
				case current == nil:
					return invalid(fmt.Errorf("MON container is detached from %s, as by InterruptNetwork; restore its endpoint before starting it", network))
				case current.IPAMConfig != nil && (current.IPAMConfig.IPv4Address.IsValid() || current.IPAMConfig.IPv6Address.IsValid()):
					if current.IPAMConfig.IPv4Address == recorded.IPAMConfig.IPv4Address && current.IPAMConfig.IPv6Address == recorded.IPAMConfig.IPv6Address {
						return nil
					}
					return fmt.Errorf("MON endpoint on %s requests %s, not the monmap address %s", network, current.IPAMConfig.IPv4Address, recorded.IPAMConfig.IPv4Address)
				}
				if _, err := docker.NetworkDisconnect(ctx, network, mobycl.NetworkDisconnectOptions{Container: ctr.GetContainerID()}); err != nil {
					return err
				}
				_, err := docker.NetworkConnect(ctx, network, mobycl.NetworkConnectOptions{Container: ctr.GetContainerID(), EndpointConfig: recorded.Copy()})
				return err
			})
			if err != nil {
				return fmt.Errorf("keep MON address %s: %w", recorded.IPAMConfig.IPv4Address, err)
			}
			return nil
		}},
	})
}

// monitorEndpoint inspects the container and passes whether it runs and its
// endpoint on network, nil when it is not attached, to use. It returns the
// endpoint.
func monitorEndpoint(ctx context.Context, id, network string, use func(networkLinkClient, bool, *dockernetwork.EndpointSettings) error) (*dockernetwork.EndpointSettings, error) {
	if id == "" {
		return nil, errors.New("MON container has no ID")
	}
	docker, err := newMonitorAddressDocker(ctx)
	if err != nil {
		return nil, err
	}
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, errors.Join(err, docker.Close())
	}
	var endpoint *dockernetwork.EndpointSettings
	if inspection.Container.NetworkSettings != nil {
		endpoint = inspection.Container.NetworkSettings.Networks[network]
	}
	running := inspection.Container.State != nil && inspection.Container.State.Running
	return endpoint, errors.Join(use(docker, running, endpoint), docker.Close())
}
