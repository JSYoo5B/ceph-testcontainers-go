// Package dockerbridge creates Testcontainers bridges whose allocated subnet
// can also accept endpoints restored to their original IP on older Engines.
package dockerbridge

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
)

const allocationAttempts = 4
const cleanupTimeout = 10 * time.Second

type inspector interface {
	NetworkInspect(context.Context, string, mobycl.NetworkInspectOptions) (mobycl.NetworkInspectResult, error)
	Close() error
}

type createNetwork func(context.Context, *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error)
type removeNetwork func(context.Context, *testcontainers.DockerNetwork) error

// New first lets Docker choose an unused IPv4 subnet from its configured address
// pools, then recreates an empty bridge with that subnet explicitly configured.
// This permits reconnecting a Ceph endpoint at its advertised IP on Engines
// which require an explicit subnet for static endpoint addresses. Both temporary
// and final networks retain Testcontainers labels and Ryuk lifecycle handling.
//
// Only Docker's pool-overlap error is retried, at most four allocations and
// within ctx. Failure cleanup uses its own bounded context. A non-nil network
// returned with an error remains owned by the caller and must be removed;
// inspection, removal or client-close failures never discard that ownership.
func New(ctx context.Context) (*testcontainers.DockerNetwork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	return newNetwork(ctx, docker, func(ctx context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error) {
		if ipam == nil {
			return tcnetwork.New(ctx)
		}
		return tcnetwork.New(ctx, tcnetwork.WithIPAM(ipam))
	}, func(ctx context.Context, network *testcontainers.DockerNetwork) error {
		return network.Remove(ctx)
	})
}

func newNetwork(ctx context.Context, docker inspector, create createNetwork, remove removeNetwork) (owned *testcontainers.DockerNetwork, err error) {
	defer func() {
		if closeErr := docker.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close bridge inspection client: %w", closeErr))
		}
	}()
	for attempt := 0; attempt < allocationAttempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		probe, createErr := create(ctx, nil)
		if createErr != nil {
			return cleanupFailedNetwork(ctx, probe, fmt.Errorf("allocate bridge subnet: %w", createErr), remove)
		}
		ipam, inspectErr := inspectBridge(ctx, docker, probe)
		if inspectErr != nil {
			return cleanupFailedNetwork(ctx, probe, fmt.Errorf("inspect allocated bridge: %w", inspectErr), remove)
		}
		if removeErr := remove(ctx, probe); removeErr != nil && !errdefs.IsNotFound(removeErr) {
			return cleanupFailedNetwork(ctx, probe, fmt.Errorf("remove bridge allocation probe: %w", removeErr), remove)
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		bridge, createErr := create(ctx, ipam)
		if createErr != nil {
			// An owned partial result is an uncertain outcome, never a safe
			// collision retry. Remove it and let the caller see the error.
			if bridge == nil && poolOverlap(createErr) {
				err = fmt.Errorf("create bridge with allocated subnet (attempt %d): %w", attempt+1, createErr)
				continue
			}
			return cleanupFailedNetwork(ctx, bridge, fmt.Errorf("create bridge with allocated subnet: %w", createErr), remove)
		}
		actual, inspectErr := inspectBridge(ctx, docker, bridge)
		if inspectErr == nil && (actual.Config[0].Subnet != ipam.Config[0].Subnet || actual.Config[0].Gateway != ipam.Config[0].Gateway) {
			inspectErr = errors.New("created bridge does not retain the selected IPv4 subnet and gateway")
		}
		if inspectErr != nil {
			return cleanupFailedNetwork(ctx, bridge, fmt.Errorf("verify explicit bridge: %w", inspectErr), remove)
		}
		return bridge, nil
	}
	return nil, err
}

func cleanupFailedNetwork(ctx context.Context, network *testcontainers.DockerNetwork, cause error, remove removeNetwork) (*testcontainers.DockerNetwork, error) {
	if network == nil {
		return nil, cause
	}
	if network.ID == "" {
		// There is no safe native identity to pass to NetworkRemove.
		return network, cause
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if cleanupErr := remove(cleanupCtx, network); cleanupErr != nil && !errdefs.IsNotFound(cleanupErr) {
		return network, errors.Join(cause, fmt.Errorf("remove failed bridge: %w", cleanupErr))
	}
	return nil, cause
}

func inspectBridge(ctx context.Context, docker inspector, network *testcontainers.DockerNetwork) (*dockernetwork.IPAM, error) {
	if network == nil || network.ID == "" || network.Name == "" || network.Driver != "bridge" {
		return nil, errors.New("bridge creation returned no valid owned network identity")
	}
	inspection, err := docker.NetworkInspect(ctx, network.ID, mobycl.NetworkInspectOptions{})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native := inspection.Network
	if native.ID != network.ID || native.Name != network.Name || native.Driver != "bridge" || native.EnableIPv6 || len(native.Containers) != 0 {
		return nil, errors.New("allocated bridge native identity or empty IPv4-only state changed")
	}
	var selected *dockernetwork.IPAMConfig
	for _, config := range native.IPAM.Config {
		if !config.Subnet.IsValid() || !config.Subnet.Addr().Is4() || config.Subnet != config.Subnet.Masked() {
			return nil, errors.New("allocated bridge has an invalid IPv4 subnet")
		}
		if selected != nil {
			return nil, errors.New("allocated bridge has more than one IPv4 subnet")
		}
		if !usableGateway(config.Subnet, config.Gateway) {
			return nil, errors.New("allocated bridge has no valid IPv4 gateway in its subnet")
		}
		if config.IPRange.IsValid() && (!config.IPRange.Addr().Is4() || config.IPRange != config.IPRange.Masked() || config.IPRange.Bits() < config.Subnet.Bits() || !config.Subnet.Contains(config.IPRange.Addr())) {
			return nil, errors.New("allocated bridge has an invalid IPv4 allocation range")
		}
		config.AuxAddress = maps.Clone(config.AuxAddress)
		selected = &config
	}
	if selected == nil {
		return nil, errors.New("allocated bridge has no IPv4 subnet")
	}
	return &dockernetwork.IPAM{Driver: native.IPAM.Driver, Options: maps.Clone(native.IPAM.Options), Config: []dockernetwork.IPAMConfig{*selected}}, nil
}

func usableGateway(subnet netip.Prefix, gateway netip.Addr) bool {
	return gateway.IsValid() && gateway.Is4() && !gateway.IsUnspecified() && !gateway.IsMulticast() && subnet.Contains(gateway)
}

func poolOverlap(err error) bool {
	return errdefs.IsInvalidArgument(err) && strings.Contains(strings.ToLower(err.Error()), "pool overlaps with other one on this address space")
}
