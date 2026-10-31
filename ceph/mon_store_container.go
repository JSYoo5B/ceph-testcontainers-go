package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// monitorGenerations is the production container side of one MON store
// volume (monStoreGenerations), for bridge networking with IPv4 addresses,
// which is what MonitorStorage.WithVolume admits. Every generation is built
// from recipe, the options of that MON without a start; Create adds the store
// mount, the entrypoint mode, the recorded address of a later generation and
// the owned label. The containers it created stay in created, by ID, so the
// caller can take the handle of the generation it started.
//
// It takes no lock: its caller holds Container.mu, as every monStoreVolume
// method requires.
type monitorGenerations struct {
	c       *Container
	name    string
	recipe  []testcontainers.ContainerCustomizer
	ready   []string
	created map[string]testcontainers.Container
	docker  func(context.Context) (monitorGenerationDocker, error)
	// beforeCreate runs right before each container creation, after the
	// previous generation is confirmed gone: the replacement clears the
	// admin socket the old daemon left in the shared run directory there.
	beforeCreate func(context.Context) error
}

// monitorGenerationDocker is the Docker surface beside testcontainers: the
// address of a running generation, the users of an address and the removal of
// a container without its volumes.
type monitorGenerationDocker interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	NetworkInspect(context.Context, string, mobycl.NetworkInspectOptions) (mobycl.NetworkInspectResult, error)
	ContainerRemove(context.Context, string, mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error)
	Close() error
}

func (c *Container) monitorGenerations(name string, recipe []testcontainers.ContainerCustomizer, ready []string) *monitorGenerations {
	return &monitorGenerations{c: c, name: name, recipe: recipe, ready: ready, created: map[string]testcontainers.Container{},
		docker: func(ctx context.Context) (monitorGenerationDocker, error) {
			client, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				return nil, err
			}
			return client, nil
		}}
}

func (g *monitorGenerations) network() string { return g.c.nets.public.Name }

// Create makes the container without starting it. The store volume is mounted
// with no copy at the MON data directory; a store that must exist is required
// by the entrypoint; a later generation asks the bridge for the recorded IPv4
// address.
func (g *monitorGenerations) Create(ctx context.Context, launch monStoreLaunch, image string) (string, error) {
	if g.beforeCreate != nil {
		if err := g.beforeCreate(ctx); err != nil {
			return "", fmt.Errorf("prepare MON %s generation %d: %w", g.name, launch.generation, err)
		}
	}
	opts := append([]testcontainers.ContainerCustomizer(nil), g.recipe...)
	opts = append(opts, testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
		h.Mounts = append(h.Mounts, mount.Mount{Type: mount.TypeVolume, Source: launch.volume, Target: "/var/lib/ceph/mon/ceph-" + g.name,
			VolumeOptions: &mount.VolumeOptions{NoCopy: true}})
	}))
	if launch.mode == monStoreRequire {
		opts = append(opts, testcontainers.WithEnv(map[string]string{"CEPH_MON_STORE_MODE": "require"}))
	}
	if launch.endpoint != nil {
		ip, err := netip.ParseAddr(launch.endpoint.ip)
		if err != nil || !ip.Is4() || launch.endpoint.network != g.network() {
			return "", fmt.Errorf("MON %s address %s on %q is not an IPv4 address on the cluster network", g.name, launch.endpoint.ip, launch.endpoint.network)
		}
		network := g.network()
		opts = append(opts, testcontainers.WithEndpointSettingsModifier(func(settings map[string]*dockernetwork.EndpointSettings) {
			endpoint := settings[network]
			if endpoint == nil {
				endpoint = &dockernetwork.EndpointSettings{}
				settings[network] = endpoint
			}
			endpoint.IPAMConfig = &dockernetwork.EndpointIPAMConfig{IPv4Address: ip}
		}))
	}
	opts = append(opts, testcontainers.WithNoStart(), g.c.startup.ownedLabel())
	ctr, err := g.c.runDaemon(ctx, image, opts...)
	if ctr == nil {
		return "", err
	}
	id := ctr.GetContainerID()
	if id != "" {
		g.created[id] = ctr
	}
	return id, err
}

func (g *monitorGenerations) container(id string) (testcontainers.Container, error) {
	ctr := g.created[id]
	if ctr == nil {
		return nil, fmt.Errorf("MON %s container %s was not created by this generation side", g.name, id)
	}
	return ctr, nil
}

// Start starts the created container; its wait strategy and the address hooks
// of the recipe run as for any MON.
func (g *monitorGenerations) Start(ctx context.Context, id string) error {
	ctr, err := g.container(id)
	if err != nil {
		return err
	}
	return ctr.Start(ctx)
}

// WaitReady runs the MON's ready check once more after Start.
func (g *monitorGenerations) WaitReady(ctx context.Context, id string) error {
	ctr, err := g.container(id)
	if err != nil {
		return err
	}
	_, err = command(ctx, ctr, g.ready...)
	return err
}

// Observe reads the IPv4 address the running container has on the cluster
// network.
func (g *monitorGenerations) Observe(ctx context.Context, id string) (monStoreEndpoint, error) {
	docker, err := g.docker(ctx)
	if err != nil {
		return monStoreEndpoint{}, err
	}
	defer func() { _ = docker.Close() }()
	inspect, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return monStoreEndpoint{}, err
	}
	if inspect.Container.NetworkSettings == nil || inspect.Container.NetworkSettings.Networks[g.network()] == nil {
		return monStoreEndpoint{}, fmt.Errorf("MON %s container is not attached to %s", g.name, g.network())
	}
	ip := inspect.Container.NetworkSettings.Networks[g.network()].IPAddress
	if !ip.Is4() {
		return monStoreEndpoint{}, fmt.Errorf("MON %s container has no IPv4 address on %s", g.name, g.network())
	}
	return monStoreEndpoint{ip: ip.String(), network: g.network()}, nil
}

// Hold checks that no container of the cluster network has the address. On a
// bridge nothing can reserve it; every container of the cluster is created
// under Container.mu, and a taken address fails the creation, so the check is
// all a hold needs to do.
func (g *monitorGenerations) Hold(ctx context.Context, address monStoreEndpoint) (monStoreEndpointHold, error) {
	docker, err := g.docker(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	network, err := docker.NetworkInspect(ctx, address.network, mobycl.NetworkInspectOptions{})
	if err != nil {
		return nil, err
	}
	for id, endpoint := range network.Network.Containers {
		if endpoint.IPv4Address.Addr().String() == address.ip {
			return nil, fmt.Errorf("MON %s address %s is taken by container %s", g.name, address.ip, id)
		}
	}
	return monitorAddressCheck{}, nil
}

// monitorAddressCheck is the hold of a bridge address: nothing to release.
type monitorAddressCheck struct{}

func (monitorAddressCheck) Release(context.Context) error { return nil }

// Remove removes the container by force and never its volumes. A container
// that is already gone is not an error.
func (g *monitorGenerations) Remove(ctx context.Context, id string) error {
	docker, err := g.docker(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ContainerRemove(ctx, id, mobycl.ContainerRemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// startMonitorStore publishes the store record of a new MON, creates its volume
// and starts the first generation through gens. A first generation that failed
// with its container confirmed gone discards the fresh store, so a retry never
// mounts bytes an interrupted mkfs may have left. The caller holds c.mu.
func (c *Container) startMonitorStore(ctx context.Context, name, image string, gens *monitorGenerations) (testcontainers.Container, error) {
	store, err := c.monStores.begin(name)
	if err != nil {
		return nil, err
	}
	if err := store.create(ctx); err != nil {
		return nil, err
	}
	id, err := store.startGeneration(ctx, gens, image)
	ctr := gens.created[id]
	if err != nil && ctr == nil && id == "" {
		return nil, errors.Join(err, c.monStores.discardFresh(ctx, name))
	}
	return ctr, err
}
