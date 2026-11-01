package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/monstore"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// monitorGenerations is the production container side of one MON store
// volume (monStoreGenerations), for the two cases MonitorStorage.WithVolume
// admits: bridge networking with IPv4 addresses, and the host network. Every
// generation is built from recipe, the options of that MON without a start for
// the host ports it listens on (nil on a bridge); Create adds the store mount,
// the entrypoint mode, the recorded address of a later generation and the owned
// label. The containers it created stay in created, by ID, and the host ports
// each was created for in ports, so the caller can take the handle of the
// generation it started and Observe can check what it bound.
//
// It takes no lock: its caller holds Container.mu, as every monStoreVolume
// method requires.
type monitorGenerations struct {
	c       *Container
	name    string
	recipe  func(ports []int) []testcontainers.ContainerCustomizer
	ready   []string
	host    bool
	created map[string]testcontainers.Container
	ports   map[string][]int
	docker  func(context.Context) (monitorGenerationDocker, error)
	// reserve takes host ports: count chosen ports for a first generation
	// (exact nil), or exactly the recorded ones. Tests replace it.
	//
	//testdouble:wired-operation
	reserve func(ctx context.Context, count int, exact []int) (*hostPortLease, error)
	// track publishes a host port reservation for Terminate as soon as its
	// allocator exists.
	track func(*hostPortLease)
	// listening returns the TCP listeners of the ceph-mon process in a running
	// host-network container. Tests replace it.
	//
	//testdouble:wired-operation
	listening func(ctx context.Context, ctr testcontainers.Container) ([]monstore.Listener, error)
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

// monitorGenerations builds the container side of MON name. The caller holds
// c.mu, or the cluster is not shared yet (Run).
func (c *Container) monitorGenerations(name string, recipe func(ports []int) []testcontainers.ContainerCustomizer, ready []string) *monitorGenerations {
	g := &monitorGenerations{c: c, name: name, recipe: recipe, ready: ready, host: c.settings.hostNetwork,
		created: map[string]testcontainers.Container{}, ports: map[string][]int{},
		docker: func(ctx context.Context) (monitorGenerationDocker, error) {
			client, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				return nil, err
			}
			return client, nil
		},
		listening: monitorListeners}
	g.track = func(lease *hostPortLease) { c.portLeases.track(lease) }
	g.reserve = func(ctx context.Context, count int, exact []int) (*hostPortLease, error) {
		address := c.staticFacts().PublicAddress
		if exact != nil {
			return reserveExactHostPorts(ctx, c.settings.essentialImage, address, exact, c.settings.startupTimeout, c.startup.ownedLabel())
		}
		return reserveHostPorts(ctx, c.settings.essentialImage, address, count, c.settings.startupTimeout, c.startup.ownedLabel())
	}
	return g
}

func (g *monitorGenerations) network() string { return g.c.nets.public.Name }

// Create makes the container without starting it. The store volume is mounted
// with no copy at the MON data directory; a store that must exist is required
// by the entrypoint. On a bridge a later generation asks for the recorded IPv4
// address. On the host network a later generation listens on the recorded
// ports, which the caller holds (Hold); a first generation reserves ports
// chosen by the kernel, keeps them while the container is created and releases
// them before it returns, right before the caller starts it, as the first MON
// without a volume does (runMonitor). The caller holds c.mu.
func (g *monitorGenerations) Create(ctx context.Context, launch monstore.Launch, image string) (id string, err error) {
	if g.beforeCreate != nil {
		if err := g.beforeCreate(ctx); err != nil {
			return "", fmt.Errorf("prepare MON %s generation %d: %w", g.name, launch.Generation, err)
		}
	}
	var ports []int
	if g.host {
		if launch.Endpoint != nil {
			if err := g.hostEndpoint(*launch.Endpoint); err != nil {
				return "", err
			}
			ports = slices.Clone(launch.Endpoint.Ports)
		} else {
			lease, reserveErr := g.reserve(ctx, g.c.settings.monitorPortCount(), nil)
			// The allocator may exist also with an error; Terminate must find it.
			g.track(lease)
			if reserveErr != nil {
				return "", fmt.Errorf("reserve the ports of MON %s generation %d: %w", g.name, launch.Generation, reserveErr)
			}
			if lease == nil {
				return "", fmt.Errorf("reserve the ports of MON %s generation %d: no lease", g.name, launch.Generation)
			}
			ports = slices.Clone(lease.Ports)
			defer func() {
				release, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
				defer cancel()
				err = errors.Join(err, lease.Release(release))
			}()
		}
	}
	opts := append([]testcontainers.ContainerCustomizer(nil), g.recipe(ports)...)
	opts = append(opts, testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
		h.Mounts = append(h.Mounts, mount.Mount{Type: mount.TypeVolume, Source: launch.Volume, Target: "/var/lib/ceph/mon/ceph-" + g.name,
			VolumeOptions: &mount.VolumeOptions{NoCopy: true}})
	}))
	if launch.Mode == monstore.Require {
		opts = append(opts, testcontainers.WithEnv(map[string]string{"CEPH_MON_STORE_MODE": "require"}))
	}
	if launch.Endpoint != nil && !g.host {
		ip, err := netip.ParseAddr(launch.Endpoint.IP)
		if err != nil || !ip.Is4() || launch.Endpoint.Network != g.network() {
			return "", fmt.Errorf("MON %s address %s on %q is not an IPv4 address on the cluster network", g.name, launch.Endpoint.IP, launch.Endpoint.Network)
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
	id = ctr.GetContainerID()
	if id != "" {
		g.created[id] = ctr
		if ports != nil {
			g.ports[id] = ports
		}
	}
	return id, err
}

// hostEndpoint checks that a recorded endpoint is one this cluster's host
// network can give a MON: its public address and the MON's number of ports.
func (g *monitorGenerations) hostEndpoint(endpoint monstore.Endpoint) error {
	if endpoint.Network != "" || endpoint.IP != g.c.staticFacts().PublicAddress || len(endpoint.Ports) != g.c.settings.monitorPortCount() {
		return fmt.Errorf("MON %s address %s ports %v is not an endpoint of the host network on %s", g.name, endpoint.IP, endpoint.Ports, g.c.staticFacts().PublicAddress)
	}
	return nil
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
// network, or on the host network the address and ports its ceph-mon process
// listens on, which must be exactly the ports the container was created for.
func (g *monitorGenerations) Observe(ctx context.Context, id string) (monstore.Endpoint, error) {
	if g.host {
		return g.observeHost(ctx, id)
	}
	docker, err := g.docker(ctx)
	if err != nil {
		return monstore.Endpoint{}, err
	}
	defer func() { _ = docker.Close() }()
	inspect, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return monstore.Endpoint{}, err
	}
	if inspect.Container.NetworkSettings == nil || inspect.Container.NetworkSettings.Networks[g.network()] == nil {
		return monstore.Endpoint{}, fmt.Errorf("MON %s container is not attached to %s", g.name, g.network())
	}
	ip := inspect.Container.NetworkSettings.Networks[g.network()].IPAddress
	if !ip.Is4() {
		return monstore.Endpoint{}, fmt.Errorf("MON %s container has no IPv4 address on %s", g.name, g.network())
	}
	return monstore.Endpoint{IP: ip.String(), Network: g.network()}, nil
}

// Hold checks that no container of the cluster network has the address. On a
// bridge nothing can reserve it; every container of the cluster is created
// under Container.mu, and a taken address fails the creation, so the check is
// all a hold needs to do. On the host network it takes the exact ports: an
// allocator container listens on them until the hold is released, and a port
// that any process already listens on fails the hold. The caller holds c.mu.
func (g *monitorGenerations) Hold(ctx context.Context, address monstore.Endpoint) (monstore.EndpointHold, error) {
	if g.host {
		if err := g.hostEndpoint(address); err != nil {
			return nil, err
		}
		lease, err := g.reserve(ctx, len(address.Ports), address.Ports)
		g.track(lease)
		if lease == nil {
			return nil, err
		}
		return lease, err
	}
	docker, err := g.docker(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	network, err := docker.NetworkInspect(ctx, address.Network, mobycl.NetworkInspectOptions{})
	if err != nil {
		return nil, err
	}
	for id, endpoint := range network.Network.Containers {
		if endpoint.IPv4Address.Addr().String() == address.IP {
			return nil, fmt.Errorf("MON %s address %s is taken by container %s", g.name, address.IP, id)
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
// mounts bytes an interrupted mkfs may have left. On the host network a first
// generation that lost its chosen ports to another process between their
// release and its start is removed, its fresh store discarded, and the MON
// started again on a new volume and new ports, as many times as a MON without a
// volume retries (hostPortAttempts). The caller holds c.mu.
func (c *Container) startMonitorStore(ctx context.Context, name, image string, gens *monitorGenerations) (testcontainers.Container, error) {
	for attempt := 1; ; attempt++ {
		store, err := c.monStores.Begin(name)
		if err != nil {
			return nil, err
		}
		if err := store.Create(ctx); err != nil {
			return nil, err
		}
		id, err := store.StartGeneration(ctx, gens, image)
		ctr := gens.created[id]
		if err == nil {
			return ctr, nil
		}
		if ctr == nil && id == "" {
			return nil, errors.Join(err, c.monStores.DiscardFresh(ctx, name))
		}
		if !gens.host || attempt == hostPortAttempts || ctx.Err() != nil || store.Phase != monstore.Fresh || !c.lostMonitorPorts(ctx, ctr) {
			return ctr, err
		}
		if stopErr := store.StopGeneration(ctx, gens); stopErr != nil {
			return ctr, errors.Join(err, stopErr)
		}
		if discardErr := c.monStores.DiscardFresh(ctx, name); discardErr != nil {
			// The container is gone; the record keeps the volume for Terminate.
			return nil, errors.Join(err, discardErr)
		}
	}
}

// lostMonitorPorts reports whether the MON of ctr exited because a port it was
// given was already in use.
func (c *Container) lostMonitorPorts(ctx context.Context, ctr testcontainers.Container) bool {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return isPortConflict(cleanup, ctr)
}
