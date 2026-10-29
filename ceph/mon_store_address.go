package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
)

// This file is the address half of the MON replacement. The monmap pins the
// address a MON joined with, and ceph-mon binds exactly that address, so the
// container that replaces a MON must bind the one the old container had. The
// store record therefore remembers the address its first generation bound,
// every later generation is created for exactly that address, and every
// generation is checked to have bound it.
//
// What the cluster can keep for itself differs by network mode.
//
//   - Host network: the endpoint is the public address and the MON's ports.
//     While the old container runs it owns them, so nothing can be reserved
//     ahead. After it is removed any process on the host can take a port, and
//     the cluster is not the only allocator on that host. The record therefore
//     takes an exact hold on the ports once the old container is confirmed
//     gone, keeps it while the new container is created without being started,
//     and releases it immediately before the start. This is the same shape as
//     the first MON's random-port lease (reserveHostPorts, runMonitor), with the
//     ports named instead of chosen.
//   - Bridge network: the endpoint is an IP on the fixture network, passed to
//     the container as a static assignment. Docker takes no address for a
//     container that does not exist, and every container of the cluster is
//     created under Container.mu, so nothing of the cluster can take it. The
//     container side verifies that the address is free and its Hold is
//     the identity of that check; a creation that cannot get the address fails.
//
// The window between the release and the start remains, as it does for the
// first MON. It fails closed: a taken port or IP makes the start or the
// creation fail, the container and the record stay, and the volume is neither
// formatted nor released. Nothing here claims how a real Docker engine behaves.
//
// The file adds no lock and calls no method that takes one; the record is
// guarded by Container.mu like the rest of monStoreVolume.

// monStoreEndpoint is the endpoint of a MON as the monmap pins it. Exactly one
// of network (bridge: a Docker network name) and ports (host network: the
// listening ports, in the order the MON takes them) is set.
type monStoreEndpoint struct {
	ip      string
	network string
	ports   []int
}

func (a monStoreEndpoint) validate() error {
	ip, err := netip.ParseAddr(a.ip)
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("MON address %q is not a usable IP address", a.ip)
	}
	switch {
	case a.network != "" && len(a.ports) != 0:
		return errors.New("MON address names both a bridge network and host ports")
	case a.network == "" && len(a.ports) == 0:
		return errors.New("MON address names neither a bridge network nor host ports")
	}
	seen := map[int]bool{}
	for _, p := range a.ports {
		if p < 1 || p > 65535 || seen[p] {
			return fmt.Errorf("MON address has an invalid or repeated port %d", p)
		}
		seen[p] = true
	}
	return nil
}

func (a monStoreEndpoint) clone() monStoreEndpoint {
	a.ports = slices.Clone(a.ports)
	return a
}

func (a monStoreEndpoint) equal(b monStoreEndpoint) bool {
	return a.ip == b.ip && a.network == b.network && slices.Equal(a.ports, b.ports)
}

// monStoreEndpointHold keeps an address from every other user until Release.
// A failed Release stays retryable and the address must not be used until one
// succeeds.
type monStoreEndpointHold interface {
	Release(ctx context.Context) error
}

// observeAddress records the address the first generation bound, or checks that
// a later generation bound the recorded one. A generation that bound another
// address could not have joined the monmap, so the difference is an error and
// the recorded address is not replaced.
func (s *monStoreVolume) observeAddress(ctx context.Context, g monStoreGenerations, id string) error {
	observed, err := g.Observe(ctx, id)
	if err != nil {
		return fmt.Errorf("observe the address of MON %s generation %d: %w", s.mon, s.generation, err)
	}
	if err := observed.validate(); err != nil {
		return fmt.Errorf("observe the address of MON %s generation %d: %w", s.mon, s.generation, err)
	}
	if s.endpoint == nil {
		cloned := observed.clone()
		s.endpoint = &cloned
		return nil
	}
	if !s.endpoint.equal(observed) {
		return fmt.Errorf("MON %s generation %d bound another address than the one its monmap pins", s.mon, s.generation)
	}
	return nil
}

// holdAddress takes the exact hold on the recorded address. A hold whose
// release failed earlier is released first, so there is never more than one.
func (s *monStoreVolume) holdAddress(ctx context.Context, g monStoreGenerations) error {
	if err := s.releaseAddressHold(ctx); err != nil {
		return err
	}
	hold, err := g.Hold(ctx, s.endpoint.clone())
	if err != nil {
		return fmt.Errorf("hold the address of MON %s: %w", s.mon, err)
	}
	if isNilPort(hold) {
		return fmt.Errorf("hold the address of MON %s: the container side returned no hold", s.mon)
	}
	s.hold = hold
	return nil
}

// releaseAddressHold releases the hold, if any. The hold is forgotten only by a
// successful release, so a failure is retried by the next call and by Terminate.
func (s *monStoreVolume) releaseAddressHold(ctx context.Context) error {
	if s.hold == nil {
		return nil
	}
	if err := s.hold.Release(ctx); err != nil {
		return fmt.Errorf("release the address hold of MON %s: %w", s.mon, err)
	}
	s.hold = nil
	return nil
}
