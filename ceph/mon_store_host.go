package ceph

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/monstore"
	"github.com/testcontainers/testcontainers-go"
)

// monitorListeners runs monstore.ListenersScript in ctr.
func monitorListeners(ctx context.Context, ctr testcontainers.Container) ([]monstore.Listener, error) {
	out, err := command(ctx, ctr, "python3", "-I", "-c", monstore.ListenersScript)
	if err != nil {
		return nil, err
	}
	var listeners []monstore.Listener
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &listeners); err != nil {
		return nil, fmt.Errorf("decode the listeners of ceph-mon: %w", err)
	}
	return listeners, nil
}

// observeHost returns the endpoint of a host-network generation: the public
// address and the ports it was created for, in that order, once its ceph-mon
// listens on exactly those ports of that address and on nothing else.
func (g *monitorGenerations) observeHost(ctx context.Context, id string) (monstore.Endpoint, error) {
	ctr, err := g.container(id)
	if err != nil {
		return monstore.Endpoint{}, err
	}
	ports := g.ports[id]
	if len(ports) == 0 {
		return monstore.Endpoint{}, fmt.Errorf("MON %s container %.12s has no recorded host ports", g.name, id)
	}
	listeners, err := g.listening(ctx, ctr)
	if err != nil {
		return monstore.Endpoint{}, fmt.Errorf("read the listeners of MON %s: %w", g.name, err)
	}
	address := g.c.staticFacts().PublicAddress
	if err := monstore.MatchListeners(listeners, address, ports); err != nil {
		return monstore.Endpoint{}, fmt.Errorf("MON %s: %w", g.name, err)
	}
	return monstore.Endpoint{IP: address, Ports: slices.Clone(ports)}, nil
}
