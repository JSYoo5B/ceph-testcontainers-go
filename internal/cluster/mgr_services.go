package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
)

// ManagerServices returns the service URLs, keyed by module name such as
// prometheus or dashboard, that the active manager advertises in the MGR map.
// A module appears only after it has started its listener, and a failover
// replaces the URLs with the new active manager's. The URLs carry the address
// the module bound: a container address reachable from WithClient containers
// in bridge mode, or the configured address in host mode. They are not mapped
// to Docker host ports. An empty map means no module advertises a service.
func (c *Container) ManagerServices(ctx context.Context) (map[string]string, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	data, err := c.Ceph(ctx, "mgr", "services", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeManagerServices(data)
}

func decodeManagerServices(data []byte) (map[string]string, error) {
	var services map[string]string
	if err := json.Unmarshal(data, &services); err != nil || services == nil {
		return nil, errors.New("decode native manager services")
	}
	for name, address := range services {
		parsed, err := url.Parse(address)
		if name == "" || err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return nil, errors.New("native manager service has an invalid URL")
		}
	}
	return services, nil
}
