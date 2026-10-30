package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
)

// managerServices implements Observer.ManagerServices.
func (c *Container) managerServices(ctx context.Context) (map[string]string, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readManagerServices(ctx, c.conn().ceph)
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
