package ceph

import (
	"context"
	"encoding/json"
	"fmt"
)

// Status contains the subset of ceph status JSON used for readiness.
// Use Ceph for other commands or the full JSON response.
type Status struct {
	FSID   string `json:"fsid"`
	Health struct {
		Status string `json:"status"`
	} `json:"health"`
	MgrMap struct {
		Available bool `json:"available"`
	} `json:"mgrmap"`
	OSDMap struct {
		NumOSDs   int `json:"num_osds"`
		NumUpOSDs int `json:"num_up_osds"`
		NumInOSDs int `json:"num_in_osds"`
	} `json:"osdmap"`
	PGMap struct {
		NumPGs     int `json:"num_pgs"`
		PGsByState []struct {
			StateName string `json:"state_name"`
			Count     int    `json:"count"`
		} `json:"pgs_by_state"`
	} `json:"pgmap"`
}

// Status queries the monitor's JSON API through the container-local CLI.
func (c *Container) Status(ctx context.Context) (Status, error) {
	var s Status
	data, err := c.Ceph(ctx, "status", "--format", "json")
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("decode ceph status: %w", err)
	}
	return s, nil
}

// WaitForClean waits for every PG to be active+clean and every owned OSD up/in.
// At least one pool/PG must exist. HEALTH_WARN alone does not prevent readiness.
func (c *Container) WaitForClean(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	expected := len(c.osds)
	c.mu.Unlock()
	return c.poll(ctx, func() (bool, error) {
		s, err := c.Status(ctx)
		if err != nil {
			return false, err
		}
		if !s.MgrMap.Available || s.OSDMap.NumOSDs != expected || s.OSDMap.NumUpOSDs != expected || s.OSDMap.NumInOSDs != expected || s.PGMap.NumPGs == 0 {
			return false, nil
		}
		clean := 0
		for _, state := range s.PGMap.PGsByState {
			if state.StateName != "active+clean" {
				return false, nil
			}
			clean += state.Count
		}
		return clean == s.PGMap.NumPGs, nil
	})
}
