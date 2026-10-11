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

// WaitForClean waits until a manager is available, the native OSD count
// equals the owned OSD count with every OSD up and in, at least one PG exists
// and every PG is exactly active+clean. HEALTH_WARN alone does not block it.
// PG states with extra flags, such as active+clean+scrubbing, do not count. It
// never succeeds while an owned OSD is intentionally stopped or out; use
// WaitForPGClean then. The cluster's startup timeout also bounds the wait.
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
