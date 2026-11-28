package ceph

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
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

// waitForClean implements the observer wait and is available to cluster startup.
func (c *Container) waitForClean(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	expected := c.fleet.osdHandles.count()
	c.mu.Unlock()
	return clusterutil.Poll(ctx, func() (bool, error) {
		s, err := readStatus(ctx, c.conn().ceph)
		if err != nil {
			return false, err
		}
		return s.OSDMap.NumOSDs == expected && s.OSDMap.NumUpOSDs == expected && s.OSDMap.NumInOSDs == expected && pgsAreClean(s), nil
	})
}
