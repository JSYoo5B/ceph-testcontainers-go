//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

func TestMGRConfigRestoreAfterMissedSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, _ := harness.NewServiceClusterWithOptions(t, image, append(opts,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2).WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	cmd := func(args ...string) {
		t.Helper()
		if _, err := cluster.Ceph(ctx, args...); err != nil {
			t.Fatal(err)
		}
	}
	observer := harness.ObserveCluster(cluster)
	find := func(section, name string) *ceph.ConfigEntry {
		t.Helper()
		entries, err := observer.Configuration(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Section == section && entry.Name == name && entry.Mask == "" {
				return &entry
			}
		}
		return nil
	}
	waitMode := func(want string) {
		t.Helper()
		_, err := harness.Observe(ctx, 30*time.Second, 5*time.Second, 250*time.Millisecond, func(probe context.Context) (string, bool, error) {
			data, err := cluster.Ceph(probe, "balancer", "status", "--format", "json")
			var status struct{ Mode string }
			if err == nil {
				err = json.Unmarshal(data, &status)
			}
			return status.Mode, status.Mode == want, err
		})
		if err != nil {
			t.Fatalf("balancer did not report %q: %v", want, err)
		}
	}
	const option = "mgr/balancer/mode"
	const marker = "mon_lease_ack_timeout_factor"
	waitMode("upmap")
	cmd("config", "set", "mon", "mon_mgr_beacon_grace", "600")
	managers := clusteraccess.Managers(t, cluster)
	if len(managers) != 2 {
		t.Fatal("expected two owned managers")
	}
	first := "mgr." + managers[0].DaemonName
	cmd("config", "set", first, marker, "4.25")
	change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: option, Value: "none"})
	if err != nil {
		t.Fatal(err)
	}
	waitMode("none")
	initial, err := harness.Observe(ctx, 30*time.Second, 5*time.Second, 250*time.Millisecond, func(probe context.Context) (ceph.ManagerStatus, bool, error) {
		status, err := observer.ManagerStatus(probe)
		return status, status.Available && status.ActiveGID != 0 && len(status.Standbys) == 1, err
	})
	if err != nil {
		t.Fatal(err)
	}
	before := initial.Value
	var paused []*fault.PauseLease
	for _, mgr := range managers {
		lease, err := fault.Pause(ctx, cluster, mgr)
		if lease != nil {
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := lease.Resume(cleanup); err != nil {
					t.Errorf("resume manager: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		paused = append(paused, lease)
	}
	mon := clusteraccess.Monitors(t, cluster)[0]
	grace := 5 * time.Second
	if err := mon.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := mon.Start(ctx); err != nil {
		t.Fatal(err)
	}
	interrupted, stop := context.WithTimeout(ctx, 2*time.Second)
	err = change.Restore(interrupted)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore completed while neither manager could receive the empty value: %v", err)
	}
	entry := find("mgr", option)
	mark := find(first, marker)
	if entry == nil || entry.Value != "" || mark == nil {
		t.Fatal("interrupted restoration did not retain the empty module entry and its delivery marker")
	}
	if value, err := strconv.ParseFloat(mark.Value, 64); err != nil || value == 4.25 {
		t.Fatal("interrupted restoration did not publish a fresh delivery marker")
	}
	for _, lease := range paused {
		if err := lease.Resume(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := change.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if find("mgr", option) != nil || find("mgr."+managers[1].DaemonName, marker) != nil {
		t.Fatal("retry left a new configuration entry behind")
	}
	if prior := find(first, marker); prior == nil {
		t.Fatal("retry removed the prior marker setting")
	} else if value, err := strconv.ParseFloat(prior.Value, 64); err != nil || value != 4.25 {
		t.Fatal("retry did not restore the prior marker setting")
	}
	after, err := observer.ManagerStatus(ctx)
	if err != nil || after.ActiveGID != before.ActiveGID || len(after.Standbys) != 1 || after.Standbys[0] != before.Standbys[0] {
		t.Fatal("retry was observed on a different manager generation")
	}
	waitMode("upmap")
	cmd("mgr", "fail", before.ActiveName)
	_, err = harness.Observe(ctx, 30*time.Second, 5*time.Second, 250*time.Millisecond, func(probe context.Context) (ceph.ManagerStatus, bool, error) {
		status, err := observer.ManagerStatus(probe)
		return status, status.ActiveGID == before.Standbys[0].GID, err
	})
	if err != nil {
		t.Fatalf("original standby was not promoted: %v", err)
	}
	waitMode("upmap")
	t.Log("interrupted restore retained its entries; retry verified both original managers and restored the prior database")
}
