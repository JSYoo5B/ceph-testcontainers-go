//go:build all || integration

package harness

import (
	"context"
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The helpers find the handle of a daemon or pool that the cluster owns now.
// They fail the test when there is none, so a scenario that expects a
// refusal for a missing daemon must hold the handle it expects the refusal
// for, not look it up here.

func FindOSD(ctx context.Context, c *ceph.Container, id int) (*ceph.OSDContainer, error) {
	osds, err := c.OSDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list OSDs: %w", err)
	}
	for _, osd := range osds {
		if osd.ID == id {
			return osd, nil
		}
	}
	return nil, fmt.Errorf("osd.%d is not owned by the cluster", id)
}

func FindMonitor(ctx context.Context, c *ceph.Container, name string) (*ceph.MonitorContainer, error) {
	monitors, err := c.Monitors(ctx)
	if err != nil {
		return nil, fmt.Errorf("list monitors: %w", err)
	}
	for _, mon := range monitors {
		if mon.DaemonName == name {
			return mon, nil
		}
	}
	return nil, fmt.Errorf("monitor %s is not owned by the cluster", name)
}

func FindManager(ctx context.Context, c *ceph.Container, name string) (*ceph.ManagerContainer, error) {
	managers, err := c.Managers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managers: %w", err)
	}
	for _, mgr := range managers {
		if mgr.DaemonName == name {
			return mgr, nil
		}
	}
	return nil, fmt.Errorf("manager %s is not owned by the cluster", name)
}

func MonitorHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.MonitorContainer {
	t.Helper()
	mon, err := FindMonitor(ctx, c, name)
	if err != nil {
		t.Fatal(err)
	}
	return mon
}

func ManagerHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.ManagerContainer {
	t.Helper()
	mgr, err := FindManager(ctx, c, name)
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func FindPool(ctx context.Context, c *ceph.Container, name string) (*ceph.Pool, error) {
	pool, err := c.Pool(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("look up pool %q: %w", name, err)
	}
	return pool, nil
}

func PoolHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.Pool {
	t.Helper()
	pool, err := c.Pool(ctx, name)
	if err != nil {
		t.Fatalf("look up pool %q: %v", name, err)
	}
	return pool
}
