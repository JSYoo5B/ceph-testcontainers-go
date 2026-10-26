//go:build all || integration

package integration_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The helpers find the handle of a daemon or pool that the cluster owns now.
// They fail the test when there is none, so a scenario that expects a
// refusal for a missing daemon must hold the handle it expects the refusal
// for, not look it up here.

func osdHandle(t testing.TB, ctx context.Context, c *ceph.Container, id int) *ceph.OSDContainer {
	t.Helper()
	osds, err := c.OSDs(ctx)
	if err != nil {
		t.Fatalf("list OSDs: %v", err)
	}
	for _, osd := range osds {
		if osd.ID == id {
			return osd
		}
	}
	t.Fatalf("osd.%d is not owned by the cluster", id)
	return nil
}

func monitorHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.MonitorContainer {
	t.Helper()
	monitors, err := c.Monitors(ctx)
	if err != nil {
		t.Fatalf("list monitors: %v", err)
	}
	for _, mon := range monitors {
		if mon.DaemonName == name {
			return mon
		}
	}
	t.Fatalf("monitor %s is not owned by the cluster", name)
	return nil
}

func managerHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.ManagerContainer {
	t.Helper()
	managers, err := c.Managers(ctx)
	if err != nil {
		t.Fatalf("list managers: %v", err)
	}
	for _, mgr := range managers {
		if mgr.DaemonName == name {
			return mgr
		}
	}
	t.Fatalf("manager %s is not owned by the cluster", name)
	return nil
}

func poolHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.Pool {
	t.Helper()
	pool, err := c.Pool(ctx, name)
	if err != nil {
		t.Fatalf("look up pool %q: %v", name, err)
	}
	return pool
}

// adminSocket runs args on the admin socket of the daemon that daemon names,
// such as "mon.a", "mgr.a" or "osd.0". The daemon must be owned by the
// cluster.
func adminSocket(t testing.TB, ctx context.Context, c *ceph.Container, daemon string, args ...string) ([]byte, error) {
	t.Helper()
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mon":
		return monitorHandle(t, ctx, c, id).AdminSocket(ctx, args...)
	case "mgr":
		return managerHandle(t, ctx, c, id).AdminSocket(ctx, args...)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			t.Fatalf("daemon %q is not an osd.<id>", daemon)
		}
		return osdHandle(t, ctx, c, n).AdminSocket(ctx, args...)
	}
	t.Fatalf("daemon %q is not a mon.<name>, mgr.<name> or osd.<id>", daemon)
	return nil, nil
}

// rotateKey rotates the Cephx key of the MGR or OSD that daemon names, such as
// "mgr.a" or "osd.0". A MON has no RotateKey.
func rotateKey(t testing.TB, ctx context.Context, c *ceph.Container, daemon string) error {
	t.Helper()
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mgr":
		return managerHandle(t, ctx, c, id).RotateKey(ctx)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			t.Fatalf("daemon %q is not an osd.<id>", daemon)
		}
		return osdHandle(t, ctx, c, n).RotateKey(ctx)
	}
	t.Fatalf("daemon %q is not a mgr.<name> or osd.<id>", daemon)
	return nil
}

// socketCluster adapts a cluster to the evidence helpers that name a daemon by
// its Ceph name, such as "mon.a" or "osd.0", when they read its admin socket.
type socketCluster struct {
	*ceph.Container
	t testing.TB
}

func withSockets(t testing.TB, c *ceph.Container) socketCluster { return socketCluster{c, t} }

// AdminSocket runs args on the admin socket of the owned daemon that daemon
// names.
func (s socketCluster) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	s.t.Helper()
	return adminSocket(s.t, ctx, s.Container, daemon, args...)
}

// RemoveMonitor removes the owned monitor that name names.
func (s socketCluster) RemoveMonitor(ctx context.Context, name string) error {
	s.t.Helper()
	return monitorHandle(s.t, ctx, s.Container, name).Remove(ctx)
}
