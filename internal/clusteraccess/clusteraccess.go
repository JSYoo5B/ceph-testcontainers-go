// Package clusteraccess gives tests one-line access to the context-taking
// topology accessors of ceph.Container and rgw. Each helper fails the test on
// an error, so a refused call is never mistaken for an empty topology.
package clusteraccess

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// ctx detaches from the test's cancellation, which ends before t.Cleanup
// functions run, so the helpers also work inside cleanups.
func ctx(tb testing.TB) context.Context { return context.WithoutCancel(tb.Context()) }

// OSDs returns the owned OSDs of c sorted by ID.
func OSDs(tb testing.TB, c *ceph.Container) []*ceph.OSDContainer {
	tb.Helper()
	osds, err := c.OSDs(ctx(tb))
	if err != nil {
		tb.Fatalf("list OSDs: %v", err)
	}
	return osds
}

// Monitors returns the owned MONs of c sorted by daemon name.
func Monitors(tb testing.TB, c *ceph.Container) []*ceph.MonitorContainer {
	tb.Helper()
	monitors, err := c.Monitors(ctx(tb))
	if err != nil {
		tb.Fatalf("list MONs: %v", err)
	}
	return monitors
}

// Managers returns the owned MGR candidates of c sorted by daemon name.
func Managers(tb testing.TB, c *ceph.Container) []*ceph.ManagerContainer {
	tb.Helper()
	managers, err := c.Managers(ctx(tb))
	if err != nil {
		tb.Fatalf("list MGRs: %v", err)
	}
	return managers
}

// InitialManager returns the container of MGR a, the manager Run starts, or
// nil when no manager named a is a current candidate (it was removed, or the
// cluster started without managers and a later one has another name).
func InitialManager(tb testing.TB, c *ceph.Container) testcontainers.Container {
	tb.Helper()
	for _, manager := range Managers(tb, c) {
		if manager.DaemonName == "a" {
			return manager.Container
		}
	}
	return nil
}

// ControlContainer returns the stable CLI container of c.
func ControlContainer(tb testing.TB, c *ceph.Container) testcontainers.Container {
	tb.Helper()
	control, err := c.ControlContainer(ctx(tb))
	if err != nil {
		tb.Fatalf("control container: %v", err)
	}
	return control
}

// Gateways returns the RGW gateways of c sorted by name.
func Gateways(tb testing.TB, c *ceph.Container) []*rgw.Gateway {
	tb.Helper()
	gateways, err := rgw.Gateways(ctx(tb), c)
	if err != nil {
		tb.Fatalf("list gateways: %v", err)
	}
	return gateways
}

// RGWGateways returns the RGW gateways of the cluster started by rgw.Run.
func RGWGateways(tb testing.TB, c *rgw.Container) []*rgw.Gateway {
	tb.Helper()
	gateways, err := rgw.Gateways(ctx(tb), c.Container)
	if err != nil {
		tb.Fatalf("list gateways: %v", err)
	}
	return gateways
}
