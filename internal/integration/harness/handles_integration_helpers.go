//go:build all || integration

package harness

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
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

func findMonitor(ctx context.Context, c *ceph.Container, name string) (*ceph.MonitorContainer, error) {
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

func findManager(ctx context.Context, c *ceph.Container, name string) (*ceph.ManagerContainer, error) {
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

// OSDHandle, MonitorHandle and ManagerHandle fail the test when the lookup
// fails. Call them from the test goroutine with a context that is not
// canceled on purpose; code that runs in a goroutine or under such a context
// resolves the handle first or uses the find functions, which return errors.
func OSDHandle(t testing.TB, ctx context.Context, c *ceph.Container, id int) *ceph.OSDContainer {
	t.Helper()
	osd, err := FindOSD(ctx, c, id)
	if err != nil {
		t.Fatal(err)
	}
	return osd
}

func MonitorHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.MonitorContainer {
	t.Helper()
	mon, err := findMonitor(ctx, c, name)
	if err != nil {
		t.Fatal(err)
	}
	return mon
}

func ManagerHandle(t testing.TB, ctx context.Context, c *ceph.Container, name string) *ceph.ManagerContainer {
	t.Helper()
	mgr, err := findManager(ctx, c, name)
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

// AdminSocket runs args on the admin socket of the daemon that daemon names,
// such as "mon.a", "mgr.a" or "osd.0". The daemon must be owned by the
// cluster. A failed lookup is returned as an error, never a test failure, so
// a canceled context or a goroutine can call it.
func AdminSocket(_ testing.TB, ctx context.Context, c *ceph.Container, daemon string, args ...string) ([]byte, error) {
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mon":
		mon, err := findMonitor(ctx, c, id)
		if err != nil {
			return nil, err
		}
		return mon.AdminSocket(ctx, args...)
	case "mgr":
		mgr, err := findManager(ctx, c, id)
		if err != nil {
			return nil, err
		}
		return mgr.AdminSocket(ctx, args...)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			return nil, fmt.Errorf("daemon %q is not an osd.<id>", daemon)
		}
		osd, err := FindOSD(ctx, c, n)
		if err != nil {
			return nil, err
		}
		return osd.AdminSocket(ctx, args...)
	}
	return nil, fmt.Errorf("daemon %q is not a mon.<name>, mgr.<name> or osd.<id>", daemon)
}

// rotateKey rotates the Cephx key of the MGR or OSD that daemon names, such as
// "mgr.a" or "osd.0". A MON has no RotateKey. Lookup failures are errors.
func rotateKey(_ testing.TB, ctx context.Context, c *ceph.Container, daemon string) error {
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mgr":
		mgr, err := findManager(ctx, c, id)
		if err != nil {
			return err
		}
		return mgr.RotateKey(ctx)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			return fmt.Errorf("daemon %q is not an osd.<id>", daemon)
		}
		osd, err := FindOSD(ctx, c, n)
		if err != nil {
			return err
		}
		return osd.RotateKey(ctx)
	}
	return fmt.Errorf("daemon %q is not a mgr.<name> or osd.<id>", daemon)
}

// socketCluster adapts a cluster to the evidence helpers that name a daemon by
// its Ceph name, such as "mon.a" or "osd.0", when they read its admin socket.
type socketCluster struct {
	*ceph.Container
	t testing.TB
}

func WithSockets(t testing.TB, c *ceph.Container) socketCluster { return socketCluster{c, t} }

// AdminSocket runs args on the admin socket of the owned daemon that daemon
// names.
func (s socketCluster) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	s.t.Helper()
	return AdminSocket(s.t, ctx, s.Container, daemon, args...)
}

// RemoveMonitor removes the owned monitor that name names.
func (s socketCluster) RemoveMonitor(ctx context.Context, name string) error {
	mon, err := findMonitor(ctx, s.Container, name)
	if err != nil {
		return err
	}
	return mon.Remove(ctx)
}

// RedactSecrets masks the cluster's secrets and values in data with
// diagnostics.Redact.
func (s socketCluster) RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error) {
	return diagnostics.Redact(ctx, s.Container, data, values...)
}

// CollectDiagnostics collects the cluster's diagnostics with
// diagnostics.Collect.
func (s socketCluster) CollectDiagnostics(ctx context.Context, config diagnostics.Config) (*diagnostics.Report, error) {
	return diagnostics.Collect(ctx, s.Container, config)
}

// ManagerStatus reads the manager map through the cluster's Observer.
func (s socketCluster) ManagerStatus(ctx context.Context) (ceph.ManagerStatus, error) {
	return ObserveCluster(s.Container).ManagerStatus(ctx)
}

// QuorumStatus reads the monitor quorum through the cluster's Observer.
func (s socketCluster) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	return ObserveCluster(s.Container).QuorumStatus(ctx)
}
