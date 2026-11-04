//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// osdHandle, MonitorHandle and ManagerHandle fail the test when the lookup
// fails. Call them from the test goroutine with a context that is not
// canceled on purpose; code that runs in a goroutine or under such a context
// resolves the handle first or uses the find functions, which return errors.
func osdHandle(t testing.TB, ctx context.Context, c *ceph.Container, id int) *ceph.OSDContainer {
	t.Helper()
	osd, err := harness.FindOSD(ctx, c, id)
	if err != nil {
		t.Fatal(err)
	}
	return osd
}

// rotateKey rotates the Cephx key of the MGR or OSD that daemon names, such as
// "mgr.a" or "osd.0". A MON has no RotateKey. Lookup failures are errors.
func rotateKey(_ testing.TB, ctx context.Context, c *ceph.Container, daemon string) error {
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mgr":
		mgr, err := harness.FindManager(ctx, c, id)
		if err != nil {
			return err
		}
		return mgr.RotateKey(ctx)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			return fmt.Errorf("daemon %q is not an osd.<id>", daemon)
		}
		osd, err := harness.FindOSD(ctx, c, n)
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

// AdminSocket runs args on the admin socket of the owned daemon that daemon
// names.
func (s socketCluster) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	s.t.Helper()
	return harness.AdminSocket(ctx, s.Container, daemon, args...)
}

// RemoveMonitor removes the owned monitor that name names.
func (s socketCluster) RemoveMonitor(ctx context.Context, name string) error {
	mon, err := harness.FindMonitor(ctx, s.Container, name)
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
	return harness.ObserveCluster(s.Container).ManagerStatus(ctx)
}

// QuorumStatus reads the monitor quorum through the cluster's Observer.
func (s socketCluster) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	return harness.ObserveCluster(s.Container).QuorumStatus(ctx)
}

func withSockets(t testing.TB, c *ceph.Container) socketCluster { return socketCluster{c, t} }
