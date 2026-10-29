//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// newSharedCluster describes a shared cluster of the integration images with
// one OSD and closes it after t, as ceph.RunTests would after TestMain.
func newSharedCluster(t *testing.T, opts ...ceph.SharedOption) *ceph.SharedCluster {
	t.Helper()
	image, run := integrationImages(t)
	opts = append([]ceph.SharedOption{ceph.WithSharedRunOptions(run.WithCustomizers(ceph.WithOSDCount(1)))}, opts...)
	shared := ceph.NewShared(image, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := shared.Close(ctx); err != nil {
			t.Errorf("close the shared cluster: %v", err)
		}
		t.Logf("shared cluster stats: %+v", shared.Stats())
	})
	return shared
}

func sharedFSID(t *testing.T, ctx context.Context, tenant *ceph.Tenant) string {
	t.Helper()
	status, err := observeTenant(tenant).Status(ctx)
	if err != nil || status.FSID == "" {
		t.Fatalf("tenant status: %q, %v", status.FSID, err)
	}
	return status.FSID
}

// startTenantClient starts a client container with the tenant's client.
func startTenantClient(t *testing.T, ctx context.Context, tenant *ceph.Tenant) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, essentialImage(t)), tenant.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// startTenantClientAs starts a client container with one of the tenant's
// clients.
func startTenantClientAs(t *testing.T, ctx context.Context, tenant *ceph.Tenant, client *ceph.ClientConfig) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, essentialImage(t)), tenant.WithClientIdentity(client), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// tenantExpect runs a ceph or rados command as the tenant's client in ctr.
func tenantExpect(t *testing.T, ctx context.Context, tenant *ceph.Tenant, ctr testcontainers.Container, wantOK bool, label string, argv ...string) {
	t.Helper()
	tenantExpectAs(t, ctx, tenant, ctr, tenant.Client(), wantOK, label, argv...)
}

// tenantExpectAs runs a ceph or rados command as client and checks that it
// succeeds, or that Ceph denies it.
func tenantExpectAs(t *testing.T, ctx context.Context, tenant *ceph.Tenant, ctr testcontainers.Container, client *ceph.ClientConfig, wantOK bool, label string, argv ...string) {
	t.Helper()
	command := append([]string{argv[0], "--id", client.User(), "--connect-timeout", "10"}, argv[1:]...)
	if argv[0] == "rados" {
		command = append([]string{argv[0], "--id", client.User()}, argv[1:]...)
	}
	result, err := tenant.RunNativeCommand(ctx, ctr, ceph.NativeCommand{Argv: command, Timeout: time.Minute,
		MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: label})
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	output := strings.TrimSpace(string(result.Stdout) + string(result.Stderr))
	t.Logf("%s as %s: exit %d", label, client.Name(), result.ExitCode)
	switch {
	case wantOK && result.ExitCode != 0:
		t.Errorf("%s failed: exit %d: %s", label, result.ExitCode, output)
	case !wantOK && result.ExitCode == 0:
		t.Errorf("%s was allowed", label)
	case !wantOK && !deniedOutput(output):
		t.Errorf("%s failed for another reason than a denial: exit %d: %s", label, result.ExitCode, output)
	}
}

func deniedOutput(output string) bool {
	for _, denial := range []string{"Operation not permitted", "EACCES", "EPERM", "access denied", "Permission denied"} {
		if strings.Contains(output, denial) {
			return true
		}
	}
	return false
}

// cleanupTB lets a test end a lease before its own end and observe what the
// release reports: it records errors and cleanups instead of passing them to
// the test, and finish runs the cleanups.
type cleanupTB struct {
	testing.TB

	mu       sync.Mutex
	failed   bool
	errors   []string
	cleanups []func()
}

func (c *cleanupTB) Failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

func (c *cleanupTB) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed = true
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
}

func (c *cleanupTB) Cleanup(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups = append(c.cleanups, fn)
}

func (c *cleanupTB) finish() {
	c.mu.Lock()
	cleanups := c.cleanups
	c.cleanups = nil
	c.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

// namedTB is a cleanupTB with a test name of its own, a subtest name of the
// test it wraps, so that a SharedCluster tells its leases apart from the
// test's. Fatalf records the failure like Errorf, so it may be called from
// another goroutine.
type namedTB struct {
	*cleanupTB
	name string
}

func newNamedTB(t testing.TB, name string) *namedTB {
	return &namedTB{cleanupTB: &cleanupTB{TB: t}, name: t.Name() + "/" + name}
}

func (n *namedTB) Name() string { return n.name }

func (n *namedTB) Fatalf(format string, args ...any) { n.Errorf(format, args...) }

func (n *namedTB) errorText() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Join(n.errors, "\n")
}
