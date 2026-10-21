//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedRBDPool is the RBD pool of the shared cluster of the services test.
const sharedRBDPool = "rbd"

func leaseRBD(t *testing.T, ctx context.Context, tenant *ceph.Tenant) *rbd.TenantNamespace {
	t.Helper()
	began := time.Now()
	namespace, err := rbd.Lease(ctx, tenant, sharedRBDPool)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rbd.Lease of %s took %s", tenant.ID(), time.Since(began).Round(time.Millisecond))
	return namespace
}

func leaseCephFS(t *testing.T, ctx context.Context, tenant *ceph.Tenant) *cephfs.TenantGroup {
	t.Helper()
	began := time.Now()
	group, err := cephfs.Lease(ctx, tenant, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cephfs.Lease of %s took %s", tenant.ID(), time.Since(began).Round(time.Millisecond))
	return group
}

func leaseRGW(t *testing.T, ctx context.Context, tenant *ceph.Tenant) *rgw.TenantUser {
	t.Helper()
	began := time.Now()
	user, err := rgw.Lease(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rgw.Lease of %s took %s", tenant.ID(), time.Since(began).Round(time.Millisecond))
	return user
}

// startTenantClientWith starts an idle client container attached by a
// tenant's or a service lease's WithClient.
func startTenantClientWith(t *testing.T, ctx context.Context, attach testcontainers.CustomizeRequestOption) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, controlImage(t)), attach, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// startHolder starts a client container that runs a Python script, which
// prints ready once it holds what it holds, and keeps it until the
// container ends.
func startHolder(t *testing.T, ctx context.Context, attach testcontainers.CustomizeRequestOption, ready, script string, args ...string) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, controlImage(t)), attach,
		testcontainers.WithEntrypoint(append([]string{"python3", "-c", script}, args...)...),
		testcontainers.WithWaitStrategy(wait.ForLog(ready).WithStartupTimeout(2*time.Minute)))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// testcontainersStopNow kills a container at once, as a crashed client
// ends.
func testcontainersStopNow() testcontainers.TerminateOption { return testcontainers.StopTimeout(0) }

// tenantExpectArgv runs argv as given in a tenant's client container and
// checks that it succeeds, or that Ceph denies it.
func tenantExpectArgv(t *testing.T, ctx context.Context, tenant *ceph.Tenant, ctr testcontainers.Container, wantOK bool, label string, argv ...string) {
	t.Helper()
	result, err := tenant.RunNativeCommand(ctx, ctr, ceph.NativeCommand{Argv: argv, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: label})
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	output := strings.TrimSpace(string(result.Stdout) + string(result.Stderr))
	t.Logf("%s: exit %d", label, result.ExitCode)
	switch {
	case wantOK && result.ExitCode != 0:
		t.Errorf("%s failed: exit %d: %s", label, result.ExitCode, output)
	case !wantOK && result.ExitCode == 0:
		t.Errorf("%s was allowed", label)
	case !wantOK && !deniedOutput(output):
		t.Errorf("%s failed for another reason than a denial: exit %d: %s", label, result.ExitCode, output)
	}
}

func tenantS3(t *testing.T, ctx context.Context, user *rgw.TenantUser) s3HTTPClient {
	t.Helper()
	endpoint, err := user.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accessKey, secretKey := user.Credentials()
	return s3HTTPClient{endpoint: endpoint, accessKey: accessKey, secretKey: secretKey, region: user.Region(),
		http: &http.Client{Timeout: 30 * time.Second}}
}

// decodeCommand runs a CLI command in the control container and decodes
// its JSON output.
func decodeCommand(t *testing.T, ctx context.Context, control testcontainers.Container, into any, args ...string) {
	t.Helper()
	data, err := servicehook.Command(ctx, control, args...)
	if err != nil {
		t.Fatalf("%s: %v", strings.Join(args, " "), err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("%s: %v: %s", strings.Join(args, " "), err, data)
	}
}
