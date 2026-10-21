//go:build (all || integration) && ((shared_benchmark && !ci) || (ci_optional && (!ci_batch || ci_batch_shared_benchmark)))

//ci: timeout=60m job-timeout=75 image-roles=essential,client

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// TestSharedClusterBenchmark compares N small RADOS tests, each writing and
// reading one object through a namespace-scoped client in a client
// container, run against a cluster of their own and against tenants of one
// shared cluster, sequentially and four at a time. It is not part of the
// required CI batches: run it with -tags integration,shared_benchmark, or as
// the optional batch shared_benchmark. SHARED_BENCHMARK_N
// lists the values of N, 1,5,20 by default. Clusters of their own run only
// sequentially, so that one cluster at a time fits a small Docker VM.
func TestSharedClusterBenchmark(t *testing.T) {
	counts := []int{1, 5, 20}
	if value := os.Getenv("SHARED_BENCHMARK_N"); value != "" {
		counts = nil
		for _, field := range strings.Split(value, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(field))
			if err != nil || n < 1 {
				t.Fatalf("SHARED_BENCHMARK_N=%q", value)
			}
			counts = append(counts, n)
		}
	}
	image, run := integrationImages(t)
	run = append(run, ceph.WithOSDCount(1))
	var report []string
	for _, n := range counts {
		began := time.Now()
		t.Run(fmt.Sprintf("own-%d", n), func(t *testing.T) {
			for i := range n {
				t.Run(strconv.Itoa(i), func(t *testing.T) { ownClusterRADOSTest(t, image, run) })
			}
		})
		report = append(report, fmt.Sprintf("N=%-2d own clusters, sequential:   %8s", n, time.Since(began).Round(time.Second)))
		for _, parallel := range []bool{false, true} {
			shared := ceph.NewShared(image, ceph.WithSharedRunOptions(run...), ceph.WithMaxTenants(4))
			began := time.Now()
			t.Run(fmt.Sprintf("shared-%d-parallel-%v", n, parallel), func(t *testing.T) {
				for i := range n {
					t.Run(strconv.Itoa(i), func(t *testing.T) {
						if parallel {
							t.Parallel()
						}
						tenantRADOSTest(t, shared.Lease(t))
					})
				}
			})
			total := time.Since(began)
			stats := shared.Stats()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			if err := shared.Close(ctx); err != nil {
				t.Errorf("close the shared cluster: %v", err)
			}
			cancel()
			mode := "sequential"
			if parallel {
				mode = "4 at a time"
			}
			report = append(report, fmt.Sprintf("N=%-2d shared cluster, %-11s %8s (start %s, lease setup %s, lease wait %s, release %s, starts %d, poisonings %d)",
				n, mode+":", total.Round(time.Second), stats.StartTime.Round(100*time.Millisecond), stats.LeaseSetup.Round(100*time.Millisecond),
				stats.LeaseWait.Round(100*time.Millisecond), stats.Release.Round(100*time.Millisecond), stats.Starts, stats.Poisonings))
		}
	}
	t.Logf("shared cluster benchmark:\n%s", strings.Join(report, "\n"))
}

const benchmarkRADOS = `rados --id "$1" -p "$2" -N "$3" put object /etc/hostname && rados --id "$1" -p "$2" -N "$3" get object /tmp/object && cmp /etc/hostname /tmp/object`

// ownClusterRADOSTest is the small test with a cluster of its own.
func ownClusterRADOSTest(t *testing.T, image string, run []testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, err := ceph.Run(ctx, image, run...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	const pool, namespace = "bench", "ns"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.CreateClient(ctx, "bench", ceph.ClientCaps{Mon: "allow r", OSD: "allow rw pool=" + pool + " namespace=" + namespace})
	if err != nil {
		t.Fatal(err)
	}
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, cluster.ControlImage()), cluster.WithClientIdentity(client), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	result, err := cluster.RunNativeCommand(ctx, ctr, ceph.NativeCommand{Argv: []string{"sh", "-c", benchmarkRADOS, "sh", client.User(), pool, namespace},
		Timeout: time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("RADOS test: %v, exit %d: %s", err, result.ExitCode, result.Stderr)
	}
}

// tenantRADOSTest is the same test as a tenant of a shared cluster.
func tenantRADOSTest(t *testing.T, tenant *ceph.Tenant) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	ctr := startTenantClient(t, ctx, tenant)
	result, err := tenant.RunNativeCommand(ctx, ctr, ceph.NativeCommand{Argv: []string{"sh", "-c", benchmarkRADOS, "sh", tenant.Client().User(), tenant.Pool(), tenant.Namespace()},
		Timeout: time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("RADOS test: %v, exit %d: %s", err, result.ExitCode, result.Stderr)
	}
}
