//go:build all || (integration && diagnostics && (!ci || (ci_topology && (!ci_batch || ci_batch_diagnostics))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Diagnostics retain their capability tag for manual runs and also belong to
// the all suite and their own CI topology batch.
func TestClusterDiagnostics(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testClusterDiagnostics(t, host) })
	}
}

func TestPartialClusterDiagnostics(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithStartupTimeout(5*time.Second),
		testcontainers.WithEntrypoint("/bin/sh", "-c", "exit 17"), testcontainers.WithCmd())
	cluster, runErr := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if cluster == nil || runErr == nil || cluster.Container == nil {
		t.Fatal("expected inspectable partial cluster after the actual initial MON failure")
	}
	containers := diagnosticsOwnedContainers(cluster)
	before := diagnosticsCaptureState(t, ctx, cluster, containers, false)
	report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{Timeout: 30 * time.Second, OperationTimeout: 2 * time.Second})
	if report == nil || err == nil || report.Complete {
		t.Fatal("partial bootstrap diagnostics did not retain a report and CLI errors")
	}
	diagnosticsAssertReport(t, report, false, true)
	inspect := diagnosticsFindArtifact(t, report, "container-inspect", "", cluster.GetContainerID())
	running, found := diagnosticsJSONBool(inspect.Data, "running")
	if inspect.Error != "" || !found || running {
		t.Fatal("partial bootstrap lost its stopped initial MON inspection")
	}
	if artifact := diagnosticsFindArtifact(t, report, "container-logs", "", cluster.GetContainerID()); artifact.Error != "" {
		t.Fatal("partial bootstrap lost its initial MON logs")
	}
	for _, name := range []string{"version", "status", "health", "pgs", "quorum", "monmap", "mgrmap", "osdmap", "fsmap"} {
		if artifact := diagnosticsFindArtifact(t, report, "ceph", name, ""); artifact.Error == "" {
			t.Fatalf("stopped initial MON unexpectedly supplied native %s", name)
		}
	}
	if diagnosticsCountKind(report, "network-inspect") != 1 {
		t.Fatal("partial bootstrap did not preserve its created Docker network metadata")
	}
	diagnosticsAssertUnchanged(t, before, diagnosticsCaptureState(t, ctx, cluster, containers, false), "partial bootstrap")
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("actual failed bootstrap: stopped MON inspection/logs and created network preserved, CLI failures retained; diagnostics created no replacement/control/client container and did not start the MON")
}
