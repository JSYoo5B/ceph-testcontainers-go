//go:build integration && ci_optional && ci_batch_block_io

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault/blockio"
	"github.com/testcontainers/testcontainers-go"
)

func testBlockIOMainDeviceRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	helper := os.Getenv("CEPH_TEST_BLOCKIO_HELPER_IMAGE")
	if helper == "" {
		t.Fatal("CEPH_TEST_BLOCKIO_HELPER_IMAGE is required for this selected native batch")
	}
	backend, err := blockio.Run(ctx, helper, blockio.WithConfig(blockio.Config{Size: 1 << 30}))
	if backend != nil {
		// Register the backend first, so the cluster is cleaned up first.
		testcontainers.CleanupContainer(t, backend)
	}
	if err != nil {
		if errors.Is(err, ceph.ErrEnvironmentUnavailable) {
			t.Logf("BLOCKIO_NATIVE eligible=false phase=setup kind=ErrEnvironmentUnavailable native_fault_verified=false error=%v", err)
		}
		t.Fatal(err)
	}
	image := os.Getenv("CEPH_TEST_IMAGE")
	if image == "" {
		image = ceph.DefaultImage
	}
	opts := []testcontainers.ContainerCustomizer{
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip)),
	}
	essential, clientImage := os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), os.Getenv("CEPH_TEST_CLIENT_IMAGE")
	if essential != "" || clientImage != "" || os.Getenv("SCENARIO_IMAGE_LAYOUT") == "roles" {
		if essential == "" || clientImage == "" {
			t.Fatal("both essential and client role images are required")
		}
		image = essential
		opts = append(opts, ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{Essential: essential, Client: clientImage})))
	}
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if err != nil {
		t.Fatal(err)
	}
	osd, err := backend.AttachOSD(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	attached := blockIOStatus(t, ctx, backend)
	if attached.Tables.Openers < 1 {
		t.Fatal("attached OSD did not hold the owned main device")
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "blockio-proof", Application: "rados", PGNum: 1, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte((i*37 + 19) % 251)
	}
	if err := client.CopyToContainer(ctx, payload, "/tmp/blockio-payload", 0o600); err != nil {
		t.Fatal(err)
	}
	put := func(object string) time.Duration {
		t.Helper()
		start := time.Now()
		result := blockIOCommand(t, ctx, cluster, client, "put", object, "/tmp/blockio-payload")
		if !result.Complete() || result.ExitCode != 0 {
			t.Fatalf("acknowledged put failed: object=%s exit=%d timeout=%t stderr=%s", object, result.ExitCode, result.TimedOut, result.Stderr)
		}
		return time.Since(start)
	}
	read := func(object string) {
		t.Helper()
		result := blockIOCommand(t, ctx, cluster, client, "get", object, "-")
		if !result.Complete() || result.ExitCode != 0 || !bytes.Equal(result.Stdout, payload) {
			t.Fatalf("acknowledged bytes differ: object=%s exit=%d timeout=%t bytes=%d", object, result.ExitCode, result.TimedOut, len(result.Stdout))
		}
	}
	baseline := put("acknowledged")
	read("acknowledged")
	const delay = 250 * time.Millisecond
	delayEligible := true
	if err := backend.Apply(ctx, blockio.Profile{Delay: delay}); err != nil {
		if !errors.Is(err, ceph.ErrEnvironmentUnavailable) {
			t.Fatal(err)
		}
		delayEligible = false
		if os.Getenv("CEPH_TEST_BLOCKIO_REQUIRE_DELAY") == "1" {
			t.Fatalf("qualification requires a registered delay target: %v", err)
		}
		t.Logf("BLOCKIO_DELAY eligible=false kind=ErrEnvironmentUnavailable native_delay_verified=false error=%v", err)
	} else {
		status := blockIOStatus(t, ctx, backend)
		if status.Requested.Delay != delay || !strings.Contains(status.Tables.Live, " delay ") {
			t.Fatalf("delay request and observed kernel table differ: %+v", status)
		}
		delayed := put("delayed")
		read("delayed")
		if err := backend.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		restored := put("restored")
		if delayed < baseline+delay/2 || delayed < restored+delay/2 {
			t.Fatalf("delay lacks observed baseline/delay/restore effect: baseline=%s delayed=%s restored=%s", baseline, delayed, restored)
		}
		t.Logf("BLOCKIO_DELAY eligible=true native_delay_verified=true baseline=%s delayed=%s restored=%s", baseline, delayed, restored)
	}
	// Snapshot the exact log prefix immediately before installing EIO. An
	// earlier failure from setup or the delay phase cannot satisfy this check.
	logs, err := osd.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	baselineHash := sha256.New()
	logBytes, readErr := io.Copy(baselineHash, logs)
	if err := errors.Join(readErr, logs.Close()); err != nil {
		t.Fatal(err)
	}
	if err := backend.Apply(ctx, blockio.Profile{EIO: true}); err != nil {
		t.Fatal(err)
	}
	status := blockIOStatus(t, ctx, backend)
	if !status.Requested.EIO || !strings.Contains(status.Tables.Live, " error") {
		t.Fatalf("EIO request and observed kernel table differ: %+v", status)
	}
	failed := blockIOCommand(t, ctx, cluster, client, "put", "during-eio", "/tmp/blockio-payload")
	if !blockIOExpectedFailure(failed) {
		t.Fatalf("EIO put failed for an unexpected reason: exit=%d timedOut=%t stderr=%s", failed.ExitCode, failed.TimedOut, failed.Stderr)
	}
	// Read through the end of the fault window before restoring the table.
	logs, err = osd.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = checkBlockIOLogWindow(logs, logBytes, baselineHash.Sum(nil))
	if err := errors.Join(err, logs.Close()); err != nil {
		t.Fatal(err)
	}
	if err := backend.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	if err := osd.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	read("acknowledged")
	put("after-recovery")
	read("after-recovery")
	if err := backend.Terminate(ctx); !errors.Is(err, ceph.ErrNativeFailure) {
		t.Fatalf("live cluster must retain its backend with ErrNativeFailure: %v", err)
	}
	status = blockIOStatus(t, ctx, backend)
	state, err := backend.State(ctx)
	if err != nil || state == nil || !state.Running || status.Released {
		t.Fatalf("live consumer did not retain its keeper: state=%+v status=%+v err=%v", state, status, err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backend.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backend.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = backend.Status(ctx)
	if err != nil || !status.Released {
		t.Fatalf("final backend release was not confirmed: status=%+v err=%v", status, err)
	}
	t.Logf("BLOCKIO_NATIVE eligible=true delay_eligible=%t eio_client_failure=true osd_io_error=true acknowledged_bytes=%d recovery_write=true busy_retained=true released=true", delayEligible, len(payload))
}

func blockIOCommand(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, args ...string) ceph.NativeCommandResult {
	t.Helper()
	argv := []string{"rados", "--client_mount_timeout", "5", "--rados_mon_op_timeout", "5", "--rados_osd_op_timeout", "15", "-p", "blockio-proof"}
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: append(argv, args...), Timeout: 25 * time.Second, MaxStdout: 2 << 20, MaxStderr: 64 << 10, Label: "block I/O native oracle"})
	if err != nil && !errors.Is(err, ceph.ErrNativeCommandIncomplete) {
		t.Fatal(err)
	}
	return result
}

func blockIOStatus(t *testing.T, ctx context.Context, backend *blockio.Container) blockio.Status {
	t.Helper()
	status, err := backend.Status(ctx)
	if err != nil || status.Identity == nil || status.Tables == nil || status.PendingStep != "" || status.Released {
		t.Fatalf("backend lacks settled observed identity/tables: status=%+v err=%v", status, err)
	}
	if status.Identity.Size != 1<<30 || status.Identity.UUID == "" {
		t.Fatalf("unexpected owned device identity: %+v", status.Identity)
	}
	return status
}
