//go:build all || (integration && (!ci || (ci_environment && (!ci_batch || ci_batch_packet_capture_killed))))

//ci: timeout=15m job-timeout=25

package capture_test

import (
	"archive/tar"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/pcapfile"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The capture helper is not published. Without CEPH_TEST_CAPTURE_IMAGE the
// test builds the reference helper of contract 1 from the same pinned
// ceph-testcontainers-images commit as the capture package's own scenario.
const (
	killedCaptureHelperSource = "https://github.com/JSYoo5B/ceph-testcontainers-images.git#cbee3950dfab150b196fb5b4bf955045079f9fc9:image/extensions/capture"
	killedCaptureHelperTag    = "ceph-testcontainers-capture-helper:cbee3950"
	// killedCaptureFile is the helper's capture file as contract 1 fixes it.
	killedCaptureFile = "/tmp/ceph-testcontainers-capture.pcap"
)

func killedCaptureImage(t *testing.T, ctx context.Context) string {
	t.Helper()
	if image := os.Getenv("CEPH_TEST_CAPTURE_IMAGE"); image != "" {
		return image
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	result, err := docker.ImageBuild(ctx, nil, mobycl.ImageBuildOptions{RemoteContext: killedCaptureHelperSource,
		Tags: []string{killedCaptureHelperTag}, Remove: true, ForceRemove: true})
	if err != nil {
		t.Fatalf("build the capture helper: %v", err)
	}
	defer func() { _ = result.Body.Close() }()
	scanner := bufio.NewScanner(result.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		var message struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &message) == nil && message.Error != "" {
			t.Fatalf("build the capture helper: %s", message.Error)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("build the capture helper: %v", err)
	}
	return killedCaptureHelperTag
}

func killedCaptureRun(t *testing.T, ctx context.Context, network string, cmd ...string) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{Image: "busybox:1.37.0", Cmd: cmd, Networks: []string{network}},
		Started:          true,
	})
	if ctr != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := testcontainers.TerminateContainer(ctr, testcontainers.StopContext(cleanupCtx)); err != nil {
				t.Errorf("remove a capture test container: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// killedCaptureHelpers lists the capture helpers, running or not, that
// joined the network namespace of target.
func killedCaptureHelpers(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, target string) []container.Summary {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true,
		Filters: mobycl.Filters{}.Add("label", "org.testcontainers.ceph.role=capture-helper")})
	if err != nil {
		t.Fatal(err)
	}
	var joined []container.Summary
	for _, helper := range list.Items {
		if helper.HostConfig.NetworkMode == "container:"+target {
			joined = append(joined, helper)
		}
	}
	return joined
}

// killedCaptureSnapshot reads the helper's capture file from a running
// helper: its size and bytes at the moment of the copy.
func killedCaptureSnapshot(ctx context.Context, docker *testcontainers.DockerClient, helper string) ([]byte, error) {
	result, err := docker.CopyFromContainer(ctx, helper, mobycl.CopyFromContainerOptions{SourcePath: killedCaptureFile})
	if err != nil {
		return nil, err
	}
	defer func() { _ = result.Content.Close() }()
	archive := tar.NewReader(result.Content)
	header, err := archive.Next()
	if err != nil {
		return nil, err
	}
	if header.Typeflag != tar.TypeReg || header.Size > 64<<20 {
		return nil, errors.New("the capture file is not a regular file of a readable size")
	}
	return io.ReadAll(io.LimitReader(archive, header.Size))
}

// killedCaptureFrozenSnapshot freezes the helper for the copy: the engine's
// copy of a file that tcpdump keeps appending to can fail on its changing
// size. The helper is unfrozen again before it returns; the client and the
// target keep running, so the traffic stays in flight.
func killedCaptureFrozenSnapshot(ctx context.Context, docker *testcontainers.DockerClient, helper string) (snapshot []byte, err error) {
	if _, err := docker.ContainerPause(ctx, helper, mobycl.ContainerPauseOptions{}); err != nil {
		return nil, err
	}
	defer func() {
		// A fresh context: the copy's own may be the reason for the failure.
		unpauseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if _, unpauseErr := docker.ContainerUnpause(unpauseCtx, helper, mobycl.ContainerUnpauseOptions{}); unpauseErr != nil {
			err = errors.Join(err, unpauseErr)
		}
	}()
	return killedCaptureSnapshot(ctx, docker, helper)
}

// killedCaptureRequests is the number of requests the client loop has
// completed; the loop writes it after each one.
func killedCaptureRequests(ctx context.Context, client testcontainers.Container) int {
	code, reader, err := client.Exec(ctx, []string{"cat", "/tmp/n"})
	if err != nil || code != 0 {
		return -1
	}
	data, _ := io.ReadAll(reader)
	// The exec stream is multiplexed; keep the digits.
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, string(data))
	n, err := strconv.Atoi(digits)
	if err != nil {
		return -1
	}
	return n
}

// TestCaptureKilledHelperObservation kills the capture helper of a real
// capture while a client keeps requesting from a caller-owned target, then
// lets the capture package retrieve and classify whatever was written. The
// outcome is recorded as observed: the test asserts the invariants of the
// observation, never a particular tail shape, record count or end reason.
func TestCaptureKilledHelperObservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	helper := killedCaptureImage(t, ctx)
	bridge, err := dockerbridge.New(ctx)
	if bridge != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := bridge.Remove(cleanupCtx); err != nil {
				t.Errorf("remove the capture bridge: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	// A 64 KiB response makes each request many packets, so that the
	// client's loop keeps traffic in flight.
	target := killedCaptureRun(t, ctx, bridge.Name, "sh", "-c",
		"mkdir -p /www && head -c 65536 /dev/zero > /www/big && httpd -p 7000 -h /www && exec sleep infinity")
	targetID := target.GetContainerID()
	inspection, err := target.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[bridge.Name]
	if endpoint == nil || !endpoint.IPAddress.Is4() {
		t.Fatalf("no IPv4 endpoint on %s", bridge.Name)
	}
	before := inspection
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })

	request := capture.Request{HelperImage: helper, Network: bridge.Name, Interface: "eth0", Filter: "tcp port 7000",
		SnapLen: 96, MaxPackets: 500000, Output: filepath.Join(t.TempDir(), "killed.pcap")}
	session, err := capture.Start(ctx, target, request)
	if session != nil {
		// A fatal path before the explicit Stop still ends and removes the
		// helper; after it, the same report is returned.
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stopCancel()
			if _, stopErr := session.Stop(stopCtx); stopErr != nil {
				t.Logf("fallback Stop: %v", stopErr)
			}
			if left := killedCaptureHelpers(t, stopCtx, docker, targetID); len(left) != 0 {
				t.Errorf("%d capture helpers left after the fallback Stop", len(left))
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	// Traffic starts only after readiness: Start returned.
	client := killedCaptureRun(t, ctx, bridge.Name, "sh", "-c",
		"n=0; while wget -q -O /dev/null http://"+endpoint.IPAddress.String()+":7000/big; do n=$((n+1)); echo $n > /tmp/n; done")

	joined := killedCaptureHelpers(t, ctx, docker, targetID)
	if len(joined) != 1 || joined[0].State != container.StateRunning {
		t.Fatalf("%d capture helpers joined the target before the kill", len(joined))
	}
	helperID := joined[0].ID

	// Wait for a completed request, so that the client's counter is live.
	for deadline := time.Now().Add(60 * time.Second); killedCaptureRequests(ctx, client) < 1; time.Sleep(200 * time.Millisecond) {
		if !time.Now().Before(deadline) {
			t.Fatal("the client completed no request in time")
		}
	}
	// Wait until the running capture has written records, then snapshot.
	var snapshot []byte
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		stat, err := docker.ContainerStatPath(ctx, helperID, mobycl.ContainerStatPathOptions{Path: killedCaptureFile})
		if err == nil && stat.Stat.Size >= 32<<10 {
			if snapshot, err = killedCaptureFrozenSnapshot(ctx, docker, helperID); err != nil {
				t.Fatalf("snapshot the running capture: %v", err)
			}
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("the running capture wrote no 32 KiB of records in time (stat %+v, %v)", stat.Stat, err)
		}
	}
	input := captureKillInput{Snapshot: snapshot, Limits: pcapfile.Limits{MaxFileBytes: 24 + int64(request.MaxPackets)*(16+int64(request.SnapLen)), MaxRecordBytes: uint32(request.SnapLen)}}

	// Kill the helper, not the target: SIGKILL gives tcpdump no chance to
	// flush or print statistics.
	input.RequestsBefore = killedCaptureRequests(ctx, client)
	state, err := docker.ContainerInspect(ctx, helperID, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input.HelperRunningBeforeKill = state.Container.State != nil && state.Container.State.Running && !state.Container.State.Paused
	if _, err := docker.ContainerKill(ctx, helperID, mobycl.ContainerKillOptions{Signal: "SIGKILL"}); err != nil {
		t.Fatalf("kill the capture helper: %v", err)
	}
	var killed container.State
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		state, err = docker.ContainerInspect(ctx, helperID, mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if state.Container.State != nil && !state.Container.State.Running {
			killed = *state.Container.State
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("the killed helper still ran after 30 s")
		}
	}
	input.HelperRunningAfterKill, input.HelperExitCode = killed.Running, killed.ExitCode
	t.Logf("helper after SIGKILL: exit %d, oom-killed %t, engine error %q, finished %s", killed.ExitCode, killed.OOMKilled, killed.Error, killed.FinishedAt)
	// The client's loop keeps going past the kill: the traffic was in flight.
	time.Sleep(time.Second)
	input.RequestsAfter = killedCaptureRequests(ctx, client)

	report, stopErr := session.Stop(ctx)
	data, _ := json.MarshalIndent(report, "", "  ")
	t.Logf("capture report: %s\nStop error: %v", data, stopErr)
	// The package reports a helper that ended nonzero as a failed capture.
	if !errors.Is(stopErr, capture.ErrFailed) {
		t.Errorf("Stop error %v does not report a failed capture", stopErr)
	}
	input.Report = report
	if input.Retrieved, err = os.ReadFile(report.Output); err != nil {
		t.Fatalf("read the delivered file: %v", err)
	}
	after, err := target.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	input.TargetUnchanged = after.State.Running && after.State.StartedAt == before.State.StartedAt && len(after.HostConfig.CapAdd) == 0 &&
		after.NetworkSettings.SandboxID == before.NetworkSettings.SandboxID

	observation, err := observeCaptureKill(input)
	if err != nil {
		t.Fatal(err)
	}
	recorded, _ := json.MarshalIndent(observation, "", "  ")
	t.Logf("recorded observation: %s", recorded)
	for _, violation := range observation.Violations {
		t.Error(violation)
	}
	if left := killedCaptureHelpers(t, ctx, docker, targetID); len(left) != 0 {
		t.Errorf("%d capture helpers left after Stop", len(left))
	}
}
