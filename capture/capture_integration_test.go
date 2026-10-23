//go:build all || (integration && (!ci || (ci_environment && (!ci_batch || ci_batch_packet_capture))))

//ci: timeout=15m job-timeout=25

package capture_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The capture helper is not published yet. Without CEPH_TEST_CAPTURE_IMAGE
// the test builds the reference helper of contract 1 from the
// ceph-testcontainers-images commit that merged it, so the engine runs that
// source's Dockerfile; this module produces no image of its own.
const helperSource = "https://github.com/JSYoo5B/ceph-testcontainers-images.git#cbee3950dfab150b196fb5b4bf955045079f9fc9:image/extensions/capture"

func captureHelper(t *testing.T, ctx context.Context) string {
	t.Helper()
	if image := os.Getenv("CEPH_TEST_CAPTURE_IMAGE"); image != "" {
		return image
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	tag := "ceph-testcontainers-capture-helper:cbee3950"
	result, err := docker.ImageBuild(ctx, nil, mobycl.ImageBuildOptions{RemoteContext: helperSource, Tags: []string{tag}, Remove: true, ForceRemove: true})
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
	return tag
}

type flow struct {
	srcPort, dstPort uint16
	src, dst         [4]byte
	payload          []byte
}

// flows decodes the Ethernet, IPv4 and TCP headers of every record of a
// little- or big-endian classic pcap; the test needs no other protocol.
func flows(t *testing.T, data []byte) []flow {
	t.Helper()
	if len(data) < 24 {
		t.Fatalf("capture of %d bytes", len(data))
	}
	var order binary.ByteOrder = binary.LittleEndian
	if binary.BigEndian.Uint32(data) == 0xa1b2c3d4 {
		order = binary.BigEndian
	}
	var result []flow
	for offset := 24; offset < len(data); {
		captured := int(order.Uint32(data[offset+8:]))
		frame := data[offset+16 : offset+16+captured]
		offset += 16 + captured
		if len(frame) < 14+20 || binary.BigEndian.Uint16(frame[12:]) != 0x0800 {
			t.Fatalf("a record is not IPv4 over Ethernet: % x", frame[:min(len(frame), 16)])
		}
		ip := frame[14:]
		header := int(ip[0]&0x0f) * 4
		if ip[9] != 6 || len(ip) < header+20 {
			t.Fatalf("a record is not TCP: protocol %d", ip[9])
		}
		tcp := ip[header:]
		total := int(binary.BigEndian.Uint16(ip[2:]))
		f := flow{srcPort: binary.BigEndian.Uint16(tcp), dstPort: binary.BigEndian.Uint16(tcp[2:])}
		copy(f.src[:], ip[12:16])
		copy(f.dst[:], ip[16:20])
		if offset := header + int(tcp[12]>>4)*4; offset < min(total, len(ip)) {
			f.payload = ip[offset:min(total, len(ip))]
		}
		result = append(result, f)
	}
	return result
}

func runBusybox(t *testing.T, ctx context.Context, network string, cmd ...string) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{Image: "busybox:1.37.0", Cmd: cmd, Networks: []string{network}},
		Started:          true,
	})
	if ctr != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			// A target-loss scenario may already have removed it.
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

func address(t *testing.T, ctx context.Context, ctr testcontainers.Container, network string) netip.Addr {
	t.Helper()
	inspection, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[network]
	if endpoint == nil || !endpoint.IPAddress.Is4() {
		t.Fatalf("no IPv4 endpoint on %s", network)
	}
	return endpoint.IPAddress
}

func fetch(t *testing.T, ctx context.Context, client testcontainers.Container, url string) string {
	t.Helper()
	code, reader, err := client.Exec(ctx, []string{"wget", "-q", "-O", "-", url})
	if err != nil || code != 0 {
		t.Fatalf("wget %s: %d %v", url, code, err)
	}
	data, _ := io.ReadAll(reader)
	return string(data)
}

// helpers lists the capture helpers, running or not, that joined target's
// network namespace.
func helpers(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, target string) []container.Summary {
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

// waitHelperEnd waits up to limit for the one capture helper of target to
// end on its own, and reports whether it did.
func waitHelperEnd(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, target string, limit time.Duration) bool {
	t.Helper()
	for deadline := time.Now().Add(limit); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		joined := helpers(t, ctx, docker, target)
		if len(joined) != 1 {
			t.Fatalf("%d capture helpers joined the target", len(joined))
		}
		if joined[0].State != container.StateRunning {
			return true
		}
	}
	return false
}

// requireHelpersRemoved fails when a capture helper of target is left.
func requireHelpersRemoved(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, target string) {
	t.Helper()
	if left := helpers(t, ctx, docker, target); len(left) != 0 {
		t.Fatalf("%d capture helpers left after the capture", len(left))
	}
}

func logReport(t *testing.T, report capture.Report, err error) {
	t.Helper()
	data, _ := json.MarshalIndent(report, "", "  ")
	t.Logf("report: %s\nerror: %v", data, err)
}

// TestCaptureTargetTraffic captures known TCP traffic of a caller-owned
// target on a private bridge, with an unrelated flow that the filter
// excludes, then a valid empty capture and refused requests. It needs an
// engine that grants NET_RAW to the helper.
func TestCaptureTargetTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	helper := captureHelper(t, ctx)
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
	target := runBusybox(t, ctx, bridge.Name, "sh", "-c",
		"mkdir -p /www && echo capture-payload-7000 > /www/index.html && httpd -p 7000 -h /www && httpd -p 7001 -h /www && exec sleep infinity")
	client := runBusybox(t, ctx, bridge.Name, "sleep", "infinity")
	targetAddr, clientAddr := address(t, ctx, target, bridge.Name), address(t, ctx, client, bridge.Name)
	targetIP, clientIP := targetAddr.As4(), clientAddr.As4()
	targetURL, clientHost := "http://"+targetAddr.String(), clientAddr.String()
	before, err := target.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	request := func(name, filter string) capture.Request {
		return capture.Request{HelperImage: helper, Network: bridge.Name, Interface: "eth0", Filter: filter,
			SnapLen: 65535, MaxPackets: 1000, Output: filepath.Join(t.TempDir(), name+".pcap")}
	}

	t.Run("bidirectional", func(t *testing.T) {
		session, err := capture.Start(ctx, target, request("traffic", "tcp port 7000 and host "+clientHost))
		if err != nil {
			t.Fatal(err)
		}
		// The workload starts only after readiness.
		if body := fetch(t, ctx, client, targetURL+":7000/"); !strings.Contains(body, "capture-payload-7000") {
			t.Fatalf("unexpected body %q", body)
		}
		if body := fetch(t, ctx, client, targetURL+":7001/"); !strings.Contains(body, "capture-payload-7000") {
			t.Fatalf("unexpected body %q", body)
		}
		time.Sleep(time.Second)
		report, err := session.Stop(ctx)
		data, _ := json.MarshalIndent(report, "", "  ")
		t.Logf("report: %s", data)
		if err != nil {
			t.Fatal(err)
		}
		if report.End != capture.EndStopped || report.ExitCode != 0 || report.Target != capture.TargetUnchanged || !report.HelperRemoved {
			t.Fatalf("end %s/%d, target %s, removed %t", report.End, report.ExitCode, report.Target, report.HelperRemoved)
		}
		if report.File.Status != capture.FileValid || !report.File.Complete || report.File.LinkType != 1 || report.File.Packets == 0 {
			t.Fatalf("file %+v", report.File)
		}
		if report.Statistics.Captured == nil || *report.Statistics.Captured != uint64(report.File.Packets) || report.Statistics.ReceivedByFilter == nil {
			t.Fatalf("statistics %+v for %d packets", report.Statistics, report.File.Packets)
		}
		// The helper is gone; the delivered file stays readable.
		file, err := os.ReadFile(report.Output)
		if err != nil || int64(len(file)) != report.RetrievedBytes {
			t.Fatalf("delivered file: %d bytes, %v", len(file), err)
		}
		var toTarget, fromTarget, payload bool
		for _, f := range flows(t, file) {
			switch {
			case f.srcPort == 7001 || f.dstPort == 7001:
				t.Fatalf("the filter kept the unrelated flow: %+v", f)
			case f.src == clientIP && f.dst == targetIP && f.dstPort == 7000:
				toTarget = true
			case f.src == targetIP && f.dst == clientIP && f.srcPort == 7000:
				fromTarget = true
				payload = payload || bytes.Contains(f.payload, []byte("capture-payload-7000"))
			default:
				t.Fatalf("a packet outside the selected flow: %+v", f)
			}
		}
		if !toTarget || !fromTarget || !payload {
			t.Fatalf("directions: to %t, from %t, payload %t", toTarget, fromTarget, payload)
		}
	})

	t.Run("empty", func(t *testing.T) {
		session, err := capture.Start(ctx, target, request("empty", "tcp port 7999"))
		if err != nil {
			t.Fatal(err)
		}
		fetch(t, ctx, client, targetURL+":7000/")
		report, err := session.Stop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if report.End != capture.EndStopped || report.File.Status != capture.FileValid || report.File.Packets != 0 ||
			report.Statistics.Captured == nil || *report.Statistics.Captured != 0 || report.ReadyAt.IsZero() {
			t.Fatalf("empty capture report %+v", report)
		}
	})

	t.Run("refused", func(t *testing.T) {
		for name, req := range map[string]capture.Request{
			"filter":    request("filter", "tcp portx 7000"),
			"interface": func() capture.Request { r := request("interface", "tcp"); r.Interface = "eth1"; return r }(),
		} {
			session, err := capture.Start(ctx, target, req)
			if session != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Errorf("%s: session %v, err %v", name, session, err)
			}
			if _, statErr := os.Stat(req.Output); !errors.Is(statErr, os.ErrNotExist) {
				t.Errorf("%s: a refused capture kept its output", name)
			}
		}
	})

	t.Run("packet limit", func(t *testing.T) {
		req := request("limit", "tcp port 7000 and host "+clientHost)
		req.SnapLen, req.MaxPackets = 128, 4
		session, err := capture.Start(ctx, target, req)
		if err != nil {
			t.Fatal(err)
		}
		// One HTTP exchange has more than four packets on the selected flow.
		fetch(t, ctx, client, targetURL+":7000/")
		if !waitHelperEnd(t, ctx, docker, target.GetContainerID(), 30*time.Second) {
			t.Fatal("tcpdump did not end at its packet limit")
		}
		report, err := session.Stop(ctx)
		logReport(t, report, err)
		if err != nil {
			t.Fatal(err)
		}
		if report.End != capture.EndPacketLimit || report.ExitCode != 0 || !report.EndedAt.Before(report.StopRequestedAt) ||
			report.Target != capture.TargetUnchanged || !report.HelperRemoved {
			t.Fatalf("end %s/%d at %s (stop %s), target %s, removed %t", report.End, report.ExitCode,
				report.EndedAt, report.StopRequestedAt, report.Target, report.HelperRemoved)
		}
		if report.File.Status != capture.FileValid || !report.File.Complete || report.File.Packets != req.MaxPackets ||
			report.File.SnapLen != uint32(req.SnapLen) {
			t.Fatalf("file %+v", report.File)
		}
		if captured := report.Statistics.Captured; captured != nil && *captured != uint64(req.MaxPackets) {
			t.Fatalf("tcpdump captured %d packets", *captured)
		}
		file, err := os.ReadFile(report.Output)
		if err != nil || int64(len(file)) != report.RetrievedBytes {
			t.Fatalf("delivered file: %d bytes, %v", len(file), err)
		}
		for _, f := range flows(t, file) {
			if f.srcPort != 7000 && f.dstPort != 7000 {
				t.Fatalf("a packet outside the selected flow: %+v", f)
			}
		}
		requireHelpersRemoved(t, ctx, docker, target.GetContainerID())
	})

	t.Run("canceled workload", func(t *testing.T) {
		workload, cancelWorkload := context.WithCancel(ctx)
		defer cancelWorkload()
		session, err := capture.Start(workload, target, request("canceled", "tcp port 7000 and host "+clientHost))
		if err != nil {
			t.Fatal(err)
		}
		fetch(t, workload, client, targetURL+":7000/")
		time.Sleep(time.Second)
		cancelWorkload()
		// Stop gets the workload's canceled context, as a deferred cleanup
		// after a failed workload would.
		report, err := session.Stop(workload)
		logReport(t, report, err)
		if err != nil {
			t.Fatal(err)
		}
		if report.End != capture.EndStopped || report.ExitCode != 0 || report.Target != capture.TargetUnchanged || !report.HelperRemoved {
			t.Fatalf("end %s/%d, target %s, removed %t", report.End, report.ExitCode, report.Target, report.HelperRemoved)
		}
		if report.File.Status != capture.FileValid || !report.File.Complete || report.File.Packets == 0 {
			t.Fatalf("file %+v", report.File)
		}
		requireHelpersRemoved(t, ctx, docker, target.GetContainerID())
	})

	t.Run("denied NET_RAW", func(t *testing.T) {
		// The public API always grants NET_RAW; this test hook withholds it
		// from the capture helper alone, as an engine that refuses it would.
		req := request("denied", "tcp port 7000")
		session, err := capture.StartWithoutNetRaw(ctx, target, req)
		t.Logf("error: %v", err)
		if session != nil {
			defer func() { _, _ = session.Stop(ctx) }()
		}
		if session != nil || !errors.Is(err, capture.ErrPermissionDenied) || !errors.Is(err, ceph.ErrEnvironmentUnavailable) ||
			errors.Is(err, capture.ErrFailed) || errors.Is(err, capture.ErrImageUnsupported) {
			t.Fatalf("session %v, err %v", session, err)
		}
		if _, statErr := os.Stat(req.Output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatal("a refused capture kept its output")
		}
		requireHelpersRemoved(t, ctx, docker, target.GetContainerID())
	})

	// The target-loss scenarios end their own target: the capture is bound
	// to the generation it joined and never follows it.
	for _, loss := range []struct {
		name  string
		end   func(t *testing.T, id string)
		state capture.TargetState
	}{
		{"target removed", func(t *testing.T, id string) {
			if _, err := docker.ContainerRemove(ctx, id, mobycl.ContainerRemoveOptions{Force: true}); err != nil {
				t.Fatal(err)
			}
		}, capture.TargetGone},
		{"target restarted", func(t *testing.T, id string) {
			now := 0
			if _, err := docker.ContainerRestart(ctx, id, mobycl.ContainerRestartOptions{Timeout: &now}); err != nil {
				t.Fatal(err)
			}
		}, capture.TargetChanged},
	} {
		t.Run(loss.name, func(t *testing.T) {
			lost := runBusybox(t, ctx, bridge.Name, "sh", "-c",
				"mkdir -p /www && echo capture-payload-7000 > /www/index.html && httpd -p 7000 -h /www && exec sleep infinity")
			lostURL, id := "http://"+address(t, ctx, lost, bridge.Name).String(), lost.GetContainerID()
			session, err := capture.Start(ctx, lost, request(strings.ReplaceAll(loss.name, " ", "-"), "tcp port 7000 and host "+clientHost))
			if err != nil {
				t.Fatal(err)
			}
			fetch(t, ctx, client, lostURL+":7000/")
			time.Sleep(time.Second)
			loss.end(t, id)
			// tcpdump ends on its own once the interface is gone; when it
			// does not within the wait, Stop ends it.
			ended := waitHelperEnd(t, ctx, docker, id, 30*time.Second)
			report, err := session.Stop(ctx)
			logReport(t, report, err)
			t.Logf("tcpdump ended before Stop: %t", ended)
			if !errors.Is(err, capture.ErrFailed) || report.Target != loss.state || !report.HelperRemoved {
				t.Fatalf("err %v, target %s, removed %t", err, report.Target, report.HelperRemoved)
			}
			if ended && (report.End != capture.EndFailed || report.ExitCode == 0) {
				t.Fatalf("tcpdump ended on its own as %s/%d", report.End, report.ExitCode)
			}
			if !ended && report.End != capture.EndStopped {
				t.Fatalf("tcpdump ended after Stop as %s/%d", report.End, report.ExitCode)
			}
			// The traffic before the loss is still delivered and checked.
			if !report.Retrieved || report.File.Status != capture.FileValid || !report.File.Complete || report.File.Packets == 0 {
				t.Fatalf("file %+v", report.File)
			}
			requireHelpersRemoved(t, ctx, docker, id)
		})
	}

	after, err := target.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after.State.Running || after.State.StartedAt != before.State.StartedAt || len(after.HostConfig.CapAdd) != 0 ||
		after.NetworkSettings.SandboxID != before.NetworkSettings.SandboxID {
		t.Fatal("the capture changed the caller-owned target")
	}
}
