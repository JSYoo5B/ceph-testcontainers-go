//go:build all || (integration && (!ci || (ci_environment && (!ci_batch || ci_batch_packet_capture))))

//ci: timeout=15m job-timeout=25

package capture_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// gatedServer is an HTTP server on port 7000 whose every start waits at a
// gate. Its FIFOs live in a tmpfs, which each container start mounts empty,
// so a restarted generation accepts no connection until openGate writes the
// gate again.
const gatedServer = `set -e
mkdir -p /www
echo before-restart > /www/before.html
echo after-restart > /www/after.html
mkfifo /gate/ready /gate/open
read -r _ < /gate/open
httpd -p 7000 -h /www
echo listening > /gate/ready
exec sleep infinity`

// openGate lets the current generation of a gatedServer serve and returns
// once its httpd listens.
func openGate(t *testing.T, ctx context.Context, server testcontainers.Container) {
	t.Helper()
	code, _, err := server.Exec(ctx, []string{"sh", "-c",
		`until [ -p /gate/open ]; do sleep 1; done; echo open > /gate/open; read -r state < /gate/ready; [ "$state" = listening ]`})
	if err != nil || code != 0 {
		t.Fatalf("open the server gate: %d %v", code, err)
	}
}

func runGatedServer(t *testing.T, ctx context.Context, network string) testcontainers.Container {
	t.Helper()
	return runTestContainer(t, ctx, testcontainers.ContainerRequest{Image: "busybox:1.37.0", Cmd: []string{"sh", "-c", gatedServer},
		Networks: []string{network}, Tmpfs: map[string]string{"/gate": "rw"}})
}

// runHostClient starts a client in the engine host's network namespace, as a
// test process on the host is: it reaches bridge endpoints through the
// bridge gateway.
func runHostClient(t *testing.T, ctx context.Context) testcontainers.Container {
	t.Helper()
	return runTestContainer(t, ctx, testcontainers.ContainerRequest{Image: "busybox:1.37.0", Cmd: []string{"sleep", "infinity"},
		HostConfigModifier: func(host *container.HostConfig) { host.NetworkMode = "host" }})
}

func runTestContainer(t *testing.T, ctx context.Context, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
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

func bridgeGateway(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, network string) netip.Addr {
	t.Helper()
	inspection, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range inspection.Network.IPAM.Config {
		if config.Gateway.Is4() {
			return config.Gateway
		}
	}
	t.Fatalf("no IPv4 gateway on %s", network)
	return netip.Addr{}
}

// segment is what the recipe keeps of one capture segment: its order, the
// whole report (target ID, generation, timing, selection, bounds, counters,
// file) and the error, also of a segment that ended with its generation.
type segment struct {
	Order  int            `json:"order"`
	Report capture.Report `json:"report"`
	Error  string         `json:"error,omitempty"`
}

// gap is an interval that no segment covers: from the restart request,
// after which the old segment may miss traffic, to the next segment's
// readiness.
type gap struct {
	After int       `json:"after_segment"`
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
}

func newSegment(order int, report capture.Report, err error) segment {
	s := segment{Order: order, Report: report}
	if err != nil {
		s.Error = err.Error()
	}
	return s
}

// seen summarizes the packets of one capture file between the two peers.
type seen struct {
	syn, synAck   int
	before, after bool
}

const (
	tcpSYN = 0x02
	tcpACK = 0x10
)

// classify summarizes the packets of one capture file between client and
// the server, whose address may change with its generation.
func classify(t *testing.T, report capture.Report, client netip.Addr, servers ...netip.Addr) seen {
	t.Helper()
	data := readFile(t, report)
	var result seen
	clientIP := client.As4()
	server := func(ip [4]byte) bool {
		for _, s := range servers {
			if s.As4() == ip {
				return true
			}
		}
		return false
	}
	for _, f := range flows(t, data) {
		switch {
		case f.src == clientIP && server(f.dst) && f.dstPort == 7000:
			if f.flags&(tcpSYN|tcpACK) == tcpSYN {
				result.syn++
			}
			result.before = result.before || bytes.Contains(f.payload, []byte("GET /before.html"))
			result.after = result.after || bytes.Contains(f.payload, []byte("GET /after.html"))
		case server(f.src) && f.dst == clientIP && f.srcPort == 7000:
			if f.flags&(tcpSYN|tcpACK) == tcpSYN|tcpACK {
				result.synAck++
			}
		default:
			t.Fatalf("a packet outside the selected flow: %+v", f)
		}
	}
	return result
}

func readFile(t *testing.T, report capture.Report) []byte {
	t.Helper()
	if !report.Retrieved || report.File.Status != capture.FileValid || !report.File.Complete {
		t.Fatalf("segment file %+v", report.File)
	}
	data, err := os.ReadFile(report.Output)
	if err != nil || int64(len(data)) != report.RetrievedBytes {
		t.Fatalf("delivered file: %d bytes, %v", len(data), err)
	}
	return data
}

// TestCaptureAcrossTargetRestart is the recipe for evidence around a
// deliberate restart of the captured peer. A session never follows a new
// generation, so a host client's traffic to a restarted target is captured
// in one segment per generation, with a gate that keeps the new generation
// from accepting before its segment is ready; a stable observer, the
// namespace of a containerized client, sees both generations in one file.
func TestCaptureAcrossTargetRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
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
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	request := func(name, filter string) capture.Request {
		return capture.Request{HelperImage: helper, Network: bridge.Name, Interface: "eth0", Filter: filter,
			SnapLen: 65535, MaxPackets: 1000, Output: filepath.Join(t.TempDir(), name+".pcap")}
	}
	restart := func(t *testing.T, id string) {
		t.Helper()
		now := 0
		if _, err := docker.ContainerRestart(ctx, id, mobycl.ContainerRestartOptions{Timeout: &now}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("per-generation segments", func(t *testing.T) {
		server := runGatedServer(t, ctx, bridge.Name)
		client := runHostClient(t, ctx)
		id := server.GetContainerID()
		serverBefore, clientAddr := address(t, ctx, server, bridge.Name), bridgeGateway(t, ctx, docker, bridge.Name)
		filter := "tcp port 7000 and host " + clientAddr.String()
		startSegment := func(order int) *capture.Session {
			session, err := startOwned(t, docker, id, func() (*capture.Session, error) {
				return capture.Start(ctx, server, request("segment-"+strconv.Itoa(order), filter))
			})
			if err != nil {
				t.Fatal(err)
			}
			return session
		}

		first := startSegment(1)
		openGate(t, ctx, server)
		if body := fetch(t, ctx, client, "http://"+serverBefore.String()+":7000/before.html"); !strings.Contains(body, "before-restart") {
			t.Fatalf("unexpected body %q", body)
		}
		restartRequested := time.Now().UTC()
		restart(t, id)
		// The old segment ends with its generation: the report says so and
		// keeps the traffic captured before the restart.
		report, err := first.Stop(ctx)
		segments := []segment{newSegment(1, report, err)}
		if !errors.Is(err, capture.ErrFailed) || report.Target != capture.TargetChanged || !report.HelperRemoved {
			t.Fatalf("first segment: err %v, target %s, removed %t", err, report.Target, report.HelperRemoved)
		}

		// A client that keeps redialing, as a reconnecting SUT does. The
		// closed gate refuses it until the second segment is ready.
		serverAfter := address(t, ctx, server, bridge.Name)
		redial := make(chan error, 1)
		go func() {
			code, _, err := client.Exec(ctx, []string{"sh", "-c",
				"until wget -q -O - http://" + serverAfter.String() + ":7000/after.html; do sleep 1; done"})
			if err == nil && code != 0 {
				err = errors.New("the redialing client exited with status " + strconv.Itoa(code))
			}
			redial <- err
		}()
		second := startSegment(2)
		openGate(t, ctx, server)
		if err := <-redial; err != nil {
			t.Fatal(err)
		}
		report, err = second.Stop(ctx)
		segments = append(segments, newSegment(2, report, err))
		uncovered := gap{After: 1, From: restartRequested, To: report.ReadyAt}
		manifest, _ := json.MarshalIndent(struct {
			Segments []segment `json:"segments"`
			Gaps     []gap     `json:"gaps"`
		}{segments, []gap{uncovered}}, "", "  ")
		t.Logf("segments: %s", manifest)
		if err != nil || report.End != capture.EndStopped || report.Target != capture.TargetUnchanged {
			t.Fatalf("second segment: err %v, end %s, target %s", err, report.End, report.Target)
		}

		one, two := segments[0].Report, segments[1].Report
		if one.ContainerID != id || two.ContainerID != id || one.TargetStartedAt == two.TargetStartedAt {
			t.Fatalf("segments of %s at %s and %s at %s: not two generations of one target",
				one.ContainerID, one.TargetStartedAt, two.ContainerID, two.TargetStartedAt)
		}
		if one.Filter != two.Filter || one.Interface != two.Interface || one.MaxBytes != two.MaxBytes {
			t.Fatalf("the segments selected differently: %+v, %+v", one, two)
		}
		if !one.ReadyAt.Before(restartRequested) || two.ReadyAt.Before(restartRequested) {
			t.Fatalf("segment order: first ready %s, restart %s, second ready %s", one.ReadyAt, restartRequested, two.ReadyAt)
		}
		before, after := classify(t, one, clientAddr, serverBefore), classify(t, two, clientAddr, serverAfter)
		if !before.before || before.after || before.synAck == 0 {
			t.Fatalf("first segment: %+v", before)
		}
		// The gate opened after the second segment was ready, so the
		// handshake that the new generation accepted is in it.
		if after.before || !after.after || after.syn == 0 || after.synAck == 0 {
			t.Fatalf("second segment: %+v", after)
		}
		requireHelpersRemoved(t, ctx, docker, id)
	})

	t.Run("stable client namespace", func(t *testing.T) {
		server := runGatedServer(t, ctx, bridge.Name)
		client := runBusybox(t, ctx, bridge.Name, "sleep", "infinity")
		clientAddr := address(t, ctx, client, bridge.Name)
		session, err := startOwned(t, docker, client.GetContainerID(), func() (*capture.Session, error) {
			return capture.Start(ctx, client, request("client", "tcp port 7000"))
		})
		if err != nil {
			t.Fatal(err)
		}
		openGate(t, ctx, server)
		serverBefore := address(t, ctx, server, bridge.Name)
		fetch(t, ctx, client, "http://"+serverBefore.String()+":7000/before.html")
		restart(t, server.GetContainerID())
		openGate(t, ctx, server)
		serverAfter := address(t, ctx, server, bridge.Name)
		fetch(t, ctx, client, "http://"+serverAfter.String()+":7000/after.html")
		report, err := session.Stop(ctx)
		logReport(t, report, err)
		if err != nil || report.End != capture.EndStopped || report.Target != capture.TargetUnchanged {
			t.Fatalf("err %v, end %s, target %s", err, report.End, report.Target)
		}
		// One file, one observer generation, both server generations.
		traffic := classify(t, report, clientAddr, serverBefore, serverAfter)
		if !traffic.before || !traffic.after || traffic.synAck < 2 {
			t.Fatalf("client capture: %+v", traffic)
		}
		requireHelpersRemoved(t, ctx, docker, client.GetContainerID())
	})
}
