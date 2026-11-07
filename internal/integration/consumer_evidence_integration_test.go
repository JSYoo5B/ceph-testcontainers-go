//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_client_profiles))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/consumerevidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// ipcReply is one line of the ipcfixture protocol.
type ipcReply struct {
	Reply  string          `json:"reply"`
	OK     bool            `json:"ok"`
	Error  string          `json:"error"`
	Result json.RawMessage `json:"result"`
}

// ipcReady is the ready line of the ipcfixture protocol.
type ipcReady struct {
	DaemonContainers map[string]string `json:"daemon_containers"`
	FSID             string            `json:"fsid"`
	Monitors         []string          `json:"monitors"`
	Managers         []string          `json:"managers"`
	Containers       []string          `json:"containers"`
	Network          string            `json:"network"`
}

// The public-API consumer example of internal/consumerevidence runs as a
// separate fixture process without a testing.TB, driven over its standard
// input and output. The native MGR keyring gate is clean before and after a
// stand-in SUT's rejection; the first-failure bundle of that rejection is
// complete, keeps the rejection, finds the target MGR registered and leaves
// generation equality unverified; the restart of the active MGR is accepted
// with its MgrMap changes kept. The driver stops that MGR again, observes
// its absent slot with a successful control on the survivor, and waits for
// a clean gate after the stopped MGR rejoins. It sends a failure signal and
// closes: the fixture writes its diagnostics before Terminate and still exits
// with status 1. The driver checks the cleanup itself through Docker:
// every container, volume and network that it saw is gone.
func TestConsumerEvidenceIPCFixture(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })

	binary := filepath.Join(t.TempDir(), "ipcfixture")
	dir, err := filepath.Abs(filepath.Join("..", "consumerevidence", "ipcfixture"))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build ipcfixture: %v\n%s", err, out)
	}
	args := []string{"-out", t.TempDir()}
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") == "" && os.Getenv("SCENARIO_IMAGE_LAYOUT") != "roles" {
		args = append(args, "-image", harness.EssentialImage(t))
	} else {
		args = append(args, "-image", os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), "-client-image", os.Getenv("CEPH_TEST_CLIENT_IMAGE"))
	}
	out := args[1]

	// Only Terminate may remove the fixture: no reaper of the fixture
	// process does it after the fact.
	fixture := exec.CommandContext(ctx, binary, args...)
	fixture.Env = append(os.Environ(), "TESTCONTAINERS_RYUK_DISABLED=true")
	fixture.Cancel = func() error { return fixture.Process.Signal(syscall.SIGTERM) }
	fixture.WaitDelay = 8 * time.Minute
	var stderr bytes.Buffer
	fixture.Stderr = &stderr
	stdin, err := fixture.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := fixture.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Start(); err != nil {
		t.Fatal(err)
	}
	var seen ipcReady
	var volumes []string
	t.Cleanup(func() { removeLeftovers(t, docker, seen, volumes) })
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = stdin.Close()
			_ = fixture.Wait()
		}
		if t.Failed() {
			t.Logf("ipcfixture stderr (tail):\n%s", textTail(stderr.String(), 8<<10))
		}
	})
	lines := bufio.NewScanner(stdout)
	lines.Buffer(make([]byte, 64<<10), 16<<20)
	next := func(want string) ipcReply {
		t.Helper()
		for lines.Scan() {
			var reply ipcReply
			if err := json.Unmarshal(lines.Bytes(), &reply); err != nil || reply.Reply == "" {
				t.Logf("ipcfixture stdout: %.512s", lines.Text())
				continue
			}
			if reply.Reply != want {
				t.Fatalf("reply %q, want %q: %.2048s", reply.Reply, want, lines.Text())
			}
			return reply
		}
		t.Fatalf("ipcfixture ended before its %q reply: %v", want, lines.Err())
		return ipcReply{}
	}
	send := func(command string) {
		t.Helper()
		if _, err := stdin.Write([]byte(command + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	decode := func(reply ipcReply, into any) {
		t.Helper()
		if !reply.OK {
			t.Fatalf("%s: %s: %.4096s", reply.Reply, reply.Error, reply.Result)
		}
		if err := json.Unmarshal(reply.Result, into); err != nil {
			t.Fatalf("%s: %v", reply.Reply, err)
		}
	}

	decode(next("ready"), &seen)
	if len(seen.Monitors) != 3 || len(seen.Managers) != 2 || seen.Network == "" {
		t.Fatalf("ready %+v", seen)
	}
	// The driver's own record of the fixture: the containers on its network
	// besides those it reported, and their volumes.
	network, err := docker.NetworkInspect(ctx, seen.Network, mobycl.NetworkInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for id := range network.Network.Containers {
		if !slices.Contains(seen.Containers, id) {
			seen.Containers = append(seen.Containers, id)
		}
	}
	for _, id := range seen.Containers {
		inspect, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, point := range inspect.Container.Mounts {
			if point.Type == mount.TypeVolume && point.Name != "" {
				volumes = append(volumes, point.Name)
			}
		}
	}

	readGate := func(label string) (consumerevidence.GateReceipt, error) {
		t.Helper()
		send("gate " + label)
		reply := next("gate")
		var receipt consumerevidence.GateReceipt
		if err := json.Unmarshal(reply.Result, &receipt); err != nil {
			t.Fatal(err)
		}
		if !reply.OK {
			return receipt, errors.New(reply.Error)
		}
		return receipt, nil
	}
	gate := func(label string) {
		t.Helper()
		receipt, err := readGate(label)
		if err != nil || !receipt.Complete || receipt.FixtureError != "" || !consumerActiveControl(receipt) || len(receipt.Daemons) != 2 || receipt.GenerationEquality != consumerevidence.GenerationUnavailable {
			t.Fatalf("gate %s: %+v, %v", label, receipt, err)
		}
		for _, daemon := range receipt.Daemons {
			if daemon.Slot != consumerevidence.SlotRegistered || len(daemon.BeaconMonitors) == 0 {
				t.Fatalf("gate %s: %+v", label, daemon)
			}
		}
		t.Logf("MGR_KEYRING_GATE %s: %+v", label, receipt.Daemons)
	}
	gate("pre-dial")

	send("reject")
	var rejection consumerevidence.Rejection
	decode(next("reject"), &rejection)
	if rejection.Result != -13 || rejection.TargetGID == 0 {
		t.Fatalf("rejection %+v", rejection)
	}
	request := consumerevidence.AuthRequest{
		Provenance: consumerevidence.Provenance{Producer: "ceph-testcontainers-go internal/integration", Subject: "stand-in SUT with a wrong key", Run: t.Name()},
		Expected: consumerevidence.Expected{FSID: seen.FSID, Monitors: seen.Monitors, Manager: strings.TrimPrefix(rejection.Target, "mgr."),
			GID: rejection.TargetGID},
		Rejection: rejection,
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	send("bundle " + string(data))
	var collected struct {
		Bundle  consumerevidence.AuthBundle  `json:"bundle"`
		Verdict consumerevidence.AuthVerdict `json:"verdict"`
	}
	decode(next("bundle"), &collected)
	bundle, verdict := collected.Bundle, collected.Verdict
	if !verdict.Complete || verdict.Target != consumerevidence.TargetRegistered || !verdict.ControlsSucceeded || verdict.Result != -13 ||
		verdict.GenerationEquality != consumerevidence.GenerationUnavailable || len(bundle.Monitors) != 3 || len(bundle.Managers) != 2 {
		t.Fatalf("verdict %+v of bundle %+v", verdict, bundle)
	}
	if !bundle.Rejection.At.Equal(rejection.At) || bundle.Rejection.Error != rejection.Error || bundle.Rejection.Target != rejection.Target {
		t.Fatalf("the rejection changed: %+v, sent %+v", bundle.Rejection, rejection)
	}
	t.Logf("AUTH_FIRST_FAILURE_BUNDLE verdict %+v, %d queries", verdict, len(bundle.Queries))
	gate("post-failure")

	send("transition")
	var receipt consumerevidence.TransitionReceipt
	decode(next("transition"), &receipt)
	if receipt.Result != consumerevidence.SampleAccepted || len(receipt.Changes) < 2 {
		t.Fatalf("transition receipt %+v", receipt)
	}
	restarted := receipt.Changes[len(receipt.Changes)-1]
	gid, frozen := uint64(0), uint64(0)
	for _, registration := range append([]consumerevidence.Registration{restarted.Active}, restarted.Standbys...) {
		if registration.Name == receipt.Restarted {
			gid = registration.GID
		}
	}
	if receipt.Frozen.Map.Active.Name == receipt.Restarted {
		frozen = receipt.Frozen.Map.Active.GID
	}
	if gid == 0 || gid == frozen {
		t.Fatalf("transition receipt %+v", receipt)
	}
	t.Logf("MGR_TRANSITION_RECEIPT mgr.%s GID %d -> %d over %d observed maps, %d epochs not observed, counts %v",
		receipt.Restarted, frozen, gid, len(receipt.Changes), receipt.EpochsNotObserved, receipt.Counts)

	// Stop the restarted process. Its container remains owned, but the MONs
	// remove its MGR slot. The other MGR must still answer the native control.
	manager := "mgr." + receipt.Restarted
	id := seen.DaemonContainers[manager]
	if id == "" || !slices.Contains(seen.Containers, id) {
		t.Fatalf("missing owned container for %s", manager)
	}
	if _, err := docker.ContainerStop(ctx, id, mobycl.ContainerStopOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitGate := func(label string, accept func(consumerevidence.GateReceipt) bool) {
		t.Helper()
		deadline, done := context.WithTimeout(ctx, 3*time.Minute)
		defer done()
		for {
			g, err := readGate(label)
			if err == nil && g.Complete && consumerActiveControl(g) && accept(g) {
				return
			}
			select {
			case <-deadline.Done():
				t.Fatalf("gate %s for %s: %+v, %v: %v", label, manager, g, err, deadline.Err())
			case <-time.After(time.Second):
			}
		}
	}
	awaitGate("stopped-after-restart", func(g consumerevidence.GateReceipt) bool {
		return strings.Contains(g.FixtureError, manager+" ("+consumerevidence.SlotAbsent+")") && slices.ContainsFunc(g.Daemons, func(d consumerevidence.GateDaemon) bool {
			return d.Daemon == manager && d.Slot == consumerevidence.SlotAbsent && d.Stale
		})
	})
	if _, err := docker.ContainerStart(ctx, id, mobycl.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitGate("rejoined", func(g consumerevidence.GateReceipt) bool {
		return g.FixtureError == "" && slices.ContainsFunc(g.Daemons, func(d consumerevidence.GateDaemon) bool {
			return d.Daemon == manager && d.Slot == consumerevidence.SlotRegistered && d.GID != 0 && d.GID != gid
		})
	})

	send("fail consumer-signal")
	if reply := next("fail"); !reply.OK {
		t.Fatalf("fail: %s", reply.Error)
	}
	send("close")
	finished := next("finished")
	err = fixture.Wait()
	exited = true
	var exit *exec.ExitError
	if finished.OK || finished.Error != "driver failure signal: consumer-signal" || !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("finished %+v, exit %v", finished, err)
	}
	report, err := os.ReadFile(filepath.Join(out, "diagnostics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var diagnosed diagnostics.Report
	if err := json.Unmarshal(report, &diagnosed); err != nil {
		t.Fatalf("diagnostics report of %d bytes: %v", len(report), err)
	}

	for role, names := range map[string][]string{"mon": seen.Monitors, "mgr": seen.Managers} {
		for _, name := range names {
			if err := consumerRunningInspection(diagnosed, role, name, seen.DaemonContainers[role+"."+name]); err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, id := range seen.Containers {
		if _, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{}); !cerrdefs.IsNotFound(err) {
			t.Errorf("container %.12s remains: %v", id, err)
		}
	}
	for _, name := range volumes {
		if _, err := docker.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{}); !cerrdefs.IsNotFound(err) {
			t.Errorf("volume %s remains: %v", name, err)
		}
	}
	if _, err := docker.NetworkInspect(ctx, seen.Network, mobycl.NetworkInspectOptions{}); !cerrdefs.IsNotFound(err) {
		t.Errorf("network %s remains: %v", seen.Network, err)
	}
	t.Logf("CONSUMER_EVIDENCE_CLEANUP %d containers, %d volumes and the network are gone; diagnostics had %d artifacts", len(seen.Containers), len(volumes), len(diagnosed.Artifacts))
}

// removeLeftovers removes what a failed run of the fixture left behind, from a
// fresh context.
func removeLeftovers(t *testing.T, docker *testcontainers.DockerClient, seen ipcReady, volumes []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, id := range seen.Containers {
		if _, err := docker.ContainerRemove(ctx, id, mobycl.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil && !cerrdefs.IsNotFound(err) {
			t.Logf("remove container %.12s: %v", id, err)
		}
	}
	for _, name := range volumes {
		if _, err := docker.VolumeRemove(ctx, name, mobycl.VolumeRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
			t.Logf("remove volume %s: %v", name, err)
		}
	}
	if seen.Network != "" {
		if _, err := docker.NetworkRemove(ctx, seen.Network, mobycl.NetworkRemoveOptions{}); err != nil && !cerrdefs.IsNotFound(err) {
			t.Logf("remove network %s: %v", seen.Network, err)
		}
	}
}

// textTail returns the last limit bytes of text.
func textTail(text string, limit int) string {
	return text[max(0, len(text)-limit):]
}
