package ceph

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
)

func TestHostContainerCustomizerRejectsUnusablePublicAddresses(t *testing.T) {
	for _, address := range []string{"", "localhost", "0.0.0.0", "::", "224.0.0.1", "255.255.255.255", "[::1]", "127.0.0.1:3300"} {
		if err := hostContainerCustomizer(address)(&testcontainers.GenericContainerRequest{}); err == nil {
			t.Errorf("accepted an unusable public IP: %q", address)
		}
	}
	for _, address := range []string{"127.0.0.1", "192.0.2.10", "::1"} {
		if err := hostContainerCustomizer(address)(&testcontainers.GenericContainerRequest{}); err != nil {
			t.Errorf("rejected valid public IP %q: %v", address, err)
		}
	}
}

func TestHostContainerCustomizerRemovesPublishingAndPreservesOtherSettings(t *testing.T) {
	req := &testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Networks: []string{"old-bridge"}, NetworkAliases: map[string][]string{"old-bridge": {"ceph"}},
		NetworkMode: "bridge", ExposedPorts: []string{"3300/tcp"},
		ConfigModifier: func(config *container.Config) { config.User = "167" },
		HostConfigModifier: func(config *container.HostConfig) {
			config.Resources.Memory = 64 << 20
			config.NetworkMode = "bridge"
			config.PublishAllPorts = true
			config.PortBindings = network.PortMap{network.MustParsePort("3300/tcp"): {{HostPort: "3300"}}}
		},
	}}
	if err := hostContainerCustomizer("127.0.0.1")(req); err != nil {
		t.Fatal(err)
	}
	if req.Networks != nil || req.NetworkAliases != nil || req.ExposedPorts != nil || req.NetworkMode != "" {
		t.Fatalf("host request retained bridge/publishing metadata: %+v", req.ContainerRequest)
	}
	config := &container.Config{ExposedPorts: network.PortSet{network.MustParsePort("3300/tcp"): {}}}
	req.ConfigModifier(config)
	host := &container.HostConfig{}
	req.HostConfigModifier(host)
	if config.User != "167" || config.ExposedPorts != nil || host.Resources.Memory != 64<<20 || host.NetworkMode != "host" || host.PublishAllPorts || host.PortBindings != nil {
		t.Fatalf("host customizer lost another modifier or retained port publishing: config=%+v host=%+v", config, host)
	}
}

func TestPortConflictRequiresExitedDaemonAndActualSocketError(t *testing.T) {
	for _, test := range []struct {
		name  string
		state container.State
		logs  string
		want  bool
	}{
		{"Ceph bind errno", container.State{Status: "exited", ExitCode: 1}, "stderr: failed to bind v2:127.0.0.1:3300: (98) Address already in use\n", true},
		{"Ceph numeric bind errno", container.State{Status: "exited", ExitCode: 1}, "stderr: bind failed: (98)\n", true},
		{"explicit EADDRINUSE", container.State{Status: "exited", ExitCode: 1}, "socket.error EADDRINUSE\n", true},
		{"errno diagnostic", container.State{Status: "exited", ExitCode: 1}, "error: (98) Address already in use\n", true},
		{"running warning", container.State{Status: "running", Running: true, ExitCode: 1}, "bind: Address already in use", false},
		{"success exit", container.State{Status: "exited", ExitCode: 0}, "bind: Address already in use", false},
		{"out of memory", container.State{Status: "exited", ExitCode: 137, OOMKilled: true}, "bind: Address already in use", false},
		{"authentication error", container.State{Status: "exited", ExitCode: 1}, "handle_auth_bad_method: permission denied", false},
		{"invalid address", container.State{Status: "exited", ExitCode: 1}, "bind failed: (99) Cannot assign requested address", false},
		{"unrelated text", container.State{Status: "exited", ExitCode: 1}, "test config note: address already in use\ninvalid fsid", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &hostNetworkFixtureContainer{state: &test.state, logs: test.logs}
			if got := isPortConflict(t.Context(), ctr); got != test.want {
				t.Fatalf("isPortConflict=%v, want %v", got, test.want)
			}
		})
	}
}

func TestHostPortLeaseReleaseRetainsFailedCleanupForRetry(t *testing.T) {
	ctr := &hostNetworkFixtureContainer{failTerminationOnce: true}
	lease := &hostPortLease{Ports: []int{45678}, ctr: ctr}
	if err := lease.Release(t.Context()); err == nil || lease.ctr == nil {
		t.Fatalf("failed lease removal lost ownership: error=%v container=%v", err, lease.ctr)
	}
	if err := lease.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(t.Context()); err != nil || ctr.terminations != 2 || lease.ctr != nil {
		t.Fatalf("lease cleanup is not retryable/idempotent: error=%v calls=%d", err, ctr.terminations)
	}
}

type hostMissingSDKError struct{ error }

func (err hostMissingSDKError) Unwrap() error    { return err.error }
func (hostMissingSDKError) Is(target error) bool { return target == errdefs.ErrNotFound }

func TestHostPortLeaseReleaseAcceptsOnlyWhollyMissingResources(t *testing.T) {
	cleanupFailure := errors.New("termination cleanup failed")
	for _, test := range []struct {
		name string
		err  error
		safe bool
	}{
		{"removed container", errdefs.ErrNotFound, true},
		{"wrapped missing", fmt.Errorf("remove: %w", errdefs.ErrNotFound), true},
		{"joined missing", errors.Join(errdefs.ErrNotFound, fmt.Errorf("stop: %w", errdefs.ErrNotFound)), true},
		{"SDK missing classifier", hostMissingSDKError{errors.New("Docker object is absent")}, true},
		{"real failure", cleanupFailure, false},
		{"mixed missing and failure", errors.Join(errdefs.ErrNotFound, cleanupFailure), false},
		{"wrapped mixed failure", fmt.Errorf("cleanup: %w", errors.Join(errdefs.ErrNotFound, cleanupFailure)), false},
		{"SDK classified mixed failure", hostMissingSDKError{errors.Join(errdefs.ErrNotFound, cleanupFailure)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &hostNetworkFixtureContainer{terminationErr: test.err}
			lease := &hostPortLease{ctr: ctr}
			err := lease.Release(t.Context())
			if (err == nil) != test.safe || (lease.ctr == nil) != test.safe {
				t.Fatalf("cleanup lost ownership or accepted a real failure: error=%v retained=%t safe=%t", err, lease.ctr != nil, test.safe)
			}
			if !test.safe && !errors.Is(err, cleanupFailure) {
				t.Fatal("cleanup changed the underlying failure")
			}
			ctr.terminationErr = nil
			if err := lease.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if !test.safe {
				wantCalls = 2
			}
			if ctr.terminations != wantCalls {
				t.Fatalf("cleanup repeated a missing resource or skipped a failed retry: %d calls", ctr.terminations)
			}
		})
	}
}

func TestHostPortLeaseRejectsInvalidAllocatorOutput(t *testing.T) {
	for _, output := range []string{"missing readiness", hostPortLeasePrefix + "[3300]", hostPortLeasePrefix + "[3300,3300]", hostPortLeasePrefix + "[0,3300]", hostPortLeasePrefix + "[3300,65536]", hostPortLeasePrefix + "[\"3300\",6789]"} {
		if ports, err := parseHostPortLease([]byte(output), 2); err == nil {
			t.Errorf("accepted invalid reservation output %q: %v", output, ports)
		}
	}
}

func TestHostPortAllocatorHoldsAllSocketsUntilProcessEnds(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required to exercise the container allocator script")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-u", "-c", hostPortAllocatorScript, "127.0.0.1", "3")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics strings.Builder
	cmd.Stderr = &diagnostics
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		cmd.Wait()
		if strings.Contains(diagnostics.String(), "PermissionError") && strings.Contains(diagnostics.String(), "Operation not permitted") {
			t.Skip("environment does not allow binding temporary localhost sockets")
		}
		t.Fatalf("allocator did not return reservations: %s", diagnostics.String())
	}
	ports, err := parseHostPortLease(scanner.Bytes(), 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range ports {
		address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		listener, err := net.Listen("tcp", address)
		if err == nil {
			listener.Close()
			t.Fatalf("allocator returned a port it did not hold: %s", address)
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("held port %s failed for an unrelated reason: %v", address, err)
		}
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("allocator did not exit cleanly on SIGTERM: %v", err)
	}
	for _, port := range ports {
		listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatalf("allocator process exit did not release port %d: %v", port, err)
		}
		listener.Close()
	}
}

type hostNetworkFixtureContainer struct {
	testcontainers.Container
	state               *container.State
	logs                string
	terminations        int
	failTerminationOnce bool
	terminationErr      error
}

func (ctr *hostNetworkFixtureContainer) State(context.Context) (*container.State, error) {
	return ctr.state, nil
}

func (ctr *hostNetworkFixtureContainer) Logs(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(ctr.logs)), nil
}

func (ctr *hostNetworkFixtureContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	ctr.terminations++
	if ctr.failTerminationOnce && ctr.terminations == 1 {
		return errors.New("Docker removal failed")
	}
	return ctr.terminationErr
}

func bannerListener(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow temporary localhost listeners: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			serve(conn)
			conn.Close()
		}
	}()
	return listener.Addr().String()
}

func TestProbeBannerAcceptsMonitorBanner(t *testing.T) {
	endpoint := bannerListener(t, func(conn net.Conn) { io.WriteString(conn, monitorBanner+"\x10\x00") })
	if err := probeBanner(t.Context(), endpoint, monitorBanner, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestProbeBannerRejectsForeignResponderWithoutWaiting(t *testing.T) {
	endpoint := bannerListener(t, func(conn net.Conn) { io.WriteString(conn, "SSH-2.0-OpenSSH\r\n") })
	started := time.Now()
	err := probeBanner(t.Context(), endpoint, monitorBanner, 5*time.Second)
	if !errors.Is(err, errForeignResponder) {
		t.Fatalf("foreign listener was not identified: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("a definite foreign responder must not wait for the probe window")
	}
}

func TestProbeBannerRetriesUntilEndpointAppears(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow temporary localhost listeners: %v", err)
	}
	endpoint := reserved.Addr().String()
	reserved.Close()
	go func() {
		time.Sleep(3 * hostProbeRetry)
		listener, err := net.Listen("tcp", endpoint)
		if err != nil {
			return
		}
		t.Cleanup(func() { listener.Close() })
		if conn, err := listener.Accept(); err == nil {
			io.WriteString(conn, monitorBanner)
			conn.Close()
		}
	}()
	if err := probeBanner(t.Context(), endpoint, monitorBanner, 5*time.Second); err != nil {
		t.Fatalf("delayed forwarding was not retried: %v", err)
	}
}

func TestProbeBannerFailsWithinWindow(t *testing.T) {
	silentClose := bannerListener(t, func(net.Conn) {})
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow temporary localhost listeners: %v", err)
	}
	closed := refused.Addr().String()
	refused.Close()
	for name, endpoint := range map[string]string{"refused": closed, "accept then close": silentClose} {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			err := probeBanner(t.Context(), endpoint, monitorBanner, 600*time.Millisecond)
			if err == nil || errors.Is(err, errForeignResponder) {
				t.Fatalf("unreachable endpoint was accepted or misclassified: %v", err)
			}
			if elapsed := time.Since(started); elapsed > 600*time.Millisecond+hostProbeDial {
				t.Fatalf("probe exceeded its window: %s", elapsed)
			}
		})
	}
}

func TestVerifyHostMonitorWrapsSentinel(t *testing.T) {
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow temporary localhost listeners: %v", err)
	}
	port := refused.Addr().(*net.TCPAddr).Port
	refused.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err := verifyHostMonitor(ctx, "127.0.0.1", port); !errors.Is(err, ErrHostNetworkUnavailable) {
		t.Fatalf("unreachable MON did not report ErrHostNetworkUnavailable: %v", err)
	}
}

func TestDescribeHostNetworkFailureNamesTheLikelyCause(t *testing.T) {
	for _, test := range []struct{ name, os, daemon, address, want string }{
		{"Docker Desktop", "Docker Desktop", "unix:///Users/u/.docker/run/docker.sock", "127.0.0.1", "host networking enabled"},
		{"Docker Desktop VM address", "Docker Desktop", "unix:///Users/u/.docker/run/docker.sock", "192.168.65.3", "omit WithHostAddress"},
		{"remote loopback", "Ubuntu 24.04 LTS", "tcp://192.0.2.10:2376", "127.0.0.1", "WithHostAddress"},
		{"remote explicit address", "Ubuntu 24.04 LTS", "tcp://192.0.2.10:2376", "192.0.2.10", "bridge container"},
		{"local Linux", "Ubuntu 24.04 LTS", "unix:///var/run/docker.sock", "127.0.0.1", "bridge container"},
		{"unknown engine", "", "", "127.0.0.1", "bridge container"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := describeHostNetworkFailure(test.os, test.daemon, test.address); !strings.Contains(got, test.want) {
				t.Fatalf("hint %q does not mention %q", got, test.want)
			}
		})
	}
}

func TestAllocatorFailureReportsUnassignedAddress(t *testing.T) {
	cause := errors.New("container exited with code 1")
	unassigned := &hostNetworkFixtureContainer{logs: "OSError: [Errno 99] Cannot assign requested address\n"}
	if err := allocatorFailure(unassigned, "192.0.2.10", cause); !errors.Is(err, ErrHostNetworkUnavailable) || !errors.Is(err, cause) || !strings.Contains(err.Error(), "192.0.2.10") {
		t.Fatalf("unassigned host address was not reported as unavailable: %v", err)
	}
	for name, ctr := range map[string]testcontainers.Container{
		"other failure": &hostNetworkFixtureContainer{logs: "ModuleNotFoundError: No module named 'ipaddress'\n"},
		"no container":  nil,
	} {
		if err := allocatorFailure(ctr, "127.0.0.1", cause); errors.Is(err, ErrHostNetworkUnavailable) || !errors.Is(err, cause) {
			t.Fatalf("%s was misclassified: %v", name, err)
		}
	}
}
