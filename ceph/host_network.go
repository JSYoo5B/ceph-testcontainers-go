package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const hostPortLeasePrefix = "TC_CEPH_HOST_PORT_LEASE "

// ErrHostNetworkUnavailable reports that host mode cannot serve native clients
// in the test process: the engine does not run Linux containers, the selected
// address is not assigned on the Docker host, or the first MON's advertised
// endpoint is not reachable from this process. Run returns it before creating
// MGR/OSD daemons; later MON/RGW port reservations may also return it.
var ErrHostNetworkUnavailable = errors.New("ceph host network is unavailable to the test process")

// A msgr2 listener sends this banner before reading from the peer.
const monitorBanner = "ceph v2\n"

const (
	hostProbeWindow = 5 * time.Second
	hostProbeDial   = 2 * time.Second
	hostProbeRetry  = 200 * time.Millisecond
)

// The sockets live in the Docker engine's host network namespace, which may
// differ from the Go process's namespace. Keep every socket open until the
// caller creates its daemon container and explicitly releases the lease.
const hostPortAllocatorScript = `import ipaddress, json, signal, socket, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
address, count = sys.argv[1], int(sys.argv[2])
family = socket.AF_INET6 if ipaddress.ip_address(address).version == 6 else socket.AF_INET
sockets = []
for unused in range(count):
    candidate = socket.socket(family, socket.SOCK_STREAM)
    if family == socket.AF_INET6:
        candidate.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    candidate.bind((address, 0))
    candidate.listen(1)
    sockets.append(candidate)
print('TC_CEPH_HOST_PORT_LEASE ' + json.dumps([candidate.getsockname()[1] for candidate in sockets]), flush=True)
while True:
    time.sleep(86400)
`

type hostPortLease struct {
	Ports []int
	mu    sync.Mutex
	ctr   testcontainers.Container
}

// Release closes all reservations by removing their allocator container. A
// failed removal remains retryable; ports must not be used until it succeeds.
func (lease *hostPortLease) Release(ctx context.Context) error {
	if lease == nil {
		return nil
	}
	if err := lockTopologyMutex(ctx, &lease.mu); err != nil {
		return err
	}
	defer lease.mu.Unlock()
	if lease.ctr == nil {
		return nil
	}
	if err := lease.ctr.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
		return fmt.Errorf("release host port reservations: %w", err)
	}
	lease.ctr = nil
	return nil
}

// A timed-out removal may have succeeded on the Docker host. Its retry can
// report NotFound. Accept only wholly missing resources, preserving unrelated
// errors joined by Terminate hooks or SDK cleanup. Inspect joins before SDK
// wrapper classifiers, which could otherwise hide a non-missing child.
func onlyMissingHostResource(err error) bool {
	if err == nil {
		return true
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			hasChild := false
			for _, child := range joined.Unwrap() {
				if child == nil {
					continue
				}
				hasChild = true
				if !onlyMissingHostResource(child) {
					return false
				}
			}
			return hasChild
		}
	}
	return errdefs.IsNotFound(err)
}

func reserveHostPorts(ctx context.Context, image, address string, count int, timeout time.Duration) (*hostPortLease, error) {
	if strings.TrimSpace(image) == "" || count <= 0 || timeout <= 0 {
		return nil, errors.New("host port reservation requires an image, a positive count and a positive timeout")
	}
	if err := validateHostAllocatorAddress(address); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, image,
		hostContainerCustomizer(address),
		testcontainers.WithEntrypoint("python3"),
		testcontainers.WithCmd("-u", "-c", hostPortAllocatorScript, address, strconv.Itoa(count)),
		testcontainers.WithWaitStrategy(wait.ForLog(hostPortLeasePrefix).WithStartupTimeout(timeout)),
	)
	lease := &hostPortLease{ctr: ctr}
	if err != nil {
		var started testcontainers.Container
		if ctr != nil {
			started = ctr
		}
		return cleanupFailedHostPortLease(lease, allocatorFailure(started, address, err))
	}
	logs, err := ctr.Logs(ctx)
	if err != nil {
		return cleanupFailedHostPortLease(lease, fmt.Errorf("read host port reservations: %w", err))
	}
	output, err := io.ReadAll(logs)
	logs.Close()
	if err == nil {
		lease.Ports, err = parseHostPortLease(output, count)
	}
	if err != nil {
		return cleanupFailedHostPortLease(lease, fmt.Errorf("decode host port reservations: %w", err))
	}
	return lease, nil
}

// The allocator exits before readiness when the address is not assigned on the
// Docker host. Report that as an environment mismatch, not a generic exit.
func allocatorFailure(ctr testcontainers.Container, address string, cause error) error {
	err := fmt.Errorf("reserve host ports on %s: %w", address, cause)
	if ctr == nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logs, logErr := ctr.Logs(ctx)
	if logErr != nil {
		return err
	}
	output, _ := io.ReadAll(logs)
	logs.Close()
	if strings.Contains(string(output), "Cannot assign requested address") {
		return fmt.Errorf("%w: address %s is not assigned on the Docker host: %w", ErrHostNetworkUnavailable, address, err)
	}
	return err
}

func cleanupFailedHostPortLease(lease *hostPortLease, cause error) (*hostPortLease, error) {
	// The reservation context may already be canceled. Cleanup needs its own
	// bound; return ownership if removal fails so the caller can retry it.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		return lease, errors.Join(cause, err)
	}
	return nil, cause
}

func parseHostPortLease(output []byte, count int) ([]int, error) {
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.HasPrefix(line, hostPortLeasePrefix) {
			continue
		}
		var ports []int
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, hostPortLeasePrefix)), &ports); err != nil {
			return nil, err
		}
		if len(ports) != count {
			return nil, fmt.Errorf("allocator returned %d ports, expected %d", len(ports), count)
		}
		seen := make(map[int]bool, count)
		for _, port := range ports {
			if port <= 0 || port > 65535 || seen[port] {
				return nil, fmt.Errorf("allocator returned an invalid or repeated port: %d", port)
			}
			seen[port] = true
		}
		return ports, nil
	}
	return nil, errors.New("allocator readiness output has no port lease")
}

func validateHostAllocatorAddress(address string) error {
	ip := net.ParseIP(address)
	if ip == nil || !(ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsLinkLocalUnicast()) {
		return fmt.Errorf("host network public address must be a concrete unicast IP: %q", address)
	}
	return nil
}

// Apply after the cluster's bridge defaults. Preserve unrelated modifiers but
// remove port publishing and network attachments, which conflict with host mode.
func hostContainerCustomizer(address string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if err := validateHostAllocatorAddress(address); err != nil {
			return err
		}
		req.Networks = nil
		req.NetworkAliases = nil
		req.NetworkMode = ""
		req.ExposedPorts = nil
		previousConfig := req.ConfigModifier
		req.ConfigModifier = func(config *container.Config) {
			if previousConfig != nil {
				previousConfig(config)
			}
			config.ExposedPorts = nil
		}
		previousHostConfig := req.HostConfigModifier
		req.HostConfigModifier = func(config *container.HostConfig) {
			if previousHostConfig != nil {
				previousHostConfig(config)
			}
			config.NetworkMode = "host"
			config.PortBindings = nil
			config.PublishAllPorts = false
		}
		return nil
	}
}

// checkHostNetworkEngine rejects engines that do not run Linux containers,
// before any cluster resource is created.
func checkHostNetworkEngine(ctx context.Context) error {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("inspect Docker engine for host network mode: %w", err)
	}
	defer docker.Close()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return fmt.Errorf("inspect Docker engine for host network mode: %w", err)
	}
	if info.Info.OSType != "linux" {
		return fmt.Errorf("%w: Docker engine runs %q containers, host network mode requires a Linux engine", ErrHostNetworkUnavailable, info.Info.OSType)
	}
	return nil
}

// verifyHostMonitor connects from the test process to the advertised MON
// endpoint, as a native client would. Docker Desktop may forward host-mode
// ports asynchronously, so a refused, timed-out or bannerless connection is
// retried within a window. A different banner fails at once.
func verifyHostMonitor(ctx context.Context, address string, port int) error {
	endpoint := net.JoinHostPort(address, strconv.Itoa(port))
	if err := probeBanner(ctx, endpoint, monitorBanner, hostProbeWindow); err != nil {
		return fmt.Errorf("%w: MON %s: %v; %s", ErrHostNetworkUnavailable, endpoint, err, hostNetworkHint(ctx, address))
	}
	return nil
}

var errForeignResponder = errors.New("another process answered")

func probeBanner(ctx context.Context, endpoint, banner string, window time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	dialer := net.Dialer{Timeout: hostProbeDial}
	var last error
	for {
		conn, err := dialer.DialContext(ctx, "tcp", endpoint)
		if err == nil {
			last = readBanner(conn, banner)
			conn.Close()
			if last == nil || errors.Is(last, errForeignResponder) {
				return last
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not reachable within %s: %w", window, last)
		case <-time.After(hostProbeRetry):
		}
	}
}

func readBanner(conn net.Conn, banner string) error {
	if err := conn.SetReadDeadline(time.Now().Add(hostProbeDial)); err != nil {
		return err
	}
	got := make([]byte, len(banner))
	n, err := io.ReadFull(conn, got)
	if n > 0 && string(got[:n]) != banner[:n] {
		return fmt.Errorf("%w: %q", errForeignResponder, got[:n])
	}
	if err != nil {
		// A port forwarder may accept and then close while its backend is absent.
		return fmt.Errorf("no Ceph banner: %w", err)
	}
	return nil
}

func hostNetworkHint(ctx context.Context, address string) string {
	var operatingSystem, daemonHost string
	if docker, err := testcontainers.NewDockerClientWithOpts(ctx); err == nil {
		if info, err := docker.Info(ctx, mobycl.InfoOptions{}); err == nil {
			operatingSystem = info.Info.OperatingSystem
		}
		daemonHost = docker.DaemonHost()
		docker.Close()
	}
	return describeHostNetworkFailure(operatingSystem, daemonHost, address)
}

func describeHostNetworkFailure(operatingSystem, daemonHost, address string) string {
	local := strings.HasPrefix(daemonHost, "unix://") || strings.HasPrefix(daemonHost, "npipe://")
	loopback := net.ParseIP(address).IsLoopback()
	switch {
	case strings.Contains(operatingSystem, "Docker Desktop") && loopback:
		return "Docker Desktop forwards host-mode ports only with host networking enabled (Settings > Resources > Network)"
	case strings.Contains(operatingSystem, "Docker Desktop"):
		return "Docker Desktop forwards host-mode ports only to this machine's loopback; omit WithHostAddress to use 127.0.0.1"
	case daemonHost != "" && !local && loopback:
		return fmt.Sprintf("Docker daemon %s is remote; select its address reachable from this process with WithHostAddress", daemonHost)
	default:
		return "the test process must reach the Docker host's network; a process inside a bridge container cannot reach host-mode daemons"
	}
}

// A lease cannot make release -> daemon start atomic. Retry that narrow race
// only when the daemon actually exits with a socket address-in-use error.
func isPortConflict(ctx context.Context, ctr testcontainers.Container) bool {
	if ctr == nil {
		return false
	}
	state, err := ctr.State(ctx)
	if err != nil || state == nil || state.Running || state.Restarting || state.OOMKilled || state.ExitCode == 0 || state.Status != "exited" {
		return false
	}
	logs, err := ctr.Logs(ctx)
	if err != nil {
		return false
	}
	defer logs.Close()
	output, err := io.ReadAll(logs)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(strings.ToLower(string(output)), "\n") {
		if strings.Contains(line, "eaddrinuse") {
			return true
		}
		if strings.Contains(line, "bind") && (strings.Contains(line, "address already in use") || strings.Contains(line, "(98)")) {
			return true
		}
		if strings.Contains(line, "address already in use") && strings.Contains(line, "(98)") {
			return true
		}
	}
	return false
}
