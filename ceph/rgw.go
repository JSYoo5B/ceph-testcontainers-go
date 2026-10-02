package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RGWContainer is an optional S3 gateway owned by its Ceph cluster. AccessKey
// and SecretKey belong to a generated ordinary S3 user for trusted tests.
type RGWContainer struct {
	testcontainers.Container
	AccessKey     string
	SecretKey     string
	Region        string
	port          int
	publicAddress string
}

// StartRGW starts a gateway using the cluster's Ceph CLI credentials, waits for
// the S3 HTTP listener and creates a test user with radosgw-admin. The daemon
// uses the same ephemeral admin CephX credentials as WithClient. Cluster
// Terminate also terminates gateways, including one returned with an error.
func (c *Container) StartRGW(ctx context.Context) (*RGWContainer, error) {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ctr, port, err := c.startRGWDaemon(ctx)
	var rgw *RGWContainer
	if ctr != nil {
		rgw = &RGWContainer{Container: ctr, Region: "us-east-1", port: port, publicAddress: c.PublicAddress()}
	}
	if err != nil {
		return rgw, err
	}
	user, err := command(ctx, ctr, "radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "user", "create",
		"--uid", "tc-"+uuid.NewString(), "--display-name", "Testcontainers", "--format", "json")
	if err != nil {
		return rgw, fmt.Errorf("create RGW S3 user: %w", err)
	}
	var credentials struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(user, &credentials); err != nil {
		return rgw, fmt.Errorf("decode RGW S3 credentials: %w", err)
	}
	if len(credentials.Keys) != 1 || credentials.Keys[0].AccessKey == "" || credentials.Keys[0].SecretKey == "" {
		return rgw, fmt.Errorf("RGW did not generate one S3 access/secret key pair")
	}
	rgw.AccessKey = credentials.Keys[0].AccessKey
	rgw.SecretKey = credentials.Keys[0].SecretKey
	return rgw, nil
}

func (c *Container) rgwDaemonOptions(port int) []testcontainers.ContainerCustomizer {
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/bin/sh", "/tc/rgw.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithFiles(scriptFile("rgw")),
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_PORT": strconv.Itoa(port)}),
	}
	if !c.UsesHostNetwork() {
		return append(opts, testcontainers.WithExposedPorts("7480/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("7480/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK || status == http.StatusForbidden }).
				WithStartupTimeout(c.settings.startupTimeout)))
	}
	endpoint := net.JoinHostPort(c.PublicAddress(), strconv.Itoa(port))
	// The control image supplies Python; slim RGW images need only the shell
	// and core utilities already used by their bootstrap scripts.
	return append(opts,
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_ENDPOINT": endpoint}),
		testcontainers.WithWaitStrategy(c.hostRGWReadiness(port, endpoint)),
	)
}

// A shared host port could already serve another RGW. Verify that the daemon
// in this container owns the listening socket before accepting an HTTP reply.
const rgwOwnsListener = `porthex=$(printf '%04X' "$1")
for inode in $(awk -v port="$porthex" '$4 == "0A" && $2 ~ (":" port "$") {print $10}' /proc/net/tcp /proc/net/tcp6 2>/dev/null); do
    for fd in /proc/1/fd/*; do
        if [ "$(readlink "$fd" 2>/dev/null)" = "socket:[$inode]" ]; then
            exit 0
        fi
    done
done
exit 1
`

const rgwHTTPReady = `import sys, urllib.error, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(sys.argv[1], timeout=2) as response:
        status = response.status
except urllib.error.HTTPError as response:
    status = response.code
sys.exit(0 if status in (200, 403) else 1)
`

func (c *Container) hostRGWReadiness(port int, endpoint string) *wait.NopStrategy {
	return wait.ForNop(func(ctx context.Context, target wait.StrategyTarget) error {
		ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
		defer cancel()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var lastHTTPError error
		for {
			state, err := target.State(ctx)
			if err != nil {
				return fmt.Errorf("inspect RGW startup: %w", err)
			}
			if state == nil || !state.Running {
				return errors.New("RGW stopped before owning its host listener")
			}
			code, output, err := target.Exec(ctx, []string{"/bin/sh", "-c", rgwOwnsListener, "rgw-listener", strconv.Itoa(port)})
			if err != nil {
				return fmt.Errorf("inspect RGW host listener: %w", err)
			}
			if output != nil {
				if _, err := io.Copy(io.Discard, output); err != nil {
					return fmt.Errorf("read RGW listener probe: %w", err)
				}
			}
			if code == 0 {
				_, lastHTTPError = command(ctx, c.Container, "python3", "-c", rgwHTTPReady, "http://"+endpoint+"/")
				if lastHTTPError == nil {
					return nil
				}
			}
			select {
			case <-ctx.Done():
				return errors.Join(fmt.Errorf("wait for RGW host listener: %w", ctx.Err()), lastHTTPError)
			case <-ticker.C:
			}
		}
	}).WithStartupTimeout(c.settings.startupTimeout)
}

func (c *Container) startRGWDaemon(ctx context.Context) (testcontainers.Container, int, error) {
	if !c.UsesHostNetwork() {
		ctr, err := c.startService(ctx, "rgw", c.settings.rgwImage, c.rgwDaemonOptions(7480)...)
		return ctr, 7480, err
	}
	for attempt := 0; attempt < hostPortAttempts; attempt++ {
		lease, err := reserveHostPorts(ctx, c.settings.controlImage, c.PublicAddress(), 1, c.settings.startupTimeout)
		c.trackHostPortLease(lease)
		if err != nil {
			return nil, 0, fmt.Errorf("reserve RGW host port: %w", err)
		}
		port := lease.Ports[0]
		// Create the target while the reservation still holds its socket. Only
		// release immediately before Start, minimizing the unavoidable bind race.
		opts := append(c.rgwDaemonOptions(port), testcontainers.WithNoStart())
		ctr, createErr := c.startService(ctx, "rgw", c.settings.rgwImage, opts...)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		releaseErr := lease.Release(cleanupCtx)
		cancel()
		if createErr != nil || releaseErr != nil {
			return ctr, port, errors.Join(createErr, releaseErr)
		}
		if err := ctr.Start(ctx); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			conflict := isPortConflict(cleanupCtx, ctr)
			if !conflict || attempt+1 == hostPortAttempts || ctx.Err() != nil {
				cancel()
				return ctr, port, fmt.Errorf("start RGW on host port %d: %w", port, err)
			}
			cleanupErr := c.discardRGWAttempt(cleanupCtx, ctr)
			cancel()
			if cleanupErr != nil {
				return ctr, port, errors.Join(err, cleanupErr)
			}
			continue
		}
		return ctr, port, nil
	}
	return nil, 0, errors.New("RGW host port attempts exhausted")
}

func (c *Container) discardRGWAttempt(ctx context.Context, ctr testcontainers.Container) error {
	if err := ctr.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
		// Retain the partial service so cluster Terminate can retry cleanup.
		return fmt.Errorf("terminate conflicted RGW: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owned, ok := c.services["rgw"]; ok && owned.GetContainerID() == ctr.GetContainerID() {
		delete(c.services, "rgw")
	}
	return nil
}

// S3Endpoint returns the HTTP endpoint for host S3 clients. Bridge mode uses
// its mapped port; host mode uses the selected daemon port. The default
// 127.0.0.1 address uses the Docker provider host, while a custom host address
// is returned directly, matching the gateway's bind address.
// Configure clients to use path-style addressing and the test credentials.
func (c *RGWContainer) S3Endpoint(ctx context.Context) (string, error) {
	port := c.port
	if port == 0 {
		// Gateways constructed by the multicluster bridge use the default.
		port = 7480
	}
	if c.publicAddress != "" && c.publicAddress != "127.0.0.1" {
		return "http://" + net.JoinHostPort(c.publicAddress, strconv.Itoa(port)), nil
	}
	return c.PortEndpoint(ctx, strconv.Itoa(port)+"/tcp", "http")
}
