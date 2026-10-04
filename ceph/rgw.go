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
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RGWContainer is an optional S3 gateway owned by its Ceph cluster. AccessKey
// and SecretKey belong to a generated ordinary S3 user for trusted tests.
type RGWContainer struct {
	testcontainers.Container
	GatewayName   string
	AccessKey     string
	SecretKey     string
	Region        string
	port          int
	tlsPort       int
	publicAddress string
	networkName   string
	owner         *Container
	config        RGWConfig
}

// RGWConfig selects a gateway instance and optional native RGW multisite scope.
// Name defaults to default; different names allow multiple gateways in one zone.
// Realm, Zonegroup and Zone must already exist when supplied. Region defaults to
// us-east-1. SkipUserCreation is useful when sharing existing S3 credentials or
// configuring multisite users through radosgw-admin.
type RGWConfig struct {
	Name, Realm, Zonegroup, Zone, Region string
	SkipUserCreation                     bool
	TLS                                  *RGWTLSConfig
}

// StartRGW starts a gateway using the cluster's Ceph CLI credentials, waits for
// the S3 HTTP listener and creates a test user with radosgw-admin. The daemon
// uses the same ephemeral admin CephX credentials as WithClient. Cluster
// Terminate also terminates gateways, including one returned with an error.
func (c *Container) StartRGW(ctx context.Context) (*RGWContainer, error) {
	return c.StartRGWWithConfig(ctx, RGWConfig{})
}

// RemoveRGW removes one owned gateway container. Zone configuration, S3 users,
// buckets and data remain available through other gateways or a replacement.
// Multisite period endpoints are not changed automatically. The last gateway
// can be removed because RGW is optional. Failed cleanup remains tracked.
func (c *Container) RemoveRGW(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if name == "" {
		name = "default"
	}
	gateway, ok := c.gateways[name]
	if !ok || gateway.Container == nil {
		return fmt.Errorf("RGW gateway %q is not owned by this cluster", name)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := gateway.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
		return fmt.Errorf("remove RGW gateway %s: %w", name, err)
	}
	delete(c.services, rgwServiceName(RGWConfig{Name: name}))
	delete(c.gateways, name)
	return nil
}

// StartRGWWithConfig starts an independently owned, named gateway. Customizers
// are applied last; preserve the required command, network and listener settings.
// A non-nil result returned with an error remains owned by the cluster for cleanup.
func (c *Container) StartRGWWithConfig(ctx context.Context, config RGWConfig, opts ...testcontainers.ContainerCustomizer) (*RGWContainer, error) {
	config, err := normalizeRGWConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ctr, port, tlsPort, err := c.startNamedRGWDaemon(ctx, config, opts...)
	var rgw *RGWContainer
	if ctr != nil {
		rgw = &RGWContainer{Container: ctr, GatewayName: config.Name, Region: config.Region, port: port, tlsPort: tlsPort, publicAddress: c.PublicAddress(), networkName: c.NetworkName(), owner: c, config: config}
		// Publish after credential initialization, including partial error
		// returns. Gateways readers then see an immutable descriptor. The
		// generic service already owns the container during initialization.
		defer func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			owned := c.services[rgwServiceName(config)]
			if owned == nil || owned.GetContainerID() != ctr.GetContainerID() {
				return // Concurrent cluster cleanup already removed this service.
			}
			if c.gateways == nil {
				c.gateways = make(map[string]*RGWContainer)
			}
			c.gateways[config.Name] = rgw
		}()
	}
	if err != nil {
		return rgw, err
	}
	if config.SkipUserCreation {
		return rgw, nil
	}
	args := []string{"radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring"}
	for _, setting := range []struct{ flag, value string }{{"--rgw-realm", config.Realm}, {"--rgw-zonegroup", config.Zonegroup}, {"--rgw-zone", config.Zone}} {
		if setting.value != "" {
			args = append(args, setting.flag, setting.value)
		}
	}
	args = append(args, "user", "create", "--uid", "tc-"+uuid.NewString(), "--display-name", "Testcontainers", "--format", "json")
	user, err := command(ctx, ctr, args...)
	if err != nil {
		return rgw, fmt.Errorf("create RGW S3 user for gateway %s failed", config.Name)
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

func normalizeRGWConfig(config RGWConfig) (RGWConfig, error) {
	if config.Name == "" {
		config.Name = "default"
	}
	if !daemonNamePattern.MatchString(config.Name) {
		return RGWConfig{}, errors.New("RGW name must use letters, digits, dots, underscores or hyphens")
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	for _, value := range []string{config.Realm, config.Zonegroup, config.Zone, config.Region} {
		if strings.TrimSpace(value) != value || strings.HasPrefix(value, "-") || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return RGWConfig{}, errors.New("invalid RGW realm, zonegroup, zone or region")
		}
	}
	if config.TLS != nil {
		var err error
		config.TLS, err = normalizeRGWTLSConfig(config.TLS)
		if err != nil {
			return RGWConfig{}, err
		}
	}
	return config, nil
}

func rgwServiceName(config RGWConfig) string {
	if config.Name == "default" {
		return "rgw"
	}
	return "rgw:" + config.Name
}

func (c *Container) namedRGWDaemonOptions(port, tlsPort int, config RGWConfig) []testcontainers.ContainerCustomizer {
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/bin/sh", "/tc/rgw.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithFiles(scriptFile("rgw")),
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_PORT": strconv.Itoa(port), "CEPH_RGW_REALM": config.Realm, "CEPH_RGW_ZONEGROUP": config.Zonegroup, "CEPH_RGW_ZONE": config.Zone}),
	}
	if config.TLS != nil {
		opts = append(opts, rgwTLSOptions(tlsPort, config.TLS)...)
	}
	if !c.UsesHostNetwork() {
		publicNetwork := c.NetworkName()
		// Prefer the public bridge for published S3 ports when a gateway's
		// additional peer endpoint is disconnected. Equal priorities let
		// Docker select that endpoint by its random network name instead.
		opts = append(opts, testcontainers.WithEndpointSettingsModifier(func(endpoints map[string]*dockernetwork.EndpointSettings) {
			if endpoint := endpoints[publicNetwork]; endpoint != nil {
				endpoint.GwPriority = 1
			}
		}))
		if config.TLS != nil {
			opts = append(opts, testcontainers.WithExposedPorts("7481/tcp"))
		}
		opts = append(opts, testcontainers.WithExposedPorts("7480/tcp"),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("7480/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK || status == http.StatusForbidden }).
				WithStartupTimeout(c.settings.startupTimeout)))
		if config.TLS != nil {
			opts = append(opts, testcontainers.WithAdditionalWaitStrategy(c.rgwTLSReadiness(tlsPort, config.TLS)))
		}
		return opts
	}
	endpoint := net.JoinHostPort(c.PublicAddress(), strconv.Itoa(port))
	// The control image supplies Python; slim RGW images need only the shell
	// and core utilities already used by their bootstrap scripts.
	opts = append(opts,
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_ENDPOINT": endpoint}),
		testcontainers.WithWaitStrategy(c.hostRGWReadiness(port, endpoint)),
	)
	if config.TLS != nil {
		opts = append(opts, testcontainers.WithEnv(map[string]string{"CEPH_RGW_TLS_ENDPOINT": net.JoinHostPort(c.PublicAddress(), strconv.Itoa(tlsPort))}),
			testcontainers.WithAdditionalWaitStrategy(c.rgwTLSReadiness(tlsPort, config.TLS)))
	}
	return opts
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
				_, lastHTTPError = command(ctx, c.cliContainer(), "python3", "-c", rgwHTTPReady, "http://"+endpoint+"/")
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

func (c *Container) startNamedRGWDaemon(ctx context.Context, config RGWConfig, customizers ...testcontainers.ContainerCustomizer) (testcontainers.Container, int, int, error) {
	serviceName := rgwServiceName(config)
	if !c.UsesHostNetwork() {
		tlsPort := 0
		if config.TLS != nil {
			tlsPort = 7481
		}
		opts := append(c.namedRGWDaemonOptions(7480, tlsPort, config), customizers...)
		ctr, err := c.startService(ctx, serviceName, c.settings.rgwImage, opts...)
		return ctr, 7480, tlsPort, err
	}
	for attempt := 0; attempt < hostPortAttempts; attempt++ {
		count := 1
		if config.TLS != nil {
			count = 2
		}
		lease, err := reserveHostPorts(ctx, c.settings.controlImage, c.PublicAddress(), count, c.settings.startupTimeout)
		c.trackHostPortLease(lease)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("reserve RGW host port: %w", err)
		}
		port := lease.Ports[0]
		tlsPort := 0
		if config.TLS != nil {
			tlsPort = lease.Ports[1]
		}
		// Create the target while the reservation still holds its socket. Only
		// release immediately before Start, minimizing the unavoidable bind race.
		opts := append(c.namedRGWDaemonOptions(port, tlsPort, config), customizers...)
		opts = append(opts, testcontainers.WithNoStart())
		ctr, createErr := c.startService(ctx, serviceName, c.settings.rgwImage, opts...)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		releaseErr := lease.Release(cleanupCtx)
		cancel()
		if createErr != nil || releaseErr != nil {
			return ctr, port, tlsPort, errors.Join(createErr, releaseErr)
		}
		if err := ctr.Start(ctx); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			conflict := isPortConflict(cleanupCtx, ctr)
			if !conflict || attempt+1 == hostPortAttempts || ctx.Err() != nil {
				cancel()
				return ctr, port, tlsPort, fmt.Errorf("start RGW on host port %d: %w", port, err)
			}
			cleanupErr := c.discardNamedRGWAttempt(cleanupCtx, serviceName, ctr)
			cancel()
			if cleanupErr != nil {
				return ctr, port, tlsPort, errors.Join(err, cleanupErr)
			}
			continue
		}
		return ctr, port, tlsPort, nil
	}
	return nil, 0, 0, errors.New("RGW host port attempts exhausted")
}

func (c *Container) discardRGWAttempt(ctx context.Context, ctr testcontainers.Container) error {
	return c.discardNamedRGWAttempt(ctx, "rgw", ctr)
}

func (c *Container) discardNamedRGWAttempt(ctx context.Context, serviceName string, ctr testcontainers.Container) error {
	if err := ctr.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
		// Retain the partial service so cluster Terminate can retry cleanup.
		return fmt.Errorf("terminate conflicted RGW: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owned, ok := c.services[serviceName]; ok && owned.GetContainerID() == ctr.GetContainerID() {
		delete(c.services, serviceName)
	}
	return nil
}

// DaemonEndpoint returns the HTTP address used by peers in the gateway's Docker
// network or daemon-host namespace. Host mode returns the advertised address;
// bridge mode returns the gateway's IP in its Ceph cluster network. The caller
// must share or reach that network. Use S3Endpoint for host application clients.
func (c *RGWContainer) DaemonEndpoint(ctx context.Context) (string, error) {
	port := c.port
	if port == 0 {
		port = 7480
	}
	address := c.publicAddress
	if address == "" {
		if c.networkName != "" {
			inspect, err := c.Inspect(ctx)
			if err != nil {
				return "", err
			}
			if inspect != nil && inspect.NetworkSettings != nil {
				if endpoint := inspect.NetworkSettings.Networks[c.networkName]; endpoint != nil && endpoint.IPAddress.IsValid() {
					address = endpoint.IPAddress.String()
				}
			}
			if address == "" {
				return "", errors.New("RGW has no address in its Ceph cluster network")
			}
		} else {
			var err error
			address, err = c.ContainerIP(ctx)
			if err != nil {
				return "", err
			}
		}
	}
	return "http://" + net.JoinHostPort(address, strconv.Itoa(port)), nil
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
