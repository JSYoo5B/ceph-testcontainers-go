package rgw

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
	"sync"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/google/uuid"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Gateway is an optional S3 gateway owned by its Ceph cluster. AccessKey
// and SecretKey belong to a generated ordinary S3 user for trusted tests.
//
// Gateway embeds testcontainers.Container, so Stop, Start, Logs, Exec and
// Inspect act on the gateway container. Formatting a Gateway, User, Account,
// Placement or TLSConfig with any fmt verb, including %+v and %#v, prints only
// an identifying label such as "RGW gateway s3"; credentials and TLS keys are
// read explicitly from the fields or User.Credentials. A gateway runs with 4
// worker threads: enough for functional tests, not for load tests. Rate limits
// are enforced by each gateway separately.
type Gateway struct {
	testcontainers.Container
	GatewayName string
	AccessKey   string
	SecretKey   string
	// Region is the SigV4 signing region: Config.Region, which defaults to
	// us-east-1, or for a multisite gateway its zonegroup name. RGW itself
	// is not configured with it.
	Region        string
	port          int
	tlsPort       int
	publicAddress string
	networkName   string
	owner         *ceph.Container
	config        Config
}

// Config selects a gateway instance and optional native RGW multisite scope.
// Name defaults to default; different names allow multiple gateways in one zone.
// Realm, Zonegroup and Zone must already exist when supplied. Region defaults to
// us-east-1. SkipUserCreation is useful when sharing existing S3 credentials or
// configuring multisite users through radosgw-admin.
type Config struct {
	Name, Realm, Zonegroup, Zone, Region string
	SkipUserCreation                     bool
	TLS                                  *TLSConfig
}

// removeGateway removes one owned gateway container. Zone configuration, S3 users,
// buckets and data remain available through other gateways or a replacement.
// Multisite period endpoints are not changed automatically. The last gateway
// can be removed because RGW is optional. Failed cleanup remains tracked.
func removeGateway(ctx context.Context, c *ceph.Container, handle *Gateway) error {
	name := handle.GatewayName
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return err
	}
	defer servicehook.Unlock(c)
	if servicehook.Closed(c) {
		return servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	if name == "" {
		name = "default"
	}
	gateway, ok := gateways(c).byName[name]
	if !ok || gateway != handle || gateway.Container == nil {
		return servicehook.Invalid(fmt.Errorf("RGW gateway %q is not owned by this cluster, or this descriptor is stale", name))
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	if err := gateway.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
		return fmt.Errorf("remove RGW gateway %s: %w", name, err)
	}
	servicehook.DeleteService(c, rgwServiceName(Config{Name: name}))
	delete(gateways(c).byName, name)
	return nil
}

// startGateway starts an independently owned, named gateway. Customizers
// are applied last; preserve the required command, network and listener settings.
// A non-nil result returned with an error remains owned by the cluster for cleanup.
func startGateway(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Gateway, error) {
	return startGatewayOn(ctx, c, config, nil, opts...)
}

// startGatewayOn is startGateway on the host port that reserved holds, when
// it is not nil: one attempt, without moving to another port on a conflict,
// because the caller has already published that port.
func startGatewayOn(ctx context.Context, c *ceph.Container, config Config, reserved *hostPortReservation, opts ...testcontainers.ContainerCustomizer) (*Gateway, error) {
	facts, err := ceph.FactsOf(c)
	if err != nil {
		return nil, err
	}
	if facts.Images.RGW == "" {
		return nil, servicehook.Invalid(fmt.Errorf("rgw image is required before gateway setup"))
	}
	config, err = normalizeRGWConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	ctr, port, tlsPort, err := startNamedDaemon(ctx, c, facts, config, reserved, opts...)
	var rgw *Gateway
	if ctr != nil {
		rgw = &Gateway{Container: ctr, GatewayName: config.Name, Region: config.Region, port: port, tlsPort: tlsPort, publicAddress: facts.PublicAddress, networkName: facts.NetworkName, owner: c, config: config}
		// Publish after credential initialization, including partial error
		// returns. Gateways readers then see an immutable descriptor. The
		// generic service already owns the container during initialization.
		defer func() {
			servicehook.Lock(c)
			defer servicehook.Unlock(c)
			owned := servicehook.Service(c, rgwServiceName(config))
			if owned == nil || owned.GetContainerID() != ctr.GetContainerID() {
				return // Concurrent cluster cleanup already removed this service.
			}
			gateways(c).byName[config.Name] = rgw
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
	user, err := servicehook.Command(ctx, ctr, args...)
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

func normalizeRGWConfig(config Config) (Config, error) {
	if config.Name == "" {
		config.Name = "default"
	}
	if !naming.DaemonName.MatchString(config.Name) {
		return Config{}, servicehook.Invalid(errors.New("RGW name must use letters, digits, dots, underscores or hyphens"))
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	for _, value := range []string{config.Realm, config.Zonegroup, config.Zone, config.Region} {
		if strings.TrimSpace(value) != value || strings.HasPrefix(value, "-") || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return Config{}, servicehook.Invalid(errors.New("invalid RGW realm, zonegroup, zone or region"))
		}
	}
	if config.TLS != nil {
		var err error
		config.TLS, err = normalizeRGWTLSConfig(config.TLS)
		if err != nil {
			return Config{}, err
		}
	}
	return config, nil
}

func rgwServiceName(config Config) string {
	if config.Name == "default" {
		return "rgw"
	}
	return "rgw:" + config.Name
}

func namedDaemonOptions(c *ceph.Container, facts ceph.ClusterFacts, port, tlsPort int, config Config) []testcontainers.ContainerCustomizer {
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/bin/sh", "/tc/rgw.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithFiles(servicehook.ScriptFile("rgw")),
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_PORT": strconv.Itoa(port), "CEPH_RGW_REALM": config.Realm, "CEPH_RGW_ZONEGROUP": config.Zonegroup, "CEPH_RGW_ZONE": config.Zone}),
	}
	if config.TLS != nil {
		opts = append(opts, rgwTLSOptions(tlsPort, config.TLS)...)
	}
	if !facts.UsesHostNetwork {
		publicNetwork := facts.NetworkName
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
				WithStartupTimeout(servicehook.StartupTimeout(c))))
		if config.TLS != nil {
			opts = append(opts, testcontainers.WithAdditionalWaitStrategy(tlsReadiness(c, facts, tlsPort, config.TLS)))
		}
		return opts
	}
	endpoint := net.JoinHostPort(facts.PublicAddress, strconv.Itoa(port))
	// The control-plane executor (essential image) supplies Python for the HTTP
	// probe; the gateway image needs only the shell and core utilities already
	// used by its bootstrap scripts.
	opts = append(opts,
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_ENDPOINT": endpoint}),
		testcontainers.WithWaitStrategy(hostReadiness(c, port, endpoint)),
	)
	if config.TLS != nil {
		opts = append(opts, testcontainers.WithEnv(map[string]string{"CEPH_RGW_TLS_ENDPOINT": net.JoinHostPort(facts.PublicAddress, strconv.Itoa(tlsPort))}),
			testcontainers.WithAdditionalWaitStrategy(tlsReadiness(c, facts, tlsPort, config.TLS)))
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

func hostReadiness(c *ceph.Container, port int, endpoint string) *wait.NopStrategy {
	return wait.ForNop(func(ctx context.Context, target wait.StrategyTarget) error {
		ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
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
				_, lastHTTPError = servicehook.Command(ctx, servicehook.CLI(c), "python3", "-c", rgwHTTPReady, "http://"+endpoint+"/")
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
	}).WithStartupTimeout(servicehook.StartupTimeout(c))
}

func startNamedDaemon(ctx context.Context, c *ceph.Container, facts ceph.ClusterFacts, config Config, reserved *hostPortReservation, customizers ...testcontainers.ContainerCustomizer) (testcontainers.Container, int, int, error) {
	serviceName := rgwServiceName(config)
	if !facts.UsesHostNetwork {
		if reserved != nil {
			return nil, 0, 0, servicehook.Invalid(errors.New("a reserved RGW host port needs a host-network cluster"))
		}
		tlsPort := 0
		if config.TLS != nil {
			tlsPort = 7481
		}
		opts := append(namedDaemonOptions(c, facts, 7480, tlsPort, config), customizers...)
		ctr, err := servicehook.StartService(ctx, c, serviceName, servicehook.ServiceImage(c, "rgw"), opts...)
		return ctr, 7480, tlsPort, err
	}
	if reserved != nil {
		if config.TLS != nil {
			return nil, 0, 0, servicehook.Invalid(errors.New("a reserved RGW host port serves plain HTTP only"))
		}
		ports, release, err := reserved.take()
		if err != nil {
			return nil, 0, 0, err
		}
		ctr, port, tlsPort, _, err := startHostAttempt(ctx, c, facts, config, ports, release, customizers...)
		return ctr, port, tlsPort, err
	}
	for attempt := 0; attempt < servicehook.HostPortAttempts(); attempt++ {
		count := 1
		if config.TLS != nil {
			count = 2
		}
		ports, release, err := servicehook.ReserveHostPorts(ctx, c, count)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("reserve RGW host port: %w", err)
		}
		ctr, port, tlsPort, conflict, err := startHostAttempt(ctx, c, facts, config, ports, release, customizers...)
		if err == nil || !conflict || attempt+1 == servicehook.HostPortAttempts() || ctx.Err() != nil {
			return ctr, port, tlsPort, err
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		cleanupErr := discardNamedAttempt(cleanupCtx, c, serviceName, ctr)
		cancel()
		if cleanupErr != nil {
			return ctr, port, tlsPort, errors.Join(err, cleanupErr)
		}
	}
	return nil, 0, 0, errors.New("RGW host port attempts exhausted")
}

// startHostAttempt creates a host gateway on ports while their reservation
// still holds them, releases them immediately before Start, minimizing the
// unavoidable bind race, and starts it. conflict reports a Start that failed
// because another process took the port.
func startHostAttempt(ctx context.Context, c *ceph.Container, facts ceph.ClusterFacts, config Config, ports []int, release func(context.Context) error, customizers ...testcontainers.ContainerCustomizer) (testcontainers.Container, int, int, bool, error) {
	port := ports[0]
	tlsPort := 0
	if config.TLS != nil {
		tlsPort = ports[1]
	}
	opts := append(namedDaemonOptions(c, facts, port, tlsPort, config), customizers...)
	opts = append(opts, testcontainers.WithNoStart())
	ctr, createErr := servicehook.StartService(ctx, c, rgwServiceName(config), servicehook.ServiceImage(c, "rgw"), opts...)
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	releaseErr := release(cleanupCtx)
	cancel()
	if createErr != nil || releaseErr != nil {
		return ctr, port, tlsPort, false, errors.Join(createErr, releaseErr)
	}
	if err := ctr.Start(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		conflict := servicehook.IsPortConflict(cleanupCtx, ctr)
		cancel()
		return ctr, port, tlsPort, conflict, fmt.Errorf("start RGW on host port %d: %w", port, err)
	}
	return ctr, port, tlsPort, false, nil
}

// hostPortReservation holds one host port of a host-network cluster for a
// gateway that starts later, so that a multisite zone can commit the
// gateway's endpoint before the gateway first serves a period. It is used
// once: by the start that takes it, or by releaseUnused.
type hostPortReservation struct {
	mu      sync.Mutex
	ports   []int
	release func(context.Context) error
	used    bool
}

// reserveGatewayPort reserves a host port for a plain-HTTP gateway of c and
// returns the peer endpoint that DaemonEndpoint returns once a gateway runs
// on it.
func reserveGatewayPort(ctx context.Context, c *ceph.Container) (*hostPortReservation, string, error) {
	facts, err := ceph.FactsOf(c)
	if err != nil {
		return nil, "", err
	}
	if !facts.UsesHostNetwork {
		return nil, "", servicehook.Invalid(errors.New("an RGW host port reservation needs a host-network cluster"))
	}
	ports, release, err := servicehook.ReserveHostPorts(ctx, c, 1)
	if err != nil {
		return nil, "", fmt.Errorf("reserve RGW host port: %w", err)
	}
	endpoint := "http://" + net.JoinHostPort(facts.PublicAddress, strconv.Itoa(ports[0]))
	return &hostPortReservation{ports: ports, release: release}, endpoint, nil
}

func (r *hostPortReservation) take() ([]int, func(context.Context) error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used {
		return nil, nil, servicehook.Invalid(errors.New("the RGW host port reservation was already used"))
	}
	r.used = true
	return r.ports, r.release, nil
}

// releaseUnused frees a reservation that no gateway took; after a start took it,
// it does nothing.
func (r *hostPortReservation) releaseUnused(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used {
		return nil
	}
	r.used = true
	return r.release(ctx)
}

func discardAttempt(ctx context.Context, c *ceph.Container, ctr testcontainers.Container) error {
	return discardNamedAttempt(ctx, c, "rgw", ctr)
}

func discardNamedAttempt(ctx context.Context, c *ceph.Container, serviceName string, ctr testcontainers.Container) error {
	if err := ctr.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
		// Retain the partial service so cluster Terminate can retry cleanup.
		return fmt.Errorf("terminate conflicted RGW: %w", err)
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if owned := servicehook.Service(c, serviceName); owned != nil && owned.GetContainerID() == ctr.GetContainerID() {
		servicehook.DeleteService(c, serviceName)
	}
	return nil
}

// DaemonEndpoint returns the HTTP address used by peers in the gateway's Docker
// network or daemon-host namespace. Host mode returns the advertised address;
// bridge mode returns the gateway's IP in its Ceph cluster network. The caller
// must share or reach that network. Use S3Endpoint for host application clients.
//
// On a ceph.AddressDualStack cluster it returns the gateway's IPv4 address; the
// gateway listens on 0.0.0.0:7480 and [::]:7480, so clients in the cluster
// network reach it over its IPv6 address on port 7480 too.
//
//purpose:handoff
func (c *Gateway) DaemonEndpoint(ctx context.Context) (string, error) {
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
//
// Sign with Gateway.Region. In bridge mode the mapped port can change whenever
// the gateway container restarts (ApplyPlacement, ReloadPlacement, multisite
// sync group and zone changes, or the caller's own Stop and Start): call
// S3Endpoint again afterwards.
//
//purpose:handoff
func (c *Gateway) S3Endpoint(ctx context.Context) (string, error) {
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
