package dashboard

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	neturl "net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/moby/moby/api/types/network"
)

// TLSConfig is the certificate the Dashboard listener presents: a PEM
// certificate chain and its unencrypted RSA private key. The module verifies
// the pair with pyOpenSSL before it serves, and that check rejects other key
// types such as ECDSA, so Start refuses them. The fixture installs it
// for every manager of the cluster, so a manager that becomes active after a
// failover presents the same certificate. The SUT verifies it with its own
// trust anchor and the name in the certificate; the fixture never verifies a
// chain, it only checks that the listener presents this certificate.
type TLSConfig struct {
	CertificatePEM, PrivateKeyPEM []byte
}

// String describes the configuration without the certificate or key.
func (TLSConfig) String() string { return "Dashboard TLS certificate and private key" }

// GoString formats the configuration like String, so %#v does not print the
// key.
func (c TLSConfig) GoString() string { return c.String() }

// Format prints String for every verb and flag.
func (c TLSConfig) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, c.String()) }

// Config selects the listener certificate and the owned account of Start.
//
// Username names the account; empty generates tc-dashboard-<random>. An
// account of that name must not exist: Start never adopts one. Role is the
// one role of the account, a built-in role such as read-only (the default),
// block-manager or administrator, or a role that already exists; Start
// creates no role. The password is generated and satisfies the Dashboard's
// default password policy.
type Config struct {
	TLS      TLSConfig
	Username string
	Role     string
}

// Dashboard is the Ceph Dashboard that Start prepared on one cluster. Copies
// share its state. Its methods are safe for concurrent use. Formatting it
// with any verb prints only its account name, never the password.
type Dashboard struct {
	*state
}

// state is the shared state of a Dashboard.
type state struct {
	mu       sync.Mutex
	cluster  *ceph.Container
	registry *registry
	username string
	password string
	role     string
	// leaf is the fingerprint of the installed certificate.
	leaf string
	cert []byte
	key  []byte

	// Restoration state, in the order Restore undoes it.
	accountOwned bool
	certificate  *configKey
	privateKey   *configKey
	multicluster *multiclusterState
	module       *override.MGRModuleHandle
	settings     []*override.ConfigHandle
	link         *GatewayLink
	restored     bool
	attempted    bool
}

// String names the Dashboard by its account without the password.
func (d Dashboard) String() string {
	if d.state == nil {
		return "Ceph Dashboard"
	}
	return "Ceph Dashboard account " + d.username
}

// GoString formats the Dashboard like String.
func (d Dashboard) GoString() string { return d.String() }

// Format prints String for every verb and flag.
func (d Dashboard) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, d.String()) }

// registry is the per-cluster Dashboard state: the one active Dashboard and
// every secret the fixture created, which diagnostics reports mask.
type registry struct {
	mu      sync.Mutex
	active  *state
	secrets []string
}

const registryKind = "dashboard"

func registryOf(c *ceph.Container) *registry {
	return servicehook.Extension(c, registryKind, func() any { return &registry{} }).(*registry)
}

// Secrets returns the passwords and session tokens the fixture created, also
// of restored Dashboards.
func (r *registry) Secrets() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.secrets)
}

func (r *registry) addSecret(secret string) {
	if secret == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !slices.Contains(r.secrets, secret) {
		r.secrets = append(r.secrets, secret)
	}
}

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// Start enables the dashboard module of c, installs cfg.TLS as the
// certificate of every manager, creates the owned account and returns once
// the active manager's listener passed the readiness check of Endpoint.
//
// In a host-network cluster every manager present at Start gets its own free
// host port on the cluster's public address, because the managers share the
// host's ports; a manager added later uses the module's default port 8443.
// The ports are reserved and released just before the module binds them, so
// a port another process takes in between fails the readiness check. In
// bridge mode the listener is the manager's container address on the cluster
// network with port 8443, which containers on that network reach; a process
// on the Docker host reaches it through the host port that
// ceph.WithManagerPorts(8443) publishes, at the URL of HostEndpoint.
//
// The module's enabled state and the listener options are held through
// override handles, so a second Start on the same cluster, or an
// override.SetMGRModule of the dashboard module, is refused while a Dashboard
// is active. Start refuses a manager image without the dashboard module or
// with a dashboard module that cannot run, with ceph.ErrImageCapability and
// the module's native error, before any change.
//
// A Start that fails after its first change returns the Dashboard together
// with the error: its Restore undoes what Start changed. Errors never contain
// the password, the private key or a session token.
//
//purpose:construction,handoff
func Start(ctx context.Context, c *ceph.Container, cfg Config) (*Dashboard, error) {
	const op = "dashboard.Start"
	if c == nil {
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")}
	}
	cfg, leaf, err := normalizeConfig(cfg)
	if err != nil {
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reg := registryOf(c)
	s := &state{cluster: c, registry: reg, username: cfg.Username, role: cfg.Role, leaf: leaf,
		cert: slices.Clone(cfg.TLS.CertificatePEM), key: slices.Clone(cfg.TLS.PrivateKeyPEM)}
	reg.mu.Lock()
	if reg.active != nil {
		reg.mu.Unlock()
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster already has an active fixture Dashboard")}
	}
	reg.active = s
	reg.mu.Unlock()
	if s.password, err = generatePassword(); err != nil {
		s.release()
		return nil, err
	}
	reg.addSecret(s.password)
	d := &Dashboard{s}
	if err := s.start(ctx); err != nil {
		if !s.attempted {
			s.release()
			return nil, err
		}
		return d, fmt.Errorf("%s: %w", op, err)
	}
	return d, nil
}

func normalizeConfig(cfg Config) (Config, string, error) {
	pair, err := tls.X509KeyPair(cfg.TLS.CertificatePEM, cfg.TLS.PrivateKeyPEM)
	if err != nil || len(pair.Certificate) == 0 {
		return cfg, "", errors.New("dashboard TLS requires a valid PEM certificate chain and matching unencrypted private key")
	}
	if _, ok := pair.PrivateKey.(*rsa.PrivateKey); !ok {
		return cfg, "", errors.New("dashboard TLS requires an RSA private key; the module's certificate check rejects other key types")
	}
	if cfg.Username == "" {
		suffix := make([]byte, 4)
		if _, err := rand.Read(suffix); err != nil {
			return cfg, "", err
		}
		cfg.Username = "tc-dashboard-" + hex.EncodeToString(suffix)
	}
	if cfg.Role == "" {
		cfg.Role = "read-only"
	}
	if !namePattern.MatchString(cfg.Username) || !namePattern.MatchString(cfg.Role) {
		return cfg, "", errors.New("dashboard username and role must be lower-case names of letters, digits, '_', '.' and '-'")
	}
	return cfg, fingerprint(pair.Certificate[0]), nil
}

// release forgets s as the cluster's active Dashboard.
func (s *state) release() {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if s.registry.active == s {
		s.registry.active = nil
	}
}

func (s *state) start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(s.cluster))
	defer cancel()
	observer, err := ceph.Observe(s.cluster)
	if err != nil {
		return err
	}
	if err := checkModule(ctx, observer); err != nil {
		return err
	}
	facts, err := ceph.FactsOf(s.cluster)
	if err != nil {
		return err
	}
	// The account must not exist before anything changes. The module is
	// not loaded yet, so this is checked again once it is.
	s.attempted = true
	if facts.UsesHostNetwork {
		if err := s.hostListeners(ctx, observer, facts.PublicAddress); err != nil {
			return err
		}
	}
	if s.multicluster, err = readMulticluster(ctx, s.cluster); err != nil {
		return err
	}
	module, err := override.SetMGRModule(ctx, s.cluster, "dashboard", true)
	s.module = module
	if err != nil {
		return err
	}
	if err := waitCommands(ctx, s.cluster); err != nil {
		return err
	}
	if err := s.installCertificate(ctx); err != nil {
		return err
	}
	if err := s.createAccount(ctx); err != nil {
		return err
	}
	_, err = s.endpoint(ctx, nil)
	return err
}

// checkModule refuses a manager image without a runnable dashboard module.
func checkModule(ctx context.Context, observer *ceph.Observer) error {
	modules, err := observer.MGRModules(ctx)
	if err != nil {
		return err
	}
	for _, module := range modules {
		if module.Name != "dashboard" {
			continue
		}
		if !module.Available || !module.CanRun {
			return &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: "dashboard module", Target: "active manager", Capability: "dashboard",
				Err: fmt.Errorf("the dashboard module cannot run: %s", module.ErrorString)}
		}
		return nil
	}
	return &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: "dashboard module", Target: "active manager", Capability: "dashboard",
		Err: errors.New("the active manager's image does not ship the dashboard module; select an image with it, such as the official image, with ceph.ImageSet.MGR")}
}

// hostListeners gives every current manager its own reserved host port on
// address and makes the module advertise address.
func (s *state) hostListeners(ctx context.Context, observer *ceph.Observer, address string) (err error) {
	status, err := observer.ManagerStatus(ctx)
	if err != nil {
		return err
	}
	names := []string{}
	if status.ActiveName != "" {
		names = append(names, status.ActiveName)
	}
	for _, standby := range status.Standbys {
		names = append(names, standby.Name)
	}
	ports, release, err := servicehook.ReserveHostPorts(ctx, s.cluster, len(names))
	if err != nil {
		return fmt.Errorf("reserve dashboard host ports: %w", err)
	}
	// The reservation holds the ports until it is released, and the module
	// must bind them, so it is released before the module is enabled. Another
	// process can take a port in between; the listener then fails to bind and
	// the readiness check of Start fails.
	defer func() {
		if releaseErr := release(context.WithoutCancel(ctx)); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release dashboard host ports: %w", releaseErr))
		}
	}()
	settings := []ceph.ConfigSetting{{Section: "mgr", Name: "mgr/dashboard/server_addr", Value: address}}
	for i, name := range names {
		settings = append(settings, ceph.ConfigSetting{Section: "mgr", Name: "mgr/dashboard/" + name + "/ssl_server_port", Value: strconv.Itoa(ports[i])})
	}
	for _, setting := range settings {
		handle, setErr := override.SetConfig(ctx, s.cluster, setting)
		s.settings = append(s.settings, handle)
		if setErr != nil {
			return setErr
		}
	}
	return nil
}

// waitCommands waits until the loaded module accepts its commands; a module
// that is enabled but still loading refuses them with ENOTSUP.
func waitCommands(ctx context.Context, c *ceph.Container) error {
	var last error
	for {
		if _, last = c.Ceph(ctx, "dashboard", "get-jwt-token-ttl"); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

// installCertificate records the stored certificate and key and installs the
// configured ones for every manager.
func (s *state) installCertificate(ctx context.Context) error {
	var err error
	if s.certificate, err = readConfigKey(ctx, s.cluster, "mgr/dashboard/crt"); err != nil {
		return err
	}
	if s.privateKey, err = readConfigKey(ctx, s.cluster, "mgr/dashboard/key"); err != nil {
		return err
	}
	s.certificate.installed, s.privateKey.installed = s.cert, s.key
	if err := runWithInput(ctx, s.cluster, "install dashboard certificate", s.cert, "dashboard", "set-ssl-certificate", "-i", "-"); err != nil {
		return err
	}
	return runWithInput(ctx, s.cluster, "install dashboard private key", s.key, "dashboard", "set-ssl-certificate-key", "-i", "-")
}

// createAccount creates the owned account; an existing one is refused.
func (s *state) createAccount(ctx context.Context) error {
	if _, err := s.cluster.Ceph(ctx, "dashboard", "ac-user-show", s.username); err == nil {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "create dashboard account", Err: fmt.Errorf("dashboard account %s already exists", s.username)}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.accountOwned = true
	return runWithInput(ctx, s.cluster, "create dashboard account", []byte(s.password), "dashboard", "ac-user-create", s.username, "-i", "-", s.role)
}

// runWithInput runs a ceph CLI command in the control container with input as
// its standard input, so that secrets stay out of its arguments.
func runWithInput(ctx context.Context, c *ceph.Container, label string, input []byte, args ...string) error {
	stdin := ceph.NewNativeInput()
	go func() {
		_ = stdin.Write(ctx, input)
		_ = stdin.Close()
	}()
	result, err := c.RunNativeCommand(ctx, servicehook.CLI(c), ceph.NativeCommand{
		Argv: append([]string{"ceph", "--connect-timeout", "10"}, args...), Timeout: time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: label, Input: stdin, MaxInput: len(input),
	})
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return servicehook.NativeFailure("ceph", result.ExitCode, nil, result.Stderr)
	}
	return nil
}

// Credentials returns the owned account's name and password for the SUT.
//
//purpose:handoff
func (d *Dashboard) Credentials() (username, password string) {
	if d == nil || d.state == nil {
		return "", ""
	}
	return d.username, d.password
}

// Endpoint returns the HTTPS URL of the active manager's Dashboard, as the
// MGR map advertises it, once that listener passed the readiness check: it
// presents the installed certificate on every connection of the check, it
// refuses the account with a wrong password, it accepts the account's
// password, and GET /api/summary with the session token is answered by the
// manager that the MGR map names active. The check compares the certificate
// of each connection before it sends a password or the session token, and
// sends nothing more after a mismatch. A standby answers with a redirect to
// the active manager, which the check refuses, so the call waits, within the
// cluster's startup timeout, until a failover settles.
//
// The URL is the one the MGR map advertises: in bridge mode the active
// manager's address on the cluster network, in host mode the cluster's
// public address and the manager's own port. A failover changes it; call
// Endpoint again after one. The check runs from the control container. A
// process on the Docker host of a bridge-mode cluster uses HostEndpoint.
//
//purpose:observation,handoff
func (d *Dashboard) Endpoint(ctx context.Context) (string, error) {
	if d == nil || d.state == nil {
		return "", errors.New("dashboard is unavailable")
	}
	if err := d.ready(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(d.cluster))
	defer cancel()
	return d.endpoint(ctx, nil)
}

func (s *state) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restored {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "dashboard", Err: errors.New("the dashboard was restored")}
	}
	return nil
}

// endpoint polls the advertised URL until it passes the readiness check,
// with paths requested too, and returns the URL and the last probe report.
func (s *state) endpoint(ctx context.Context, report *probeReport, paths ...string) (string, error) {
	observer, err := ceph.Observe(s.cluster)
	if err != nil {
		return "", err
	}
	var last error
	for {
		listener, current, err := s.probeActive(ctx, observer, paths...)
		if err == nil {
			if report != nil {
				*report = current
			}
			return listener.url, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return "", errors.Join(ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

// activeListener is the listener that passed a readiness check: the URL the
// MGR map advertised and the active manager it named during the check.
type activeListener struct {
	url     string
	manager string
	gid     uint64
}

// probeActive probes the URL the MGR map advertises for the active manager.
func (s *state) probeActive(ctx context.Context, observer *ceph.Observer, paths ...string) (activeListener, probeReport, error) {
	status, err := observer.ManagerStatus(ctx)
	if err != nil {
		return activeListener{}, probeReport{}, err
	}
	if !status.Available || status.ActiveName == "" {
		return activeListener{}, probeReport{}, errors.New("no active manager")
	}
	services, err := observer.ManagerServices(ctx)
	if err != nil {
		return activeListener{}, probeReport{}, err
	}
	url, ok := services["dashboard"]
	if !ok {
		return activeListener{}, probeReport{}, errors.New("the MGR map advertises no dashboard service")
	}
	if !strings.HasPrefix(url, "https://") {
		return activeListener{}, probeReport{}, fmt.Errorf("the MGR map advertises a dashboard service that is not HTTPS: %s", url)
	}
	report, err := runProbe(ctx, s.cluster, url, s.leaf, s.username, s.password, append([]string{summaryPath}, paths...)...)
	if err != nil {
		return activeListener{}, probeReport{}, err
	}
	s.registry.addSecret(report.Token)
	if err := checkReadiness(report, status.ActiveName); err != nil {
		return activeListener{}, report, fmt.Errorf("%s: %w", url, err)
	}
	// The MGR map is read again: an active manager that changed during the
	// probe was not the one checked.
	listener := activeListener{url: url, manager: status.ActiveName, gid: status.ActiveGID}
	if err := sameActive(ctx, observer, listener); err != nil {
		return activeListener{}, report, err
	}
	return listener, report, nil
}

// sameActive fails when the MGR map no longer names the manager of listener
// active.
func sameActive(ctx context.Context, observer *ceph.Observer, listener activeListener) error {
	again, err := observer.ManagerStatus(ctx)
	if err != nil {
		return err
	}
	if again.ActiveName != listener.manager || again.ActiveGID != listener.gid {
		return errors.New("the active manager changed during the readiness check")
	}
	return nil
}

// HostEndpoint returns the HTTPS URL at which a process on the Docker host,
// such as the test process or a SUT that runs there, reaches the active
// manager's Dashboard. It runs the readiness check of Endpoint first, then
// connects from the calling process to the returned URL and requires the
// listener to present the installed certificate; that connection sends no
// request.
//
// In bridge mode the URL is the Docker host and the host port that Docker
// mapped to the advertised port of the active manager's container, so the
// cluster must have been started with ceph.WithManagerPorts(8443), the
// module's port; without it HostEndpoint fails with ceph.ErrInvalidRequest.
// Every manager publishes its own host port, so a failover changes the URL:
// call HostEndpoint again after one. A standby's redirect names the active
// manager's container address, which the host may not reach, so the SUT
// follows the new HostEndpoint instead of the redirect. The host name differs
// from the advertised one; the SUT verifies the certificate's name, as with
// Endpoint, for example with the TLS server name. In host mode the URL is the
// one Endpoint returns.
//
//purpose:observation,handoff
func (d *Dashboard) HostEndpoint(ctx context.Context) (string, error) {
	if d == nil || d.state == nil {
		return "", errors.New("dashboard is unavailable")
	}
	if err := d.ready(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(d.cluster))
	defer cancel()
	facts, err := ceph.FactsOf(d.cluster)
	if err != nil {
		return "", err
	}
	observer, err := ceph.Observe(d.cluster)
	if err != nil {
		return "", err
	}
	var last error
	for {
		target, err := d.probeHost(ctx, observer, facts.UsesHostNetwork)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) {
			return target, err
		}
		last = err
		select {
		case <-ctx.Done():
			return "", errors.Join(ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

// probeHost runs the readiness check, translates the advertised URL to the
// active manager's published host port in bridge mode, and checks from the
// calling process that the URL presents the installed certificate.
func (s *state) probeHost(ctx context.Context, observer *ceph.Observer, hostNetwork bool) (string, error) {
	listener, _, err := s.probeActive(ctx, observer)
	if err != nil {
		return "", err
	}
	target := listener.url
	if !hostNetwork {
		if target, err = s.publishedURL(ctx, listener); err != nil {
			return "", err
		}
	}
	if err := presentsCertificate(ctx, target, s.leaf); err != nil {
		return "", fmt.Errorf("%s: %w", target, err)
	}
	if err := sameActive(ctx, observer, listener); err != nil {
		return "", err
	}
	return target, nil
}

// publishedURL is listener's URL with the Docker host and the host port of
// the active manager's container.
func (s *state) publishedURL(ctx context.Context, listener activeListener) (string, error) {
	managers, err := s.cluster.Managers(ctx)
	if err != nil {
		return "", err
	}
	for _, manager := range managers {
		if manager.DaemonName != listener.manager {
			continue
		}
		inspect, err := manager.Inspect(ctx)
		if err != nil {
			return "", fmt.Errorf("inspect mgr.%s: %w", manager.DaemonName, err)
		}
		host, err := manager.Host(ctx)
		if err != nil {
			return "", err
		}
		var ports network.PortMap
		if inspect.NetworkSettings != nil {
			ports = inspect.NetworkSettings.Ports
		}
		return hostURL(listener.url, host, ports)
	}
	return "", &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: hostEndpointOp,
		Err: fmt.Errorf("the active manager mgr.%s is not a manager of the cluster's fixture, so its published ports are unknown", listener.manager)}
}

const hostEndpointOp = "dashboard host endpoint"

// hostURL replaces the host and port of the advertised URL with host and the
// host port that ports binds to the advertised TCP port.
func hostURL(advertised, host string, ports network.PortMap) (string, error) {
	parsed, err := neturl.Parse(advertised)
	if err != nil || parsed.Port() == "" {
		return "", fmt.Errorf("the advertised dashboard URL %q names no port", advertised)
	}
	port, err := network.ParsePort(parsed.Port() + "/tcp")
	if err != nil {
		return "", fmt.Errorf("the advertised dashboard URL %q: %w", advertised, err)
	}
	for _, binding := range ports[port] {
		if binding.HostPort != "" {
			parsed.Host = net.JoinHostPort(host, binding.HostPort)
			return parsed.String(), nil
		}
	}
	return "", &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: hostEndpointOp,
		Err: fmt.Errorf("the active manager publishes no host port for %s; start the cluster with ceph.WithManagerPorts(%d)", port, port.Num())}
}

// presentsCertificate connects to the HTTPS URL target and requires the
// listener to present the certificate whose SHA-256 fingerprint is want. It
// sends no request. The chain is not verified, for the reason given at
// probeScript.
func presentsCertificate(ctx context.Context, target, want string) error {
	parsed, err := neturl.Parse(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	dialer := &tls.Dialer{Config: &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || fingerprint(state.PeerCertificates[0].Raw) != want {
				return errors.New("the listener presents another certificate than the installed one")
			}
			return nil
		},
	}}
	conn, err := dialer.DialContext(ctx, "tcp", parsed.Host)
	if err != nil {
		return err
	}
	return conn.Close()
}

// Restore removes the gateway link of AddGateway, if any, deletes the owned
// account, puts the certificate and key that were stored before Start back
// (or removes them), removes the module's multi-cluster record when only the
// owned account wrote it, restores the module's enabled state and the
// listener options.
//
// It touches only what Start changed. It refuses, and leaves as it is, a
// certificate or key that was changed after Start and a listener option or
// module state that the override handles find changed; it then continues with
// the other steps and returns the joined errors, and a later Restore retries
// what failed. When it disables the module it returns once the MGR map no
// longer advertises the dashboard service. A deleted account is not an
// error. The module's own records, such as its access database and
// session-token signing key, stay. Restore is idempotent.
//
//purpose:construction,handoff,lifetime
func (d *Dashboard) Restore(ctx context.Context) error {
	if d == nil || d.state == nil {
		return errors.New("dashboard is unavailable")
	}
	return d.restore(ctx)
}

func (s *state) restore(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restored {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(s.cluster))
	defer cancel()
	var errs []error
	if s.link != nil {
		if err := s.link.remove(ctx); err != nil {
			errs = append(errs, err)
		} else {
			s.link = nil
		}
	}
	// The dashboard commands need the module, so they run before the module
	// is restored.
	if s.accountOwned {
		if err := s.deleteAccount(ctx); err != nil {
			errs = append(errs, err)
		} else {
			s.accountOwned = false
		}
	}
	if s.multicluster != nil {
		if err := s.multicluster.restore(ctx, s.cluster, s.username); err != nil {
			errs = append(errs, err)
		} else {
			s.multicluster = nil
		}
	}
	for _, key := range []**configKey{&s.certificate, &s.privateKey} {
		if *key == nil {
			continue
		}
		if err := (*key).restore(ctx, s.cluster); err != nil {
			errs = append(errs, err)
		} else {
			*key = nil
		}
	}
	if len(errs) == 0 && s.module != nil {
		if err := restoreModule(ctx, s.module); err != nil {
			errs = append(errs, err)
		} else if err := waitServiceWithdrawn(ctx, s.cluster); err != nil {
			errs = append(errs, err)
		} else {
			s.module = nil
		}
	}
	for i := len(s.settings) - 1; i >= 0; i-- {
		if err := s.settings[i].Restore(ctx); err != nil {
			errs = append(errs, err)
		} else {
			s.settings = slices.Delete(s.settings, i, i+1)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("restore dashboard: %w", errors.Join(errs...))
	}
	s.restored = true
	s.release()
	return nil
}

// restoreModule restores the module's enabled state. Disabling or enabling a
// module makes every manager respawn, and the handle's readback fails while
// no manager is available, so a failure other than a refusal is retried
// until ctx ends; the handle reconciles an earlier uncertain restoration.
func restoreModule(ctx context.Context, module *override.MGRModuleHandle) error {
	for {
		err := module.Restore(ctx)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(2 * time.Second):
		}
	}
}

// waitServiceWithdrawn waits, when the restored module is disabled, until
// the MGR map no longer advertises the dashboard service. The map keeps the
// service of a disabled module until the respawned active manager reports
// again, which takes seconds on Squid.
func waitServiceWithdrawn(ctx context.Context, c *ceph.Container) error {
	observer, err := ceph.Observe(c)
	if err != nil {
		return err
	}
	var last error
	for {
		modules, err := observer.MGRModules(ctx)
		if err == nil {
			enabled := false
			for _, module := range modules {
				if module.Name == "dashboard" {
					enabled = module.Enabled
				}
			}
			if enabled {
				return nil
			}
			var services map[string]string
			if services, err = observer.ManagerServices(ctx); err == nil {
				if _, found := services["dashboard"]; !found {
					return nil
				}
				err = errors.New("the MGR map still advertises the dashboard service")
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *state) deleteAccount(ctx context.Context) error {
	if _, err := s.cluster.Ceph(ctx, "dashboard", "ac-user-delete", s.username); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete dashboard account %s: %w", s.username, err)
	}
	return nil
}

// configKey is one config-key entry that Start replaced: its prior value and
// whether it existed, and the value Start installed.
type configKey struct {
	name     string
	existed  bool
	previous []byte
	// installed is the value Start wrote, nil before it tried.
	installed []byte
}

func readConfigKey(ctx context.Context, c *ceph.Container, name string) (*configKey, error) {
	value, err := c.Ceph(ctx, "config-key", "get", name)
	if errors.Is(err, fs.ErrNotExist) {
		return &configKey{name: name}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return &configKey{name: name, existed: true, previous: value}, nil
}

// restore puts the prior entry back, or removes an entry that did not exist
// before Start, when the stored entry is still the one Start installed.
func (k *configKey) restore(ctx context.Context, c *ceph.Container) error {
	if k.installed == nil {
		return nil
	}
	current, err := c.Ceph(ctx, "config-key", "get", k.name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "restore " + k.name, Err: errors.New("removed outside the dashboard fixture; refusing restoration")}
	case err != nil:
		return fmt.Errorf("read %s: %w", k.name, err)
	}
	if !bytes.Equal(bytes.TrimSpace(current), bytes.TrimSpace(k.installed)) {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "restore " + k.name, Err: errors.New("changed outside the dashboard fixture; refusing restoration")}
	}
	if !k.existed {
		_, err = c.Ceph(ctx, "config-key", "rm", k.name)
	} else {
		err = runWithInput(ctx, c, "restore "+k.name, k.previous, "config-key", "set", k.name, "-i", "-")
	}
	if err != nil {
		return fmt.Errorf("restore %s: %w", k.name, err)
	}
	return nil
}

// multiclusterState is the module's multi-cluster record before Start. The
// module writes it, with the session token, at every login; Restore removes a
// record that did not exist before Start and that names no account but the
// owned one.
type multiclusterState struct {
	existed bool
}

const multiclusterOption = "mgr/dashboard/MULTICLUSTER_CONFIG"

func readMulticluster(ctx context.Context, c *ceph.Container) (*multiclusterState, error) {
	_, found, err := multiclusterValue(ctx, c)
	if err != nil {
		return nil, err
	}
	return &multiclusterState{existed: found}, nil
}

func multiclusterValue(ctx context.Context, c *ceph.Container) (string, bool, error) {
	return managerOption(ctx, c, multiclusterOption)
}

// managerOption reads one option of the mgr section from the central
// configuration, and whether it is stored.
func managerOption(ctx context.Context, c *ceph.Container, name string) (string, bool, error) {
	data, err := c.Ceph(ctx, "config", "dump", "--format", "json")
	if err != nil {
		return "", false, err
	}
	var entries []struct {
		Section string `json:"section"`
		Name    string `json:"name"`
		Value   string `json:"value"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return "", false, errors.New("decode config dump")
	}
	for _, entry := range entries {
		if entry.Section == "mgr" && entry.Name == name {
			return entry.Value, true, nil
		}
	}
	return "", false, nil
}

func (m *multiclusterState) restore(ctx context.Context, c *ceph.Container, username string) error {
	if m.existed {
		return nil
	}
	value, found, err := multiclusterValue(ctx, c)
	if err != nil || !found {
		return err
	}
	if !onlyUser(value, username) {
		return nil
	}
	if _, err := c.Ceph(ctx, "config", "rm", "mgr", multiclusterOption); err != nil {
		return fmt.Errorf("remove the dashboard multi-cluster record: %w", err)
	}
	return nil
}

// onlyUser reports whether every account that the multi-cluster record names
// is username.
func onlyUser(value, username string) bool {
	var record struct {
		CurrentUser *string `json:"current_user"`
		Config      map[string][]struct {
			User *string `json:"user"`
		} `json:"config"`
	}
	if json.Unmarshal([]byte(value), &record) != nil {
		return false
	}
	if record.CurrentUser != nil && *record.CurrentUser != username {
		return false
	}
	for _, clusters := range record.Config {
		for _, cluster := range clusters {
			if cluster.User != nil && *cluster.User != username {
				return false
			}
		}
	}
	return true
}

// passwordAlphabets avoid vowels and the letters of the words that the
// default password policy excludes, such as osd, rbd and nfs.
var passwordAlphabets = []string{"BCDGHJKMPQTVWXZ", "bcdghjkmpqtvwxz", "23456789", "-_"}

// generatePassword returns a 24-character password that cycles through the
// character classes, so it has every class and no repeated or sequential
// neighbours, which the default policy accepts.
func generatePassword() (string, error) {
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	password := make([]byte, 0, len(random))
	for i, b := range random {
		alphabet := passwordAlphabets[i%len(passwordAlphabets)]
		password = append(password, alphabet[int(b)%len(alphabet)])
	}
	return string(password), nil
}
