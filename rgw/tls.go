package rgw

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TLSConfig enables a native Beast HTTPS listener alongside HTTP. Supply
// a PEM certificate chain and matching unencrypted private key. No proxy is
// involved. Clients must explicitly trust its CA and use a matching hostname.
// Files live only in the owned RGW container; descriptors never log key bytes.
//
// The bytes are copied; the certificate and key are written to /tc/rgw-tls.pem
// with mode 0600 inside the gateway container only. In bridge mode container
// ports 7480 (HTTP) and 7481 (HTTPS) are mapped to random host ports; in host
// mode two free host ports are reserved. Startup verifies that the gateway
// process owns the HTTPS socket and that the listener presents the configured
// certificate. ceph.NetworkConfig.WithMessengerMode does not enable HTTPS.
// Gateway.ReplaceTLS replaces the material of a running gateway.
type TLSConfig struct {
	CertificatePEM, PrivateKeyPEM []byte
}

// String describes the configuration without the certificate or key.
func (TLSConfig) String() string { return "RGW TLS certificate and private key" }

// GoString formats the configuration like String, so %#v does not print the
// key.
func (c TLSConfig) GoString() string { return c.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (c TLSConfig) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, c.String()) }

// String names the gateway without its S3 credentials or TLS key.
func (g Gateway) String() string { return "RGW gateway " + g.config.Name }

// GoString formats the gateway like String, so %#v does not print its
// credentials.
func (g Gateway) GoString() string { return g.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (g Gateway) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, g.String()) }

func normalizeRGWTLSConfig(config *TLSConfig) (*TLSConfig, error) {
	if config == nil {
		return nil, nil
	}
	if _, err := tls.X509KeyPair(config.CertificatePEM, config.PrivateKeyPEM); err != nil {
		return nil, servicehook.Invalid(errors.New("RGW TLS requires a valid PEM certificate chain and matching unencrypted private key"))
	}
	return &TLSConfig{CertificatePEM: slices.Clone(config.CertificatePEM), PrivateKeyPEM: slices.Clone(config.PrivateKeyPEM)}, nil
}

// rgwTLSFile is the combined certificate chain and private key that the
// native Beast frontend loads when the gateway starts.
const rgwTLSFile = "/tc/rgw-tls.pem"

// rgwTLSFileContent is the content of rgwTLSFile for config.
func rgwTLSFileContent(config *TLSConfig) []byte {
	pem := append(slices.Clone(config.CertificatePEM), '\n')
	return append(pem, config.PrivateKeyPEM...)
}

func rgwTLSOptions(port int, config *TLSConfig) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_TLS_PORT": strconv.Itoa(port)}),
		testcontainers.WithFiles(testcontainers.ContainerFile{Reader: bytes.NewReader(rgwTLSFileContent(config)), ContainerFilePath: rgwTLSFile, FileMode: 0o600}),
	}
}

// tlsListener is the certificate material that one gateway's native HTTPS
// listener is expected to serve. Copies of the Gateway share it, and the
// readiness check of every start of the gateway container, including the
// restarts of placement, multisite and the caller, reads it then.
type tlsListener struct {
	mu     sync.Mutex
	config *TLSConfig
}

// newTLSListener returns the listener state of normalized config; nil for a
// gateway without TLS.
func newTLSListener(config *TLSConfig) *tlsListener {
	if config == nil {
		return nil
	}
	return &tlsListener{config: config}
}

func (l *tlsListener) material() *TLSConfig {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.config
}

func (l *tlsListener) set(config *TLSConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.config = config
}

// fingerprint is the hex SHA-256 of the expected leaf certificate.
func (l *tlsListener) fingerprint() string {
	config := l.material()
	pair, _ := tls.X509KeyPair(config.CertificatePEM, config.PrivateKeyPEM)
	digest := sha256.Sum256(pair.Certificate[0])
	return hex.EncodeToString(digest[:])
}

// ReplaceTLS replaces the certificate chain and private key that this
// gateway's native HTTPS listener serves, for a test of how the SUT meets a
// renewed certificate or a changed CA. Beast loads them only when the gateway
// starts, so ReplaceTLS installs them in the gateway container and restarts
// it: the gateway refuses connections while it restarts, connections
// established before are closed rather than moved to the new certificate,
// and requests in flight can fail with an unknown outcome. Only connections
// made afterwards see the new certificate. The restart completes when the
// listener presents the new leaf certificate, and every later start of the
// container expects it too.
//
// In bridge mode the restarted gateway can get new mapped host ports: call
// S3SecureEndpoint and S3Endpoint again afterwards. In host mode it keeps its
// selected ports. The gateway must be running, and config must hold a valid
// PEM chain and its unencrypted private key; the bytes are copied. Trust is
// the SUT's own: ReplaceTLS changes no client configuration, so a test
// supplies the new CA to the SUT the way it reads its configuration. When the
// gateway does not stop, the previous material is put back; when it stopped
// and fails to start, the error is returned with the new material installed
// and no rollback, which would need another restart.
//
//purpose:condition
func (g *Gateway) ReplaceTLS(ctx context.Context, config TLSConfig) error {
	cluster := g.base().Cluster()
	if cluster == nil {
		return errNilCluster
	}
	next, err := normalizeRGWTLSConfig(&config)
	if err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(cluster)
	if servicehook.Closed(cluster) {
		return servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	owned, ok := gateways(cluster).byName[g.GatewayName]
	if !ok || owned == nil || owned.tls == nil || owned.tls != g.tls || owned.Container == nil {
		if g.tls == nil {
			return servicehook.Invalid(errors.New("RGW native TLS listener is not configured"))
		}
		return servicehook.Invalid(fmt.Errorf("RGW gateway %q is not owned by this cluster, or this descriptor is stale", g.GatewayName))
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(cluster))
	defer cancel()
	return replaceTLS(ctx, owned.Container, owned.tls, next)
}

// replaceTLS installs next in the running gateway container ctr, whose
// expected material listener holds, and restarts it.
func replaceTLS(ctx context.Context, ctr testcontainers.Container, listener *tlsListener, next *TLSConfig) error {
	state, err := ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect RGW before replacing its TLS certificate: %w", err)
	}
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return servicehook.Invalid(errors.New("RGW gateway is not running; its TLS certificate is replaced only in a running gateway"))
	}
	previous := listener.material()
	if err := installTLSFile(ctx, ctr, next); err != nil {
		return err
	}
	listener.set(next)
	grace := 3 * time.Second
	if err := ctr.Stop(ctx, &grace); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		restoreErr := installTLSFile(cleanupCtx, ctr, previous)
		if restoreErr == nil {
			listener.set(previous)
		}
		return errors.Join(fmt.Errorf("stop RGW to replace its TLS certificate: %w", err), restoreErr)
	}
	if err := ctr.Start(ctx); err != nil {
		return fmt.Errorf("restart RGW with the replaced TLS certificate, which stays installed: %w", err)
	}
	return nil
}

// installTLSFile writes the material of config beside rgwTLSFile with mode
// 0600 and renames it over rgwTLSFile, so a start never reads a partial file.
func installTLSFile(ctx context.Context, ctr testcontainers.Container, config *TLSConfig) error {
	const staged = rgwTLSFile + ".next"
	if err := ctr.CopyToContainer(ctx, rgwTLSFileContent(config), staged, 0o600); err != nil {
		return fmt.Errorf("copy the RGW TLS certificate: %w", err)
	}
	if _, err := servicehook.Command(ctx, ctr, "mv", "-f", staged, rgwTLSFile); err != nil {
		return fmt.Errorf("install the RGW TLS certificate: %w", err)
	}
	return nil
}

// S3SecureEndpoint returns the native HTTPS listener's mapped or selected host
// endpoint. It returns an error when TLS was not configured. This method does
// not disable certificate verification or alter application TLS settings.
//
//purpose:handoff
func (g *Gateway) S3SecureEndpoint(ctx context.Context) (string, error) {
	if g == nil || g.Container == nil || g.tlsPort == 0 {
		return "", errors.New("RGW native TLS listener is not configured")
	}
	if g.publicAddress != "" && g.publicAddress != "127.0.0.1" {
		return "https://" + net.JoinHostPort(g.publicAddress, strconv.Itoa(g.tlsPort)), nil
	}
	return g.PortEndpoint(ctx, strconv.Itoa(g.tlsPort)+"/tcp", "https")
}

// The startup probe verifies private socket ownership and the exact configured
// certificate fingerprint. This is certificate pinning for readiness only;
// application tests verify the CA and DNS/IP SAN using their ordinary TLS client.
// Each wait reads the fingerprint that listener expects at that start.
func tlsReadiness(c *ceph.Container, facts ceph.ClusterFacts, port int, listener *tlsListener) *wait.NopStrategy {
	return wait.ForNop(func(ctx context.Context, target wait.StrategyTarget) error {
		probe := rgwTLSProbe{cluster: c, facts: facts, port: strconv.Itoa(port), fingerprint: listener.fingerprint()}
		ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
		defer cancel()
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		address, err := probe.address(ctx, target)
		if err != nil {
			return err
		}
		for {
			retry, err := probe.attempt(ctx, target, address)
			if err != nil {
				return err
			}
			if retry == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return errors.Join(ctx.Err(), retry)
			case <-ticker.C:
			}
		}
	}).WithStartupTimeout(servicehook.StartupTimeout(c))
}

// rgwTLSProbe is one gateway's TLS readiness check: the listener port and the
// SHA-256 fingerprint of the configured leaf certificate.
type rgwTLSProbe struct {
	cluster           *ceph.Container
	facts             ceph.ClusterFacts
	port, fingerprint string
}

// address is where the cluster's CLI container reaches the listener: the
// public address on the host network, otherwise the gateway's address on the
// cluster network.
func (p rgwTLSProbe) address(ctx context.Context, target wait.StrategyTarget) (string, error) {
	if p.facts.UsesHostNetwork {
		return p.facts.PublicAddress, nil
	}
	info, err := target.Inspect(ctx)
	if err != nil {
		return "", err
	}
	if info == nil || info.NetworkSettings == nil {
		return "", errors.New("RGW TLS network is absent")
	}
	endpoint := info.NetworkSettings.Networks[p.facts.NetworkName]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		return "", errors.New("RGW TLS public network address is absent")
	}
	return endpoint.IPAddress.String(), nil
}

// attempt runs one probe. A stopped gateway or a failed inspection is fatal
// (err); a listener not yet owned by RGW or not yet serving the pinned
// certificate is worth retrying (retry). Both nil means ready.
func (p rgwTLSProbe) attempt(ctx context.Context, target wait.StrategyTarget, address string) (retry, err error) {
	state, err := target.State(ctx)
	if err != nil {
		return nil, err
	}
	if state == nil || !state.Running {
		return nil, errors.New("RGW stopped before owning its TLS listener")
	}
	code, reader, err := target.Exec(ctx, []string{"sh", "-c", rgwOwnsListener, "rgw-tls-listener", p.port})
	if err != nil {
		return nil, err
	}
	if reader != nil {
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return nil, err
		}
	}
	if code != 0 {
		return errors.New("RGW does not yet own its TLS listener"), nil
	}
	if _, err := servicehook.Command(ctx, servicehook.CLI(p.cluster), "python3", "-c", rgwTLSReady, address, p.port, p.fingerprint); err != nil {
		return err, nil
	}
	return nil, nil
}

const rgwTLSReady = `import hashlib, socket, ssl, sys
context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.check_hostname=False
context.verify_mode=ssl.CERT_NONE
with socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=2) as raw:
    with context.wrap_socket(raw,server_hostname=sys.argv[1]) as stream:
        assert hashlib.sha256(stream.getpeercert(binary_form=True)).hexdigest()==sys.argv[3], 'unexpected TLS certificate'
        stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
        status=stream.recv(4096).split(b'\r\n',1)[0].split()
        assert len(status)>=2 and status[1] in (b'200',b'403'), 'HTTPS listener not ready'
`
