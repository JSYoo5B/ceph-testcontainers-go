package ceph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RGWTLSConfig enables a native Beast HTTPS listener alongside HTTP. Supply
// a PEM certificate chain and matching unencrypted private key. No proxy is
// involved. Clients must explicitly trust its CA and use a matching hostname.
// Files live only in the owned RGW container; descriptors never log key bytes.
type RGWTLSConfig struct {
	CertificatePEM, PrivateKeyPEM []byte
}

func (RGWTLSConfig) String() string     { return "RGW TLS certificate and private key" }
func (c RGWTLSConfig) GoString() string { return c.String() }

// Formatting a gateway must not recurse into its S3 credentials or TLS key.
func (g RGWContainer) String() string   { return "RGW gateway " + g.config.Name }
func (g RGWContainer) GoString() string { return g.String() }

func normalizeRGWTLSConfig(config *RGWTLSConfig) (*RGWTLSConfig, error) {
	if config == nil {
		return nil, nil
	}
	if _, err := tls.X509KeyPair(config.CertificatePEM, config.PrivateKeyPEM); err != nil {
		return nil, errors.New("RGW TLS requires a valid PEM certificate chain and matching unencrypted private key")
	}
	return &RGWTLSConfig{CertificatePEM: slices.Clone(config.CertificatePEM), PrivateKeyPEM: slices.Clone(config.PrivateKeyPEM)}, nil
}

func rgwTLSOptions(port int, config *RGWTLSConfig) []testcontainers.ContainerCustomizer {
	pem := append(slices.Clone(config.CertificatePEM), '\n')
	pem = append(pem, config.PrivateKeyPEM...)
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(map[string]string{"CEPH_RGW_TLS_PORT": strconv.Itoa(port)}),
		testcontainers.WithFiles(testcontainers.ContainerFile{Reader: bytes.NewReader(pem), ContainerFilePath: "/tc/rgw-tls.pem", FileMode: 0o600}),
	}
}

// S3SecureEndpoint returns the native HTTPS listener's mapped or selected host
// endpoint. It returns an error when TLS was not configured. This method does
// not disable certificate verification or alter application TLS settings.
func (g *RGWContainer) S3SecureEndpoint(ctx context.Context) (string, error) {
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
func (c *Container) rgwTLSReadiness(port int, config *RGWTLSConfig) *wait.NopStrategy {
	pair, _ := tls.X509KeyPair(config.CertificatePEM, config.PrivateKeyPEM)
	digest := sha256.Sum256(pair.Certificate[0])
	fingerprint := hex.EncodeToString(digest[:])
	return wait.ForNop(func(ctx context.Context, target wait.StrategyTarget) error {
		ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
		defer cancel()
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		address := c.PublicAddress()
		if !c.UsesHostNetwork() {
			info, err := target.Inspect(ctx)
			if err != nil {
				return err
			}
			if info == nil || info.NetworkSettings == nil {
				return errors.New("RGW TLS network is absent")
			}
			endpoint := info.NetworkSettings.Networks[c.NetworkName()]
			if endpoint == nil || !endpoint.IPAddress.IsValid() {
				return errors.New("RGW TLS public network address is absent")
			}
			address = endpoint.IPAddress.String()
		}
		var lastErr error
		for {
			state, err := target.State(ctx)
			if err != nil {
				return err
			}
			if state == nil || !state.Running {
				return errors.New("RGW stopped before owning its TLS listener")
			}
			code, reader, err := target.Exec(ctx, []string{"sh", "-c", rgwOwnsListener, "rgw-tls-listener", strconv.Itoa(port)})
			if err != nil {
				return err
			}
			if reader != nil {
				if _, err := io.Copy(io.Discard, reader); err != nil {
					return err
				}
			}
			if code == 0 {
				if _, err := command(ctx, c.cliContainer(), "python3", "-c", rgwTLSReady, address, strconv.Itoa(port), fingerprint); err == nil {
					return nil
				} else {
					lastErr = err
				}
			} else {
				lastErr = errors.New("RGW does not yet own its TLS listener")
			}
			select {
			case <-ctx.Done():
				return errors.Join(ctx.Err(), lastErr)
			case <-ticker.C:
			}
		}
	}).WithStartupTimeout(c.settings.startupTimeout)
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
