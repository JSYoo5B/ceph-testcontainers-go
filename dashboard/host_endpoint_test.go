package dashboard

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/network"
)

// The advertised URL of a bridge-mode manager keeps its scheme and path and
// takes the Docker host and the host port bound to its own port.
func TestHostURL(t *testing.T) {
	bound := network.PortMap{
		network.MustParsePort("8443/tcp"): {{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: "32771"}, {HostIP: netip.MustParseAddr("::"), HostPort: "32771"}},
		network.MustParsePort("9283/tcp"): {{HostPort: "32772"}},
	}
	for name, test := range map[string]struct {
		advertised, host, want string
	}{
		"IPv4 container":   {"https://172.18.0.3:8443/", "localhost", "https://localhost:32771/"},
		"IPv6 container":   {"https://[fd00::3]:8443/", "localhost", "https://localhost:32771/"},
		"IPv6 Docker host": {"https://172.18.0.3:8443/", "::1", "https://[::1]:32771/"},
		"remote engine":    {"https://172.18.0.5:8443/", "192.0.2.10", "https://192.0.2.10:32771/"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := hostURL(test.advertised, test.host, bound)
			if err != nil || got != test.want {
				t.Fatalf("hostURL(%s) = %q, %v; want %s", test.advertised, got, err, test.want)
			}
		})
	}
	// A port the manager does not publish, or publishes without a host
	// binding (a stopped container), is a missing WithManagerPorts.
	for name, ports := range map[string]network.PortMap{
		"none":       nil,
		"other port": {network.MustParsePort("9283/tcp"): {{HostPort: "32772"}}},
		"udp":        {network.MustParsePort("8443/udp"): {{HostPort: "32773"}}},
		"unbound":    {network.MustParsePort("8443/tcp"): nil},
	} {
		t.Run("refused "+name, func(t *testing.T) {
			if got, err := hostURL("https://172.18.0.3:8443/", "localhost", ports); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("hostURL = %q, %v", got, err)
			}
		})
	}
	if _, err := hostURL("https://172.18.0.3/", "localhost", bound); err == nil || errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an advertised URL without a port: %v", err)
	}
}

// The host-side check accepts only the installed certificate and sends no
// request to either listener.
func TestPresentsCertificate(t *testing.T) {
	installed, want := probeCertificate(t)
	other, _ := probeCertificate(t)
	good := newProbeListener(t, installed, other, 1<<30)
	if err := presentsCertificate(t.Context(), good.server.URL, want); err != nil {
		t.Fatal(err)
	}
	foreign := newProbeListener(t, other, other, 0)
	if err := presentsCertificate(t.Context(), foreign.server.URL, want); err == nil {
		t.Fatal("accepted another certificate")
	}
	if seen := append(good.seen(), foreign.seen()...); len(seen) != 0 {
		t.Fatalf("the check sent requests: %+v", seen)
	}
}
