package rgw

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// rgwTLSGatewayContainer is a gateway container that records, in order, the
// file copies, commands, stops and starts that a TLS replacement sends it,
// and fails the step named by fail.
type rgwTLSGatewayContainer struct {
	testcontainers.Container
	state  container.State
	fail   string
	steps  []string
	copied map[string][]byte
}

func (c *rgwTLSGatewayContainer) GetContainerID() string { return "rgw-tls" }

func (c *rgwTLSGatewayContainer) State(context.Context) (*container.State, error) {
	return &c.state, nil
}

func (c *rgwTLSGatewayContainer) CopyToContainer(_ context.Context, content []byte, path string, mode int64) error {
	c.steps = append(c.steps, "copy "+path)
	if mode != 0o600 {
		return errors.New("TLS file mode is not 0600")
	}
	if c.copied == nil {
		c.copied = make(map[string][]byte)
	}
	c.copied[path] = slices.Clone(content)
	return nil
}

func (c *rgwTLSGatewayContainer) Exec(_ context.Context, cmd []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.steps = append(c.steps, strings.Join(cmd, " "))
	if len(cmd) == 4 && cmd[0] == "mv" {
		c.copied[cmd[3]] = c.copied[cmd[2]]
		delete(c.copied, cmd[2])
	}
	return 0, strings.NewReader(""), nil
}

func (c *rgwTLSGatewayContainer) Stop(context.Context, *time.Duration) error {
	c.steps = append(c.steps, "stop")
	if c.fail == "stop" {
		return errors.New("stop refused")
	}
	return nil
}

func (c *rgwTLSGatewayContainer) Start(context.Context) error {
	c.steps = append(c.steps, "start")
	if c.fail == "start" {
		return errors.New("readiness failed")
	}
	return nil
}

// tlsReplacementGateway registers a TLS gateway on ctr in a synthetic cluster
// whose listener expects material.
func tlsReplacementGateway(t *testing.T, ctr testcontainers.Container, material *TLSConfig) *Gateway {
	t.Helper()
	cluster := testCluster(&rgwTestContainer{}, testhelper.TestConfig{StartupTimeout: time.Second}, nil)
	config, err := normalizeRGWConfig(Config{Name: "tls", TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	return registerTestGateway(newGateway(cluster, ctr, config, 7480, 7481, newTLSListener(config.TLS), "", ""))
}

func TestRGWReplaceTLSInstallsMaterialAndRestarts(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}}
	gateway := tlsReplacementGateway(t, ctr, first)
	before := gateway.tls.fingerprint()
	copied := *gateway
	if err := copied.ReplaceTLS(t.Context(), *second); err != nil {
		t.Fatal(err)
	}
	want := []string{"copy /tc/rgw-tls.pem.next", "mv -f /tc/rgw-tls.pem.next /tc/rgw-tls.pem", "stop", "start"}
	if !slices.Equal(ctr.steps, want) {
		t.Fatalf("replacement steps %q, want %q", ctr.steps, want)
	}
	if !bytes.Equal(ctr.copied[rgwTLSFile], rgwTLSFileContent(second)) {
		t.Fatal("the gateway file does not hold the new material")
	}
	if after := gateway.tls.fingerprint(); after == before || after != newTLSListener(second).fingerprint() {
		t.Fatal("readiness does not expect the new leaf certificate")
	}
	second.PrivateKeyPEM[0] = '!'
	if _, err := normalizeRGWTLSConfig(gateway.tls.material()); err != nil {
		t.Fatal("caller mutation changed the installed material")
	}
}

func TestRGWReplaceTLSRestoresMaterialWhenTheGatewayDoesNotStop(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: "stop"}
	gateway := tlsReplacementGateway(t, ctr, first)
	before := gateway.tls.fingerprint()
	if err := gateway.ReplaceTLS(t.Context(), *second); err == nil || !strings.Contains(err.Error(), "stop refused") {
		t.Fatalf("a failed stop was not reported: %v", err)
	}
	if !bytes.Equal(ctr.copied[rgwTLSFile], rgwTLSFileContent(first)) || gateway.tls.fingerprint() != before {
		t.Fatal("the previous material was not put back")
	}
	if slices.Contains(ctr.steps, "start") {
		t.Fatal("a gateway that did not stop was started")
	}
}

func TestRGWReplaceTLSKeepsNewMaterialWhenTheRestartFails(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: "start"}
	gateway := tlsReplacementGateway(t, ctr, first)
	err := gateway.ReplaceTLS(t.Context(), *second)
	if err == nil || !strings.Contains(err.Error(), "stays installed") {
		t.Fatalf("a failed restart was not reported: %v", err)
	}
	if gateway.tls.fingerprint() != newTLSListener(second).fingerprint() {
		t.Fatal("a later start would expect the previous certificate")
	}
}

func TestRGWReplaceTLSRefusesBeforeTouchingTheGateway(t *testing.T) {
	material, other := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	if err := (*Gateway)(nil).ReplaceTLS(t.Context(), *material); err == nil {
		t.Fatal("a nil gateway was accepted")
	}
	for name, test := range map[string]struct {
		gateway func(*rgwTLSGatewayContainer) *Gateway
		config  TLSConfig
	}{
		"invalid material": {func(ctr *rgwTLSGatewayContainer) *Gateway { return tlsReplacementGateway(t, ctr, material) },
			TLSConfig{CertificatePEM: material.CertificatePEM, PrivateKeyPEM: other.PrivateKeyPEM}},
		"plain HTTP gateway": {func(ctr *rgwTLSGatewayContainer) *Gateway {
			cluster := testCluster(&rgwTestContainer{}, testhelper.TestConfig{StartupTimeout: time.Second}, nil)
			return registerTestGateway(newGateway(cluster, ctr, Config{Name: "plain"}, 7480, 0, nil, "", ""))
		}, *other},
		"stale descriptor": {func(ctr *rgwTLSGatewayContainer) *Gateway {
			registered := tlsReplacementGateway(t, ctr, material)
			stale := newGateway(registered.base().Cluster(), ctr, Config{Name: "tls"}, 7480, 7481, newTLSListener(material), "", "")
			return stale
		}, *other},
		"stopped gateway": {func(ctr *rgwTLSGatewayContainer) *Gateway {
			ctr.state = container.State{Status: "exited"}
			return tlsReplacementGateway(t, ctr, material)
		}, *other},
	} {
		t.Run(name, func(t *testing.T) {
			ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}}
			gateway := test.gateway(ctr)
			if err := gateway.ReplaceTLS(t.Context(), test.config); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("refusal is not an invalid request: %v", err)
			}
			if len(ctr.steps) != 0 {
				t.Fatalf("a refused replacement touched the gateway: %q", ctr.steps)
			}
		})
	}
}

// rgwTLSProbeRecorder is the control container of a TLS readiness probe: it
// records the fingerprint that each probe pins.
type rgwTLSProbeRecorder struct {
	rgwTestContainer
	pinned []string
}

func (c *rgwTLSProbeRecorder) Exec(_ context.Context, cmd []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.pinned = append(c.pinned, cmd[len(cmd)-1])
	return 0, strings.NewReader(""), nil
}

func TestRGWTLSReadinessExpectsTheCurrentCertificateAtEveryStart(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	control := &rgwTLSProbeRecorder{}
	cluster := testCluster(control, testhelper.TestConfig{HostNetwork: true, PublicAddress: "127.0.0.1", StartupTimeout: time.Second}, nil)
	listener := newTLSListener(first)
	readiness := tlsReadiness(cluster, testFacts(t, cluster), 41302, listener)
	target := &rgwReadinessTarget{state: container.State{Running: true}}
	for _, material := range []*TLSConfig{first, second} {
		listener.set(material)
		if err := readiness.WaitUntilReady(t.Context(), wait.StrategyTarget(target)); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{newTLSListener(first).fingerprint(), newTLSListener(second).fingerprint()}
	if !slices.Equal(control.pinned, want) || want[0] == want[1] {
		t.Fatalf("readiness pinned %q, want %q", control.pinned, want)
	}
}
