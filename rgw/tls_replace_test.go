package rgw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
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
	state                container.State
	fail                 string
	steps                []string
	copied               map[string][]byte
	listener             *tlsListener
	readFail, cancelFail bool
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
	original := cmd
	if len(cmd) > 2 && cmd[2] == supervised.ShellCancelScript {
		if c.cancelFail {
			return 0, nil, errors.New("cancel unavailable")
		}
		return 0, nil, nil
	}
	if len(cmd) > 2 && cmd[2] == supervised.ShellScript {
		cmd = cmd[7:]
	}
	c.steps = append(c.steps, strings.Join(cmd, " "))
	if cmd[0] == "cat" {
		if c.readFail {
			return 0, nil, errors.New("read unavailable")
		}
		return gwtest.Reply(original, 0, gwtest.Stream(false, c.copied[rgwTLSFile]))
	}
	if len(cmd) == 4 && cmd[0] == "mv" {
		if c.fail != "rename-before" {
			c.copied[cmd[3]] = c.copied[cmd[2]]
			delete(c.copied, cmd[2])
		}
		if strings.HasPrefix(c.fail, "rename-") {
			return 0, nil, errors.New("rename reply lost")
		}
	}
	return gwtest.Reply(original, 0, gwtest.Stream(false, nil))
}

func (c *rgwTLSGatewayContainer) Stop(context.Context, *time.Duration) error {
	c.steps = append(c.steps, "stop")
	if c.fail == "stop" {
		return errors.New("stop refused")
	}
	c.state.Running = false
	if c.fail == "stop-complete" {
		return errors.New("stop reply lost")
	}
	return nil
}

func (c *rgwTLSGatewayContainer) Start(ctx context.Context) error {
	c.steps = append(c.steps, "start")
	if c.fail == "start" {
		return errors.New("readiness failed")
	}
	c.state.Running, c.state.StartedAt = true, "next"
	if c.listener != nil {
		if err := c.listener.reconcile(ctx); err != nil {
			return err
		}
		if !bytes.Equal(c.copied[rgwTLSFile], rgwTLSFileContent(c.listener.material())) {
			return errors.New("start expected a different certificate")
		}
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
	gateway := registerTestGateway(newGateway(cluster, ctr, config, 7480, 7481, newTLSListener(config.TLS), "", ""))
	if model, ok := ctr.(*rgwTLSGatewayContainer); ok {
		model.copied = map[string][]byte{rgwTLSFile: rgwTLSFileContent(material)}
		model.listener = gateway.tls
		model.state.StartedAt = "first"
	}
	return gateway
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
	if len(ctr.steps) != 4 || !strings.HasPrefix(ctr.steps[0], "copy /tc/rgw-tls.pem.") ||
		!strings.HasPrefix(ctr.steps[1], "mv -f /tc/rgw-tls.pem.") || ctr.steps[2] != "stop" || ctr.steps[3] != "start" {
		t.Fatalf("replacement steps %q", ctr.steps)
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

func TestRGWReplaceTLSRetainsInstalledMaterialWhenStopIsUnconfirmed(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: "stop"}
	gateway := tlsReplacementGateway(t, ctr, first)
	if err := gateway.ReplaceTLS(t.Context(), *second); err == nil || !strings.Contains(err.Error(), "stop refused") {
		t.Fatalf("a failed stop was not reported: %v", err)
	}
	if !bytes.Equal(ctr.copied[rgwTLSFile], rgwTLSFileContent(second)) || gateway.tls.fingerprint() != newTLSListener(second).fingerprint() {
		t.Fatal("installed material and expected certificate disagree")
	}
	if err := gateway.ReplaceTLS(t.Context(), *second); err == nil || !strings.Contains(err.Error(), "unconfirmed") {
		t.Fatalf("unconfirmed stop retry: %v", err)
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

func TestRGWTLSInstallationReplyLossRecoversExpectedMaterial(t *testing.T) {
	for _, tc := range []struct {
		fail     string
		readFail bool
	}{{"rename-before", false}, {"rename-after", false}, {"rename-after", true}} {
		t.Run(fmt.Sprintf("%s/readFailure=%v", tc.fail, tc.readFail), func(t *testing.T) {
			first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
			ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: tc.fail, readFail: tc.readFail}
			gateway := tlsReplacementGateway(t, ctr, first)
			if err := gateway.ReplaceTLS(t.Context(), *second); err == nil {
				t.Fatal("lost reply was hidden")
			}
			if slices.Contains(ctr.steps, "stop") {
				t.Fatal("installation error dispatched Stop")
			}
			if tc.readFail && gateway.tls.pending == nil {
				t.Fatal("unknown readback was discarded")
			}
			ctr.fail, ctr.readFail = "", false
			if err := ctr.Start(t.Context()); err != nil {
				t.Fatalf("ordinary start: %v", err)
			}
			want := second
			if tc.fail == "rename-before" {
				want = first
			}
			if gateway.tls.fingerprint() != newTLSListener(want).fingerprint() {
				t.Fatal("wrong expected fingerprint")
			}
			if err := gateway.ReplaceTLS(t.Context(), *second); err != nil {
				t.Fatalf("replacement retry: %v", err)
			}
		})
	}
}

func TestRGWTLSCompletedStopReplyLossResumesSameReplacement(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: "stop-complete"}
	gateway := tlsReplacementGateway(t, ctr, first)
	if err := gateway.ReplaceTLS(t.Context(), *second); err == nil || ctr.state.Running {
		t.Fatalf("completed Stop: %v", err)
	}
	if err := gateway.ReplaceTLS(t.Context(), *second); err != nil || !ctr.state.Running {
		t.Fatalf("retry: %v", err)
	}
}

func TestRGWTLSUnstoppedInstallerKeepsReadinessUnconfirmed(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	ctr := &rgwTLSGatewayContainer{state: container.State{Running: true}, fail: "rename-before", cancelFail: true}
	gateway := tlsReplacementGateway(t, ctr, first)
	if err := gateway.ReplaceTLS(t.Context(), *second); err == nil || gateway.tls.pending == nil {
		t.Fatalf("uncertain installer: %v", err)
	}
	if err := ctr.Start(t.Context()); err == nil {
		t.Fatal("readiness ignored unconfirmed installer")
	}
	ctr.cancelFail, ctr.fail = false, ""
	if err := ctr.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if gateway.tls.fingerprint() != newTLSListener(first).fingerprint() {
		t.Fatal("canceled installer changed expected material")
	}
}
