package finalize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidencehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// credentialInventoryControl returns only synthetic credentials through the
// public client lifecycle. The real auth ledger supplies the later inventory.
type credentialInventoryControl struct {
	testcontainers.Container
	secret  string
	deleted bool
}

func (c *credentialInventoryControl) GetContainerID() string { return "synthetic-control" }
func (c *credentialInventoryControl) CopyToContainer(context.Context, []byte, string, int64) error {
	return nil
}
func (c *credentialInventoryControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	supervisedRun := len(args) > 6 && args[0] == "python3" && args[2] == supervised.Script
	if supervisedRun {
		args = args[6:]
	}
	if args[0] == "ceph" {
		args = args[3:]
	}
	var output string
	code := 0
	switch {
	case args[0] == "ceph-authtool":
		output = c.secret + "\n"
	case slices.Equal(args, []string{"auth", "ls", "--format", "json"}):
		output = `{"auth_dump":[]}`
	case len(args) >= 3 && args[0] == "auth" && args[1] == "get" && c.deleted:
		code, output = 2, "Error ENOENT: failed to find "+args[2]+" in keyring"
	case len(args) >= 3 && args[0] == "auth" && args[1] == "get":
		if slices.Contains(args, "json") {
			data, _ := json.Marshal([]any{map[string]any{"entity": args[2], "key": c.secret, "caps": map[string]string{"mon": "allow r"}}})
			output = string(data)
		} else {
			output = "[" + args[2] + "]\nkey = " + c.secret + "\n"
		}
	case args[0] == "rm", args[0] == "sh":
	case len(args) >= 2 && args[0] == "auth" && args[1] == "add":
	case len(args) >= 5 && args[0] == "python3":
		var command struct{ Prefix string }
		if json.Unmarshal([]byte(args[4]), &command) != nil || command.Prefix != "auth rm" {
			return 0, nil, errors.New("unexpected synthetic MON command")
		}
		c.deleted = true
	default:
		return 0, nil, fmt.Errorf("unexpected synthetic credential operation %q", args)
	}
	if supervisedRun {
		stdout, stderr := []byte(output), []byte(nil)
		if code != 0 {
			stdout, stderr = nil, []byte(output)
		}
		return 0, bytes.NewReader(clustertest.QuorumEnvelope(stdout, stderr, code, false, false)), nil
	}
	stream := byte(1)
	if code != 0 {
		stream = 2
	}
	return code, bytes.NewReader(diagnosticsFrame(stream, output)), nil
}

func TestVerificationRefreshesTrackedCredentialRedaction(t *testing.T) {
	const fresh = "opaque-fresh-synthetic-827"
	const old = "opaque-old-synthetic-827"
	control := &credentialInventoryControl{secret: fresh}
	cluster := testhelper.TestCluster(control, testhelper.TestConfig{StartupTimeout: time.Second, HostNetwork: true, PublicAddress: "127.0.0.1"})
	testhelper.TestSetConfig(cluster, []byte("[global]\nmon_host = synthetic-mon\n"))
	fx := newVerifyFixture()
	fx.config.ArchiveLogs = true
	fx.snapshot.secrets = []string{old}
	v := fx.activate(t)
	client, err := cluster.CreateClient(t.Context(), "synthetic-reader", ceph.ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := evidencehook.Inventory(t.Context(), cluster)
	if err != nil || !slices.Contains(inventory.Secrets, fresh) {
		t.Fatal("new public client credential missing from inventory", err)
	}
	if err := client.Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	inventory, err = evidencehook.Inventory(t.Context(), cluster)
	if err != nil || !slices.Contains(inventory.Secrets, fresh) {
		t.Fatal("revoked public client credential missing from inventory", err)
	}
	fx.snapshot.secrets = inventory.Secrets
	message := "opaque material " + old + " " + fresh
	fx.line("osd0", 2*time.Minute, daemonLogLine("cluster", "ERR", message))
	crash := "2026-10-23T01:01:00Z_synthetic"
	fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, crash)
	fx.docker.containers["osd0"].messages = map[string]string{crash: message}
	report := fx.evaluate(t, v)
	checkVerificationSecrets(t, report, old, fresh)
	// A later inventory that omits a previously seen value must not unmask
	// old lines of this same activation window.
	fx.snapshot.secrets = nil
	checkVerificationSecrets(t, fx.evaluate(t, v), old, fresh)
}

func checkVerificationSecrets(t *testing.T, report *VerificationReport, secrets ...string) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := json.Marshal(report.logLines)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if strings.Contains(string(data)+string(lines)+report.summary(), secret) {
			t.Fatal("verification retained synthetic secret")
		}
	}
	if report.Logs.Unexpected == 0 || report.Crashes.Unexpected == 0 || !strings.Contains(string(data), "[REDACTED]") {
		t.Fatal("redaction control did not retain the observed events")
	}
}

func TestVerificationRedactsPartialInventorySecrets(t *testing.T) {
	const secret = "opaque-partial-synthetic-827"
	fx := newVerifyFixture()
	fx.config.ArchiveLogs = true
	v := fx.activate(t)
	v.snapshot = func(context.Context) (clusterSnapshot, error) {
		return clusterSnapshot{secrets: []string{secret}}, errors.New("inventory incomplete " + secret)
	}
	fx.line("osd0", 2*time.Minute, daemonLogLine("cluster", "ERR", "opaque material "+secret))
	report := fx.evaluate(t, v)
	data, _ := json.Marshal(report)
	if strings.Contains(string(data)+report.summary(), secret) || !strings.Contains(gapReasons(report.Logs), "inventory incomplete [REDACTED]") {
		t.Fatal("partial inventory lost redaction or its coverage gap")
	}
	v.snapshot = func(context.Context) (clusterSnapshot, error) {
		return clusterSnapshot{}, errors.New("no inventory available")
	}
	report = fx.evaluate(t, v)
	data, _ = json.Marshal(report)
	if strings.Contains(string(data), secret) {
		t.Fatal("failed later inventory forgot known secret")
	}
}

func TestVerificationArchivesNewOpaqueSecrets(t *testing.T) {
	const secret = "opaque-archive-synthetic-827"
	lines, report, archive := archivedVerificationLines(t, VerificationConfig{}, func(fx *verifyFixture) {
		fx.snapshot.secrets = append(fx.snapshot.secrets, secret)
		fx.line("osd0", 2*time.Minute, daemonLogLine("cluster", "ERR", "opaque material "+secret))
		crash := "2026-10-23T01:01:00Z_synthetic"
		fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, crash)
		fx.docker.containers["osd0"].messages = map[string]string{crash: "opaque material " + secret}
	})
	if len(lines) == 0 || report.Logs.Unexpected == 0 || report.Crashes.Unexpected == 0 || strings.Contains(archive+report.summary(), secret) || !strings.Contains(archive, "opaque material [REDACTED]") {
		t.Fatal("archive did not preserve redacted new-credential evidence")
	}
}

func TestVerificationArchivesCutNewSecret(t *testing.T) {
	const secret = "opaque-boundary-synthetic-827"
	stamp := verifyStart.Add(time.Minute).Format(time.RFC3339Nano)
	prefix := secret[:len(secret)-4]
	cutEvent := stamp + " " + daemonLogLine("cluster", "ERR", "opaque material "+prefix)
	lines, _, archive := archivedVerificationLines(t, VerificationConfig{}, func(fx *verifyFixture) {
		fx.snapshot.secrets = append(fx.snapshot.secrets, secret)
		frame := diagnosticsFrame(2, cutEvent+secret[len(secret)-4:]+" more\n")
		fx.docker.containers["osd0"].rawLogs = frame[:8+len(cutEvent)]
	})
	if strings.Contains(archive, prefix) {
		t.Fatal("new secret prefix survived the truncated log boundary")
	}
	for _, line := range lines {
		if line.Source == "osd.0" && line.Truncated && strings.HasSuffix(line.Text, "opaque material [REDACTED]") {
			return
		}
	}
	t.Fatal("missing redacted truncated line")
}
