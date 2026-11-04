//go:build all || integration

package rgw_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rgwKeyFaultShim stands in for radosgw-admin, ahead of it on the gateway's
// PATH. It runs the real command and keeps its output; while
// /tmp/tc-key-fault names an operation (create or rm), it then reports exit
// status 70 for the next key command of that operation, once. The change of
// that command has already happened, which is the partial failure the
// fixture must settle without replaying the command.
const rgwKeyFaultShim = `#!/bin/sh
REAL
status=$?
fault=$(cat /tmp/tc-key-fault 2>/dev/null)
if [ -n "$fault" ]; then
	key=0
	for arg in "$@"; do
		[ "$arg" = key ] && key=1
		if [ "$key" = 1 ] && [ "$arg" = "$fault" ]; then
			rm -f /tmp/tc-key-fault
			exit 70
		fi
	done
fi
exit $status
`

// rgwGatewayExec runs a command in the gateway container and returns its
// exit code and combined output.
func rgwGatewayExec(t *testing.T, ctx context.Context, gateway *rgw.Gateway, args ...string) (int, string) {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	code, r, err := gateway.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return code, string(out)
}

// installRGWKeyFaultShim puts rgwKeyFaultShim ahead of the real radosgw-admin
// and returns the function that arms a fault for the next key command of
// operation.
func installRGWKeyFaultShim(t *testing.T, ctx context.Context, gateway *rgw.Gateway) func(operation string) {
	t.Helper()
	code, out := rgwGatewayExec(t, ctx, gateway, "sh", "-c", "command -v radosgw-admin")
	real := strings.TrimSpace(out)
	if code != 0 || !strings.HasPrefix(real, "/") || real == "/usr/local/sbin/radosgw-admin" {
		t.Fatalf("locate the real radosgw-admin: %q", out)
	}
	shim := strings.Replace(rgwKeyFaultShim, "REAL", real+` "$@"`, 1)
	if err := gateway.CopyToContainer(ctx, []byte(shim), "/usr/local/sbin/radosgw-admin", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, out := rgwGatewayExec(t, ctx, gateway, "sh", "-c", "command -v radosgw-admin"); strings.TrimSpace(out) != "/usr/local/sbin/radosgw-admin" {
		t.Fatalf("the shim is not first on the gateway's PATH: %q", out)
	}
	return func(operation string) {
		t.Helper()
		if code, out := rgwGatewayExec(t, ctx, gateway, "sh", "-c", "echo "+operation+" > /tmp/tc-key-fault"); code != 0 {
			t.Fatalf("arm the key fault: %s", out)
		}
	}
}

// removeRGWKeyFaultShim restores the real radosgw-admin and checks that no
// armed fault is left.
func removeRGWKeyFaultShim(t *testing.T, ctx context.Context, gateway *rgw.Gateway) {
	t.Helper()
	if code, out := rgwGatewayExec(t, ctx, gateway, "sh", "-c", "test ! -e /tmp/tc-key-fault && rm -f /usr/local/sbin/radosgw-admin"); code != 0 {
		t.Fatalf("an armed key fault was not consumed, or the shim stayed: %s", out)
	}
}

// rgwNativeKeys reads the access keys of user id independently of the typed
// API, through radosgw-admin user info.
func rgwNativeKeys(t *testing.T, ctx context.Context, gateway *rgw.Gateway, id string) map[string]string {
	t.Helper()
	data, err := gateway.Admin(ctx, "user", "info", "--uid", id)
	if err != nil {
		t.Fatalf("native user info: %v", err)
	}
	return rgwDecodeKeys(t, data)
}

func rgwDecodeKeys(t *testing.T, data []byte) map[string]string {
	t.Helper()
	var native struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		t.Fatal("decode native user keys")
	}
	keys := make(map[string]string, len(native.Keys))
	for _, key := range native.Keys {
		keys[key.AccessKey] = key.SecretKey
	}
	return keys
}

// rgwS3Status sends one signed GET with client and returns its HTTP status.
// It never prints the response, which can name the access key.
func rgwS3Status(t *testing.T, ctx context.Context, client harness.S3HTTPClient, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.Endpoint+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Sign(req, nil, time.Now().UTC())
	response, err := client.HTTP.Do(req)
	if err != nil {
		t.Fatalf("S3 GET %s: %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

// s3Probe is the AWS SDK probe binary of internal/integration/awssdk/probe,
// built for this test run: the SUT that the fixture hands credentials to.
type s3Probe struct {
	path, endpoint, region string
}

// s3ProbeResult is the one JSON line that the probe prints.
type s3ProbeResult struct {
	Operation  string `json:"operation"`
	Role       string `json:"role"`
	AssumedARN string `json:"assumed_arn"`
	Outcome    string `json:"outcome"`
	SHA256     string `json:"sha256"`
	SDK        string `json:"sdk"`
}

// buildS3Probe builds the probe module, outside this module and its
// workspace, with the go command that runs the test.
func buildS3Probe(t *testing.T, ctx context.Context, endpoint, region string) s3Probe {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "awssdk", "probe"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "s3-probe")
	buildCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-mod=readonly", "-o", path, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the AWS SDK probe: %v\n%s", err, out)
	}
	return s3Probe{path: path, endpoint: endpoint, region: region}
}

// run starts a fresh probe process with key's credentials in its
// environment and one operation on bucket/object.
func (p s3Probe) run(t *testing.T, ctx context.Context, key *rgw.S3Key, operation, bucket, object, data string) s3ProbeResult {
	t.Helper()
	return p.runAs(t, ctx, key, "", operation, bucket, object, data)
}

// runAs runs the probe as run does; with a non-empty roleARN, the probe first
// assumes that role with STS and performs the operation with its temporary
// credentials.
func (p s3Probe) runAs(t *testing.T, ctx context.Context, key *rgw.S3Key, roleARN, operation, bucket, object, data string) s3ProbeResult {
	t.Helper()
	access, secret, err := key.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	return p.exec(t, ctx, access, secret, "-endpoint", p.endpoint, "-region", p.region, "-bucket", bucket, "-object", object, "-operation", operation, "-data", data, "-role-arn", roleARN)
}

// exec starts a fresh probe process with the key pair access and secret in
// its environment and args on its command line, and returns the line it
// printed for its operation.
func (p s3Probe) exec(t *testing.T, ctx context.Context, access, secret string, args ...string) s3ProbeResult {
	t.Helper()
	operation := ""
	for i, arg := range args {
		if arg == "-operation" && i+1 < len(args) {
			operation = args[i+1]
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runCtx, p.path, args...)
	cmd.Env = []string{"AWS_ACCESS_KEY_ID=" + access, "AWS_SECRET_ACCESS_KEY=" + secret, "HOME=" + t.TempDir()}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("AWS SDK probe %s: %v: %s", operation, err, stderr.String())
	}
	var result s3ProbeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Operation != operation {
		t.Fatalf("AWS SDK probe %s printed %q", operation, stdout.String())
	}
	return result
}

// waitOutcome runs the probe until it reports outcome, within a minute.
func (p s3Probe) waitOutcome(t *testing.T, ctx context.Context, key *rgw.S3Key, operation, bucket, object, outcome string) s3ProbeResult {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		result := p.run(t, ctx, key, operation, bucket, object, "")
		if result.Outcome == outcome {
			return result
		}
		if time.Now().After(deadline) {
			t.Fatalf("AWS SDK probe %s reported %s, want %s", operation, result.Outcome, outcome)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// unrelatedKey returns the access key in keys that none of known names.
func unrelatedKey(t *testing.T, keys map[string]string, known ...string) string {
	t.Helper()
	var found []string
	for access := range keys {
		if !slices.Contains(known, access) {
			found = append(found, access)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d keys the fixture did not create, want 1", len(found))
	}
	return found[0]
}
