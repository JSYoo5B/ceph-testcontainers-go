//go:build all || (integration && hostnetwork)

package rgw_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func hostNetworkS3Request(t *testing.T, ctx context.Context, client testcontainers.Container, gateway harness.S3HTTPClient, method, path string, payload []byte, wantStatus int) []byte {
	t.Helper()
	input, err := harness.HostNetworkS3RequestInput(ctx, gateway, method, path, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyToContainer(ctx, input, "/tmp/host-s3-request.json", 0o600); err != nil {
		t.Fatal(err)
	}
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", harness.HostNetworkHTTPScript, "/tmp/host-s3-request.json"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("VM host namespace HTTP probe exited %d: %s", code, output)
	}
	var result struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode VM HTTP response: %v: %s", err, output)
	}
	body, err := base64.StdEncoding.DecodeString(result.Body)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != wantStatus {
		t.Fatalf("VM HTTP %s %s returned %d, want %d: %s", method, path, result.Status, wantStatus, body)
	}
	return body
}
