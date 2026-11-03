//go:build all || (integration && features)

package harness

import (
	"context"
	"io"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func FencingExec(t *testing.T, ctx context.Context, client testcontainers.Container, command []string) []byte {
	t.Helper()
	code, reader, err := client.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("native fencing command: exit=%d %s %v", code, data, err)
	}
	return data
}
