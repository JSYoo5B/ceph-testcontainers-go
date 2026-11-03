//go:build all || integration

package harness

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func ContainerLogLines(t *testing.T, ctx context.Context, ctr testcontainers.Container) []string {
	t.Helper()
	reader, err := ctr.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(string(data), "\n")
}
