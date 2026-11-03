//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// assertContainerFilesEqual compares the exact retained bytes without requiring
// an optional diffutils package in the client runtime.
func assertContainerFilesEqual(t *testing.T, ctx context.Context, ctr testcontainers.Container, first, second string) {
	t.Helper()
	read := func(path string) []byte {
		t.Helper()
		stream, err := ctr.CopyFileFromContainer(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(stream)
		if err := errors.Join(readErr, stream.Close()); err != nil {
			t.Fatal(err)
		}
		return data
	}
	expected, actual := read(first), read(second)
	if !bytes.Equal(expected, actual) {
		t.Fatalf("container file bytes differ: %s (%d bytes), %s (%d bytes)", first, len(expected), second, len(actual))
	}
}
