//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A stalled exec stream ends at the deadline: the read is unblocked by
// closing the stream, the goroutine is joined, and what arrived before the
// stall is kept. A stream that ends on its own returns its bytes.
func TestReadAttachedOutputEndsAStalledStream(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	go func() { _, _ = server.Write([]byte("partial")) }()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	data, err := readAttachedOutput(ctx, client)
	if !errors.Is(err, context.DeadlineExceeded) || string(data) != "partial" {
		t.Fatalf("stalled stream: %q, %v", data, err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the stalled read returned after %v", elapsed)
	}
	if _, err := client.Write([]byte("x")); err == nil {
		t.Fatal("the stalled stream was not closed")
	}

	client, server = net.Pipe()
	go func() {
		_, _ = server.Write([]byte("whole\n"))
		_ = server.Close()
	}()
	data, err = readAttachedOutput(t.Context(), client)
	if err != nil || string(data) != "whole\n" {
		t.Fatalf("finished stream: %q, %v", data, err)
	}
}
