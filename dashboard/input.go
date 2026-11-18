package dashboard

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// runInputCommand owns its writer even when command admission fails before
// RunNativeCommand takes the input. Returning never leaves that writer waiting
// for the caller's longer-lived context.
func runInputCommand(ctx context.Context, c *ceph.Container, command ceph.NativeCommand, data []byte) (ceph.NativeCommandResult, error) {
	input := ceph.NewNativeInput()
	command.Input, command.MaxInput = input, len(data)
	writerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = input.Close() }()
		_ = input.Write(writerCtx, data)
	}()
	defer func() {
		cancel()
		<-done
	}()
	return c.RunNativeCommand(ctx, servicehook.CLI(c), command)
}
