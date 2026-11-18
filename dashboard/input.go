package dashboard

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// runInputCommand owns its writer even when command admission fails before
// RunNativeCommand takes the input. Returning never leaves that writer waiting
// for the caller's longer-lived context. The admission result distinguishes a
// confirmed refusal from a command that might already have run.
func runInputCommand(ctx context.Context, c *ceph.Container, command ceph.NativeCommand, data []byte) (result ceph.NativeCommandResult, admitted bool, err error) {
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
	result, err = c.RunNativeCommand(ctx, servicehook.CLI(c), command)
	return result, servicehook.NativeInputClaimed(input), err
}
