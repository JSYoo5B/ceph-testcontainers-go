//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// attachedOutputLimit bounds what one exec in the keeper may print.
const attachedOutputLimit = 1 << 20

// readAttachedOutput reads a hijacked exec stream to its end, or until ctx
// ends. A hijacked connection belongs to its caller once the handshake is
// done, so canceling the request's context does not interrupt a read: on the
// deadline this closes the stream itself and waits for the reading goroutine
// to return before it reports the deadline. The stream is closed on every
// path.
func readAttachedOutput(ctx context.Context, stream io.ReadCloser) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(io.LimitReader(stream, attachedOutputLimit))
		done <- result{data, err}
	}()
	select {
	case r := <-done:
		return r.data, errors.Join(r.err, stream.Close())
	case <-ctx.Done():
		closeErr := stream.Close()
		r := <-done
		return r.data, errors.Join(fmt.Errorf("exec output: %w", ctx.Err()), closeErr)
	}
}
