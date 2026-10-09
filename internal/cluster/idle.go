package cluster

import (
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// WithIdleEntrypoint keeps a client or helper container running with
// sleep infinity under Docker's init process, so commands can be executed in it
// later. As PID 1, sleep ignores SIGTERM and Docker waits for the full stop
// timeout (10 seconds by default) before killing it; init forwards the signal
// so termination finishes at once. Init also reaps background processes that
// exit inside the container, such as a test server started through Exec.
// The entrypoint and command remain sleep and infinity.
func WithIdleEntrypoint() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		for _, option := range []testcontainers.CustomizeRequestOption{
			testcontainers.WithEntrypoint("sleep"),
			testcontainers.WithCmd("infinity"),
			testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
				enabled := true
				host.Init = &enabled
			}),
		} {
			if err := option(req); err != nil {
				return err
			}
		}
		return nil
	}
}
