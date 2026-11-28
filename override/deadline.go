package override

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// operationDeadline caps one override operation, an apply or a Restore, by
// the cluster's startup timeout. Call it inside the function passed to
// overridehook.WithTopology: waiting for the topology lock is outside the
// cap, as ceph.WithStartupTimeout states.
func operationDeadline(ctx context.Context, c *ceph.Container) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, overridehook.StartupTimeout(c))
}
