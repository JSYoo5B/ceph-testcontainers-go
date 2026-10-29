package rgw

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gatewayadmin"
)

// gatewayAccess is the administration of one gateway as rgw/harness sees it
// through rgw/internal/gatewayadmin.
type gatewayAccess struct{ g *Gateway }

func init() {
	gatewayadmin.Of = func(gateway any) gatewayadmin.Admin {
		g, ok := gateway.(*Gateway)
		if !ok || g == nil {
			return nil
		}
		return gatewayAccess{g: g}
	}
}

func (a gatewayAccess) Owner() *ceph.Container { return a.g.owner }

func (a gatewayAccess) Validate() error { return a.g.validateAdminGateway() }

func (a gatewayAccess) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return a.g.adminContext(ctx)
}

func (a gatewayAccess) Command(ctx context.Context, args ...string) ([]byte, error) {
	return a.g.adminCommand(ctx, args...)
}
