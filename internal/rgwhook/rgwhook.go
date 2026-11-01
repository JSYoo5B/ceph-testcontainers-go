// Package rgwhook gives rgw/multisite a host port reserved for a gateway
// before it starts, which needs the gateway start of package rgw and is not
// public API. Package rgw registers it in internal/hookreg when it is
// initialized. The handle ownership checks are typed accessors of
// rgw/internal/gwbase.
package rgwhook

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

type hooks interface {
	ReserveGatewayPort(context.Context, *ceph.Container) (string, func(context.Context, rgw.Config, ...testcontainers.ContainerCustomizer) (*rgw.Gateway, error), func(context.Context) error, error)
}

// GatewayStart starts a gateway on a reserved host port, as rgw.Start does
// but in one attempt.
type GatewayStart = func(context.Context, rgw.Config, ...testcontainers.ContainerCustomizer) (*rgw.Gateway, error)

// ReserveGatewayPort reserves a host port of host-network cluster c for a
// plain-HTTP gateway that starts later, so that its peer endpoint can be
// committed before the gateway first serves a period. start starts the
// gateway on that port once; another process can still bind the port in the
// moment between the release of the reservation and the gateway's bind, and
// start then fails without moving to another port. release frees the port
// when start did not take it; after start it does nothing. The cluster's
// Terminate also frees an unreleased reservation.
func ReserveGatewayPort(ctx context.Context, c *ceph.Container) (endpoint string, start GatewayStart, release func(context.Context) error, err error) {
	return get().ReserveGatewayPort(ctx, c)
}

func get() hooks {
	h, ok := hookreg.RGW.(hooks)
	if !ok {
		panic("rgwhook: package rgw did not register its hooks")
	}
	return h
}
