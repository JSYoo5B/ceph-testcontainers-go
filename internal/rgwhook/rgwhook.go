// Package rgwhook gives rgw/multisite the parts of the rgw handles that are
// not public API: whether a gateway can serve a user or placement handle, and
// a host port reserved for a gateway before it starts. Package rgw registers
// them in internal/hookreg when it is initialized.
package rgwhook

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

type hooks interface {
	UserServedBy(*rgw.User, *rgw.Gateway) bool
	PlacementServedBy(*rgw.Placement, *rgw.Gateway) bool
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

// UserServedBy reports whether g belongs to the cluster of u and serves the
// scope of the gateway that created u, so that it can run u's operations.
func UserServedBy(u *rgw.User, g *rgw.Gateway) bool { return get().UserServedBy(u, g) }

// PlacementServedBy reports whether g runs in the cluster that owns p, that
// is, where p's pools and resources live. Matching native realm, zonegroup
// and zone IDs do not show this: a period can be imported into another
// storage cluster.
func PlacementServedBy(p *rgw.Placement, g *rgw.Gateway) bool {
	return get().PlacementServedBy(p, g)
}
