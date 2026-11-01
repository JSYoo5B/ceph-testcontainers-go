package rgw

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// rgwHooks gives rgw/multisite, through internal/rgwhook, the handle
// ownership checks and the host port reservation that are not public API.
type rgwHooks struct{}

func init() {
	hookreg.RGW = rgwHooks{}
}

// UserServedBy reports whether g can run the operations of u.
func (rgwHooks) UserServedBy(u *User, g *Gateway) bool { return u.servedBy(g) }

// PlacementServedBy reports whether g runs in the cluster that owns p.
func (rgwHooks) PlacementServedBy(p *Placement, g *Gateway) bool { return p.servedBy(g) }

// ReserveGatewayPort reserves a host port of host-network cluster c for a
// plain-HTTP gateway that starts later. It returns the gateway's peer
// endpoint; start, which starts a gateway on that port as Start does, in one
// attempt; and release, which frees the port when start did not take it.
func (rgwHooks) ReserveGatewayPort(ctx context.Context, c *ceph.Container) (string, func(context.Context, Config, ...testcontainers.ContainerCustomizer) (*Gateway, error), func(context.Context) error, error) {
	if c == nil {
		return "", nil, nil, errNilCluster
	}
	reservation, endpoint, err := reserveGatewayPort(ctx, c)
	if err != nil {
		return "", nil, nil, err
	}
	start := func(ctx context.Context, config Config, opts ...testcontainers.ContainerCustomizer) (*Gateway, error) {
		return startGatewayOn(ctx, c, config, reservation, opts...)
	}
	return endpoint, start, reservation.releaseUnused, nil
}
