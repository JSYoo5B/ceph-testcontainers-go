package rgw

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"

// rgwHooks gives rgw/multisite, through internal/rgwhook, the handle
// ownership checks that are not public API.
type rgwHooks struct{}

func init() {
	hookreg.RGW = rgwHooks{}
}

// UserServedBy reports whether g can run the operations of u.
func (rgwHooks) UserServedBy(u *User, g *Gateway) bool { return u.servedBy(g) }

// PlacementServedBy reports whether g runs in the cluster that owns p.
func (rgwHooks) PlacementServedBy(p *Placement, g *Gateway) bool { return p.servedBy(g) }
