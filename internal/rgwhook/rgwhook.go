// Package rgwhook gives rgw/multisite the parts of the rgw handles that are
// not public API: whether a gateway can serve a user or placement handle.
// Package rgw registers them in internal/hookreg when it is initialized.
package rgwhook

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

type hooks interface {
	UserServedBy(*rgw.User, *rgw.Gateway) bool
	PlacementServedBy(*rgw.Placement, *rgw.Gateway) bool
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
