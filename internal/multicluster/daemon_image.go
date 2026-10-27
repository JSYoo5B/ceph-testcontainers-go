package multicluster

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Image roles that a cross-cluster fixture starts daemons or gateways from.
const (
	RoleRBDAux = "rbd-aux"
	RoleCephFS = "cephfs"
	RoleRGW    = "rgw"
)

// RequireRoleImage refuses an image that a cluster selects for another role
// than the one the fixture needs, for example the essential image given as the
// rbd-mirror daemon image of a split-role cluster. It cannot show that an
// arbitrary image holds the daemon: a custom image that no cluster selects is
// accepted, and so is an image that also serves the wanted role (an aggregate
// selection). Only the recorded selection is compared; nothing runs.
func RequireRoleImage(image, role string, clusters ...*ceph.Container) error {
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		facts, err := ceph.FactsOf(cluster)
		if err != nil {
			return err
		}
		images := facts.Images
		var wanted string
		switch role {
		case RoleRBDAux:
			wanted = images.RBDAux
		case RoleCephFS:
			wanted = images.CephFS
		case RoleRGW:
			wanted = images.RGW
		default:
			return invalid(fmt.Errorf("unknown image role %q", role))
		}
		if image == wanted {
			continue
		}
		for name, other := range map[string]string{"essential": images.Essential, "client": images.Client, RoleCephFS: images.CephFS, RoleRBDAux: images.RBDAux, RoleRGW: images.RGW} {
			if name != role && other != "" && image == other {
				return invalid(fmt.Errorf("image %q is the %s image of a cluster, not its %s image; use Images().%s", image, name, role, roleField(role)))
			}
		}
	}
	return nil
}

func roleField(role string) string {
	switch role {
	case RoleRBDAux:
		return "RBDAux"
	case RoleCephFS:
		return "CephFS"
	}
	return "RGW"
}
