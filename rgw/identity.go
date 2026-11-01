package rgw

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/testcontainers/testcontainers-go"
)

// gatewayHandle binds a Gateway to its foundation core (see gwbase.Core). The
// alias keeps the embedded field unexported, so *Gateway satisfies
// gwbase.Owner without promoting anything into the public API.
type gatewayHandle = gwbase.Handle

// userHandle binds a User to its private state (see gwbase.User), in the same
// way.
type userHandle = gwbase.UserHandle

// accountOwner is the sealed interface of the account handles of package
// rgw/account.
type accountOwner = gwbase.AccountOwner

// RootAccount is an owned account that UserConfig.Account names. It is
// sealed: only *account.Account, created by package rgw/account, satisfies
// it.
type RootAccount interface{ accountOwner }

// newGateway returns the descriptor of a gateway container of c, bound to its
// foundation core. The core reads the descriptor's Container field when an
// operation runs and the cluster's registered gateways through the registry.
func newGateway(c *ceph.Container, ctr testcontainers.Container, config Config, port, tlsPort int, publicAddress, networkName string) *Gateway {
	g := &Gateway{Container: ctr, GatewayName: config.Name, Region: config.Region, port: port, tlsPort: tlsPort,
		publicAddress: publicAddress, networkName: networkName, config: config}
	g.gatewayHandle = gwbase.NewHandle(gwbase.NewCore(gwbase.Params{
		Cluster: c, Name: config.Name, Scope: gwbase.Scope{Realm: config.Realm, Zonegroup: config.Zonegroup, Zone: config.Zone},
		Port: port, PublicAddress: publicAddress, Container: &g.Container,
		Gateways: func() []*gwbase.Core { return gateways(c).cores() },
	}))
	return g
}

// base returns the foundation core of g; nil for a nil g or a descriptor that
// Start did not construct.
func (g *Gateway) base() *gwbase.Core {
	if g == nil {
		return nil
	}
	return gwbase.Of(g)
}

// newUser returns the handle of u; nil for a nil u.
func newUser(u *gwbase.User) *User {
	if u == nil {
		return nil
	}
	return &User{userHandle: gwbase.NewUserHandle(u)}
}

// base returns the private state of u; nil for a nil u or a handle that
// CreateUser did not return.
func (u *User) base() *gwbase.User {
	if u == nil {
		return nil
	}
	return gwbase.UserOf(u)
}

func userInfo(info gwbase.UserInfo) UserInfo {
	var caps []AdminCapability
	if info.AdminCaps != nil {
		caps = make([]AdminCapability, len(info.AdminCaps))
		for i, capability := range info.AdminCaps {
			caps[i] = AdminCapability(capability)
		}
	}
	return UserInfo{
		ID: info.ID, DisplayName: info.DisplayName, Email: info.Email,
		Tenant: info.Tenant, LocalID: info.LocalID, Namespace: info.Namespace, Type: info.Type, AccountID: info.AccountID,
		Suspended: info.Suspended, Admin: info.Admin, System: info.System, MaxBuckets: info.MaxBuckets, AdminCaps: caps,
		UserQuota: Quota(info.UserQuota), BucketQuota: Quota(info.BucketQuota),
		DefaultPlacement: info.DefaultPlacement, DefaultStorageClass: info.DefaultStorageClass, PlacementTags: info.PlacementTags,
	}
}
