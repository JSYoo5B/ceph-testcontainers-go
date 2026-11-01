package rgw

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/testcontainers/testcontainers-go"
)

// testGateway returns the descriptor of gateway container ctr of c, built as
// Start builds it, without registering it.
func testGateway(c *ceph.Container, ctr testcontainers.Container, config Config) *Gateway {
	return newGateway(c, ctr, config, 0, 0, "", "")
}

// registerTestGateway records g in its cluster's gateway registry, where the
// user handles and the feature packages find the gateway they run through.
func registerTestGateway(g *Gateway) *Gateway {
	gateways(g.base().Cluster()).byName[g.config.Name] = g
	return g
}

// testOrigin returns a gateway core of c with scope that no descriptor holds:
// a handle created through it is served by the registered gateways of c in
// that scope.
func testOrigin(c *ceph.Container, scope Config) *gwbase.Core {
	return gwbase.NewCore(gwbase.Params{Cluster: c, Scope: gwbase.Scope{Realm: scope.Realm, Zonegroup: scope.Zonegroup, Zone: scope.Zone},
		Gateways: func() []*gwbase.Core { return gateways(c).cores() }})
}

// rgwAdminFixture is the native user information of an ordinary user.
func rgwAdminFixture(id, secret string, suspended bool, quota Quota) string {
	return gwtest.UserDocument(id, secret, suspended, gwbase.Quota(quota))
}

// rgwAdminOwnedTestUser is the confirmed ordinary user test-user created
// through gateway.
func rgwAdminOwnedTestUser(gateway *Gateway) *User {
	return newUser(&gwbase.User{Origin: gateway.base(), ID: "test-user", AccessKey: "PRIVATE-ACCESS", SecretKey: "PRIVATE-SECRET",
		State: &gwbase.UserState{Created: true, Identity: &gwbase.CreationIdentity{OriginalType: "rgw"}}})
}

// editedUser returns a separate handle over a copy of the private state of u,
// edited by edit. The copy shares the lifecycle state of u.
func editedUser(u *User, edit func(*gwbase.User)) *User {
	copied := *u.base()
	edit(&copied)
	return newUser(&copied)
}
