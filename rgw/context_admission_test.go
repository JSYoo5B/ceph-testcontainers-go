package rgw

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// The typed user operations and the Admin escape hatch wait for the cluster
// mutex only until the caller's context ends. A queued call sends no
// radosgw-admin command and changes no handle state; no waiter is left behind
// to run once the mutex is free, and the same call succeeds with a fresh
// context.
func TestRGWUserOperationsWaitForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, Quota{MaxSizeBytes: -1, MaxObjects: -1})
	suspended := rgwAdminFixture("test-user", "PRIVATE-SECRET", true, Quota{MaxSizeBytes: -1, MaxObjects: -1})
	quota := Quota{Enabled: true, MaxSizeBytes: 1048576, MaxObjects: 12}
	limited := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, quota)
	cases := map[string]struct {
		responses []rgwAdminResponse
		call      func(context.Context, *Gateway, *User) error
	}{
		"Admin": {[]rgwAdminResponse{{data: "[]"}}, func(ctx context.Context, g *Gateway, _ *User) error {
			_, err := g.Admin(ctx, "user", "list")
			return err
		}},
		"CreateUser": {[]rgwAdminResponse{{data: "[]"}, {data: native}}, func(ctx context.Context, g *Gateway, _ *User) error {
			_, err := g.CreateUser(ctx, UserConfig{ID: "test-user"})
			return err
		}},
		"Info": {[]rgwAdminResponse{{data: native}}, func(ctx context.Context, _ *Gateway, u *User) error {
			_, err := u.Info(ctx)
			return err
		}},
		"SetQuota": {[]rgwAdminResponse{{data: native}, {}, {}, {data: limited}}, func(ctx context.Context, _ *Gateway, u *User) error {
			return u.SetQuota(ctx, quota)
		}},
		"Suspend": {[]rgwAdminResponse{{data: native}, {}, {data: suspended}}, func(ctx context.Context, _ *Gateway, u *User) error {
			return u.Suspend(ctx, true)
		}},
		"Remove": {[]rgwAdminResponse{{data: `["test-user"]`}, {data: native}, {}}, func(ctx context.Context, _ *Gateway, u *User) error {
			return u.Remove(ctx)
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gateway, ctr := newRGWAdminTestGateway(Config{}, tc.responses...)
			user := rgwAdminOwnedTestUser(gateway)
			clustertest.AssertContextLockQueued(t, testhelper.TestState(gateway.base().Cluster()).Mutex, func(ctx context.Context) error {
				return tc.call(ctx, gateway, user)
			})
			if len(ctr.calls) != 0 || user.base().State.Removed || len(ctr.responses) != len(tc.responses) {
				t.Fatalf("a queued call reached radosgw-admin or changed the handle: %v", ctr.calls)
			}
			if err := tc.call(t.Context(), gateway, user); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
			if len(ctr.responses) != 0 {
				t.Fatalf("the fresh call left %d native responses unused", len(ctr.responses))
			}
		})
	}
}
