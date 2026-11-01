package rgw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/testcontainers/testcontainers-go"
)

// newRGWAccountTestGateway returns the registered default gateway over the
// native user and account model, in a cluster whose control container answers
// the service map and pool listing.
func newRGWAccountTestGateway() (*Gateway, *gwtest.AccountContainer) {
	base := gwtest.NewPlacementContainer()
	f := gwtest.NewAccountContainer(base)
	owner := testCluster(base, testhelper.TestConfig{StartupTimeout: time.Minute}, map[string]testcontainers.Container{"rgw": f})
	return registerTestGateway(newGateway(owner, f, Config{Name: "default"}, 7480, 0, "", "")), f
}

// testRootAccount records the native account id with tenant in f and returns
// a handle over its confirmed identity created through g, as package
// rgw/account creates it.
func testRootAccount(g *Gateway, f *gwtest.AccountContainer, id, tenant string) (*gwtest.RootAccount, *gwbase.Account) {
	f.SeedAccount(id, "fixture", tenant)
	account := gwtest.ConfirmedAccount(g.base(), gwtest.Scope(), id, tenant, f.VersionTag)
	return gwtest.NewRootAccount(account), account
}

func mustRGWUserJSON(t *testing.T, document map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal("encode native fixture")
	}
	return data
}

func TestRGWTenantUsersPreserveCanonicalIDForEveryOperation(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	users := make([]*User, 0, 3)
	for _, tenant := range []string{"", "team_a", "team_b"} {
		user, err := g.CreateUser(t.Context(), UserConfig{ID: "same", Tenant: tenant, AdminCaps: "users=read"})
		if err != nil {
			t.Fatal(err)
		}
		if user.ID() != gwbase.FullUserID(tenant, "", "same") {
			t.Fatal("canonical ID missing tenant")
		}
		users = append(users, user)
	}
	if _, err := g.CreateUser(t.Context(), UserConfig{ID: "same", Tenant: "team_a"}); err == nil {
		t.Fatal("existing tenanted user accepted")
	}
	f.Calls = nil
	for i, user := range users {
		quota := Quota{Enabled: true, MaxSizeBytes: 1024, MaxObjects: int64(i + 1)}
		if err := user.SetQuota(t.Context(), quota); err != nil {
			t.Fatal(err)
		}
		if err := user.SetBucketQuota(t.Context(), quota); err != nil {
			t.Fatal(err)
		}
		unlimited := Quota{MaxSizeBytes: -1, MaxObjects: -1}
		if err := user.SetQuota(t.Context(), unlimited); err != nil {
			t.Fatal(err)
		}
		if err := user.SetBucketQuota(t.Context(), unlimited); err != nil {
			t.Fatal(err)
		}
		info, err := user.Info(t.Context())
		if err != nil || info.UserQuota != unlimited || info.BucketQuota != unlimited {
			t.Fatal("ordinary or tenant negative user/bucket limits changed to zero")
		}
		if err := user.SetQuota(t.Context(), quota); err != nil {
			t.Fatal(err)
		}
		if err := user.SetBucketQuota(t.Context(), quota); err != nil {
			t.Fatal(err)
		}
		if err := user.Suspend(t.Context(), true); err != nil {
			t.Fatal(err)
		}
		info, err = user.Info(t.Context())
		if err != nil || info.ID != user.ID() || info.LocalID != "same" || info.UserQuota != quota || !info.Suspended {
			t.Fatal("tenant user native policy wrong")
		}
		if err := user.Suspend(t.Context(), false); err != nil {
			t.Fatal(err)
		}
		if err := user.Remove(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := user.Remove(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.Users) != 0 {
		t.Fatal("tenant cleanup left another user")
	}
	for _, args := range f.Calls {
		for i, arg := range args {
			if arg == "--uid" && strings.Contains(args[i+1], "team_") && !strings.Contains(args[i+1], "$") {
				t.Fatal("tenant user method used local ID")
			}
		}
	}
}

// The account half of the former fresh-root lifecycle test is in rgw/account
// (TestAccountFreshQuotaAndNonpurgeLifecycle).
func TestRGWAccountRootIdentityAndAccountOwnedQuota(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	account, _ := testRootAccount(g, f, "RGW12345678901234567", "account_team")
	root, err := g.CreateUser(t.Context(), UserConfig{ID: "fixture-root", DisplayName: "FixtureRoot", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Info(t.Context())
	if err != nil || info.AccountID != "RGW12345678901234567" || info.Type != "root" || info.Tenant != "account_team" || info.Admin || info.System {
		t.Fatal("root account identity wrong")
	}
	if err := root.SetQuota(t.Context(), Quota{Enabled: true, MaxSizeBytes: -1, MaxObjects: 1}); err == nil {
		t.Fatal("ineffective per-user account quota accepted")
	}
	if err := root.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "user") && slices.Contains(args, "create") && (!slices.Contains(args, "--account-root") || !slices.Contains(args, "RGW12345678901234567")) {
			t.Fatalf("root creation did not name its account: %v", args)
		}
	}
}

// Gateway.CreateUser runs through the gateway it is called on, so it refuses
// an account that a gateway of another cluster or scope created before any
// native command.
func TestRGWCreateUserRefusesAnAccountOfAnotherClusterOrScope(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	for name, origin := range map[string]*gwbase.Core{
		"cluster": gwtest.Foreign(),
		"scope":   testOrigin(g.base().Cluster(), Config{Zonegroup: "other-group", Zone: "other-zone"}),
	} {
		t.Run(name, func(t *testing.T) {
			_, account := testRootAccount(g, f, "RGW12345678901234567", "team")
			account.Origin = origin
			f.Calls = nil
			user, err := g.CreateUser(t.Context(), UserConfig{ID: "root", Account: gwtest.NewRootAccount(account)})
			if user != nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(f.Calls) != 0 || len(f.Users) != 0 {
				t.Fatalf("a foreign account reached root creation: %v %v calls=%v", user, err, f.Calls)
			}
		})
	}
	var missing *gwtest.RootAccount
	for _, account := range []RootAccount{missing, gwtest.NewRootAccount(nil), &gwtest.RootAccount{}} {
		if user, err := g.CreateUser(t.Context(), UserConfig{ID: "root", Account: account}); user != nil || err == nil || len(f.Calls) != 0 {
			t.Fatalf("an account without identity reached root creation: %v", err)
		}
	}
}

func TestRGWCreatedOrdinaryUserRejectsExternalAccountMigration(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"ordinary migrated to account root", func(user map[string]any) { user["type"], user["account_id"] = "root", "RGW12345678901234567" }},
		{"ordinary associated with account", func(user map[string]any) { user["account_id"] = "RGW12345678901234567" }},
		{"native type changed", func(user map[string]any) { user["type"] = "none" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWAccountTestGateway()
			user, err := g.CreateUser(t.Context(), UserConfig{ID: "original", Tenant: "team"})
			if err != nil {
				t.Fatal(err)
			}
			test.edit(f.Users[user.ID()])
			assertRGWUserAssociationRefusesOperations(t, f, user)
		})
	}
}

func TestRGWAccountRootRejectsChangedAssociationAndAccountLifetime(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*gwbase.Account, *User, *gwtest.AccountContainer)
	}{
		{"different account", func(_ *gwbase.Account, user *User, f *gwtest.AccountContainer) {
			f.Users[user.ID()]["account_id"] = "RGW76543210987654321"
		}},
		{"association cleared", func(_ *gwbase.Account, user *User, f *gwtest.AccountContainer) { f.Users[user.ID()]["account_id"] = "" }},
		{"root converted to IAM user", func(_ *gwbase.Account, user *User, f *gwtest.AccountContainer) { f.Users[user.ID()]["type"] = "rgw" }},
		{"account recreated with same ID", func(_ *gwbase.Account, _ *User, f *gwtest.AccountContainer) {
			f.VersionTag = "different-account-lifetime"
		}},
		{"account tag missing", func(_ *gwbase.Account, _ *User, f *gwtest.AccountContainer) { f.MissingTag = true }},
		{"account absent", func(_ *gwbase.Account, _ *User, f *gwtest.AccountContainer) { f.Account = nil }},
		{"account tenant changed", func(_ *gwbase.Account, _ *User, f *gwtest.AccountContainer) { f.Account["tenant"] = "other" }},
		{"account removed through copied handle", func(a *gwbase.Account, _ *User, _ *gwtest.AccountContainer) {
			copied := *a
			copied.State.Removed = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWAccountTestGateway()
			account, identity := testRootAccount(g, f, "RGW12345678901234567", "team")
			user, err := g.CreateUser(t.Context(), UserConfig{ID: "original-root", Account: account})
			if err != nil {
				t.Fatal(err)
			}
			copy := *user
			if _, err := (&copy).Info(t.Context()); err != nil {
				t.Fatalf("original account association was not usable: %v", err)
			}
			test.edit(identity, user, f)
			assertRGWUserAssociationRefusesOperations(t, f, &copy)
		})
	}
}

func assertRGWUserAssociationRefusesOperations(t *testing.T, f *gwtest.AccountContainer, user *User) {
	t.Helper()
	before := mustRGWUserJSON(t, f.Users[user.ID()])
	f.Calls = nil
	if _, err := user.Info(t.Context()); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("changed user/account identity accepted or secret leaked: %v", err)
	}
	if err := user.Suspend(t.Context(), true); err == nil {
		t.Fatal("changed user/account identity reached suspension")
	}
	if err := user.Remove(t.Context()); err == nil || user.base().State.Removed {
		t.Fatal("changed user/account identity reached deletion or lost its handle")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "suspend") || slices.Contains(args, "rm") {
			t.Fatal("failed identity check issued native mutation")
		}
	}
	if !bytes.Equal(before, mustRGWUserJSON(t, f.Users[user.ID()])) {
		t.Fatal("failed identity check changed native user keys or policy")
	}
}

func TestRGWAccountRootCapturesImmutableDescriptorAndSharedLifecycle(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	account, identity := testRootAccount(g, f, "RGW12345678901234567", "team")
	user, err := g.CreateUser(t.Context(), UserConfig{ID: "root", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	// Private descriptor mutation stands in for future public view fields: it
	// must not redirect a user to a different account identity.
	identity.ID, identity.Tenant, identity.Scope.ZoneID = "RGW76543210987654321", "foreign", "foreign"
	if info, err := user.Info(t.Context()); err != nil || info.AccountID != "RGW12345678901234567" || info.Tenant != "team" {
		t.Fatalf("mutable account descriptor redirected the confirmed root: %v", err)
	}
	f.Calls = nil
	identity.State.Removed = true
	if _, err := user.Info(t.Context()); err == nil {
		t.Fatal("copied account descriptor did not share removal state")
	}
}

func TestRGWAccountRootUnconfirmedCreationCannotAdoptLaterKeys(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*gwtest.AccountContainer)
	}{
		{"wrong association in successful reply", func(f *gwtest.AccountContainer) {
			f.OnUserCreate = func(user map[string]any) { user["account_id"] = "RGW76543210987654321" }
		}},
		{"wrong type in successful reply", func(f *gwtest.AccountContainer) { f.OnUserCreate = func(user map[string]any) { user["type"] = "rgw" } }},
		{"native creation applied but reply lost", func(f *gwtest.AccountContainer) { f.LostUserCreate = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWAccountTestGateway()
			account, _ := testRootAccount(g, f, "RGW12345678901234567", "team")
			test.edit(f)
			user, err := g.CreateUser(t.Context(), UserConfig{ID: "root", Account: account})
			if err == nil || user == nil || user.base().State.Identity != nil {
				t.Fatal("unconfirmed root creation adopted a native identity")
			}
			if _, _, err := user.Credentials(); err == nil {
				t.Fatal("unconfirmed root creation exposed unconfirmed credentials")
			}
			// A later coherent reply cannot retroactively establish ownership.
			f.Users[user.ID()]["type"], f.Users[user.ID()]["account_id"] = "root", "RGW12345678901234567"
			f.Calls = nil
			if _, err := user.Info(t.Context()); err == nil || len(f.Calls) != 0 {
				t.Fatal("unconfirmed creation adopted later native root credentials")
			}
			if err := user.Remove(t.Context()); err == nil || len(f.Calls) != 0 {
				t.Fatal("unconfirmed creation attempted native root removal")
			}
		})
	}
}

// The account half of the former validation test is in rgw/account
// (TestAccountQuotaValidationDriftAndCancellation).
func TestRGWTenantAndAccountRootValidation(t *testing.T) {
	for _, tenant := range []string{"bad$tenant", "bad:tenant", "bad-tenant", "RGW12345678901234567"} {
		g, f := newRGWAccountTestGateway()
		if _, err := g.CreateUser(t.Context(), UserConfig{ID: "same", Tenant: tenant}); err == nil || len(f.Calls) != 0 {
			t.Fatal("invalid tenant reached native command")
		}
	}
	g, f := newRGWAccountTestGateway()
	account, _ := testRootAccount(g, f, "RGW12345678901234567", "team")
	if _, err := g.CreateUser(t.Context(), UserConfig{ID: "root", Tenant: "other", Account: account}); err == nil {
		t.Fatal("cross-tenant account membership accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f.Calls = nil
	if _, err := g.CreateUser(ctx, UserConfig{ID: "cancelled", Account: account}); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatal("canceled account root creation reached native command")
	}
	if !servicehook.TryLock(g.base().Cluster()) {
		t.Fatal("canceled root creation retained the cluster mutex")
	}
	servicehook.Unlock(g.base().Cluster())
}
