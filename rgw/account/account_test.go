package account

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
)

// newAccountTestGateway returns the gateway core over the native user and
// account model, in a cluster whose control container answers the service map
// and pool listing.
func newAccountTestGateway() (*gwtest.Gateway, *gwtest.AccountContainer) {
	base := gwtest.NewPlacementContainer()
	f := gwtest.NewAccountContainer(base)
	return gwtest.NewGateway(f, base), f
}

// createRoot creates the root user id of a through g, as rgw.Gateway.CreateUser
// does with the cluster mutex held.
func createRoot(t *testing.T, g *gwtest.Gateway, a *gwbase.Account, id string) *gwbase.User {
	t.Helper()
	servicehook.Lock(g.Cluster)
	defer servicehook.Unlock(g.Cluster)
	user, err := g.Core.CreateUser(t.Context(), gwbase.UserSpec{ID: id, Tenant: a.Tenant, DisplayName: "FixtureRoot", Account: a})
	if err != nil {
		t.Fatal(err)
	}
	return user
}

// ownedRoot reads root through g, as its rgw.User operations do.
func ownedRoot(t *testing.T, g *gwtest.Gateway, root *gwbase.User) (*gwbase.NativeUser, error) {
	t.Helper()
	servicehook.Lock(g.Cluster)
	defer servicehook.Unlock(g.Cluster)
	return g.Core.OwnedUser(t.Context(), root)
}

// The root user half of the former fresh-root lifecycle test is in rgw
// (TestRGWAccountRootIdentityAndAccountOwnedQuota).
func TestAccountFreshQuotaAndNonpurgeLifecycle(t *testing.T) {
	g, f := newAccountTestGateway()
	created, err := create(t.Context(), g.Core, Config{ID: "RGW12345678901234567", Name: "fixture account", Tenant: "account_team"})
	if err != nil {
		t.Fatal(err)
	}
	a := newAccount(created)
	if _, err := create(t.Context(), g.Core, Config{ID: a.ID()}); err == nil {
		t.Fatal("existing account adopted")
	}
	root := createRoot(t, g, created, "fixture-root")
	if native, err := ownedRoot(t, g, root); err != nil || native.Info.AccountID != a.ID() || native.Info.Type != "root" || native.Info.Tenant != "account_team" {
		t.Fatalf("root account identity wrong: %v", err)
	}
	q := rgw.Quota{Enabled: true, MaxSizeBytes: 2048, MaxObjects: 3}
	if err := a.SetQuota(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	if err := a.SetBucketQuota(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	info, err := a.Info(t.Context())
	if err != nil || info.AccountQuota != q || info.BucketQuota != q {
		t.Fatal("account quota not read back")
	}
	q = rgw.Quota{Enabled: true, MaxSizeBytes: -1, MaxObjects: 1}
	if err := a.SetQuota(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	info, err = a.Info(t.Context())
	if err != nil || info.AccountQuota != q {
		t.Fatal("disabled account size was rounded to zero")
	}
	var disabledSizeArgument bool
	for _, args := range f.Calls {
		if slices.Contains(args, "quota") && slices.Contains(args, "set") && slices.Contains(args, "-2K") {
			disabledSizeArgument = true
		}
	}
	if !disabledSizeArgument {
		t.Fatal("disabled account size did not use native negative KiB")
	}
	f.FailRemove = true
	if err := a.Remove(t.Context()); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("nonempty account removal hidden or secret leaked")
	}
	if _, err := ownedRoot(t, g, root); err != nil {
		t.Fatal("failed account removal changed root")
	}
	delete(f.Users, root.ID)
	f.FailRemove = false
	copyHandle := *a
	if err := a.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := (&copyHandle).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "--purge-data") || slices.Contains(args, "--yes-i-really-mean-it") {
			t.Fatal("account helper used native destructive bypass")
		}
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d", "%t", "%10.3f"} {
		if value := fmt.Sprintf(format, a); value != "RGW account RGW12345678901234567" {
			t.Fatalf("formatted account walked its fields: %s", value)
		}
	}
}

func TestAccountRejectsUnconfirmedForeignAndReplacedLifetimes(t *testing.T) {
	for _, edit := range []func(*gwbase.Account, *gwtest.AccountContainer){
		func(a *gwbase.Account, _ *gwtest.AccountContainer) { a.Origin = gwtest.Foreign() },
		func(a *gwbase.Account, _ *gwtest.AccountContainer) { a.State.Confirmed = false },
		func(_ *gwbase.Account, f *gwtest.AccountContainer) { f.VersionTag = "replacement-lifetime" },
		func(_ *gwbase.Account, f *gwtest.AccountContainer) { f.Account["tenant"] = "different" },
	} {
		g, f := newAccountTestGateway()
		a, err := create(t.Context(), g.Core, Config{Name: "account"})
		if err != nil {
			t.Fatal(err)
		}
		edit(a, f)
		f.Calls = nil
		if err := newAccount(a).SetQuota(t.Context(), rgw.Quota{MaxSizeBytes: -1, MaxObjects: 1}); err == nil {
			t.Fatal("foreign/replaced account mutated")
		}
		for _, args := range f.Calls {
			if slices.Contains(args, "quota") {
				t.Fatal("failed ownership changed quota")
			}
		}
	}
	g, f := newAccountTestGateway()
	f.MissingTag = true
	a, err := create(t.Context(), g.Core, Config{Name: "partial"})
	if err == nil || a == nil || a.State.Confirmed {
		t.Fatal("missing metadata lifetime adopted")
	}
	if err := newAccount(a).Remove(t.Context()); err == nil {
		t.Fatal("unconfirmed account deleted")
	}
}

// The tenant and root user half of the former validation test is in rgw
// (TestRGWTenantAndAccountRootValidation).
func TestAccountQuotaValidationDriftAndCancellation(t *testing.T) {
	g, f := newAccountTestGateway()
	created, err := create(t.Context(), g.Core, Config{Name: "account", Tenant: "team"})
	if err != nil {
		t.Fatal(err)
	}
	a := newAccount(created)
	f.Calls = nil
	if err := a.SetQuota(t.Context(), rgw.Quota{MaxSizeBytes: 1, MaxObjects: 1}); err == nil || len(f.Calls) != 0 {
		t.Fatal("native KiB rounding silently changed caller byte limit")
	}
	f.UnrelatedQuota = true
	if err := a.SetQuota(t.Context(), rgw.Quota{Enabled: true, MaxSizeBytes: 1024, MaxObjects: 1}); err == nil {
		t.Fatal("unrelated account policy drift hidden")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f.Calls = nil
	if _, err := create(ctx, g.Core, Config{Name: "cancelled"}); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatal("canceled account creation lost context")
	}
	if _, err := a.Info(ctx); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatal("canceled account info reached native command")
	}
	if err := a.SetQuota(ctx, rgw.Quota{MaxSizeBytes: -1, MaxObjects: -1}); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatal("canceled account quota reached native command")
	}
	if err := a.Remove(ctx); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatal("canceled account removal reached native command")
	}
}

func TestAccountRequiresAnOwnedGatewayAndHandle(t *testing.T) {
	ctx := t.Context()
	if a, err := Create(ctx, nil, Config{}); a != nil || err == nil {
		t.Fatal("a nil gateway created an account")
	}
	if a, err := Create(ctx, &rgw.Gateway{}, Config{}); a != nil || err == nil {
		t.Fatal("a gateway without a cluster created an account")
	}
	for _, a := range []*Account{nil, {}} {
		if _, err := a.Info(ctx); err == nil {
			t.Fatal("an account without identity was read")
		}
		if err := a.SetQuota(ctx, rgw.Quota{}); err == nil {
			t.Fatal("an account without identity was changed")
		}
		if err := a.Remove(ctx); err == nil {
			t.Fatal("an account without identity was removed")
		}
		if a.ID() != "" {
			t.Fatal("an account without identity has an ID")
		}
	}
	// Only *Account satisfies the sealed root account of package rgw.
	var _ rgw.RootAccount = (*Account)(nil)
}
