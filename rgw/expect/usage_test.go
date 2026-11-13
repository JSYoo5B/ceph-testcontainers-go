package expect

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func userUsage(objects, size uint64) rgw.UserUsage {
	return rgw.UserUsage{UserID: "tc-user", Scope: "user", OwnerID: "tc-user", NumObjects: objects, SizeBytes: size,
		SizeActualBytes: objects * 4096, LastStatsSync: "2026-11-13T00:00:00.000000Z", LastStatsUpdate: "2026-11-13T00:00:00.000000Z"}
}

// accountUsage is the account aggregate as the account root root reads it.
func accountUsage(root string, objects, size uint64) rgw.UserUsage {
	return rgw.UserUsage{UserID: root, Tenant: "account_tenant", Scope: "account", OwnerID: "RGW11111111111111111",
		NumObjects: objects, SizeBytes: size, SizeActualBytes: objects * 4096}
}

func TestUserUsageMatchesTheOrdinaryUserLifecycle(t *testing.T) {
	for _, stage := range []struct{ objects, size uint64 }{{0, 0}, {4, 4096}, {5, 5120}, {0, 0}} {
		want := UserUsage{UserID: "tc-user", Scope: UsageScopeUser, Objects: stage.objects, SizeBytes: stage.size}
		got, err := want.Evaluate(userUsage(stage.objects, stage.size))
		if err != nil || !got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("stage %+v: %+v %v", stage, got, err)
		}
		// The user scope resolves the owner to the user itself.
		if got.Expectation.OwnerID != "tc-user" {
			t.Fatalf("resolved owner: %+v", got.Expectation)
		}
	}
	got, _ := UserUsage{UserID: "tc-user", Scope: UsageScopeUser}.Evaluate(userUsage(0, 0))
	if s := got.String(); s != "usage of user tc-user want scope=user owner=tc-user objects=0 size=0: match" {
		t.Fatalf("summary: %q", s)
	}
}

// Zero objects and zero bytes are expectations of their own: leftover
// objects do not pass as an empty user.
func TestUserUsageZeroIsAnExpectation(t *testing.T) {
	got, err := UserUsage{UserID: "tc-user", Scope: UsageScopeUser}.Evaluate(userUsage(1, 1024))
	want := []UserUsageMismatch{
		{Reason: UserUsageMismatchObjects, Want: "0", Observed: "1"},
		{Reason: UserUsageMismatchSize, Want: "0", Observed: "1024"},
	}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("leftover object: %+v %v", got, err)
	}
	if s := got.String(); !strings.HasSuffix(s, ": objects: want 0, observed 1; size: want 0, observed 1024") {
		t.Fatalf("summary: %q", s)
	}
}

// SizeBytes is the logical size; the rounded accounting size is not read.
func TestUserUsageComparesLogicalNotRoundedSize(t *testing.T) {
	observed := userUsage(1, 47)
	observed.SizeActualBytes = 4096
	if got, err := (UserUsage{UserID: "tc-user", Scope: UsageScopeUser, Objects: 1, SizeBytes: 47}).Evaluate(observed); err != nil || !got.Matched {
		t.Fatalf("logical size: %+v %v", got, err)
	}
	got, err := UserUsage{UserID: "tc-user", Scope: UsageScopeUser, Objects: 1, SizeBytes: 4096}.Evaluate(observed)
	if err != nil || !slices.Equal(got.Mismatches, []UserUsageMismatch{{Reason: UserUsageMismatchSize, Want: "4096", Observed: "47"}}) {
		t.Fatalf("rounded size passed as logical: %+v %v", got, err)
	}
}

// Two root users of one account report the same aggregate under their own
// user ID; neither is its own owner.
func TestUserUsageAccountRootsShareTheAggregateOwner(t *testing.T) {
	for _, root := range []string{"account_tenant$tc-account-root-0", "account_tenant$tc-account-root-1"} {
		want := UserUsage{UserID: root, Tenant: "account_tenant", Scope: UsageScopeAccount, OwnerID: "RGW11111111111111111", Objects: 1, SizeBytes: 47}
		if got, err := want.Evaluate(accountUsage(root, 1, 47)); err != nil || !got.Matched {
			t.Fatalf("root %s: %+v %v", root, got, err)
		}
	}
	// The other root's observation is not this root's.
	want := UserUsage{UserID: "account_tenant$tc-account-root-0", Scope: UsageScopeAccount, OwnerID: "RGW11111111111111111", Objects: 1, SizeBytes: 47}
	got, err := want.Evaluate(accountUsage("account_tenant$tc-account-root-1", 1, 47))
	if err != nil || !slices.Equal(got.Mismatches, []UserUsageMismatch{{Reason: UserUsageMismatchUser, Want: "account_tenant$tc-account-root-0", Observed: "account_tenant$tc-account-root-1"}}) {
		t.Fatalf("other root: %+v %v", got, err)
	}
}

// A wrong owner or scope is reported even when the counters match.
func TestUserUsageReportsOwnerAndScopeWhenCountersMatch(t *testing.T) {
	root := "account_tenant$tc-account-root-0"
	want := UserUsage{UserID: root, Tenant: "account_tenant", Scope: UsageScopeAccount, OwnerID: "RGW11111111111111111", Objects: 1, SizeBytes: 47}
	// Counters aggregated for the user alone.
	individual := rgw.UserUsage{UserID: root, Tenant: "account_tenant", Scope: "user", OwnerID: root, NumObjects: 1, SizeBytes: 47}
	got, err := want.Evaluate(individual)
	if err != nil || !slices.Equal(got.Mismatches, []UserUsageMismatch{
		{Reason: UserUsageMismatchScope, Want: "account", Observed: "user"},
		{Reason: UserUsageMismatchOwner, Want: "RGW11111111111111111", Observed: root},
	}) {
		t.Fatalf("individual aggregate: %+v %v", got, err)
	}
	// Another account's aggregate, with another tenant and wrong totals.
	other := accountUsage(root, 2, 94)
	other.Tenant, other.OwnerID = "other_tenant", "RGW22222222222222222"
	got, err = want.Evaluate(other)
	if err != nil || !slices.Equal(got.Mismatches, []UserUsageMismatch{
		{Reason: UserUsageMismatchTenant, Want: "account_tenant", Observed: "other_tenant"},
		{Reason: UserUsageMismatchOwner, Want: "RGW11111111111111111", Observed: "RGW22222222222222222"},
		{Reason: UserUsageMismatchObjects, Want: "1", Observed: "2"},
		{Reason: UserUsageMismatchSize, Want: "47", Observed: "94"},
	}) {
		t.Fatalf("other account: %+v %v", got, err)
	}
	text := got.String()
	for _, part := range []string{"usage of user " + root + " tenant=account_tenant want scope=account owner=RGW11111111111111111 objects=1 size=47: ",
		"tenant: want account_tenant, observed other_tenant", "owner: want RGW11111111111111111, observed RGW22222222222222222"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
	// An unselected tenant is not compared.
	want.Tenant = ""
	if got, err := want.Evaluate(accountUsage(root, 1, 47)); err != nil || !got.Matched {
		t.Fatalf("unselected tenant: %+v %v", got, err)
	}
}

func TestUserUsageSummaryBoundsMismatches(t *testing.T) {
	e := UserUsageEvaluation{Expectation: UserUsage{UserID: "u", Scope: UsageScopeUser, OwnerID: "u"}}
	for range evaluationShown + 2 {
		e.Mismatches = append(e.Mismatches, UserUsageMismatch{Reason: UserUsageMismatchObjects, Want: "0", Observed: "1"})
	}
	if s := e.String(); strings.Count(s, "objects:") != evaluationShown || !strings.HasSuffix(s, "; 2 more") {
		t.Fatalf("summary: %q", s)
	}
}

func TestUserUsageRefusesInvalidExpectations(t *testing.T) {
	for name, want := range map[string]UserUsage{
		"no user":               {Scope: UsageScopeUser},
		"no scope":              {UserID: "u"},
		"unknown scope":         {UserID: "u", Scope: "bucket"},
		"user scope owner":      {UserID: "u", Scope: UsageScopeUser, OwnerID: "RGW1"},
		"account without owner": {UserID: "u", Scope: UsageScopeAccount},
		"account owned by user": {UserID: "u", Scope: UsageScopeAccount, OwnerID: "u"},
	} {
		if _, err := want.Evaluate(userUsage(0, 0)); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestUserUsageMalformedObservationIsNotAMismatch(t *testing.T) {
	want := UserUsage{UserID: "tc-user", Scope: UsageScopeUser}
	for name, observed := range map[string]rgw.UserUsage{
		"zero":                  {},
		"no owner":              {UserID: "tc-user", Scope: "user"},
		"unknown scope":         {UserID: "tc-user", Scope: "bucket", OwnerID: "tc-user"},
		"user scope owned away": {UserID: "tc-user", Scope: "user", OwnerID: "RGW1"},
		"account owned by user": {UserID: "tc-user", Scope: "account", OwnerID: "tc-user"},
	} {
		got, err := want.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Mismatches != nil {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
}
