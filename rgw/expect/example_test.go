package expect_test

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

// A second root user of an account must report the account's aggregate, not
// its own: the evaluation names the scope and owner even though the counters
// match.
func ExampleUserUsage_Evaluate() {
	observed := rgw.UserUsage{
		UserID: "account_tenant$tc-account-root-1", Tenant: "account_tenant", Scope: "user", OwnerID: "account_tenant$tc-account-root-1",
		NumObjects: 1, SizeBytes: 47, SizeActualBytes: 4096,
	}
	want := expect.UserUsage{
		UserID: "account_tenant$tc-account-root-1", Scope: expect.UsageScopeAccount, OwnerID: "RGW11111111111111111",
		Objects: 1, SizeBytes: 47,
	}
	result, err := want.Evaluate(observed)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// usage of user account_tenant$tc-account-root-1 want scope=account owner=RGW11111111111111111 objects=1 size=47: scope: want account, observed user; owner: want RGW11111111111111111, observed account_tenant$tc-account-root-1
}

// While zone b cannot read from zone a, b's stream from a must be behind and
// a's stream from b caught up. Each direction is evaluated against the
// observation of its receiving zone; here a has not caught up with b either.
func ExampleMultisiteDataSync_Evaluate() {
	b := multisite.SyncStatus{Zone: "b", ZoneID: "zone-b", DataSources: []multisite.DataSyncStatus{
		{SourceZone: "a", SourceZoneID: "zone-a", Syncing: true, Shards: 128, IncrementalShards: 128, BehindShards: 3},
	}}
	a := multisite.SyncStatus{Zone: "a", ZoneID: "zone-a", DataSources: []multisite.DataSyncStatus{
		{SourceZone: "b", SourceZoneID: "zone-b", Syncing: true, Shards: 128, IncrementalShards: 128, BehindShards: 1},
	}}
	for _, check := range []struct {
		want     expect.MultisiteDataSync
		observed multisite.SyncStatus
	}{
		{expect.MultisiteDataSync{Zone: "b", Source: "a", State: expect.MultisiteDataSyncBehind}, b},
		{expect.MultisiteDataSync{Zone: "a", Source: "b", State: expect.MultisiteDataSyncCaughtUp}, a},
	} {
		result, err := check.want.Evaluate(check.observed)
		if err != nil {
			panic(err)
		}
		fmt.Println(result)
	}
	// Output:
	// data sync of zone b from a want behind (observed 128 shards: 128 incremental, 0 full, 3 behind, 0 recovering): match
	// data sync of zone a from b want caught-up (observed 128 shards: 128 incremental, 0 full, 1 behind, 0 recovering): caught-up: want true, observed false; behind-shards: want 0, observed 1
}
