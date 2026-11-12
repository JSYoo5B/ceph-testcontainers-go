package expect_test

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect"
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
