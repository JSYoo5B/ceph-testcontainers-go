// Package expect evaluates captured observations of RGW fixtures against the
// conditions a test states, and explains each mismatch. Its evaluations read
// the observation types of package rgw and of its multisite extension,
// package rgw/multisite.
//
// It is optional: running and observing gateways needs only package rgw or
// rgw/multisite, and neither imports it. A test that wants a precise verdict
// on one observation, such as "two root users of one account report the same
// account aggregate" or "zone b's data sync from zone a is behind", states it
// as an expectation and evaluates the observation it already has:
//
//	usage, err := root.Usage(ctx)
//	if err != nil {
//		t.Fatal(err) // a failed query is never a mismatch
//	}
//	want := expect.UserUsage{UserID: root.ID(), Scope: expect.UsageScopeAccount, OwnerID: account.ID(), Objects: 1, SizeBytes: 47}
//	result, err := want.Evaluate(usage)
//	if err != nil {
//		t.Fatal(err) // an invalid expectation or a malformed observation
//	}
//	if !result.Matched {
//		t.Fatal(result) // names each identity or counter that differs
//	}
//
// Every Evaluate is pure: it reads only its arguments, performs no query,
// stats synchronization, wait or retry, and returns a result that shares no
// slice or map with them. A mismatch is a result; an error means there is no
// verdict. Polling, retries and assertion failures stay with the test and its
// own tools. A match never proves that an object's content is intact.
package expect
