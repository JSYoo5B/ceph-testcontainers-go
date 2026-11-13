// Package expect evaluates captured observations of a ceph cluster against
// the conditions a test states, and explains each mismatch.
//
// It is optional: constructing, querying and watching a cluster needs only
// package ceph. A test that wants a precise verdict on one observation, such
// as "these pinned OSDs are up and in and no other owned OSD exists", states
// it as an expectation and evaluates the observation it already has:
//
//	states, err := observer.OSDStates(ctx)
//	if err != nil {
//		t.Fatal(err) // a failed query is never a mismatch
//	}
//	result, err := expect.OSDs{Select: baseline, Up: true, In: true, Exact: true}.Evaluate(states)
//	if err != nil {
//		t.Fatal(err) // an invalid expectation or a malformed observation
//	}
//	if !result.Matched {
//		t.Fatal(result) // names each OSD and the field that differs
//	}
//
// Every Evaluate is pure: it reads only its arguments, performs no query,
// wait or retry, and returns a result that shares no slice or map with them.
// A mismatch is a result; an error means there is no verdict. Polling,
// retries and assertion failures stay with the test and its own tools.
// Evaluations of service observations live in the service's own expect
// package, such as cephfs/expect, rbd/expect or rgw/expect.
package expect
