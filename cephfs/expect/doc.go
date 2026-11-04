// Package expect evaluates captured observations of a CephFS filesystem
// against the conditions a test states, and explains each mismatch.
//
// It is optional: constructing, querying and watching a filesystem needs
// only package cephfs. A test that wants a precise verdict on one
// observation, such as "rank 0 is up:active on a daemon other than the one I
// killed and the restarted daemon is a standby again", states it as an
// expectation and evaluates the observation it already has:
//
//	status, err := fs.MDSStatus(ctx)
//	if err != nil {
//		t.Fatal(err) // a failed query is never a mismatch
//	}
//	want := expect.MDSs{
//		Ranks:   []expect.MDSRank{{Rank: 0, NotGID: old.GID}},
//		Owned:   true,
//		Exact:   true,
//		Standby: &expect.MDSStandbys{Count: 1, Daemons: []expect.MDSDaemon{{Name: old.Name, NotGID: old.GID}}},
//	}
//	result, err := want.Evaluate(*status)
//	if err != nil {
//		t.Fatal(err) // an invalid expectation or a malformed observation
//	}
//	if !result.Matched {
//		t.Fatal(result) // names each rank or standby and the field that differs
//	}
//
// Every Evaluate is pure: it reads only its arguments, performs no query,
// wait or retry, and returns a result that shares no slice with them. A
// mismatch is a result; an error means there is no verdict. Polling, retries
// and assertion failures stay with the test and its own tools. Evaluations
// of cluster observations are in package ceph/expect.
package expect
