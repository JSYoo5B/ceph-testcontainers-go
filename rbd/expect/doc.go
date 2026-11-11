// Package expect evaluates captured observations of RBD fixtures against the
// conditions a test states, and explains each mismatch. Its evaluations read
// the rbd-mirror link observations of package rbd/mirror.
//
// It is optional: running, observing and watching a link needs only package
// mirror, and package rbd does not import it. A test that wants a precise verdict on one observation, such as
// "the survivor leads while the stopped member stays in the cohort" or "the
// same image pair replays through a new receiving instance", states it as an
// expectation and evaluates the observation it already has:
//
//	status, err := link.ImageStatus(ctx, "volume")
//	if err != nil {
//		t.Fatal(err) // a failed query is never a mismatch
//	}
//	result, err := expect.MirrorImage{Select: baseline, Replaying: true, NewInstance: true}.Evaluate(status)
//	if err != nil {
//		t.Fatal(err) // an invalid expectation or a malformed observation
//	}
//	if !result.Matched {
//		t.Fatal(result) // names each identity or replay field that differs
//	}
//
// Every Evaluate is pure: it reads only its arguments, performs no query,
// wait or retry, and returns a result that shares no slice or map with them.
// A mismatch is a result; an error means there is no verdict. Polling,
// retries and assertion failures stay with the test and its own tools. A
// match never proves that a write or a checkpoint arrived.
package expect
