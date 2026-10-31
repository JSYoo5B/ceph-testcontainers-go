//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import "testing"

// TestVerificationJSONFileNativeReceipt is one controlled observation of a
// single MON generation on the engine's non-rotating json-file driver. It
// connects the maintained fake-engine ledgers of the verifier (selection and
// read order, generation identity, collection limits) to the real engine for
// one source: an early structural WRN sentinel followed by INF noise that
// overflows the diagnostic tail, a quoted-WRN INF entry, and a tail WRN
// sentinel. The log-positive receipt must show both sentinels and zero log
// gaps; the refusal of an unsupported driver is a separate result that this
// test does not produce.
func TestVerificationJSONFileNativeReceipt(t *testing.T) {
	testVerificationJSONFileNativeReceipt(t)
}
