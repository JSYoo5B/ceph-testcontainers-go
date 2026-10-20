//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

// TestPolicyOutcome classifies CLI results of policy clients, recorded with
// the 20.2.4 role images. The rejection without a handle_auth_bad_method line
// is the output of CI job 115977971757
// (TestClientMessengerPolicyOverridesSecureOnly): the MON refused the
// CRC-only client, and the CLI exited before Ceph's log thread wrote the MON
// client's line. A native run with the CLI pinned to one busy CPU lost the
// line in 2 of 200 rejections.
func TestPolicyOutcome(t *testing.T) {
	const denied = "[errno 13] RADOS permission denied (error connecting to the cluster)\n"
	for _, tc := range []struct {
		name   string
		code   int
		output string
		want   string
	}{
		{"success", 0, "e1: 1 mons at {a=[v2:172.19.0.2:3300/0]} removed_ranks: {} disallowed_leaders: {}, election epoch 3, leader 0 a, quorum 0 a\n", "accepted"},
		{"rejection with the MON client's line", 13, "2026-10-17T22:33:51.415+0000 7e59b8e68640 -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (95) Operation not supported\n" + denied, "rejected"},
		{"rejection whose line was not flushed", 13, denied, "rejected"},
		{"timeout", 124, "", "unanswered"},
		// Neither a refused authentication nor a timeout.
		{"connect timeout", 1, "[errno 110] RADOS timed out (error connecting to the cluster)\n", "unexpected"},
		{"line without a refused connect", 1, "2026-10-17T22:33:51.415+0000 7e59b8e68640 -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (95) Operation not supported\n", "unexpected"},
		{"EACCES from the command", 13, "Error EACCES: access denied\n", "unexpected"},
	} {
		if got := policyOutcome(tc.code, tc.output); got != tc.want {
			t.Errorf("%s: exit %d %q is %s, want %s", tc.name, tc.code, tc.output, got, tc.want)
		}
	}
}
