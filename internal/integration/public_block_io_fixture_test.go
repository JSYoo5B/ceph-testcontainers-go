//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestBlockIOEIOFaultEvidence(t *testing.T) {
	const prior = "osd: I/O error during setup\n"
	digest := sha256.Sum256([]byte(prior))
	for _, tc := range []struct {
		log  string
		want bool
	}{
		{prior + "osd: starting\n", false},
		{"rotated: I/O error\n", false},
		{prior + "osd: Input/output error on write\n", true},
	} {
		got := checkBlockIOLogWindow(strings.NewReader(tc.log), int64(len(prior)), digest[:]) == nil
		if got != tc.want {
			t.Errorf("log %q accepted=%t, want %t", tc.log, got, tc.want)
		}
	}
	for _, tc := range []struct {
		result ceph.NativeCommandResult
		want   bool
	}{
		{ceph.NativeCommandResult{ExitCode: 1, Stderr: []byte("permission denied")}, false},
		{ceph.NativeCommandResult{ExitCode: 0, Stderr: []byte("input/output error")}, false},
		{ceph.NativeCommandResult{ExitCode: 5, Stderr: []byte("Input/output error")}, true},
		{ceph.NativeCommandResult{ExitCode: 110, Stderr: []byte("Connection timed out")}, true},
		{ceph.NativeCommandResult{TimedOut: true}, true},
	} {
		if got := blockIOExpectedFailure(tc.result); got != tc.want {
			t.Errorf("put %+v accepted=%t, want %t", tc.result, got, tc.want)
		}
	}
}
