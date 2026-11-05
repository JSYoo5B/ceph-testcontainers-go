//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// A call meant to succeed fails at its own stage on a nonzero exit, also
// when its output looks right, and on an error; the message holds the
// stage, the status and a bounded part of standard error, never standard
// output.
func TestNativeSucceededRequiresExitZero(t *testing.T) {
	version := []byte("ceph version 20.2.4 (abc) tentacle (stable)\n")
	if err := nativeSucceeded("version", ceph.NativeCommandResult{Stdout: version}, nil); err != nil {
		t.Fatalf("an exit 0 call failed: %v", err)
	}
	stderr := append([]byte("partial failure\n"), make([]byte, 4096)...)
	err := nativeSucceeded("version", ceph.NativeCommandResult{Stdout: version, Stderr: stderr, ExitCode: 3}, nil)
	if err == nil || !strings.HasPrefix(err.Error(), "version: exit 3") || !strings.Contains(err.Error(), "partial failure") ||
		strings.Contains(err.Error(), "tentacle") || len(err.Error()) > 8192 {
		t.Fatalf("a nonzero exit with the expected output returned %v", err)
	}
	err = nativeSucceeded("write object", ceph.NativeCommandResult{TimedOut: true}, ceph.ErrNativeCommandIncomplete)
	if !errors.Is(err, ceph.ErrNativeCommandIncomplete) || !strings.HasPrefix(err.Error(), "write object: ") {
		t.Fatalf("a failed call returned %v", err)
	}
}
