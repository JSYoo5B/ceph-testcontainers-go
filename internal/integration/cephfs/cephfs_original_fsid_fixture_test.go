//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestCephFSOriginalFSIDCheckSeparatesFailedReadsFromChanges keeps a failed
// or timed-out FSID read apart from a returned FSID that differs: the first
// keeps its cause and never claims a change, the second names both
// identities.
func TestCephFSOriginalFSIDCheckSeparatesFailedReadsFromChanges(t *testing.T) {
	const want = "1b0e4c5e-0000-4000-8000-000000000001"
	expired := fmt.Errorf("ceph fsid: %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name    string
		data    string
		err     error
		cause   error
		changed bool
	}{
		{name: "same identity", data: want + "\n"},
		{name: "deadline", err: expired, cause: context.DeadlineExceeded},
		{name: "command failure", data: "partial", err: errors.New("exit status 1"), cause: nil},
		{name: "changed identity", data: "1b0e4c5e-0000-4000-8000-000000000002\n", changed: true},
		{name: "empty identity", data: "\n", changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := harness.CephFSOriginalFSIDCheck("source", want, []byte(tc.data), tc.err)
			if tc.err == nil && !tc.changed {
				if err != nil {
					t.Fatalf("unchanged identity: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("no error")
			}
			if got := strings.Contains(err.Error(), "FSID changed"); got != tc.changed {
				t.Fatalf("reported a change %t, want %t: %v", got, tc.changed, err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("lost the read error %v: %v", tc.err, err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("lost the cause %v: %v", tc.cause, err)
			}
			if tc.changed && !strings.Contains(err.Error(), want) {
				t.Fatalf("change does not name the expected identity: %v", err)
			}
		})
	}
}
