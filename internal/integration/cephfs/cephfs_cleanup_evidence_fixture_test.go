//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/containerd/errdefs"
)

func TestCephFSRemovalRequiresObservedAbsence(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		containerErr, authErr error
		want                  bool
	}{
		{"absent", errdefs.ErrNotFound, fs.ErrNotExist, true},
		{"wrapped absence", fmt.Errorf("inspect: %w", errdefs.ErrNotFound), fmt.Errorf("auth get: %w", fs.ErrNotExist), true},
		{"stopped but present", nil, fs.ErrNotExist, false},
		{"container observation unavailable", context.DeadlineExceeded, fs.ErrNotExist, false},
		{"auth still present", errdefs.ErrNotFound, nil, false},
		{"auth denied", errdefs.ErrNotFound, fs.ErrPermission, false},
		{"auth unavailable", errdefs.ErrNotFound, context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := cephFSRemovedMDSEvidence(tc.containerErr, tc.authErr); (err == nil) != tc.want {
				t.Fatalf("removal evidence error=%v, want success=%t", err, tc.want)
			}
		})
	}
}
