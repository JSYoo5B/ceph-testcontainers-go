//go:build all || (integration && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

func TestCephFSDirectoryAdditionObservationDiagnostics(t *testing.T) {
	const private = "PRIVATE native content"
	for _, tc := range []struct {
		name          string
		err           error
		cancelCaller  bool
		wantOutcome   string
		wantDeadline  bool
		wantUncertain bool
	}{
		{"native timeout", &supervised.ProcessTimeoutError{Label: private, ExitCode: -9}, false, "native process timeout, exit=-9", true, false},
		{"native exit", &ceph.NativeFailure{Reason: private, ExitCode: 22}, false, "native command exit=22", false, false},
		{"exec timeout", &supervised.BackendError{Op: "exec monitor quorum supervisor", Err: context.DeadlineExceeded}, false, "Docker exec failed", true, false},
		{"read failure", &supervised.BackendError{Op: "read monitor quorum supervisor", Err: errors.New(private)}, false, "supervisor report read failed", false, false},
		{"unconfirmed supervisor", &supervised.ExitError{Label: private, Code: 137}, false, "supervisor exit=137", false, true},
		{"caller canceled", context.Canceled, true, "no command outcome", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelCaller {
				cancel()
			}
			got := directoryAdditionObservationDiagnostics(ctx, errors.Join(errors.New(private), tc.err), 5*time.Second)
			caller := "caller_context=<nil>"
			if tc.cancelCaller {
				caller = "caller_context=context canceled"
			}
			if strings.Contains(got, private) || !strings.Contains(got, "elapsed=5s") ||
				!strings.Contains(got, caller) || !strings.Contains(got, "outcome="+tc.wantOutcome) ||
				strings.Contains(got, "observation_deadline=true") != tc.wantDeadline ||
				strings.Contains(got, "observation_canceled=true") != tc.cancelCaller ||
				strings.Contains(got, "stop_unconfirmed=true") != tc.wantUncertain {
				t.Fatalf("observation diagnostics did not distinguish outcomes safely: %s", got)
			}
		})
	}
}
