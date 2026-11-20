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
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
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

func TestCephFSDirectoryAdditionObservationRetry(t *testing.T) {
	guard := errors.New("permanent CephFS observation guard")
	refusal := error(&ceph.NativeFailure{ExitCode: 13})
	timeout := error(&supervised.ProcessTimeoutError{Label: "monitor quorum", ExitCode: -9})
	observed := cephfsmirror.DirectoryAdditionStatus{Directory: "/addition-intent", PolicyPresent: true, Registered: true}
	for _, tc := range []struct {
		name         string
		errs         []error
		cancelCaller bool
		wantCalls    int
		wantErr      error
	}{
		{"answered at once", []error{nil}, false, 1, nil},
		{"unanswered monitor reads then answered", []error{timeout, timeout, nil}, false, 3, nil},
		{"guard is not repeated", []error{guard, nil}, false, 1, guard},
		{"guard after an unanswered read", []error{timeout, guard, nil}, false, 2, guard},
		{"native refusal is not repeated", []error{refusal, nil}, false, 1, refusal},
		{"canceled caller is not repeated", []error{context.Canceled, nil}, true, 1, context.Canceled},
		{"expired caller deadline is not repeated", []error{context.DeadlineExceeded, nil}, true, 1, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, retried := 0, 0
			status, err := retryDirectoryAdditionObservation(ctx, time.Minute, time.Millisecond,
				func(context.Context) (cephfsmirror.DirectoryAdditionStatus, error) {
					err := tc.errs[calls]
					calls++
					if tc.cancelCaller {
						cancel()
					}
					if err != nil {
						// A failed Status reports neither flag.
						return cephfsmirror.DirectoryAdditionStatus{Directory: observed.Directory}, err
					}
					return observed, nil
				}, func(error, time.Duration) { retried++ })
			if calls != tc.wantCalls || retried != tc.wantCalls-1 {
				t.Fatalf("observed %d times and repeated %d failures, want %d observations: %v", calls, retried, tc.wantCalls, err)
			}
			if tc.wantErr == nil {
				if err != nil || status != observed {
					t.Fatalf("answered observation was not returned: %+v, %v", status, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) || status.PolicyPresent || status.Registered {
				t.Fatalf("failed observation was not returned as its failure: %+v, %v", status, err)
			}
		})
	}
	t.Run("limit ends an unanswered observation", func(t *testing.T) {
		calls := 0
		status, err := retryDirectoryAdditionObservation(t.Context(), 20*time.Millisecond, time.Millisecond,
			func(ctx context.Context) (cephfsmirror.DirectoryAdditionStatus, error) {
				calls++
				return cephfsmirror.DirectoryAdditionStatus{}, timeout
			}, func(error, time.Duration) {})
		if calls < 2 || !errors.Is(err, context.DeadlineExceeded) || status.PolicyPresent || status.Registered {
			t.Fatalf("unanswered observation did not end with its last failure after %d reads: %+v, %v", calls, status, err)
		}
	})
}
