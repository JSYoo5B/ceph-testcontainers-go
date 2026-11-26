//go:build integration && ci_optional && ci_batch_block_io

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestBlockIORecoveryReadDeadline(t *testing.T) {
	payload := []byte("acknowledged")
	timeout := ceph.NativeCommandResult{ExitCode: 1, Stderr: []byte("error getting object: (110) Connection timed out")}
	success := ceph.NativeCommandResult{Stdout: bytes.Clone(payload)}
	for _, tc := range []struct {
		name         string
		read         func(context.Context, int) ceph.NativeCommandResult
		onTimeout    func(context.Context)
		budget       time.Duration
		retryDelay   time.Duration
		wantAttempts int
		wantReads    int
		wantErr      error
	}{
		{
			name: "timeout then exact bytes within budget",
			read: func(_ context.Context, attempt int) ceph.NativeCommandResult {
				if attempt == 1 {
					return timeout
				}
				return success
			},
			budget: time.Second, wantAttempts: 2, wantReads: 2,
		},
		{
			name: "deadline during diagnostics",
			read: func(context.Context, int) ceph.NativeCommandResult { return timeout },
			onTimeout: func(ctx context.Context) {
				<-ctx.Done()
			},
			budget: 100 * time.Millisecond, wantAttempts: 1, wantReads: 1, wantErr: context.DeadlineExceeded,
		},
		{
			name: "late success rejected",
			read: func(ctx context.Context, _ int) ceph.NativeCommandResult {
				<-ctx.Done()
				return success
			},
			budget: 100 * time.Millisecond, wantAttempts: 1, wantReads: 1, wantErr: context.DeadlineExceeded,
		},
		{
			name:   "deadline during retry wait",
			read:   func(context.Context, int) ceph.NativeCommandResult { return timeout },
			budget: 100 * time.Millisecond, retryDelay: time.Second,
			wantAttempts: 1, wantReads: 1, wantErr: context.DeadlineExceeded,
		},
		{
			name: "missing object rejected",
			read: func(context.Context, int) ceph.NativeCommandResult {
				return ceph.NativeCommandResult{ExitCode: 1, Stderr: []byte("(2) No such file or directory")}
			},
			budget: time.Second, wantAttempts: 1, wantReads: 1, wantErr: errBlockIORecoveryRead,
		},
		{
			name: "wrong bytes rejected",
			read: func(context.Context, int) ceph.NativeCommandResult {
				return ceph.NativeCommandResult{Stdout: []byte("wrong")}
			},
			budget: time.Second, wantAttempts: 1, wantReads: 1, wantErr: errBlockIORecoveryRead,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, observed := 0, 0
			result, attempts, err := blockIORecoveryRead(t.Context(), tc.budget, tc.retryDelay, payload,
				func(ctx context.Context) (ceph.NativeCommandResult, error) {
					reads++
					return tc.read(ctx, reads), nil
				},
				func(ctx context.Context, _ int, _ ceph.NativeCommandResult) {
					observed++
					if tc.onTimeout != nil {
						tc.onTimeout(ctx)
					}
				})
			if !errors.Is(err, tc.wantErr) || attempts != tc.wantAttempts || reads != tc.wantReads {
				t.Fatalf("result=%+v attempts=%d reads=%d err=%v; want attempts=%d reads=%d err=%v",
					result, attempts, reads, err, tc.wantAttempts, tc.wantReads, tc.wantErr)
			}
			if (tc.name == "timeout then exact bytes within budget" && (observed != 1 || !bytes.Equal(result.Stdout, payload))) ||
				(tc.name == "missing object rejected" && observed != 0) {
				t.Fatalf("unexpected timeout observation or payload: observed=%d result=%+v", observed, result)
			}
		})
	}
}
