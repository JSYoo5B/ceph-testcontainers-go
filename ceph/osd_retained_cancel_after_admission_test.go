package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// The fault forwards the request and every actual runtime outcome unchanged
// and cancels the caller context only after a successful container reply.
func TestRetainedCancelFaultCancelsOnlyAfterASuccessfulReply(t *testing.T) {
	actual := errors.New("actual runner failure")
	observed := &retainedReplyContainer{}
	option := &retainedReplyCustomizer{}
	for _, tc := range []struct {
		name       string
		ctr        testcontainers.Container
		err        error
		wantCancel bool
	}{
		{"completed native creation", observed, nil, true},
		{"actual partial runtime failure", observed, actual, false},
		{"actual precreation failure", nil, actual, false},
		{"nil runner result", nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fault := &retainedCancelFault{cancel: cancel, next: func(got context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				if got != ctx || image != "synthetic-image" || len(opts) != 1 || opts[0] != option {
					t.Fatal("request changed before runtime dispatch")
				}
				if got.Err() != nil {
					t.Fatal("context ended before the runner replied")
				}
				return tc.ctr, tc.err
			}}
			got, err := fault.run(ctx, "synthetic-image", option)
			if fault.calls != 1 || got != tc.ctr || err != tc.err {
				t.Fatal("runtime result was replaced or replayed")
			}
			if tc.wantCancel != (ctx.Err() != nil) || (tc.wantCancel && fault.observed != observed) || (!tc.wantCancel && fault.observed != nil) {
				t.Fatalf("cancellation differs: want %v, ctx %v", tc.wantCancel, ctx.Err())
			}
		})
	}
}

// A cancellation after the creation request was dispatched leaves the
// candidate owned, with its container, and the detached handle consumed. The
// candidate can then be detached again with a fresh context and reopened.
func TestReopenCanceledAfterCreationKeepsTheCandidateRecoverable(t *testing.T) {
	f := newRetainedFixture(t)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fault := &retainedCancelFault{next: f.cluster.fleet.stores.runner, cancel: cancel}
	f.cluster.fleet.stores.runner = fault.run
	osd, err := detached.Reopen(ctx)
	if err == nil || ctx.Err() == nil || osd == nil || osd.Container == nil || f.cluster.fleet.osdHandles.get(3) != osd || !detached.reopened || f.cluster.fleet.osdHandles.detachedOf(3) != nil {
		t.Fatalf("canceled reopen: %v %+v", err, osd)
	}
	if again, err := detached.Reopen(t.Context()); err == nil || again != nil || !strings.Contains(err.Error(), "already reopened") || f.runs != 1 {
		t.Fatalf("consumed detached handle admitted a retry: %v runs=%d", err, f.runs)
	}
	if fault.calls != 1 || fault.canceled != 1 {
		t.Fatalf("runner dispatches %d, cancellations %d", fault.calls, fault.canceled)
	}
	// A fresh context settles the candidate: it is detached and reopened as a
	// second distinct instance, with one more dispatch of the ordinary runner.
	again, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil || again == nil || f.cluster.fleet.osdHandles.get(3) != nil {
		t.Fatalf("detach of the canceled candidate: %v", err)
	}
	f.cluster.fleet.stores.runner = fault.next
	second, err := again.Reopen(t.Context())
	if err != nil || second == nil || second == osd || f.cluster.fleet.osdHandles.get(3) != second || f.runs != 2 {
		t.Fatalf("reopen after the canceled candidate: %v runs=%d", err, f.runs)
	}
}
