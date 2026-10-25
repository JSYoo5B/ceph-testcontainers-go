package ceph

import (
	"context"
	"errors"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type retainedReplyContainer struct{ testcontainers.Container }
type retainedReplyCustomizer struct{}

func (*retainedReplyCustomizer) Customize(*testcontainers.GenericContainerRequest) error { return nil }

func TestRetainedReplyFaultPreservesRuntimeFailuresAndInputs(t *testing.T) {
	injected := errors.New("injected client-side reply loss")
	actual := errors.New("actual runner failure")
	observed := &retainedReplyContainer{}
	option := &retainedReplyCustomizer{}
	for _, tc := range []struct {
		name     string
		ctr      testcontainers.Container
		err      error
		wantLost bool
	}{
		{"completed native creation", observed, nil, true},
		{"actual partial runtime failure", observed, actual, false},
		{"actual precreation failure", nil, actual, false},
		{"nil runner result", nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var delegated int
			fault := &retainedReplyFault{lost: injected, next: func(got context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				delegated++
				if got != ctx || image != "synthetic-image" || len(opts) != 1 || opts[0] != option {
					t.Fatal("request changed before runtime dispatch")
				}
				return tc.ctr, tc.err
			}}
			got, err := fault.run(ctx, "synthetic-image", option)
			if delegated != 1 || fault.calls != 1 {
				t.Fatal("runner replayed")
			}
			if tc.wantLost {
				if got != nil || err != injected || fault.observed != observed {
					t.Fatal("successful reply was not withheld")
				}
			} else if got != tc.ctr || err != tc.err || fault.observed != nil {
				t.Fatal("actual runtime result was replaced")
			}
		})
	}
}
