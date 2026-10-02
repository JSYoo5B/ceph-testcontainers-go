package federation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

func TestCleanupRetriesOnlyFailedResources(t *testing.T) {
	var owned resources
	var calls []string
	fail := true
	for _, name := range []string{"network", "client", "daemon"} {
		owned.addCleanup(name, func(context.Context) error {
			calls = append(calls, name)
			if name == "client" && fail {
				return errors.New("temporary failure")
			}
			return nil
		})
	}
	if err := owned.terminate(t.Context()); err == nil {
		t.Fatal("cleanup failure was lost")
	}
	if !reflect.DeepEqual(calls, []string{"daemon", "client", "network"}) {
		t.Fatalf("cleanup order: %v", calls)
	}
	fail = false
	if err := owned.terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"daemon", "client", "network", "client"}) {
		t.Fatalf("successful cleanup repeated: %v", calls)
	}
	if err := owned.terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 4 {
		t.Fatalf("completed cleanup was not idempotent: %v", calls)
	}
}

// SDK wrappers can classify a plain-text cause as NotFound via their Is method.
// Checking only the final unwrapped cause loses that classification.
type missingSDKError struct{ error }

func (e missingSDKError) Unwrap() error { return e.error }
func (missingSDKError) Is(target error) bool {
	return target == errdefs.ErrNotFound
}

func TestIgnoreMissingPreservesJoinedFailures(t *testing.T) {
	realFailure := errors.New("termination hook failed")
	cases := []struct {
		name string
		err  error
		safe bool
	}{
		{"nil", nil, true},
		{"missing", errdefs.ErrNotFound, true},
		{"wrapped missing", fmt.Errorf("remove network: %w", errdefs.ErrNotFound), true},
		{"SDK classifier", missingSDKError{errors.New("missing container")}, true},
		{"joined missing", errors.Join(errdefs.ErrNotFound, fmt.Errorf("network: %w", errdefs.ErrNotFound)), true},
		{"wrapped joined missing", fmt.Errorf("cleanup: %w", errors.Join(errdefs.ErrNotFound, errdefs.ErrNotFound)), true},
		{"real failure", realFailure, false},
		{"joined failure", errors.Join(errdefs.ErrNotFound, realFailure), false},
		{"wrapped joined failure", fmt.Errorf("cleanup: %w", errors.Join(errdefs.ErrNotFound, realFailure)), false},
		{"SDK classified mixed join", missingSDKError{errors.Join(errdefs.ErrNotFound, realFailure)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := ignoreMissing(tc.err)
			if (result == nil) != tc.safe {
				t.Fatalf("unexpected cleanup classification: error=%v result=%v safe=%v", tc.err, result, tc.safe)
			}
			if !tc.safe && result != tc.err {
				t.Fatal("cleanup classification changed the original failure")
			}
		})
	}
}

type cleanupContainer struct {
	testcontainers.Container
	calls int
	err   error
}

func (c *cleanupContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.calls++
	return c.err
}

func TestMissingContainerCleanupCompletesButMixedFailureRetries(t *testing.T) {
	gone := &cleanupContainer{err: fmt.Errorf("remove: %w", errdefs.ErrNotFound)}
	realFailure := errors.New("termination hook failed")
	mixed := &cleanupContainer{err: errors.Join(errdefs.ErrNotFound, realFailure)}
	var owned resources
	owned.addContainer(gone)
	owned.addContainer(mixed)
	if err := owned.terminate(t.Context()); !errors.Is(err, realFailure) {
		t.Fatalf("real cleanup failure lost: %v", err)
	}
	mixed.err = errdefs.ErrNotFound
	if err := owned.terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if gone.calls != 1 || mixed.calls != 2 {
		t.Fatalf("completed/failed resources retried incorrectly: missing=%d mixed=%d", gone.calls, mixed.calls)
	}
	if err := owned.terminate(t.Context()); err != nil || gone.calls != 1 || mixed.calls != 2 {
		t.Fatalf("completed cleanup repeated: missing=%d mixed=%d error=%v", gone.calls, mixed.calls, err)
	}
}
