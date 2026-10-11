package multicluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/containerd/errdefs"
)

func TestCleanupRetriesOnlyFailedResources(t *testing.T) {
	var owned Resources
	var calls []string
	fail := true
	for _, name := range []string{"network", "client", "daemon"} {
		owned.AddCleanup(name, func(context.Context) error {
			calls = append(calls, name)
			if name == "client" && fail {
				return errors.New("temporary failure")
			}
			return nil
		})
	}
	if err := owned.Terminate(t.Context()); err == nil {
		t.Fatal("cleanup failure was lost")
	}
	if !reflect.DeepEqual(calls, []string{"daemon", "client", "network"}) {
		t.Fatalf("cleanup order: %v", calls)
	}
	fail = false
	if err := owned.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"daemon", "client", "network", "client"}) {
		t.Fatalf("successful cleanup repeated: %v", calls)
	}
	if err := owned.Terminate(t.Context()); err != nil {
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
			result := IgnoreMissing(tc.err)
			if (result == nil) != tc.safe {
				t.Fatalf("unexpected cleanup classification: error=%v result=%v safe=%v", tc.err, result, tc.safe)
			}
			if !tc.safe && result != tc.err {
				t.Fatal("cleanup classification changed the original failure")
			}
		})
	}
}

func TestMissingContainerCleanupCompletesButMixedFailureRetries(t *testing.T) {
	gone := &clustertest.CleanupContainer{Err: fmt.Errorf("remove: %w", errdefs.ErrNotFound)}
	realFailure := errors.New("termination hook failed")
	mixed := &clustertest.CleanupContainer{Err: errors.Join(errdefs.ErrNotFound, realFailure)}
	var owned Resources
	owned.AddContainer(gone)
	owned.AddContainer(mixed)
	if err := owned.Terminate(t.Context()); !errors.Is(err, realFailure) {
		t.Fatalf("real cleanup failure lost: %v", err)
	}
	mixed.Err = errdefs.ErrNotFound
	if err := owned.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if gone.Calls != 1 || mixed.Calls != 2 {
		t.Fatalf("completed/failed resources retried incorrectly: missing=%d mixed=%d", gone.Calls, mixed.Calls)
	}
	if err := owned.Terminate(t.Context()); err != nil || gone.Calls != 1 || mixed.Calls != 2 {
		t.Fatalf("completed cleanup repeated: missing=%d mixed=%d error=%v", gone.Calls, mixed.Calls, err)
	}
}
