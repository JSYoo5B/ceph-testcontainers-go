//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_bridge))))

package rgw_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/testcontainers/testcontainers-go"
)

// reloadStopCancellation is a package-local container double. It completes a
// real Docker Stop, cancels its caller and holds the successful reply until
// released. It models caller cancellation while terminal evidence is pending.
type reloadStopCancellation struct {
	testcontainers.Container
	cancel  context.CancelFunc
	release <-chan struct{}
}

func (c *reloadStopCancellation) Stop(ctx context.Context, timeout *time.Duration) error {
	if err := c.Container.Stop(ctx, timeout); err != nil {
		return err
	}
	if c.cancel != nil {
		cancel, release := c.cancel, c.release
		c.cancel = nil
		cancel()
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func checkNativeReloadRecovery(t *testing.T, ctx context.Context, link *multisite.Fixture, group *multisite.SyncGroup) {
	t.Helper()
	original := link.Source.Container
	wrapped := &reloadStopCancellation{Container: original}
	link.Source.Container = wrapped
	t.Cleanup(func() { link.Source.Container = original })
	arm := func() (context.Context, func()) {
		callCtx, cancel := context.WithCancel(ctx)
		release := make(chan struct{})
		var once sync.Once
		finish := func() { once.Do(func() { close(release) }); cancel() }
		t.Cleanup(finish)
		wrapped.cancel, wrapped.release = cancel, release
		return callCtx, finish
	}
	state, err := original.State(ctx)
	if err != nil || state == nil || !state.Running || state.StartedAt == "" {
		t.Fatalf("running gateway before reload: %+v %v", state, err)
	}
	initialStart := state.StartedAt
	firstCtx, finishFirst := arm()
	if err := group.Apply(firstCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("first reload did not report caller cancellation: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != initialStart {
		t.Fatalf("completed Stop did not retain the original stopped generation: %+v %v", state, err)
	}
	finishFirst()
	if err := group.Apply(ctx); err != nil {
		t.Fatalf("resume fixture-owned stopped generation: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || !state.Running || state.StartedAt == initialStart {
		t.Fatalf("retry did not start a new running generation: %+v %v", state, err)
	}
	reloadedStart := state.StartedAt

	secondCtx, finishSecond := arm()
	if err := group.Apply(secondCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("second reload did not report caller cancellation: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != reloadedStart {
		t.Fatalf("second Stop changed the recorded generation: %+v %v", state, err)
	}
	// Simulate the caller starting and then fencing a different generation.
	finishSecond()
	if err := original.Start(ctx); err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	if err := original.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt == reloadedStart {
		t.Fatalf("caller did not fence a new generation: %+v %v", state, err)
	}
	fencedStart := state.StartedAt
	if err := group.Apply(ctx); err == nil || !strings.Contains(err.Error(), "stopped outside the pending reload") {
		t.Fatalf("reload did not refuse the externally fenced generation: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != fencedStart {
		t.Fatalf("refused reload revived or changed the external fence: %+v %v", state, err)
	}
	if err := group.Apply(ctx); err != nil {
		t.Fatalf("subsequent Apply with fenced gateway: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != fencedStart {
		t.Fatalf("subsequent reload revived or changed the external fence: %+v %v", state, err)
	}
	t.Log("caller cancellation with retained successful Docker Stop completion resumed its original generation; an externally restarted and fenced generation stayed stopped across Apply retries")
}
