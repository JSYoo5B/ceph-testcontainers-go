//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_bridge))))

package rgw_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/testcontainers/testcontainers-go"
)

// reloadStopReplyLoss is a package-local container double. It delegates every
// lifecycle effect to Docker and substitutes one lost reply after Stop succeeds.
// It proves recovery from a completed stop, not ordering of an outstanding stop.
type reloadStopReplyLoss struct {
	testcontainers.Container
	reply error
}

func (c *reloadStopReplyLoss) Stop(ctx context.Context, timeout *time.Duration) error {
	if err := c.Container.Stop(ctx, timeout); err != nil {
		return err
	}
	err := c.reply
	c.reply = nil
	return err
}

func checkNativeReloadRecovery(t *testing.T, ctx context.Context, link *multisite.Fixture, group *multisite.SyncGroup) {
	t.Helper()
	original := link.Source.Container
	wrapped := &reloadStopReplyLoss{Container: original}
	link.Source.Container = wrapped
	t.Cleanup(func() { link.Source.Container = original })
	lost := errors.New("completed Docker Stop reply lost")
	state, err := original.State(ctx)
	if err != nil || state == nil || !state.Running || state.StartedAt == "" {
		t.Fatalf("running gateway before reload: %+v %v", state, err)
	}
	initialStart := state.StartedAt
	wrapped.reply = lost
	if err := group.Apply(ctx); !errors.Is(err, lost) {
		t.Fatalf("first reload did not report the lost Stop reply: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != initialStart {
		t.Fatalf("completed Stop did not retain the original stopped generation: %+v %v", state, err)
	}
	if err := group.Apply(ctx); err != nil {
		t.Fatalf("resume fixture-owned stopped generation: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || !state.Running || state.StartedAt == initialStart {
		t.Fatalf("retry did not start a new running generation: %+v %v", state, err)
	}
	reloadedStart := state.StartedAt

	wrapped.reply = lost
	if err := group.Apply(ctx); !errors.Is(err, lost) {
		t.Fatalf("second reload did not report the lost Stop reply: %v", err)
	}
	state, err = original.State(ctx)
	if err != nil || state == nil || state.Running || state.StartedAt != reloadedStart {
		t.Fatalf("second Stop changed the recorded generation: %+v %v", state, err)
	}
	// Simulate the caller starting and then fencing a different generation.
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
	t.Log("completed Docker Stop with a lost reply resumed its original generation; an externally restarted and fenced generation stayed stopped across Apply retries")
}
