package ceph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func TestNativeTargetFenceDrainsOnlySelectedInstance(t *testing.T) {
	requireLinuxGroups(t)
	c := nativeCluster()
	target, neighbor := newNativeTarget(t, "removed", "tc-public"), newNativeTarget(t, "neighbor", "tc-public")
	removed, kept := make(chan error, 1), make(chan error, 1)
	neighborCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_, err := c.RunNativeCommand(t.Context(), target, waiterCommand(target, "removed", time.Minute))
		removed <- err
	}()
	go func() {
		_, err := c.RunNativeCommand(neighborCtx, neighbor, waiterCommand(neighbor, "kept", time.Minute))
		kept <- err
	}()
	pid := target.published(t, "removed", "command")
	other := neighbor.published(t, "kept", "command")
	if err := c.native.fence(t.Context(), target.id); err != nil {
		t.Fatal(err)
	}
	if err := <-removed; !errors.Is(err, errNativeTargetRemoved) || classified(err) {
		t.Fatalf("selected cancellation: %v", err)
	}
	if processAlive(pid) || !processAlive(other) || registered(c) != 1 || c.native.calls.closed {
		t.Fatal("fence killed a neighbor or failed to drain its selected command")
	}
	before := target.execCount()
	if _, err := c.RunNativeCommand(t.Context(), target, waiterCommand(target, "late", time.Minute)); !errors.Is(err, ErrInvalidRequest) || target.execCount() != before {
		t.Fatalf("fenced target dispatched: %v", err)
	}
	replacement := newNativeTarget(t, "new-instance", "tc-public")
	result, err := c.RunNativeCommand(t.Context(), replacement, NativeCommand{Argv: []string{"python3", "-c", "print('new')"}, Timeout: time.Second, MaxStdout: 128, MaxStderr: 128})
	if err != nil || string(result.Stdout) != "new\n" {
		t.Fatalf("new instance refused: %v", err)
	}
	cancel()
	if err := <-kept; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestNativeTargetFenceClosesAdmissionStateRace(t *testing.T) {
	c := nativeCluster()
	target := newNativeTarget(t, "late", "tc-public")
	target.stateHook = func() error { return c.native.fence(t.Context(), target.id) }
	if _, err := c.RunNativeCommand(t.Context(), target, waiterCommand(target, "race", time.Minute)); !errors.Is(err, ErrInvalidRequest) || target.execCount() != 0 || registered(c) != 0 {
		t.Fatalf("late command dispatched after fencing: %v", err)
	}
}

func TestNativeTargetFenceCancellationKeepsAdmission(t *testing.T) {
	c := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.native.fence(ctx, "untouched"); !errors.Is(err, context.Canceled) || len(c.native.calls.fenced) != 0 || c.native.calls.closed {
		t.Fatalf("canceled fence changed admission: %v", err)
	}
}

func TestOSDRemovalKeepsUnconfirmedCommandAndRetriesBeforePurge(t *testing.T) {
	c, control, daemon := newOSDLifecycleFixture()
	blocked := errors.New("unconfirmed stop")
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	done := make(chan struct{})
	close(done)
	inv := &nativeInvocation{owner: c, target: daemon, targetID: daemon.GetContainerID(), label: "pending OSD probe", cancel: cancel, done: done, marker: "owned-marker"}
	inv.stopCommand = func(context.Context, testcontainers.Container, string, string) error { return blocked }
	// The stop failure's fallback inspection must see a still-existing instance.
	inv.target = newNativeTarget(t, daemon.GetContainerID(), "tc-public")
	c.native.calls.invocations = map[*nativeInvocation]bool{inv: true}
	if err := osdHandle(t, t.Context(), c, 3).Remove(t.Context()); !errors.Is(err, blocked) {
		t.Fatalf("unconfirmed stop: %v", err)
	}
	if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || c.fleet.osdHandles.get(3) == nil || registered(c) != 1 || !c.native.calls.fenced[daemon.GetContainerID()] || c.native.calls.closed {
		t.Fatal("failed drain lost ownership, purged, or closed unrelated admission")
	}
	inv.stopCommand = func(context.Context, testcontainers.Container, string, string) error { return nil }
	if err := osdHandle(t, t.Context(), c, 3).Remove(t.Context()); err != nil || c.fleet.osdHandles.get(3) != nil || registered(c) != 0 || control.purges != 1 {
		t.Fatalf("fresh retry did not finish removal: %v", err)
	}
}

func TestNativeTargetFenceCanceledDrainRetainsInvocation(t *testing.T) {
	c := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	inv := &nativeInvocation{owner: c, targetID: "pending", label: "pending probe", done: done, cancel: func(error) { cancel() }}
	c.native.calls.invocations = map[*nativeInvocation]bool{inv: true}
	if err := c.native.fence(ctx, "pending"); !errors.Is(err, context.Canceled) || registered(c) != 1 || !c.native.calls.fenced["pending"] || c.native.calls.closed {
		t.Fatalf("canceled drain lost retry state: %v", err)
	}
}

func TestOSDRemovalRefusalDoesNotFenceAnInstance(t *testing.T) {
	c, control, _ := newOSDLifecycleFixture()
	state := control.native[3]
	state.UUID = osdLifecycleForeignUUID
	control.native[3] = state
	if err := osdHandle(t, t.Context(), c, 3).Remove(t.Context()); err == nil || len(c.native.calls.fenced) != 0 {
		t.Fatalf("foreign identity changed admission: %v", err)
	}
	c, _, _ = newOSDLifecycleFixture()
	c.fleet.osdHandles.forget(4)
	if err := osdHandle(t, t.Context(), c, 3).Remove(t.Context()); err == nil || len(c.native.calls.fenced) != 0 {
		t.Fatalf("last OSD refusal changed admission: %v", err)
	}
}
