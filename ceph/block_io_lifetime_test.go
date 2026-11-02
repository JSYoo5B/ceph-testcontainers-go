package ceph

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestBlockIOUndoRunsUnlockedBeforeDaemonFaultsAndStaysRetryable(t *testing.T) {
	c := &Container{}
	var order []string
	var failed = true
	register := func(kind faultKind, name string, undo func(context.Context) error) *faultEntry {
		token, _, err := c.faults.reserve(t.Context(), kind, name)
		if err != nil {
			t.Fatal(err)
		}
		c.faults.settle(token, "", name, undo, nil)
		return token
	}
	block := register(faultBlockIO, "backend", func(context.Context) error {
		if !c.mu.TryLock() {
			t.Fatal("block Restore must run outside the topology mutex")
		}
		c.mu.Unlock()
		order = append(order, "block")
		if failed {
			return errors.New("kernel operation still pending")
		}
		return nil
	})
	register(faultContainerPause, "daemon", func(context.Context) error { order = append(order, "pause"); return nil })
	if err := c.Terminate(t.Context()); err == nil {
		t.Fatal("pending restore was hidden")
	}
	if !reflect.DeepEqual(order, []string{"block", "pause"}) {
		t.Fatal(order)
	}
	if !c.faults.tracks(block) {
		t.Fatal("uncertain block restore was forgotten")
	}
	failed = false
	order = nil
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"block", "pause"}) {
		t.Fatal(order)
	}
}
