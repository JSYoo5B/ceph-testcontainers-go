package faulthook_test

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
)

func TestRegistryThroughHooks(t *testing.T) {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{})
	ctx := t.Context()

	type handle struct{ done bool }
	var first faulthook.Token
	held := &handle{}
	err := faulthook.WithTopology(ctx, c, func() error {
		token, previous, err := faulthook.Reserve(ctx, c, faulthook.ContainerPause, "osd-0")
		if err != nil || previous != nil {
			return err
		}
		first = token
		closed, err := faulthook.Settle(c, token, faulthook.Held{
			Handle:   held,
			Undo:     func(context.Context) error { held.done = true; return nil },
			Complete: func(context.Context) (bool, error) { return held.done, nil },
		})
		if err != nil || closed {
			t.Errorf("settle: closed=%v err=%v", closed, err)
		}
		if _, previous, err := faulthook.Reserve(ctx, c, faulthook.ContainerPause, "osd-0"); err == nil || previous != held {
			t.Errorf("an incomplete pause was not returned with the refusal: %v %v", previous, err)
		}
		if _, _, err := faulthook.Reserve(ctx, c, faulthook.Kind("Other"), "osd-0"); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("an unknown kind was reserved: %v", err)
		}
		if _, err := faulthook.Settle(c, faulthook.Token{}, faulthook.Held{}); err == nil {
			t.Error("a zero token settled")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	faulthook.WithTopologyNoContext(c, func() {
		if !faulthook.Tracks(c, first) || !faulthook.Release(c, first) || faulthook.Tracks(c, first) || faulthook.Release(c, first) {
			t.Error("release through the hooks did not forget the token exactly once")
		}
	})
	if err := c.Terminate(ctx); err != nil || held.done {
		t.Fatalf("Terminate undid a released fault: %v", err)
	}
	native, cluster := faulthook.Terminated()
	if native == nil || cluster == nil {
		t.Fatal("terminated errors are missing")
	}
}
