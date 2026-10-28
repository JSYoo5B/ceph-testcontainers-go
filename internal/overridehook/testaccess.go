//go:build test_support

package overridehook

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// testHooks is the test-access part of the registered override hooks. It is
// asserted separately from hooks, and only when TestHeld runs, so an ordinary
// override call does not depend on it.
type testHooks interface {
	TestHeld(c *ceph.Container, kind string) map[string]any
}

func getTest() testHooks {
	h, ok := hookreg.Override.(testHooks)
	if !ok {
		panic("overridehook: package ceph did not register its test-access hooks")
	}
	return h
}

// TestHeld snapshots the handles that one kind holds, for unit tests. Call
// with the cluster mutex held.
func TestHeld(c *ceph.Container, kind Kind) map[string]any {
	return getTest().TestHeld(c, string(kind))
}
