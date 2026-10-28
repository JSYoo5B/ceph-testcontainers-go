package faulthook

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// testHooks is the test-access part of the registered fault hooks. It is
// asserted separately from hooks, and only when TestFaults runs, so an
// ordinary Admit call does not depend on it.
type testHooks interface {
	TestFaults(c *ceph.Container, kind string) map[string]any
}

func getTest() testHooks {
	h, ok := hookreg.Fault.(testHooks)
	if !ok {
		panic("faulthook: package ceph did not register its test-access hooks")
	}
	return h
}

// TestFaults snapshots the registry for unit tests. Call with the cluster mutex held.
func TestFaults(c *ceph.Container, kind Kind) map[string]any {
	return getTest().TestFaults(c, string(kind))
}
