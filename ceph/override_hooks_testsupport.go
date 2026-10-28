//go:build test_support

package ceph

// This file is compiled only with the test_support build tag (see
// service_hooks_testsupport.go).

// TestHeld snapshots the handles that one kind holds, for unit tests. The
// caller holds the mutex, or the cluster is not shared.
func (overrideHooks) TestHeld(c *Container, kind string) map[string]any {
	k, ok := overrideKindNames[kind]
	if !ok {
		return make(map[string]any)
	}
	return c.faults.handles(k)
}
