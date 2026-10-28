//go:build test_support

package ceph

// This file is compiled only with the test_support build tag (see
// service_hooks_testsupport.go).

// TestFaults snapshots one family for cross-package contract tests. The
// caller holds the mutex, or the cluster is not shared.
func (faultHooks) TestFaults(c *Container, kind string) map[string]any {
	k, ok := faultKindNames[kind]
	if !ok {
		return make(map[string]any)
	}
	return c.faults.handles(k)
}
