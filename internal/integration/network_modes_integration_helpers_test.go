//go:build all || integration

package integration_test

import (
	"os"
	"slices"
	"testing"
)

// networkMode is one Docker network mode that a test runs its cluster in.
type networkMode struct {
	// Name is "host" or "bridge", the name of the test's subtest.
	Name string
	// Host means ceph.WithHostNetwork.
	Host bool
}

// networkModes returns the network modes that a test runs in this process.
// supported lists the modes the test can run in, its default first: host for
// the RADOS products, whose clients reach every daemon at the address its map
// advertises, and bridge for RGW's single HTTP endpoint. CEPH_TEST_NETWORK
// selects one of them; unset, the test runs in each.
//
// tools/tagcatalog reads the modes from the call, so they are string
// literals, and every test file of a batch declares the same ones. The CI
// planner runs a batch once per mode: pull requests in its default mode and
// the periodic matrix in each, so a mode that a test cannot run in is never
// scheduled rather than skipped.
func networkModes(t *testing.T, supported ...string) []networkMode {
	t.Helper()
	for _, mode := range supported {
		if mode != "host" && mode != "bridge" {
			t.Fatalf("unknown network mode %q", mode)
		}
	}
	selected := supported
	if mode := os.Getenv("CEPH_TEST_NETWORK"); mode != "" {
		if mode != "host" && mode != "bridge" {
			t.Fatalf("CEPH_TEST_NETWORK=%q: want host or bridge", mode)
		}
		if !slices.Contains(supported, mode) {
			t.Skipf("CEPH_TEST_NETWORK=%s: the test runs only in %v", mode, supported)
		}
		selected = []string{mode}
	}
	modes := make([]networkMode, 0, len(selected))
	for _, mode := range selected {
		modes = append(modes, networkMode{Name: mode, Host: mode == "host"})
	}
	return modes
}
