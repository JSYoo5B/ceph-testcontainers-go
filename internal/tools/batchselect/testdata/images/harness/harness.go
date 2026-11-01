// Package harness is the shared non-test package of the tests, like the
// integration harness.
package harness

import "example.com/images/lib"

// Old is a pinned official image of another release.
const Old = "quay.io/ceph/ceph:v20.2.3@sha256:d195020de02512030118e772cef7859e92904e91eb4cb21acb503f8b94118137"

// Official completes an image reference with the version at run time.
func Official(version string) string { return "quay.io/ceph/ceph:v" + version }

// Pins names the file of pinned role images.
func Pins() string { return "../../.github/ceph-role-images.json" }

// Netem names an image of another package.
func Netem() string { return lib.Helper }

// Unused is reached by no test.
func Unused() string { return "busybox:9.9.9" }

// Placeholders are no images: a word tag, a deep path, a reserved host and
// a name without a version.
func Placeholders() []string {
	return []string{"example/ceph:version", "sys/dev/block/253:4", "registry.example/ceph:20.2.4",
		"essential:1", "ceph-" + "core"}
}
