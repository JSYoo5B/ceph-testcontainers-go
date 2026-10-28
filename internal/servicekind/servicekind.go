// Package servicekind names the service kinds that the rbd, cephfs and rgw
// packages register under: their bootstrap kind, their shared-cluster service
// support and their extension-store key. It is a leaf package with no imports,
// so a service package and the harness package of the same service can both
// name the same kind without either naming the other's private declarations
// (docs/design/shared-harness-boundary.md, decision 8).
package servicekind

const (
	// RBD is the kind of the rbd package.
	RBD = "rbd"
	// CephFS is the kind of the cephfs package.
	CephFS = "cephfs"
	// RGW is the kind of the rgw package.
	RGW = "rgw"
)
