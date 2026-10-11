// Package hookreg holds the hooks that package ceph registers for the rbd,
// cephfs and rgw packages. It imports nothing, so ceph can register its hooks
// without importing internal/servicehook, which imports ceph and gives the
// hooks their typed form.
package hookreg

// Value is set once, during the initialization of package ceph.
var Value any
