// Package hookreg holds the hooks that package ceph registers for the rbd,
// cephfs and rgw packages, for the fault operations and for the override
// operations. It imports nothing, so ceph can register its hooks without
// importing internal/servicehook, internal/faulthook or
// internal/overridehook, which import ceph and give the hooks their typed
// form.
package hookreg

// Value is set once, during the initialization of package ceph.
var Value any

// Fault is set once, during the initialization of package ceph, to the
// fault registry hooks that internal/faulthook types.
var Fault any

// Override is set once, during the initialization of package ceph, to the
// override hooks that internal/overridehook types.
var Override any
