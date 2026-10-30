// Package hookreg holds the hooks that package ceph registers for the rbd,
// cephfs and rgw packages, for the fault operations, for the override
// operations, and the hooks that package rgw registers for rgw/multisite. It imports nothing, so ceph can register its hooks without
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

// Test is set once, during the initialization of package ceph, to the
// unit-test access seam that internal/testhelper types. It is the only link
// between test helper code and the unexported state of a cluster.
//
//testdouble:unit-seam
var Test any

// RGW is set once, during the initialization of package rgw, to the handle
// ownership checks that internal/rgwhook types for rgw/multisite.
var RGW any
