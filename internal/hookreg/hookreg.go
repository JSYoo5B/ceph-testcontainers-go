// Package hookreg holds the hooks that package ceph registers for the rbd,
// cephfs and rgw packages, for the fault operations, for the override
// operations and for the shared harness contract. It imports nothing, so ceph can register its hooks without
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

// Harness is set once, during the initialization of package ceph, to the
// core hooks that internal/harnesshook types: the error contract that the
// service packages and, later, ceph/harness share.
var Harness any

// HarnessTenant is set once, during the initialization of package
// ceph/harness, to the tenant hooks that internal/servicehook and
// internal/testhelper type: the tenant port lookup of the service harnesses
// and the test-only cluster lookup. Each consumer asserts only the method it
// uses.
var HarnessTenant any

// Test is set once, during the initialization of package ceph, to the
// unit-test access seam that internal/testhelper types. It is the only link
// between test helper code and the unexported state of a cluster.
var Test any
