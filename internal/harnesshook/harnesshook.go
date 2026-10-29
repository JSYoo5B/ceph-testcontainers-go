// Package harnesshook is the typed internal contract between package ceph and
// the packages that compose it (the rbd, cephfs and rgw packages and their
// mirror packages today, the shared harness after the package move). Package
// ceph registers the implementation in internal/hookreg when it is
// initialized, so a caller imports ceph and never ceph's private names, and
// ceph imports none of its callers.
//
// The error contract is the first member: the service packages share one
// construction of an invalid-request OperationError instead of each keeping a
// copy (docs/design/shared-harness-boundary.md, "Error handling"). It adds no
// public API.
package harnesshook

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"

type core interface {
	Invalid(err error) error
}

func get() core {
	c, ok := hookreg.Harness.(core)
	if !ok {
		panic("harnesshook: package ceph did not register its harness hooks")
	}
	return c
}

// Invalid marks err as a request that the fixture refused before any change,
// with ceph.ErrInvalidRequest. The result is an *ceph.OperationError whose
// cause is err itself, also when err is nil.
func Invalid(err error) error { return get().Invalid(err) }
