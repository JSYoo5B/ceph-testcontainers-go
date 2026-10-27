package multisite

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph"

// multisiteInvalid marks a multisite request that the fixture refused before
// any change, with ceph.ErrInvalidRequest. It is the multisite family's own
// copy of the one-line single-gateway helper, so the family needs no
// unexported declaration from the single-gateway files.
func multisiteInvalid(err error) error {
	return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err}
}
