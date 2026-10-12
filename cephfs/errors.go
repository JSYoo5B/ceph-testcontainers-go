package cephfs

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph"

// invalid marks a request that the fixture refused before any change, with
// ceph.ErrInvalidRequest.
func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }
