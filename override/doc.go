// Package override changes one cluster-wide setting of a ceph.Container for a
// while and puts it back.
//
// Each operation is a function that takes the cluster and returns a handle.
// The handle owns the change: its Restore undoes only what the call did, a
// copy of the handle shares the same state, and a restore that fails for an
// uncertain reason can be retried. Terminate does not undo an override; the
// change ends with the cluster.
//
// A call that is refused or never applied returns a non-nil handle together
// with the error. Restore on that handle is a no-op: it takes no lock, never
// reports a restoration and leaves the cluster alone, so it is safe to defer
// without checking the handle first. A call whose outcome is uncertain (the
// native command may have been applied) returns a real handle together with
// the error, and its Restore still undoes the change.
//
// A nil cluster is refused with ceph.ErrInvalidRequest.
//
// The package imports ceph; ceph never imports it.
package override
