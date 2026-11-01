// Package monstore keeps the cluster-owned MON data volumes of a Ceph
// fixture: one store record per MON (State and Volume), its state machine from
// creation to release, the generations that mount it (StartGeneration,
// StopGeneration, ReplaceGeneration), the address it pins, and the read-only
// identity observation of a stopped store.
//
// It knows nothing of ceph.Container. The container side of a generation is
// the Generations interface, which package ceph implements; a record takes no
// lock and is used under the cluster's topology lock. The design and its
// invariants are in docs/internals/mon-volume-storage.md.
package monstore
