// Package fsbase is the foundation that package cephfs and its feature
// packages share for one filesystem: its identity (the cluster, the
// authoritative name and the native pool identity captured at creation), the
// liveness check, the admission lifetime of an operation, the command
// transport of the MGR volumes module with the readers more than one feature
// needs, and the native pool reads with the subvolume data-pool checks.
//
// Package cephfs owns the filesystem registry and constructs each Core with
// NewCore, passing the registry's setup gate, its liveness predicate and the
// address of the exported descriptor name. fsbase keeps no registry, no
// global state and no mutex of its own. A Filesystem embeds a Handle, so
// *cephfs.Filesystem satisfies Owner, and Of returns its Core; Handle has no
// exported member, so nothing is promoted into the public API.
//
// Lock contract: the setup gate (cephfs filesystemRegistry.setupMu) is taken
// before the cluster topology mutex, never after it. The setup gate serializes
// filesystem creation, removal, MDS topology changes and every admitted
// operation; Admit takes both locks in that order, releases the topology
// mutex before it returns and leaves the setup gate to the returned release.
// The cluster and the name are fixed at construction and need no lock. The
// native identity is written once, by CaptureNativePools during creation with
// the setup gate held and the topology mutex taken around the write, before
// any MDS starts; it is read under the cluster mutex or the setup gate.
// Registered runs with the topology mutex held. Pool attachments recorded in
// the native identity are read and written with the setup gate held.
package fsbase
