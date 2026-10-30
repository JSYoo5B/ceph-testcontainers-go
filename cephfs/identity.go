package cephfs

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// filesystemIdentity is what identifies one filesystem to every operation on
// it: the cluster, the authoritative name, the validated setup configuration
// and the native pool identity. Filesystem holds it and lends a pointer to the
// views that act for it, such as its MDS fleet, so they need no pointer back to
// the Filesystem.
//
// cluster and name are captured before the handle is published and are never
// written again, so they need no lock to read. config is the validated setup
// configuration; only its MDS counts change afterwards, written by the fleet's
// scale with setupMu and the cluster mutex held. name is the one
// authoritative identity: every registry key, ownership predicate and native
// command target uses it; Filesystem.FilesystemName is a descriptive copy for
// callers, and editing it changes no operation's target. native is written
// once, by captureNativePoolIdentity during creation with setupMu and the
// cluster mutex held, before any MDS starts; it is read under the cluster
// mutex.
type filesystemIdentity struct {
	cluster *ceph.Container
	name    string
	config  Config
	native  *cephFSNativeIdentity
}

// mdsState is the MDS state of one filesystem: its owned daemons and, for a
// filesystem created without an initial MDS, the cold-start record. The MDS
// fleet (Filesystem.fleet) is the only code that reaches it; its lock contract
// is the roster's, documented on mdsRoster.
type mdsState struct {
	roster  mdsRoster
	coldMDS *cephFSColdMDS
}

// registered reports whether id is still the identity of the live filesystem
// of its name on an open cluster. It is the ownership invariant that every
// admission predicate and every MDS ownership proof shares; each site adds its
// own conjuncts (the exported-name check of the descriptor-integrity refusals,
// the native identity, the first-daemon container, the roster) and keeps its
// own refusal. Comparing the identity's address is comparing the Filesystem:
// each Filesystem holds its own identity. The caller holds the cluster
// topology mutex.
func (id *filesystemIdentity) registered() bool {
	if servicehook.Closed(id.cluster) {
		return false
	}
	entry := filesystems(id.cluster).byName[id.name]
	return entry != nil && &entry.id == id
}

// volumes returns the command transport of the filesystem's volumes module.
func (id *filesystemIdentity) volumes() volumeCommands {
	return volumeCommands{cluster: id.cluster, name: id.name}
}
