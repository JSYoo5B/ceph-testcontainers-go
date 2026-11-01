package fsbase

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph"

// VolumeCommands runs a filesystem's native commands: volume, subvolume,
// snapshot, pin, session and schedule reads and writes, each addressed to the
// filesystem's authoritative name. It holds only the owning cluster and that
// name, both fixed when the descriptor is published, so it needs no lock of
// its own; admission, ownership checks and the cluster lock stay with the
// operation that calls it.
type VolumeCommands struct {
	cluster *ceph.Container
	name    string
}

// Cluster returns the cluster the commands run on.
func (v VolumeCommands) Cluster() *ceph.Container { return v.cluster }

// Name returns the filesystem name the commands address.
func (v VolumeCommands) Name() string { return v.name }
