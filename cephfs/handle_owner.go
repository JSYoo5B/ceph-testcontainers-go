package cephfs

import (
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Each resource handle carries the identity its creating filesystem recorded,
// and the handle's operations run on that filesystem. These lookups only find
// it and refuse a handle the operation cannot take (nil, not returned by this
// package, not confirmed); the operation's own admission still refuses a stale,
// removed or replaced identity. The clone and data-pool lookups refuse only an
// unowned handle, because RemovePartial and RemoveUnused accept partial ones.

func (volume *Subvolume) filesystem() (*Filesystem, error) {
	if volume == nil || volume.identity == nil || volume.identity.filesystem == nil || !volume.identity.ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return volume.identity.filesystem, nil
}

func (group *SubvolumeGroup) filesystem() (*Filesystem, error) {
	if group == nil || group.identity == nil || group.identity.filesystem == nil || !group.identity.ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return group.identity.filesystem, nil
}

func (snapshot *SubvolumeSnapshot) filesystem() (*Filesystem, error) {
	if snapshot == nil || snapshot.identity == nil || !snapshot.identity.ready {
		return nil, servicehook.Invalid(errors.New("snapshot must be a confirmed resource created by this filesystem"))
	}
	if volume := snapshot.identity.subvolume; volume == nil || volume.filesystem == nil || !volume.ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return snapshot.identity.subvolume.filesystem, nil
}

func (clone *SubvolumeClone) filesystem() (*Filesystem, error) {
	if clone == nil || clone.identity == nil || clone.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume clone must be a confirmed resource created by a filesystem"))
	}
	return clone.identity.filesystem, nil
}

func (grant *SubvolumeAuthorization) filesystem() (*Filesystem, error) {
	if grant == nil || grant.identity == nil || grant.identity.volume == nil || grant.identity.volume.filesystem == nil || grant.identity.volume.filesystem.base().Cluster() == nil || !grant.identity.authorizeAttempted {
		return nil, servicehook.Invalid(errors.New("subvolume authorization must be an owned grant attempted by this filesystem"))
	}
	return grant.identity.volume.filesystem, nil
}

func (attachment *DataPool) filesystem() (*Filesystem, error) {
	if attachment == nil || attachment.identity.record == nil || attachment.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("data pool must be an attachment created by a filesystem"))
	}
	return attachment.identity.filesystem, nil
}
