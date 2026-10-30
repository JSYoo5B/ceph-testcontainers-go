package cephfs

import (
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Each resource handle carries the identity its creating filesystem recorded,
// and the handle's operations run on that filesystem. These lookups only find
// it: the operation's own admission still refuses a stale, removed or foreign
// identity. A nil handle, or one not returned by this package, is refused here.

func (volume *Subvolume) filesystem() (*Filesystem, error) {
	if volume == nil || volume.identity == nil || volume.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume must be a confirmed resource created by a filesystem"))
	}
	return volume.identity.filesystem, nil
}

func (group *SubvolumeGroup) filesystem() (*Filesystem, error) {
	if group == nil || group.identity == nil || group.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume group must be a confirmed resource created by a filesystem"))
	}
	return group.identity.filesystem, nil
}

func (snapshot *SubvolumeSnapshot) filesystem() (*Filesystem, error) {
	if snapshot == nil || snapshot.identity == nil || snapshot.identity.subvolume == nil || snapshot.identity.subvolume.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume snapshot must be a confirmed resource created by a filesystem"))
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
	if grant == nil || grant.identity == nil || grant.identity.volume == nil || grant.identity.volume.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume authorization must be a confirmed grant created by a filesystem"))
	}
	return grant.identity.volume.filesystem, nil
}

func (attachment *DataPool) filesystem() (*Filesystem, error) {
	if attachment == nil || attachment.identity == nil || attachment.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("data pool must be an attachment created by a filesystem"))
	}
	return attachment.identity.filesystem, nil
}
