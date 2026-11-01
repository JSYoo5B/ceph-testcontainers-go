package subvolume

import (
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Each resource handle carries the identity its creating filesystem recorded,
// and the handle's operations run on that filesystem's core. These lookups only
// find it and refuse a handle the operation cannot take (nil, not returned by
// this package, not confirmed); the operation's own admission still refuses a
// stale, removed or replaced identity. The clone lookup refuses only an
// unowned handle, because RemovePartial accepts partial ones.

func (volume *Subvolume) filesystem() (*fsbase.Core, error) {
	if volume == nil || volume.identity == nil || volume.identity.Core == nil || !volume.identity.Ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return volume.identity.Core, nil
}

func (group *Group) filesystem() (*fsbase.Core, error) {
	if group == nil || group.identity == nil || group.identity.Core == nil || !group.identity.Ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return group.identity.Core, nil
}

func (snapshot *Snapshot) filesystem() (*fsbase.Core, error) {
	if snapshot == nil || snapshot.identity == nil || !snapshot.identity.ready {
		return nil, servicehook.Invalid(errors.New("snapshot must be a confirmed resource created by this filesystem"))
	}
	if volume := snapshot.identity.subvolume; volume == nil || volume.Core == nil || !volume.Ready {
		return nil, servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return snapshot.identity.subvolume.Core, nil
}

func (clone *Clone) filesystem() (*fsbase.Core, error) {
	if clone == nil || clone.identity == nil || clone.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("subvolume clone must be a confirmed resource created by a filesystem"))
	}
	return clone.identity.filesystem, nil
}

func (grant *Authorization) filesystem() (*fsbase.Core, error) {
	if grant == nil || grant.identity == nil || grant.identity.volume == nil || grant.identity.volume.Core == nil || grant.identity.volume.Core.Cluster() == nil || !grant.identity.authorizeAttempted {
		return nil, servicehook.Invalid(errors.New("subvolume authorization must be an owned grant attempted by this filesystem"))
	}
	return grant.identity.volume.Core, nil
}
