package subvolume

import (
	"errors"
	"slices"

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

// removal is one owned resource's removal reconciliation. Its flags are the
// resource's shared removal state, so every handle copy observes the outcome.
type removal struct {
	name      string
	removed   *bool
	attempted *bool
	// check runs after the already-removed shortcut and before any listing.
	check func() error
	// list returns the native names that may still hold the resource.
	list func() ([]string, error)
	// verify confirms the present resource is still the owned incarnation.
	verify func() error
	// remove runs the native removal command.
	remove func() error
	// vanished, when set, lists before every attempt and is returned when the
	// resource is absent before any removal was attempted.
	vanished error
	// unconfirmed, when set, relists after a successful command and is
	// returned while the resource is still listed.
	unconfirmed error
}

// reconcile removes the resource once. A confirmed removal is a no-op; after
// an uncertain earlier attempt it relists and marks the resource removed when
// its name is gone, so a retry neither repeats the command nor deletes a
// recreated same-named resource that fails verification.
func (r removal) reconcile() error {
	if *r.removed {
		return nil
	}
	if r.check != nil {
		if err := r.check(); err != nil {
			return err
		}
	}
	if *r.attempted || r.vanished != nil {
		names, err := r.list()
		if err != nil {
			return err
		}
		if !slices.Contains(names, r.name) {
			if *r.attempted {
				*r.removed = true
				return nil
			}
			return r.vanished
		}
	}
	if err := r.verify(); err != nil {
		return err
	}
	*r.attempted = true
	if err := r.remove(); err != nil {
		return err
	}
	if r.unconfirmed != nil {
		names, err := r.list()
		if err != nil {
			return err
		}
		if slices.Contains(names, r.name) {
			return r.unconfirmed
		}
	}
	*r.removed = true
	return nil
}
