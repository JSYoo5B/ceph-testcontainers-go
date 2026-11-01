// Package volbase is the identity and the operation admission of the owned
// subvolumes and groups that package cephfs/subvolume creates, shared with the
// packages that act on subvolume handles, such as cephfs/quiesce.
//
// A subvolume.Subvolume embeds a Handle, so *subvolume.Subvolume satisfies
// Owner, and Of returns its Volume; Handle has no exported member, so nothing
// is promoted into the public API. Every copy of a handle shares one Volume.
//
// Lock contract: the fields of a Volume are read and written with the
// filesystem's setup gate held, which Begin takes for the whole operation.
// volbase keeps no state, no lock and no registration of its own.
package volbase

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
)

// Volume is the private identity of one owned subvolume or group, captured
// when it is created: its filesystem's core and native FSMap ID, its name and
// group, and its native path and birth time. Ready is set once the creation is
// confirmed; Removed once a removal is confirmed. Editing a handle's exported
// fields cannot redirect an operation, because operations use this record.
type Volume struct {
	Core                         *fsbase.Core
	FilesystemID                 int64
	Name, Group, Path, CreatedAt string
	Ready, Removed               bool
}

// Handle binds a public handle to its Volume. It has only unexported members,
// so embedding it promotes nothing into a public API.
type Handle struct{ v *Volume }

// NewHandle returns the handle of v.
func NewHandle(v *Volume) Handle { return Handle{v: v} }

func (h Handle) volume() *Volume { return h.v }

// Owner is a type that embeds a Handle. It is sealed: only types that embed
// Handle satisfy it.
type Owner interface{ volume() *Volume }

// Of returns the Volume of o; nil for a handle that was not created by
// package subvolume. o must not be a nil pointer.
func Of(o Owner) *Volume { return o.volume() }

// Begin admits one operation on the subvolumes and groups of the filesystem
// of core and holds its setup gate until done. It refuses a filesystem that is
// no longer the registered one of its cluster, whose exported descriptor name
// was edited, whose native identity was not captured or whose cluster has no
// control container. Then it reads the native pool identity, returning the
// filesystem's FSMap ID, and enables only the MGR volumes module, waiting for
// its CLI to recognize the existing filesystem; using fs volume create would
// invoke an orchestrator and create resources outside this fixture's
// ownership.
func Begin(ctx context.Context, core *fsbase.Core) (context.Context, int64, func(), error) {
	ctx, done, err := core.Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "CephFS filesystem must be an active filesystem created by this cluster",
		Check: func(ctx context.Context) (bool, error) {
			c := core.Cluster()
			if !core.Registered() || !core.DescriptorIntact() || core.Native() == nil {
				return false, nil
			}
			control, err := c.ControlContainer(ctx)
			if err != nil {
				return false, err
			}
			return control != nil, nil
		},
	})
	if err != nil {
		return ctx, 0, func() {}, err
	}
	state, err := core.ReadNativePools(ctx)
	if err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	if err := core.Volumes().EnsureVolumesModule(ctx); err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	return ctx, state.ID, done, nil
}

// CheckIdentity refuses a native resource whose filesystem ID, path or birth
// time differs from the captured identity: it was replaced outside this
// fixture.
func (v *Volume) CheckIdentity(fsID int64, currentPath, createdAt string) error {
	if v.FilesystemID != fsID || v.Path != currentPath || v.CreatedAt != createdAt {
		return errors.New("CephFS resource was replaced outside this fixture; refusing mutation")
	}
	return nil
}
