package cephfs

import (
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// A data pool handle carries the identity its creating filesystem recorded,
// and the handle's operations run on that filesystem. This lookup only finds
// it and refuses a handle that was not returned by this package; the
// operation's own admission still refuses a stale, removed or replaced
// identity. It refuses only an unowned handle, because RemoveUnused accepts
// partial ones.

func (attachment *DataPool) filesystem() (*Filesystem, error) {
	if attachment == nil || attachment.identity.record == nil || attachment.identity.filesystem == nil {
		return nil, servicehook.Invalid(errors.New("data pool must be an attachment created by a filesystem"))
	}
	return attachment.identity.filesystem, nil
}
