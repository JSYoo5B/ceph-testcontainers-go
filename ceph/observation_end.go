package ceph

import (
	"errors"

	"github.com/google/uuid"
)

// observationEnd marks the failure of an observation that no later read of
// the same target can undo: the cluster terminated, or an identity that the
// observation checks no longer holds (the original FSID, a pool's native ID
// or name). The one-shot query returns err, its own error, unchanged; a watch
// ends with cause, a classified error without native output.
type observationEnd struct {
	err, cause error
}

func (e *observationEnd) Error() string { return e.err.Error() }
func (e *observationEnd) Unwrap() error { return e.err }

// endObservation marks err as the end of the observed target, with the cause
// that ends a watch of it.
func endObservation(err, cause error) error {
	return &observationEnd{err: err, cause: cause}
}

// oneShotError removes the observationEnd mark: a one-shot query returns the
// error it always returned.
func oneShotError(err error) error {
	if end, ok := err.(*observationEnd); ok {
		return end.err
	}
	return err
}

// observationEndCause returns the watch cause of a marked error, or nil.
func observationEndCause(err error) error {
	if end, ok := err.(*observationEnd); ok {
		return end.cause
	}
	return nil
}

// errForeignFSID is the cause of an observation whose native FSID is a valid
// cluster identity other than the original one. Its text names no FSID.
var errForeignFSID = errors.New("the native FSID differs from the original cluster")

// foreignFSID reports whether native is a canonical, non-nil cluster FSID
// other than original. An unreadable or malformed FSID is a failed read
// instead, which a later read can repair.
func foreignFSID(native, original string) bool {
	parsed, err := uuid.Parse(native)
	return err == nil && parsed != uuid.Nil && parsed.String() == native && native != original
}
